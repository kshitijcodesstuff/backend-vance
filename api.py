from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from scraper import scrape_forex_data
from datetime import datetime, timedelta
import sqlite3

app = FastAPI()

# Allow frontend to access the backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://192.168.0.175:3000", 
        "https://forexview.vercel.app"
    ],  # Add all required origins here
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared in-memory SQLite connection
DATABASE_CONNECTION = sqlite3.connect(":memory:", check_same_thread=False)

class ForexQuery(BaseModel):
    from_currency: str
    to_currency: str
    period: str

def create_table_and_store_data(table_name, data):
    """
    Creates a table if it doesn't exist and stores the given data.
    """
    with DATABASE_CONNECTION:  # Use the connection context manager
        cursor = DATABASE_CONNECTION.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date TEXT PRIMARY KEY,
            open_rate REAL,
            high_rate REAL,
            low_rate REAL,
            close_rate REAL,
            adj_close_rate REAL,
            volume TEXT
        )
        """)

        for record in data:
            cursor.execute(f"""
            INSERT OR REPLACE INTO {table_name} (date, open_rate, high_rate, low_rate, close_rate, adj_close_rate, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, record)

@app.post("/api/forex-data")
def get_forex_data(query: ForexQuery):
    """
    Query forex data for a specific currency pair and period. 
    Dynamically create the table if it does not exist.
    """
    from_currency = query.from_currency
    to_currency = query.to_currency
    period = query.period
    print(f"Received from_currency: {from_currency}, to_currency: {to_currency}, period: {period}")

    # Calculate the date range
    today = datetime.now()
    if period == "1W":
        past_date = today - timedelta(days=7)
    elif period == "1M":
        past_date = today - timedelta(days=30)
    elif period == "3M":
        past_date = today - timedelta(days=90)
    elif period == "6M":
        past_date = today - timedelta(days=180)
    elif period == "1Y":
        past_date = today - timedelta(days=365)
    else:
        raise HTTPException(status_code=400, detail="Invalid period specified.")

    from_date = past_date.strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    print(f"Querying data from {from_date} to {to_date}")

    table_name = f"{from_currency}{to_currency}_data"

    try:
        # Use a separate cursor for this operation
        with DATABASE_CONNECTION:  # Use connection context manager
            cursor = DATABASE_CONNECTION.cursor()
            query = f"""
                SELECT * FROM {table_name}
                WHERE date BETWEEN ? AND ?
                ORDER BY date ASC
            """
            cursor.execute(query, (from_date, to_date))
            rows = cursor.fetchall()
            print(f"Fetched rows: {rows}")
    except sqlite3.OperationalError:
        # Table does not exist; dynamically scrape data and create the table
        print(f"Table {table_name} does not exist. Scraping data...")
        try:
            one_year_ago = today - timedelta(days=365)
            period1 = int(one_year_ago.timestamp())
            period2 = int(today.timestamp())
            url = f"https://finance.yahoo.com/quote/{from_currency}{to_currency}%3DX/history/?period1={period1}&period2={period2}"
            scraped_data = scrape_forex_data(url)
            create_table_and_store_data(table_name, scraped_data)

            # Re-execute the query after creating the table
            with DATABASE_CONNECTION:
                cursor = DATABASE_CONNECTION.cursor()
                cursor.execute(query, (from_date, to_date))
                rows = cursor.fetchall()
                print(f"Fetched rows after table creation: {rows}")
        except Exception as e:
            print(f"Failed to scrape or create table for {from_currency}{to_currency}: {e}")
            raise HTTPException(status_code=404, detail=f"Unable to fetch data for {from_currency}/{to_currency}")

    if not rows:
        raise HTTPException(status_code=404, detail="No data found for the given range.")

    return [
        {
            "date": row[0],
            "open_rate": row[1],
            "high_rate": row[2],
            "low_rate": row[3],
            "close_rate": row[4],
            "adj_close_rate": row[5],
            "volume": row[6],
        }
        for row in rows
    ]

@app.on_event("startup")
def setup_database():
    """
    Initialize the database with scraped data at startup.
    """
    currency_pairs = ["GBPINR", "AEDINR"]  # Default pairs, configurable dynamically
    for pair in currency_pairs:
        today = datetime.now()
        one_year_ago = today - timedelta(days=365)
        period1 = int(one_year_ago.timestamp())
        period2 = int(today.timestamp())
        url = f"https://finance.yahoo.com/quote/{pair}%3DX/history/?period1={period1}&period2={period2}"

        print(f"Scraping data for {pair}...")
        try:
            scraped_data = scrape_forex_data(url)
            table_name = f"{pair}_data"
            create_table_and_store_data(table_name, scraped_data)

            # Debug: Verify stored data
            with DATABASE_CONNECTION:
                cursor = DATABASE_CONNECTION.cursor()
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
                rows = cursor.fetchall()
                print(f"Sample data in {table_name}: {rows}")
        except Exception as e:
            print(f"Failed to scrape or store data for {pair}: {e}")
