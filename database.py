import sqlite3

def store_data_in_db(data, table_name="forex_data"):
    """Stores data into an SQLite in-memory database."""
    conn = sqlite3.connect(":memory:")  # In-memory database
    cursor = conn.cursor()

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

    conn.commit()
    return conn
