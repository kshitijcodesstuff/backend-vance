import schedule
import time
from scraper import scrape_forex_data
from database import store_data_in_db
from datetime import datetime, timedelta

def scrape_and_update():
    """Scrape data for all pairs and update the database."""
    pairs = ["GBPINR", "AEDINR"]
    all_data = {}

    for pair in pairs:
        try:
            print(f"Scraping data for {pair}...")
            today = datetime.now()
            one_year_ago = today - timedelta(days=365)
            period1 = int(one_year_ago.timestamp())
            period2 = int(today.timestamp())
            url = f"https://finance.yahoo.com/quote/{pair}%3DX/history/?period1={period1}&period2={period2}"

            # Scrape data for the pair
            scraped_data = scrape_forex_data(url)
            all_data[pair] = scraped_data
        except Exception as e:
            print(f"Error scraping {pair}: {e}")

    for pair, data in all_data.items():
        table_name = f"{pair}_data"
        store_data_in_db(data, table_name)
        print(f"Data for {pair} updated successfully.")

# Schedule the task to run periodically
schedule.every(1).hours.do(scrape_and_update)

if __name__ == "__main__":
    print("Starting scheduler...")
    scrape_and_update()  # Initial run
    while True:
        schedule.run_pending()
        time.sleep(1)
