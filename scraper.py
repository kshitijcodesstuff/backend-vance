import requests
from bs4 import BeautifulSoup
from datetime import datetime
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# List of user-agent strings for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
]

def create_session():
    """
    Create a requests session with retry logic and exponential backoff.
    """
    session = requests.Session()
    retries = Retry(
        total=5,  # Retry up to 5 times
        backoff_factor=1,  # Exponential backoff: 1s, 2s, 4s, etc.
        status_forcelist=[500, 502, 503, 504],  # Retry on these HTTP status codes
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def scrape_forex_data(url):
    """
    Scrape forex data from Yahoo Finance.
    :param url: URL of the Yahoo Finance page to scrape data from.
    :return: List of rows containing forex data.
    """
    session = create_session()
    headers = {"User-Agent": random.choice(USER_AGENTS)}  # Rotate User-Agent
    
    logging.info(f"Scraping data from URL: {url}")
    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()  # Raise exception for HTTP errors
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error occurred: {e}")
        raise Exception(f"Failed to fetch data from {url}: {e}")

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    if not table:
        logging.error("Failed to scrape data: No table found on the page.")
        raise Exception("No table found on the page.")

    data = []
    for row in table.find_all("tr")[1:]:  # Skip the header row
        columns = row.find_all("td")
        if len(columns) < 6:
            logging.warning("Skipping incomplete row.")
            continue  # Skip incomplete rows

        try:
            # Normalize the date format to YYYY-MM-DD
            raw_date = columns[0].text.strip()
            normalized_date = datetime.strptime(raw_date, "%b %d, %Y").strftime("%Y-%m-%d")
            
            # Parse and store data row
            data.append([
                normalized_date,
                float(columns[1].text.replace(',', '')),  # Open rate
                float(columns[2].text.replace(',', '')),  # High rate
                float(columns[3].text.replace(',', '')),  # Low rate
                float(columns[4].text.replace(',', '')),  # Close rate
                float(columns[5].text.replace(',', '')),  # Adjusted close rate
                columns[6].text.strip(),  # Volume
            ])
        except ValueError as ve:
            logging.error(f"Error parsing row data: {ve}, skipping row.")
            continue

    if not data:
        logging.error("No valid data was scraped from the page.")
        raise Exception("Scraped data is empty.")

    logging.info(f"Scraping successful, retrieved {len(data)} rows.")
    return data
