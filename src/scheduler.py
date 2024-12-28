import time
from data_fetcher import fetch_yfinance_data
from data_normalizer import normalize_yfinance_data
from db_utils import insert_stock_data,fetch_all_tickers
from dotenv import load_dotenv
import os

load_dotenv()

def schedule_historical_fetch():
    tickers = fetch_all_tickers()
    if not tickers:
        print("No tickers found in the database.")
        return
    start_date = "2023-01-01"
    end_date = "2024-12-31"

    for ticker in tickers:
        print(f"Fetching and processing data for {ticker}...")

        # Fetch historical data for the ticker
        raw_data = fetch_yfinance_data(ticker, start_date, end_date)

        # Normalize the data
        normalized_data = normalize_yfinance_data(raw_data)

        # Store the data in the database
        insert_stock_data(normalized_data, ticker)
        print(f"Data for {ticker} successfully stored!")

if __name__ == "__main__":
    schedule_historical_fetch()
