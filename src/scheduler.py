from db_utils import (
    fetch_all_tickers, 
    insert_balance_sheet, 
    create_tables, 
    insert_company_data, 
    insert_dividend_data, 
    insert_industry_data, 
    insert_sector_data, 
    insert_tickers_data, 
    update_tickers_data
)
from dotenv import load_dotenv
import yfinance as yf
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_ticker_data(ticker_data):
    """
    Process and insert data for a single ticker.
    """
    symbol = ticker_data[1]  # Assuming ticker_data is a tuple with symbol at index 1
    full_symbol = f"{symbol}.BO"
    logging.info(f"Processing ticker: {full_symbol}")

    try:
        yf_ticker = yf.Ticker(full_symbol)

        # Fetch and insert company data
        info = yf_ticker.get_info()
        info["symbol"] = symbol
        insert_company_data(info)

        # Fetch and insert dividend data
        dividends = yf_ticker.dividends.to_dict()
        insert_dividend_data(dividends, symbol)

        # # Fetch and insert balance sheet data
        balance_sheet = yf_ticker.get_balance_sheet(as_dict=True)
        insert_balance_sheet(balance_sheet, symbol)

        # Fetch and update ticker fast info
        fast_info = yf_ticker.get_fast_info()
        update_tickers_data(fast_info, symbol)

    except Exception as e:
        logging.error(f"Failed to process ticker {symbol}: {e}")

def schedule_ingest_data():
    """
    Schedules the ingestion of data from Yahoo Finance for all tickers in the database.
    """
    try:
        # Database setup and initial data insertion
        logging.info("Setting up database tables and inserting initial data...")
        create_tables()
        insert_industry_data()
        insert_sector_data()
        insert_tickers_data()

        # Fetch all tickers from the database
        logging.info("Fetching all tickers from the database...")
        tickers = fetch_all_tickers()

        if not tickers:
            logging.warning("No tickers found in the database. Exiting data ingestion.")
            return

        # Process each ticker sequentially
        for ticker_data in tickers:
            process_ticker_data(ticker_data)

        logging.info("Data ingestion completed successfully.")

    except Exception as e:
        logging.error(f"Failed to complete data ingestion: {e}")
        raise

if __name__ == "__main__":
    schedule_ingest_data()
