from db_utils import create_tables, insert_industry_data, insert_tickers_data, insert_sector_data, fetch_all_tickers, insert_dividend_metrics, insert_company_data
from dotenv import load_dotenv
import yfinance as yf
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def schedule_ingest_data():
    """
    Schedules the ingestion of data from Yahoo Finance for all tickers in the database.
    """
    try:
        # Uncomment and call necessary database setup functions
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

        # Process each ticker
        for ticker_data in tickers:
            symbol = ticker_data[1]  # Assuming ticker_data is a tuple with symbol at index 1
            full_symbol = f"{symbol}.BO"
            logging.info(f"Processing ticker: {full_symbol}")

            try:
                yf_ticker = yf.Ticker(full_symbol)
                info = yf_ticker.get_info()
                info["symbol"] = symbol
                insert_company_data(info)
            except Exception as ticker_error:
                logging.error(f"Failed to fetch data for ticker {symbol}: {ticker_error}")

        logging.info("Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Failed to complete data ingestion: {e}")
        raise

if __name__ == "__main__":
    schedule_ingest_data()
