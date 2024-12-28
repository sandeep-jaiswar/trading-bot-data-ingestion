import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")

    
def fetch_all_tickers():
    """
    Fetch all ticker symbols from the tickers table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT symbol FROM tickers;")
        tickers = [row[0] for row in cursor.fetchall()]
        return tickers
    except Exception as e:
        raise Exception(f"Failed to fetch tickers: {e}")
    finally:
        cursor.close()
        conn.close()
        

def get_ticker_id(symbol):
    """
    Fetch the ticker_id for a given ticker symbol from the tickers table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM tickers WHERE symbol = %s;", (symbol,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            raise ValueError(f"Ticker '{symbol}' not found in the database.")
    finally:
        cursor.close()
        conn.close()


def insert_stock_data(data, ticker):
    """
    Insert normalized stock data into the database.
    :param data: Pandas DataFrame containing normalized stock data.
    :param ticker: Stock ticker symbol.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch the ticker_id
    try:
        ticker_id = get_ticker_id(ticker)
    except ValueError as e:
        raise Exception(f"Ticker '{ticker}' not found in the database. {e}")

    insert_query = """
    INSERT INTO stock_data (
        ticker_id, date, open_price, high_price, low_price, close_price, 
        adjusted_close_price, volume, price_range, average_price
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker_id, date) DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        adjusted_close_price = EXCLUDED.adjusted_close_price,
        volume = EXCLUDED.volume,
        price_range = EXCLUDED.price_range,
        average_price = EXCLUDED.average_price;
    """

    # Prepare data for batch insert
    records = [
        (
            ticker_id,
            row["Date"],
            row["open_price"],
            row["high_price"],
            row["low_price"],
            row["close_price"],
            row["adjusted_close_price"],
            row["volume"],
            row["price_range"],
            row["average_price"],
        )
        for _, row in data.iterrows()
    ]

    try:
        execute_batch(cursor, insert_query, records)
        conn.commit()
        print(f"Inserted {len(records)} records for {ticker}.")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert data for {ticker}: {e}")
    finally:
        cursor.close()
        conn.close()
        
        
def insert_technical_indicators(indicators_data, ticker):
    """
    Insert technical indicators into the technical_indicators table.
    :param indicators_data: List of dictionaries containing technical indicator data.
    :param ticker: Stock ticker symbol.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch the ticker_id
    try:
        ticker_id = get_ticker_id(ticker)
    except ValueError as e:
        raise Exception(f"Ticker '{ticker}' not found in the database. {e}")

    insert_query = """
    INSERT INTO technical_indicators (
        ticker_id, date, indicator_name, value
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (ticker_id, date, indicator_name) DO UPDATE SET
        value = EXCLUDED.value;
    """

    # Prepare data for batch insert
    records = [
        (
            ticker_id,
            indicator["date"],
            indicator["indicator_name"],
            indicator["value"],
        )
        for indicator in indicators_data
    ]

    try:
        execute_batch(cursor, insert_query, records)
        conn.commit()
        print(f"Inserted {len(records)} technical indicators for {ticker}.")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert technical indicators for {ticker}: {e}")
    finally:
        cursor.close()
        conn.close()