import yfinance as yf
import pandas as pd

def fetch_yfinance_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical stock data from yfinance.
    :param ticker: Stock ticker symbol (e.g., "AAPL").
    :param start_date: Start date for fetching data (YYYY-MM-DD).
    :param end_date: End date for fetching data (YYYY-MM-DD).
    :return: Pandas DataFrame with historical data.
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        return data
    except Exception as e:
        raise Exception(f"Failed to fetch yfinance data: {e}")
