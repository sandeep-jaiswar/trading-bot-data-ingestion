import pandas as pd

def normalize_yfinance_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize and clean raw yfinance data.
    This includes filling missing values, renaming columns, and adding derived metrics.
    
    :param data: Raw data fetched from yfinance (Pandas DataFrame).
    :return: Cleaned and normalized DataFrame.
    """
    # Check if the data is empty
    if data.empty:
        raise ValueError("The input data is empty. Ensure valid data is fetched.")

    # Rename columns for clarity
    data.rename(
        columns={
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Adj Close": "adjusted_close_price",
            "Volume": "volume",
        },
        inplace=True,
    )

    # Fill missing values (if any)
    data.fillna(method="ffill", inplace=True)  # Forward fill
    data.fillna(method="bfill", inplace=True)  # Backward fill

    # Add derived metrics
    data["price_range"] = data["high_price"] - data["low_price"]
    data["average_price"] = (data["high_price"] + data["low_price"]) / 2

    # Reset index if required
    data.reset_index(inplace=True)

    return data


def normalize_custom_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder for additional normalization logic for custom data.
    Extend as required for non-yfinance data sources.
    
    :param data: Raw DataFrame from another source.
    :return: Normalized DataFrame.
    """
    # Implement custom normalization here if needed
    pass
