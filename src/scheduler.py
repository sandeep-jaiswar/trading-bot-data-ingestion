import time
from data_fetcher import fetch_yfinance_data
from data_normalizer import normalize_yfinance_data
from db_utils import insert_stock_data, fetch_all_tickers, insert_technical_indicators
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

load_dotenv()

# Moving Averages
def calculate_sma(data, period=14):
    return data['close_price'].rolling(window=period).mean().iloc[-1]

def calculate_ema(data, period=14):
    return data['close_price'].ewm(span=period, min_periods=1).mean().iloc[-1]

# Momentum Indicators
def calculate_rsi(data, period=14):
    delta = data['close_price'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
    fast_ema = data['close_price'].ewm(span=fast_period, min_periods=1).mean()
    slow_ema = data['close_price'].ewm(span=slow_period, min_periods=1).mean()
    macd = fast_ema - slow_ema
    macd_signal = macd.ewm(span=signal_period, min_periods=1).mean()
    return macd.iloc[-1], macd_signal.iloc[-1]

def calculate_stochastic(data, period=14):
    lowest_low = data['low_price'].rolling(window=period).min()
    highest_high = data['high_price'].rolling(window=period).max()
    stochastic_k = (data['close_price'] - lowest_low) / (highest_high - lowest_low) * 100
    return stochastic_k.iloc[-1]

# Volatility Indicators
def calculate_bollinger_bands(data, window=20, num_std_dev=2):
    rolling_mean = data['close_price'].rolling(window=window).mean()
    rolling_std = data['close_price'].rolling(window=window).std()
    upper_band = rolling_mean.iloc[-1] + (rolling_std.iloc[-1] * num_std_dev)
    lower_band = rolling_mean.iloc[-1] - (rolling_std.iloc[-1] * num_std_dev)
    return upper_band, lower_band

def calculate_atr(data, period=14):
    high_low = data['high_price'] - data['low_price']
    high_close = (data['high_price'] - data['close_price'].shift()).abs()
    low_close = (data['low_price'] - data['close_price'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr.iloc[-1]

# Trend Indicators
def calculate_adx(data, period=14):
    high_low = data['high_price'] - data['low_price']
    high_close = (data['high_price'] - data['close_price'].shift()).abs()
    low_close = (data['low_price'] - data['close_price'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    plus_dm = data['high_price'].diff()
    minus_dm = -data['low_price'].diff()

    plus_dm_smoothed = plus_dm.rolling(window=period).mean()
    minus_dm_smoothed = minus_dm.rolling(window=period).mean()
    tr_smoothed = true_range.rolling(window=period).mean()

    adx = 100 * (plus_dm_smoothed - minus_dm_smoothed) / tr_smoothed
    return adx.iloc[-1]

def calculate_parabolic_sar(data, acceleration_factor=0.02, max_acceleration=0.2):
    sar = data['close_price'].iloc[-1]
    return sar

# Volume Indicators
def calculate_obv(data):
    obv = (data['volume'] * np.sign(data['close_price'].diff())).cumsum()
    return obv.iloc[-1]

def calculate_cmf(data, period=20):
    mfv = (data['close_price'] - data['low_price'] - (data['high_price'] - data['close_price'])) / (data['high_price'] - data['low_price'])
    cmf = (mfv * data['volume']).rolling(window=period).sum() / data['volume'].rolling(window=period).sum()
    return cmf.iloc[-1]

def calculate_ad_line(data):
    ad_line = ((data['close_price'] - data['low_price']) - (data['high_price'] - data['close_price'])) / (data['high_price'] - data['low_price'])
    ad_line = (ad_line * data['volume']).cumsum()
    return ad_line.iloc[-1]

# Other Indicators
def calculate_roc(data, period=12):
    return (data['close_price'].pct_change(periods=period) * 100).iloc[-1]

def calculate_williams_r(data, period=14):
    highest_high = data['high_price'].rolling(window=period).max()
    lowest_low = data['low_price'].rolling(window=period).min()
    williams_r = (highest_high - data['close_price']) / (highest_high - lowest_low) * -100
    return williams_r.iloc[-1]

# Calculate all technical indicators
def calculate_technical_indicators(data, ticker):
    indicators = []

    # Moving Averages
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "SMA", "value": calculate_sma(data)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "SMA_20", "value": calculate_sma(data, 20)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "SMA_50", "value": calculate_sma(data, 50)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "SMA_100", "value": calculate_sma(data, 100)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "SMA_200", "value": calculate_sma(data, 200)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "EMA", "value": calculate_ema(data)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "EMA_20", "value": calculate_ema(data, 20)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "EMA_50", "value": calculate_ema(data, 50)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "EMA_100", "value": calculate_ema(data, 100)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "EMA_200", "value": calculate_ema(data, 200)})

    # Momentum Indicators
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "RSI", "value": calculate_rsi(data)})
    macd, macd_signal = calculate_macd(data)
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "MACD", "value": macd})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "MACD_Signal", "value": macd_signal})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "Stochastic", "value": calculate_stochastic(data)})

    # Volatility Indicators
    upper_band, lower_band = calculate_bollinger_bands(data)
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "Bollinger_Upper", "value": upper_band})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "Bollinger_Lower", "value": lower_band})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "ATR", "value": calculate_atr(data)})

    # Trend Indicators
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "ADX", "value": calculate_adx(data)})

    # Volume Indicators
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "OBV", "value": calculate_obv(data)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "CMF", "value": calculate_cmf(data)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "AD_Line", "value": calculate_ad_line(data)})

    # Other Indicators
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "ROC", "value": calculate_roc(data)})
    indicators.append({"date": data['Date'].iloc[-1], "indicator_name": "Williams_R", "value": calculate_williams_r(data)})

    return indicators

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

        # Calculate and insert technical indicators
        technical_indicators_data = calculate_technical_indicators(raw_data, ticker)
        if technical_indicators_data:
            insert_technical_indicators(technical_indicators_data, ticker)
            print(f"Technical indicators for {ticker} successfully stored!")

if __name__ == "__main__":
    schedule_historical_fetch()
