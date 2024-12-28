import unittest
from src.data_fetcher import fetch_yfinance_data

class TestDataFetcher(unittest.TestCase):
    def test_fetch_yfinance_data(self):
        data = fetch_yfinance_data("AAPL", "2023-01-01", "2023-01-31")
        self.assertFalse(data.empty, "Data should not be empty")
        self.assertIn("Open", data.columns, "Data should contain 'Open' column")

if __name__ == "__main__":
    unittest.main()
