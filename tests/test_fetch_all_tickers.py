import unittest
from db_utils import fetch_all_tickers

class TestFetchAllTickers(unittest.TestCase):
    def test_fetch_all_tickers(self):
        # Ensure the tickers are fetched correctly
        tickers = fetch_all_tickers()
        self.assertIsInstance(tickers, list)
        self.assertGreater(len(tickers), 0, "No tickers found in the database")
        print("Fetched tickers:", tickers)

if __name__ == "__main__":
    unittest.main()
