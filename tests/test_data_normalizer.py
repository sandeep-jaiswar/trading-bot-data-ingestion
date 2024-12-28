import unittest
import pandas as pd
from src.data_normalizer import normalize_yfinance_data

class TestDataNormalizer(unittest.TestCase):
    def test_normalize_yfinance_data(self):
        # Create a mock dataset
        mock_data = pd.DataFrame({
            "Open": [150.0, 152.0, None, 154.0],
            "High": [155.0, 156.0, 157.0, None],
            "Low": [149.0, 150.0, 151.0, 152.0],
            "Close": [154.0, 155.0, 156.0, 153.0],
            "Adj Close": [154.0, 155.0, 156.0, 153.0],
            "Volume": [1000000, 1200000, 1100000, None],
        }, index=pd.date_range("2023-01-01", periods=4))

        # Normalize the data
        normalized_data = normalize_yfinance_data(mock_data)

        # Test basic functionality
        self.assertIn("price_range", normalized_data.columns)
        self.assertIn("average_price", normalized_data.columns)
        self.assertFalse(normalized_data.isnull().values.any(), "Data should not contain null values")

if __name__ == "__main__":
    unittest.main()
