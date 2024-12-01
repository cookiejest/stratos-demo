import unittest
import os
from unittest.mock import patch
from src.data_util import (
    check_api_key,
    validate_date_format,
    check_date_range,
    fetch_polygon_data,
    fetch_all_stock_data,
    Stock,
    Portfolio,
)
from pyspark.sql import SparkSession

# Test getting stock ticker data from API
class RunUnitTests(unittest.TestCase):
    @patch.dict(os.environ, {"POLYGON_API_KEY": "your_api_key"})
    def test_api_key_exists(self):
        # Test case where the environment variable is set
        self.assertRaises(
            Exception, check_api_key
        )  # We expect an exception to be raised

    def test_api_key_missing(self):
        # Test case where the environment variable is missing
        with self.assertRaises(ValueError) as e:
            check_api_key()
        self.assertEqual(
            str(e.exception), "Please set the environment variable POLYGON_API_KEY"
        )

    def test_valid_date_format(self):
        valid_date = "2023-11-30"
        self.assertTrue(validate_date_format(valid_date))

    def test_invalid_date_format(self):
        invalid_date = "2023-13-32"
        self.assertFalse(validate_date_format(invalid_date))

    def test_valid_date_range(self):
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        self.assertTrue(check_date_range(start_date, end_date))

    def test_invalid_date_range_start_after_end(self):
        start_date = "2024-01-01"
        end_date = "2023-12-31"
        self.assertFalse(check_date_range(start_date, end_date))

    def test_invalid_date_range_invalid_format(self):
        start_date = "2023-13-01"
        end_date = "2023-12-31"
        self.assertFalse(check_date_range(start_date, end_date))

    def test_fetch_polygon_data(self):

        # Create a Stock object
        stockdata = fetch_polygon_data("AAPL", "2023-01-01", "2023-12-31")

        # Check if data attribute is not None
        self.assertIsNotNone(stockdata)

    def test_valid_ticker(self):
        # Create a Stock object
        stock = Stock("AAPL", "2021-01-01", "2021-06-30")

        # Check if data attribute is not None
        self.assertIsNotNone(stock.data)

    def test_invalid_ticker(self):
        # Create a Stock object

        with self.assertRaises(ValueError) as e:
            stock = Stock("NOTREALTICKER", "2021-01-01", "2021-06-30")

        self.assertEqual(
            str(e.exception), "Please set the environment variable POLYGON_API_KEY"
        )

    def test_invalid_start_date(self):
        
        with self.assertRaises(ValueError) as e:
            # Create a Stock object
            stock = Stock("NOTREALTICKER", "2021-031-01", "2021-06-30")
            
        self.assertEqual(
            str(e.exception), "Invalid start date format. Please use YYYY-MM-DD"
        )


    def test_invalid_end_date(self):
        
        with self.assertRaises(ValueError) as e:
            # Create a Stock object
            stock = Stock("NOTREALTICKER", "2021-01-01", "2021-061-30")

        self.assertEqual(
            str(e.exception), "Invalid end date format. Please use YYYY-MM-DD"
        )


    def test_start_before_end_date(self):
        # Create a Stock object
        stock = Stock("NOTREALTICKER", "2021-01-01", "2021-06-30")

        # Should return error message for start and end date
        self.assertIsNotNone(stock.data)

    def test_multiple_stocks(self):

        Portfolio1 = Portfolio("Portfolio1", ["AAPL", "TSLA", "MSFT"], "", "")

        # Check if data attribute is not None
        self.assertIsNotNone(Portfolio1.stock_data)

        # Should contain 3 unique stock names

    def test_polygon_rate_limit_hit(self):

        # Dummy spark session
        spark_session = SparkSession.builder.appName("Test").getOrCreate()

        # Will fail due to rate limiting
        fetch_all_stock_data(spark_session, ["AAPL", "TSLA", "MSFT", ""], "", "", 10)

        # Should display correct error message for rate limiting issue


if __name__ == "__main__":
    unittest.main()
