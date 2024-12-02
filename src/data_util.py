import requests
import pandas as pd
from pyspark.sql import SparkSession
import os
import time
from datetime import datetime
import pyarrow
#import duckdb
from custom_transforms import *


def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def check_date_range(start_date_str, end_date_str):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if end_date < start_date:
            return False
        return True
    except ValueError:
        return False


def check_api_key():
    if "POLYGON_API_KEY" not in os.environ:
        raise ValueError("Please set the environment variable POLYGON_API_KEY")


# Fetches stock data for the specified date range
def fetch_polygon_data(symbol, start_date, end_date):

    if start_date is None:
        raise ValueError("Please provide a start date")

    if not validate_date_format(start_date):
        raise ValueError("Invalid start date format. Please use YYYY-MM-DD")

    if not validate_date_format(end_date):
        raise ValueError("Invalid end date format. Please use YYYY-MM-DD")

    if not check_date_range(start_date, end_date):
        raise ValueError("End date must be after start date")

    # Check for polygon api environment variable
    check_api_key()

    api_key = os.environ["POLYGON_API_KEY"]

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={api_key}"
    response = requests.get(url)

    data = response.json()
    # Check for result in response and print error to console if not found
    if (
        response.status_code == 200
        and "results" in data
        and data["results"]
        and data["resultsCount"] > 0
    ):
        data = pd.DataFrame(data["results"])
        data = data.assign(symbol=symbol)  # Assign symbol column
        return data
    else:
        raise ValueError(f"Error fetching data for {symbol}: {response.text}")


# Create a method to fetch all stock data for the specified symbol symbols
def fetch_all_stock_data(
    portfolio_name, symbols, start_date, end_date, rate_limit_per_minute=4
):

    rate_limit = 60 / rate_limit_per_minute
    # temp_dir = tempfile.mkdtemp()

    for symbol in symbols:

        # Loop through each stock and populate pyspark dataframe
        # Create a stock class object
        try:
            data = Stock(symbol, start_date, end_date).data
            print(data.iloc[:3])
            
            data.to_parquet(f"data/exports/{portfolio_name}_{symbol}_data.parquet", index=False)
        
            # Avoid free account rate limiting (5 calls per second)
            time.sleep(rate_limit)

        except Exception as e:
            print(f"Error: {e}")
            continue

        if data is None:
            raise print(f"Error fetching data for {symbol}")



    return


# Reusable class for a stock (single symbol)
class Stock:
    def __init__(self, symbol, start_date, end_date):
        self.symbol = symbol
        self.data = None
        self.start_date = start_date
        self.end_date = end_date
        self.data = fetch_polygon_data(symbol, start_date, end_date)


# Reusable class for a portfolio of stocks (list of stock symbols)
class Portfolio:
    def __init__(self, portfolio_name, symbols, start_date, end_date):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.portfolio_name = portfolio_name
        self.spark_session = SparkSession.builder.appName(portfolio_name).getOrCreate()
        
        try:
            self.df = self.spark_session.read.parquet(f"data/exports/{portfolio_name}_*.parquet")
        except Exception as e:
            print(f"Please export data for the portfolio before running pipelines {e}")
            self.df = None

    def export_data(self, polygon_api_key):
        
        #Set the environment variable for the polygon API key
        os.environ["POLYGON_API_KEY"] = polygon_api_key
        
        #Clear existing parquet files that begin with this portfolio_name
        for file in os.listdir("data/exports"):
            if file.startswith(self.portfolio_name):
                os.remove(f"data/exports/{file}")
    
        #Retrieve stock data for each symbol in the portfolio
        fetch_all_stock_data(
            self.portfolio_name, self.symbols, self.start_date, self.end_date
        )
        
    def question1_pipeline(self):
        # Pyspark transforms to answer questions...
        question1_df = self.df.transform(calculate_first_last_prices)
        #question1_df.show()
        question1_df = question1_df.transform(calculate_percentage_change)
        # question1_df.show()
        question1_df = question1_df.transform(find_best_performer("percentage_change"))
        print("Best performer:")
        question1_df.show()
    
    def question2_pipeline(self):
        question2_df = self.df.transform(calculate_first_last_prices)
        question2_df = question2_df.transform(calculate_shares_purchased)
        question2_df = question2_df.transform(calculate_last_day_value)
        question2_df.show()
        question2_df = question2_df.transform(calculate_portfolio_value)
        print("Portfolio value:")
        question2_df.show()
    
    def question3_pipeline(self):
        start_date = "2023-01-01"
        end_date = "2023-06-30"
        question3_df = self.df.transform(add_date_calendar)
        question3_df = question3_df.transform(filter_by_date(start_date, end_date))
        question3_df = question3_df.transform(calculate_first_last_prices)
        question3_df = question3_df.transform(calculate_cmgr(start_date, end_date))
        question3_df.show()
        question3_df = question3_df.transform(find_best_performer("cmgr"))
        print("Best performer CMGR:")
        question3_df.show()


    def question4_pipeline(self):
        question4_df = self.df.transform(add_date_calendar)
        question4_df = question4_df.transform(aggregate_by_period("week"))
        question4_df.show()
        question4_df = question4_df.transform(calculate_percentage_change)
        question4_df = question4_df.transform(find_worst_performer("percentage_change"))
        print("Worst weekly performer:")
        question4_df.show()


if __name__ == "__main__":

    try:


        # Import the stock symbols from CSV File into a "stocks" dataframe.
        stock_df = pd.read_csv("data/stocks.csv")
        
        portfolio1 = Portfolio("portfolio1", stock_df["symbol"].tolist(), "2023-01-01", "2023-12-31")

        # Run data export method for portfolio instance if data export is required
        #Set the polygon API key here if you want to run the export (should be env var in production)
        #portfolio1.export_data("WPrrrs77Czp84b48wFp75_toF_gaa0On")
        #portfolio1.question1_pipeline()
        #portfolio1.question3_pipeline()
        portfolio1.question4_pipeline()


    except Exception as e:
        print(f"Error: {e}")
