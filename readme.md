

# Setup

1. Populate your polygon API get as an environment variable.

2. Run the requirements.txt using `pip install -r requirements.txt`

3. Run the script using `python src/index.py`

4. function unit tests can be run using `python test`


# Implementation Notes
1. FB is no longer a valid stock ticker. Changed it META for the purpose of analysis
2. Implemented as a reusable portfolio class with an `export` method and a data method
3. Implemented sleep() to avoid 5 calls per min rate limit with polygon API
3. Used pyspark for answering the questions
4. Used sparkSQL as easier to swap in a faster SQL compatible engine in the future but keep code the same
4. Did a quick example of doing the analysis without pyspark using duckdb + parquet (would be a potential option for smaller data set sizes as would reduce production cost but would still allow for large dataset analysis than only using pandas)
https://duckdb.org/


# How to use
## Define a portfolio:
    portfolio1 = Portfolio("portfolio1", ["META","TSLA"], "2023-01-01", "2023-12-31")

### Export portfolio data from polygon

    portfolio1.export_data()

### Return a pyspark dataframe

    portfolio1.pyspark_df()

### Return a populated duckdb connection (optional example)

    portfolio1.duckdb_connection()