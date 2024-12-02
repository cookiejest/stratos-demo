

# Setup
1. Output from the `analysis.py` is available in `output.txt` file
2. If you want to run `analysis.py` must put your own polygon key on line 20. (In production this should be properly handled env var)

2. Install requirements.txt using `pip install -r requirements.txt`

3. Run the script using `python src/analysis.py`


## Project Structure
- `src` 
    - `data_util.py` - contains the reusable portfolio class and methods
    - `custom_transforms.py` contains the pyspark transform methods
    - `analysis.py` - an example creating a portfolio from csv file, exporting the data and running the question pipelines.
- `data`
    - `stocks.csv` as provided
    - `exports` - where raw api call data is written to from export in parquet format. Could be switched to S3 or cloud storage provider in a real project
- `tests`
    - `data_util.py` - contains some example test cases would develop further on a real project for ci/cd purposes
- `output.txt` contains the answers and output from `analysis.py` run locally on my machine.

## Implementation Notes
1. Implemented as a reusable portfolio class with an `export_data` method and question pipeline methods. This allows for analysis of multiple stock portfolios.

3. Implemented sleep() to avoid 5 calls per min rate limit with polygon API
3. Used pyspark for answering the questions with parquet storage
4. Workflow is data is exported once from API and then stored locally in compressed parquet files for each stock in the data/exports folder. Making it easier to manually validate numbers in another tool.

## Areas for improvement
1. Would add more unit tests to make it more stable but ran out of time for now.
2. Alternative to pyspark could be duckdb to remove the hardware dependency, would be a decision based on the expected processing data quanties required for the project but could result in large hosting cost savings. Using bucket storage + duckdb compared to pyspark. https://duckdb.org/
3. Rather than using pyspark transforms could have used sparkSQL and then it would be easier to swap SQL engines if required in the future.


# How to use
## Define a portfolio:
    portfolio1 = Portfolio("portfolio1", ["META","TSLA"], "2023-01-01", "2023-12-31")

### Export portfolio data from polygon

    portfolio1.export_data(YOUR_POLYGON_API_KEY)

### Example running a question pipeline

    portfolio1.question1_pipeline()

