from pyspark.sql.functions import (
    col,
    first,
    last,
    avg,
    sum,
    lit,
    when,
    desc,
    asc,
    datediff,
    lag,
    count,
    date_format,
    from_unixtime, year, month, weekofyear,months_between
)

from pyspark.sql.window import Window


def calculate_first_last_prices(df):
    # Calculate the first and last day prices for each symbol
    windowSpec = Window.partitionBy("symbol").orderBy(col("t"))
    
    return (
        df.withColumn("first_day_price", first("c").over(windowSpec))
        .withColumn("last_day_price", last("c").over(windowSpec))
        .groupBy("symbol")
        .agg(
            first("first_day_price").alias("first_day_price"),
            last("last_day_price").alias("last_day_price")
        )
    )


def calculate_percentage_change(df):
    # Calculate the percentage change between the first and last day prices
    return df.withColumn(
        "percentage_change",
        ((col("last_day_price") - col("first_day_price")) / col("first_day_price"))
        * 100,
    )

def find_best_performer(column_name):
    # Find the best performer based on the specified column
    def find_best_performer_func(df):
        return df.orderBy(desc(column_name)).limit(1)
    return find_best_performer_func

def find_worst_performer(column_name):
    # Find the worst performer based on the specified column
    def find_best_performer_func(df):
        return df.orderBy(asc(column_name)).limit(1)
    return find_best_performer_func

def calculate_shares_purchased(df):
    # Calculate the number of shares that can be purchased with $10,000
    return df.withColumn("shares_purchased", lit(10000) / col("first_day_price"))

def calculate_last_day_value(df):
    # Calculate the value of the shares purchased on the last day
    return df.withColumn("value_today", (col("shares_purchased") * col("last_day_price")))


def calculate_portfolio_value(df):
    # Calculate the total value of the portfolio
    return df.agg(sum("value_today").alias("portfolio_value"))

def add_date_calendar(df):
    
    # Convert Unix timestamp to a timestamp column
    df = df.withColumn(
        "timestamp",
        from_unixtime(col("t") / 1000)
    )

    #Extract year, month, and week from the timestamp
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("date", date_format("timestamp", "yyyy-MM-dd"))

    return df

def filter_by_date(start_date, end_date):
    # Filter the dataframe by the specified start and end date
    def filter_by_date_func(df):
        return df.filter((col("date") >= start_date) & (col("date") <= end_date))
    
    return filter_by_date_func

def aggregate_by_period(period):
    #Aggregates the first and last day prices for each symbol by the specified period

    def aggregate_by_period_func(df):
        windowSpec = Window.partitionBy("symbol", period).orderBy(col(period))
        
        return (
            df.withColumn("first_day_price", first("c").over(windowSpec))
            .withColumn("last_day_price", last("c").over(windowSpec))
            .groupBy("symbol", period)
            .agg(
                first("first_day_price").alias("first_day_price"),
                last("last_day_price").alias("last_day_price")
            )
        )
    return aggregate_by_period_func

def calculate_cmgr(start_date, end_date):
    # Calculate the number of months between the first and last date
    def calculate_cmgr_func(df):
        
        df = df.withColumn("num_months", months_between(lit(end_date), lit(start_date)))
        
        # Calculate the cmgr based on the formula https://www.upgrowth.in/compounded-monthly-growth-rate-understanding-and-calculating-compounded-monthly-growth-rates/
        
        df = df.withColumn("cmgr", pow(( col("last_day_price") / col("first_day_price")), 1 / col("num_months")) - 1)


        return df

    return calculate_cmgr_func

