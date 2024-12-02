from pyspark.sql.functions import (
    col,
    first,
    last,
    avg,
    sum,
    lit,
    when,
    desc,
    datediff,
    lag,
    count,
)

from pyspark.sql.window import Window


def calculate_first_last_prices(df):
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
    return df.withColumn(
        "percentage_change",
        ((col("last_day_price") - col("first_day_price")) / col("first_day_price"))
        * 100,
    )

def find_best_performer(df):
    return df.orderBy(desc("percentage_change")).limit(1)

def calculate_shares_purchased(df):
    return df.withColumn("shares_purchased", lit(10000) / col("first_day_price"))

def calculate_last_day_value(df):
    return df.withColumn("value_today", (col("shares_purchased") * col("last_day_price")))

# def calculate_roi(df):
#     return df.withColumn("roi", (col("value_today") - 10000) / 10000 * 100)

def calculate_portfolio_value(df):
    return df.agg(sum("value_today").alias("portfolio_value"))

def add_date_calendar(df):
    #Adds date fields for t field for use in aggr
    pass

def filter_by_date(df):
    #do it here
    pass


def calculate_cmgr(df):
    #do it here
    pass
