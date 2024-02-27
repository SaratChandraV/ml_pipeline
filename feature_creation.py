from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lag, stddev, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, FloatType
import pandas as pd
import keys
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder.appName("Stock Features").getOrCreate()

tickers = pd.read_json('ticker_symbols.json')['ticker'].tolist()

def get_source_table_name(ticker):
    return ticker.lower() + "_daily_stock_data"

def get_feature_table_name(ticker):
    return ticker.lower() + "_features_data"

db_params = keys.db_params.db_params

def make_JDBC_url(db_params):
    return 'jdbc:postgresql://{}:{}/{}'.format(db_params['host'],db_params['port'],db_params['database'])

def make_DB_properties(db_params):
    return {"user": db_params['user'], "password": db_params['password'], "driver": "org.postgresql.Driver"}

def read_stock_data(ticker,db_params):
    table_name = get_source_table_name(ticker=ticker)
    query = f"(SELECT date, close FROM {table_name}) AS temp"
    database_url = make_JDBC_url(db_params=db_params)
    properties = make_DB_properties(db_params=db_params)
    df = spark.read.jdbc(url=database_url, table=query, properties=properties)
    # df = spark.read.options(header=True).csv(table_name + ".csv")
    return df

def compute_ema_list(prices, span=10):
    """
    Compute EMA on a list of prices.
    """
    ema_values = []
    alpha = 2 / (span + 1.0)
    ema_prev = prices[0]  # Starting EMA value set to the first price
    
    for price in prices:
        ema = (price - ema_prev) * alpha + ema_prev
        ema_values.append(ema)
        ema_prev = ema
    
    return ema_values

def compute_ema(df,column_name="close", span=10):
    """
    Add EMA column to DataFrame.
    """

    # Convert DataFrame column to list
    prices = df.select(column_name).rdd.flatMap(lambda x: x).collect()

    prices = np.array(prices).astype(float)

    prices = prices.tolist()
    
    # Compute EMA on list
    ema_values = compute_ema_list(prices, span)

    # Prepare a list of tuples from EMA values for DataFrame creation
    ema_tuples = [(value,) for value in ema_values]

    # Define the schema for the new DataFrame
    schema = StructType([StructField("EMA_{}".format(span), FloatType(), True)])

    # Create a DataFrame from the EMA values list with explicit schema
    ema_df = spark.createDataFrame(ema_tuples, schema)
    
    # Add row index to join on
    df = df.withColumn("row_index", monotonically_increasing_id())
    ema_df = ema_df.withColumn("row_index", monotonically_increasing_id())
    
    # Join the EMA values back to the original DataFrame
    df = df.join(ema_df, on=["row_index"]).drop("row_index")
    
    return df

def compute_macd(df):
    """
    Calculate Moving Average Convergence Divergence (MACD)
    """
    # Calculate EMA for 12 and 26 days
    df = compute_ema(df, column_name="close", span=12)
    df = compute_ema(df, column_name="close", span=26)
    
    # Calculate MACD
    df = df.withColumn("MACD", col("EMA_12") - col("EMA_26"))
    
    return df

def calculate_bollinger_bands(df):
    """
    Calculate Bollinger Bands
    """
    windowSpec = Window.orderBy("date").rowsBetween(-19, 0)
    df = df.withColumn("SMA_20", avg("close").over(windowSpec))
    df = df.withColumn("STD_20", stddev("close").over(windowSpec))
    df = df.withColumn("Upper_Band", col("SMA_20") + (col("STD_20") * 2))
    df = df.withColumn("Lower_Band", col("SMA_20") - (col("STD_20") * 2))
    
    return df

def calculate_volume_change(df):
    """
    Calculate daily volume change percentage
    """
    df = df.withColumn("Prev_Volume", lag("volume", 1).over(Window.orderBy("date")))
    df = df.withColumn("Volume_Change_Perc", (col("volume") - col("Prev_Volume")) / col("Prev_Volume") * 100)
    
    return df

def compute_all_features(df):
    """
    Main function to compute all stock features.
    """
    
    df = compute_ema(df)
    df = compute_macd(df)
    df = calculate_bollinger_bands(df)
    df = calculate_volume_change(df)

    return df

def get_feature_table(ticker,db_params):
    table_name = get_feature_table_name(ticker=ticker)
    database_url = make_JDBC_url(db_params=db_params)
    properties = make_DB_properties(db_params=db_params)
    try:
        df_db = spark.read.jdbc(url=database_url, table=table_name, properties=properties)
        return df_db
    except Exception as e:
        print(f"Table {table_name} does not exist.")
        return None
    
def get_feature_append_df(df,df_db):
    return df.join(df_db, df["date"] == df_db["date"], "left_anti")

def write_feature_to_DB(df,ticker,db_params):
    
    df_db = get_feature_table(ticker=ticker,db_params=db_params)
    
    mode = "overwrite" if df_db is None else "append"
    
    table_name = get_feature_table_name(ticker=ticker)
    database_url = make_JDBC_url(db_params=db_params)
    properties = make_DB_properties(db_params=db_params)
    
    if df_db is not None:
        df = get_feature_append_df(df=df,df_db=df_db)

    # Check if the resulting DataFrame is empty
    if df.rdd.isEmpty():
        print("No new records to append. All features are already present in the database.")
    else:
        # Proceed with writing new records to the database
        df.write.jdbc(url=database_url, table=table_name, mode=mode, properties=properties)


def update_features_tables(tickers):
    for ticker in tickers:
        df = read_stock_data(ticker=ticker,db_params=db_params)
        df = compute_all_features(df)
        write_feature_to_DB(df)
        print("{} features has been written.")

update_features_tables(tickers=tickers)