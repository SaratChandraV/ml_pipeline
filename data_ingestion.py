import pandas as pd
import keys
from pyspark.sql import SparkSession
import os
import data_sourcing
import feature_creation

def read_stock_data(ticker,db_params,spark):
    table_name = data_sourcing.get_source_table_name(ticker=ticker)
    
    query = f"(SELECT date,open,high,low FROM {table_name}) AS temp"
    
    database_url = feature_creation.make_JDBC_url(db_params=db_params)
    properties = feature_creation.make_DB_properties(db_params=db_params)
    
    df = spark.read.jdbc(url=database_url, table=query, properties=properties)
    
    return df

def read_features_data(ticker,db_params,spark):
    table_name = feature_creation.get_feature_table_name(ticker=ticker)
    
    query = f"(SELECT * FROM {table_name}) AS temp"
    
    database_url = feature_creation.make_JDBC_url(db_params=db_params)
    properties = feature_creation.make_DB_properties(db_params=db_params)
    
    df = spark.read.jdbc(url=database_url, table=query, properties=properties)
    
    return df

def get_all_columns_df(df_stock,df_features):
    return df_stock.join(df_features, df_stock["date"] == df_features["date"], "inner").drop(df_features.date)

def get_data(tickers,db_params):
    data = {}
    
    path_to_postgres_driver = "C:\\Users\\Administrator\\Documents\\postgresql-42.7.2.jar"
    hadoopFilesPath = "C:\\Users\\Administrator\\Documents\\hadoop-3.0.0"
    os.environ["HADOOP_HOME"] = hadoopFilesPath
    os.environ["hadoop.home.dir"] = hadoopFilesPath
    os.environ["PATH"] = os.environ["PATH"] + f";{hadoopFilesPath}\\bin"
    os.environ['PYSPARK_PYTHON'] = "C:\\Users\\Administrator\\AppData\\Local\\Programs\\Python\\Python39\\python.exe"

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Data Ingestion")\
        .config("spark.jars", path_to_postgres_driver)\
        .getOrCreate()
    
    for ticker in tickers:
        try:
            df_stock = read_stock_data(ticker=ticker,db_params=db_params,spark=spark)
            df_features = read_features_data(ticker=ticker,db_params=db_params,spark=spark)
            df = get_all_columns_df(df_stock=df_stock,df_features=df_features)
            data[ticker] = df
        except:
            pass
        print("{} finished".format(ticker))
    return data


# Usage
# tickers = pd.read_json('ticker_symbols.json')['ticker'].tolist()
# db_params = keys.db_params.db_params
# data = get_data(tickers=tickers,db_params=db_params)