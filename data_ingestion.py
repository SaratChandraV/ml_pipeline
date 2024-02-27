import pandas as pd
import numpy as np
import keys
from pyspark.sql import SparkSession
import os


tickers = pd.read_json('ticker_symbols.json')['ticker'].tolist()
db_params = keys.db_params.db_params

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

def get_source_table_name(ticker):
    return ticker.lower() + "_daily_stock_data"

def get_feature_table_name(ticker):
    return ticker.lower() + "_features_data"

def make_JDBC_url(db_params):
    return 'jdbc:postgresql://{}:{}/{}'.format(db_params['host'],db_params['port'],db_params['database'])

def make_DB_properties(db_params):
    return {"user": db_params['user'], "password": db_params['password'], "driver": "org.postgresql.Driver"}

def read_stock_data(ticker,db_params):
    table_name = get_source_table_name(ticker=ticker)
    
    query = f"(SELECT date,open,high,low,volume FROM {table_name}) AS temp"
    
    database_url = make_JDBC_url(db_params=db_params)
    properties = make_DB_properties(db_params=db_params)
    
    df = spark.read.jdbc(url=database_url, table=query, properties=properties)
    
    return df

def read_features_data(ticker,db_params):
    table_name = get_feature_table_name(ticker=ticker)
    
    query = f"(SELECT * FROM {table_name}) AS temp"
    
    database_url = make_JDBC_url(db_params=db_params)
    properties = make_DB_properties(db_params=db_params)
    
    df = spark.read.jdbc(url=database_url, table=query, properties=properties)
    
    return df

def get_all_columns_df(df_stock,df_features):
    return df_stock.join(df_features, df_stock["date"] == df_features["date"], "inner").drop(df_features.date)

for ticker in tickers[0:1]:
    df_stock = read_stock_data(ticker=ticker,db_params=db_params)
    df_features = read_features_data(ticker=ticker,db_params=db_params)
    df = get_all_columns_df(df_stock=df_stock,df_features=df_features)
    print(df.show())