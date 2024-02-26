import pandas as pd
import numpy as np
import yfinance as yf
import keys
import psycopg2

tickers = pd.read_json('ticker_symbols.json')['ticker'].tolist()

def download_max_data(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="max")
    return data[['Open','High','Low','Close','Volume']]

stock_data = {}
for ticker in tickers:
    stock_data[ticker] = download_max_data(ticker=ticker)

db_params = keys.db_params.db_params

def create_tables(stock_data):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    for ticker in stock_data:
        table_name = ticker + "_daily_stock_data"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date DATE PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            );
        """)
    conn.commit()
    cursor.close()
    conn.close()

create_tables(stock_data)

