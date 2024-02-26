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

def get_source_table_name(ticker):
    return ticker + "_daily_stock_data"

def get_source_table_trigger_name(ticker):
    return get_source_table_name(ticker=ticker) + "_last_modified_trigger"

def drop_source_table(ticker,cursor):
    table_name = get_source_table_name(ticker=ticker)
    cursor.execute(f"""DROP TABLE IF EXISTS {table_name}""")

def create_source_table(ticker,cursor):
        table_name = get_source_table_name(ticker=ticker)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date DATE PRIMARY KEY,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_modified_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

#create update trigger for last modification
def create_update_trigger(ticker,cursor):
    trigger_function_name = get_source_table_trigger_name(ticker=ticker)
    trigger_name = get_source_table_trigger_name(ticker=ticker)
    table_name = get_source_table_name(ticker=ticker)

    cursor.execute(f"""
        CREATE OR REPLACE FUNCTION {trigger_function_name}() 
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.last_modified_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};
        
        CREATE TRIGGER {trigger_name}
        BEFORE UPDATE ON {table_name}
        FOR EACH ROW EXECUTE FUNCTION {trigger_function_name}();
    """)

def setup_database(tickers):
    conn = psycopg2.connect(**db_params)

    #Drop tables if exists
    cursor = conn.cursor()
    for ticker in tickers:
        drop_source_table(ticker=ticker,cursor=cursor)
    conn.commit()
    cursor.close()

    #Create Tables
    cursor = conn.cursor()
    for ticker in tickers:
        create_source_table(ticker=ticker,cursor=cursor)
    conn.commit()
    cursor.close()

    #Create Triggers
    cursor = conn.cursor()
    for ticker in tickers:
        create_update_trigger(ticker=ticker,cursor=cursor)
    conn.commit()
    cursor.close()

    conn.close()

setup_database(tickers)

def insert_stock_data(ticker,cursor,data):
    table_name = get_source_table_name(ticker=ticker)
    for index, row in data.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
        """, (index.date(), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

def insert_source_data(tickers,data):
    conn = psycopg2.connect(**db_params)
    for ticker in tickers:
        cursor = conn.cursor()
        insert_stock_data(ticker=ticker,cursor=cursor,data=data[ticker])
        conn.commit()
        cursor.close()
    conn.close()

insert_source_data(tickers=tickers,data=stock_data)