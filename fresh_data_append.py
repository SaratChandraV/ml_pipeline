import psycopg2
from psycopg2 import sql
import yfinance as yf
from datetime import datetime, timedelta
import keys
import pandas as pd
import data_sourcing

def get_latest_date_in_DB(ticker,cursor):
    
    table_name = data_sourcing.get_source_table_name(ticker=ticker)
    
    cursor.execute(sql.SQL("SELECT MAX(date) FROM {}").format(sql.Identifier(table_name)))
    
    latest_date = cursor.fetchone()[0]
    
    if latest_date is not None:
        start_date = latest_date + timedelta(days=1)
    else:
        # If the table is empty, lets return None.
        start_date = None

    return start_date

def get_today_date():
    return datetime.now().date()

def get_stock_data_from_yf(ticker,start_date,end_date):
    return yf.download(ticker,start=start_date,end=end_date)

def handle_no_data_error(ticker,db_params):
    tickers = [ticker]
    stock_data = data_sourcing.download_all_stock_data(tickers=tickers)
    data_sourcing.setup_database(tickers=tickers,db_params=db_params)
    data_sourcing.insert_source_data(tickers=tickers,data=stock_data,db_params=db_params)

def upsert_source_data(ticker,cursor):
    start_date = get_latest_date_in_DB(ticker=ticker,cursor=cursor)
    if start_date == None:
        handle_no_data_error(ticker=ticker)
        return "Failed"
    end_date = get_today_date()
    end_date = max(start_date,end_date)
    data_df = get_stock_data_from_yf(ticker=ticker,start_date=start_date,end_date=end_date)
    if data_df.empty:
        return "Empty"
    data_df = data_df['Open', 'High', 'Low', 'Close', 'Volume']
    table_name = data_sourcing.get_source_table_name(ticker=ticker)
    for index,row in data_df.iterrows():
        data = {
                "date": index,
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume'],
            }
        query = sql.SQL("INSERT INTO {} (date, open, high, low, close, volume) VALUES (%(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)").format(sql.Identifier(table_name))
        cursor.execute(query, data)
    return "Success"
        
def upsert_source_tables(tickers,db_params):
    conn = psycopg2.connect(**db_params)
    for ticker in tickers:
        cursor = conn.cursor()
        status_msg = upsert_source_data(ticker=ticker,cursor=cursor)
        print(ticker,status_msg)
        conn.commit()
        cursor.close()
    conn.close()

#Usage
# Database connection parameters
# db_params = keys.db_params.db_params
# tickers = pd.read_json('ticker_symbols.json')
# upsert_source_tables(tickers=tickers,db_params=db_params)