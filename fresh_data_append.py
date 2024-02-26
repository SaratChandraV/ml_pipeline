import psycopg2
from psycopg2 import sql
import yfinance as yf
from datetime import datetime, timedelta
import keys
import pandas as pd

tickers = pd.read_json('ticker_symbols.json')['ticker'].tolist()

# Database connection parameters
db_params = keys.db_params.db_params

def get_source_table_name(ticker):
    return ticker + "_daily_stock_data"

def get_latest_date_in_DB(ticker,cursor):
    
    table_name = get_source_table_name(ticker=ticker)
    
    cursor.execute(sql.SQL("SELECT MAX(date) FROM {}").format(sql.Identifier(table_name)))
    
    latest_date = cursor.fetchone()[0]
    
    if latest_date is not None:
        start_date = latest_date + timedelta(days=1)
    else:
        # If the table is empty, lets return None.
        start_date = None

    return start_date

def get_today_date():
    return datetime.now()

def get_stock_data_from_yf(ticker,start_date,end_date):
    return yf.download(ticker,start=start_date,end=end_date)

def handle_no_data_error(ticker):
    pass

def upsert_source_data(ticker,cursor):
    start_date = get_latest_date_in_DB(ticker=ticker,cursor=cursor)
    if start_date == None:
        handle_no_data_error(ticker=ticker)
        return "Failed"
    end_date = get_today_date()
    data_df = get_stock_data_from_yf(ticker=ticker,start_date=start_date,end_date=end_date)
    data_df = data_df['Open', 'High', 'Low', 'Close', 'Volume']
    if data_df.empty:
        return
    table_name = get_source_table_name(ticker=ticker)
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
        
def upsert_source_tables(tickers):
    conn = psycopg2.connect(**db_params)
    for ticker in tickers:
        cursor = conn.cursor()
        upsert_source_data(ticker=ticker,cursor=cursor)
        conn.commit()
        cursor.close()
    conn.close()

upsert_source_tables(tickers=tickers)