from SmartApi import SmartConnect  # or from smartapi.smartConnect import SmartConnect
import pyotp, time, pytz
from datetime import datetime, timedelta
import pandas as pd
from config import *
import requests
import numpy as np
import json
import os
import psycopg2

SYMBOL = "NIFTY"  

# Create an object of SmartConnect
obj = SmartConnect(api_key=apikey)

def login():
    """
    Function to login and return AUTH and FEED tokens.
    """
    data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())
    refreshToken = data['data']['refreshToken']
    auth_token = data['data']['jwtToken']
    feed_token = obj.getfeedToken()
    return auth_token, feed_token

def historical_data(exchange, token, from_date, to_date, timeperiod):
    """
    Function to fetch historical data and return it as a Pandas DataFrame.
    """
    try:
        historicParam = {
            "exchange": exchange,
            "symboltoken": token,
            "interval": timeperiod,
            "fromdate": from_date, 
            "todate": to_date
        }
        api_response = obj.getCandleData(historicParam)
        data = api_response['data']
        data = [row[:5] for row in data]  # Keep only the first 5 columns
        columns = ['T', 'Open', 'High', 'Low', 'Close']
        df = pd.DataFrame(data, columns=columns)
        df['T'] = pd.to_datetime(df['T'], format='%Y-%m-%dT%H:%M:%S%z')  # Parse with timezone
        df.set_index('T', inplace=True)
        return df
    except Exception as e:
        print("Historic Api failed: {}".format(e))
        return None

def insert_into_db(df):
    """
    Function to insert DataFrame into PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname="candlestick_data",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO candlesticks (timestamp, open, high, low, close)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close
            """,
            (index.isoformat(), float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']))
        )

    conn.commit()
    cursor.close()
    conn.close()

def fetch_and_insert_historical_data():
    """
    Function to fetch and insert historical data.
    """
    auth_token, feed_token = login()

    thirty_days_ago = datetime.now() - timedelta(days=15)
    from_date = thirty_days_ago.strftime("%Y-%m-%d 09:15")
    to_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    timeperiod = "ONE_MINUTE"

    # Fetch historical data for the underlying (NSE)
    exchange = "NSE"
    token = "99926000"
    df_underlying = historical_data(exchange, token, from_date, to_date, timeperiod)

    # Insert the data into the PostgreSQL database
    if df_underlying is not None:
        insert_into_db(df_underlying)

def fetch_and_insert_latest_data():
    """
    Function to fetch and insert the latest minute of data.
    """
    auth_token, feed_token = login()

    to_date = datetime.now()
    from_date = to_date - timedelta(minutes=1)
    from_date_str = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M")

    timeperiod = "ONE_MINUTE"

    # Fetch historical data for the underlying (NSE)
    exchange = "NSE"
    token = "99926000"
    df_underlying = historical_data(exchange, token, from_date_str, to_date_str, timeperiod)

    # Insert the data into the PostgreSQL database
    if df_underlying is not None:
        insert_into_db(df_underlying)

# Fetch historical data first
fetch_and_insert_historical_data()

# Main loop to fetch and insert the latest minute of data every minute
while True:
    fetch_and_insert_latest_data()
    time.sleep(60)  # Sleep for 60 seconds (1 minute)