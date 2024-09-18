import time
import logging
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import pyotp
from SmartApi import SmartConnect
import confi as config
import pandas as pd
import pytz

# Logging setup
td = datetime.today().date()
logging.basicConfig(filename=f"ANGEL_WEBSOCKET_{td}.log", format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_handler)

# Constants
STRIKE_DIFFERENCE = 50
SYMBOL = "NIFTY"
EXCHANGE_OPTIONS = 2

# Database configuration
db_config = {
    "dbname": "candlestick_data",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Add a new database configuration for the token database
token_db_config = {
    "dbname": "token_database",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

def login():
    obj = SmartConnect(api_key=config.API_KEY)
    data = obj.generateSession(config.USERNAME, config.PIN, pyotp.TOTP(config.TOKEN).now())
    AUTH_TOKEN = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']
    FEED_TOKEN = obj.getfeedToken()
    return obj, AUTH_TOKEN, FEED_TOKEN

def historical_data(obj, exchange, token, from_date, to_date, timeperiod):
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
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S%z')
        df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
        return df
    except Exception as e:
        logger.error(f"Historic Api failed for token {token}: {e}")
        return None

def insert_into_db(df):
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # First, try to create the unique constraint if it doesn't exist
                try:
                    cur.execute("""
                        ALTER TABLE option_data 
                        ADD CONSTRAINT unique_token_timestamp 
                        UNIQUE (token, timestamp);
                    """)
                    conn.commit()
                    logger.info("Added unique constraint to option_data table")
                except psycopg2.errors.DuplicateTable:
                    # Constraint already exists, which is fine
                    conn.rollback()

                # Now proceed with the insert
                for _, row in df.iterrows():
                    insert_query = sql.SQL("""
                        INSERT INTO option_data (token, strike, option_type, timestamp, open, high, low, close)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (token, timestamp) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close
                    """)
                    cur.execute(insert_query, (
                        row['token'],
                        row['strike'],
                        row['option_type'],
                        row['timestamp'],
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close']
                    ))
            conn.commit()
        logger.info(f"Inserted {len(df)} rows into option_data table")
    except Exception as e:
        logger.error(f"Error inserting data into database: {e}")

def fetch_atm_option_tokens(symbol, underlying_price, limit=10):
    atm_strike = round(underlying_price / STRIKE_DIFFERENCE) * STRIKE_DIFFERENCE
    lower_strike = atm_strike - (2 * STRIKE_DIFFERENCE)
    upper_strike = atm_strike + (2 * STRIKE_DIFFERENCE)

    try:
        with psycopg2.connect(**token_db_config) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT token, strike, option_type
                    FROM instrument_data
                    WHERE name LIKE %s
                      AND strike IS NOT NULL
                      AND CAST(strike AS DECIMAL) BETWEEN %s AND %s
                      AND instrumenttype = 'OPTIDX'
                    ORDER BY expiry ASC, ABS(CAST(strike AS DECIMAL) - %s)
                    LIMIT %s
                """
                cur.execute(query, (f'{symbol}%', lower_strike, upper_strike, atm_strike, limit))
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching ATM option tokens: {e}")
        return []

def fetch_and_insert_historical_data(obj):
    market_open = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    to_date = datetime.now()
    from_date = market_open if market_open < to_date else to_date - timedelta(days=1)

    from_date_str = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M")

    # Fetch underlying price
    underlying_df = historical_data(obj, "NSE", "99926000", from_date_str, to_date_str, "ONE_MINUTE")
    if underlying_df is None or underlying_df.empty:
        logger.error("Failed to fetch underlying data")
        return

    underlying_price = underlying_df.iloc[-1]['close']
    atm_options = fetch_atm_option_tokens(SYMBOL, underlying_price)

    for token, strike, option_type in atm_options:
        df = historical_data(obj, "NFO", token, from_date_str, to_date_str, "ONE_MINUTE")
        if df is not None and not df.empty:
            df['token'] = token
            df['strike'] = strike
            df['option_type'] = option_type
            insert_into_db(df)

def fetch_and_insert_latest_data(obj):
    to_date = datetime.now()
    from_date = to_date - timedelta(minutes=1)
    from_date_str = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M")

    # Fetch underlying price
    underlying_df = historical_data(obj, "NSE", "99926000", from_date_str, to_date_str, "ONE_MINUTE")
    if underlying_df is None or underlying_df.empty:
        logger.error("Failed to fetch latest underlying data")
        return

    underlying_price = underlying_df.iloc[-1]['close']
    atm_options = fetch_atm_option_tokens(SYMBOL, underlying_price)

    for token, strike, option_type in atm_options:
        df = historical_data(obj, "NFO", token, from_date_str, to_date_str, "ONE_MINUTE")
        if df is not None and not df.empty:
            df['token'] = token
            df['strike'] = strike
            df['option_type'] = option_type
            insert_into_db(df)

def main():
    try:
        # Login once at the beginning
        obj, AUTH_TOKEN, FEED_TOKEN = login()
        logger.info("Logged in successfully.")

        # Fetch historical data for the day
        fetch_and_insert_historical_data(obj)
        logger.info("Historical data fetched and inserted. Starting minute-by-minute updates.")

        while True:
            fetch_and_insert_latest_data(obj)
            time.sleep(60)  # Wait for 1 minute before the next update

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Exiting.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Script execution completed.")

if __name__ == "__main__":
    main()