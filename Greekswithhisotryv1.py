from SmartApi import SmartConnect  # or from smartapi.smartConnect import SmartConnect
import pyotp, time, pytz
from datetime import datetime, timedelta
import pandas as pd
from config import *
import requests
import numpy as np
import json
import os
from collections import defaultdict
import time
from py_vollib.black_scholes import implied_volatility, greeks
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta, rho

STRIKE_DIFFERENCE = 100
SYMBOL = "SENSEX"  
EXG = "BFO"


# Create an object of SmartConnect
obj = SmartConnect(api_key=apikey)

def rate_limited_request(func, *args, **kwargs):
    """
    Rate-limited wrapper for API requests.
    Ensures only 3 requests are made per second.
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    elapsed_time = time.time() - start_time
    if elapsed_time < 1/3:  # If the request took less than 1/3 second
        time.sleep(1/3 - elapsed_time)  # Sleep for the remaining time
    return result

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
        api_response = rate_limited_request(obj.getCandleData, historicParam)
        data = api_response['data']
        data = [row[:5] for row in data]  # Keep only the first 5 columns
        columns = ['T', 'Open', 'High', 'Low', 'Close']
        df = pd.DataFrame(data, columns=columns)
        df['T'] = pd.to_datetime(df['T']).dt.tz_localize(None)  # Remove timezone
        df.set_index('T', inplace=True)
        return df
    except Exception as e:
        print("Historic Api failed: {}".format(e))
        return None

def load_json_file(json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    return data

def find_nearest_expiry(data, symbol):
    expiry_dates = set()
    for item in data:
        if item['name'] == symbol and item['instrumenttype'] == 'OPTIDX':
            expiry_dates.add(item['expiry'])
    
    current_date = datetime.now().date()
    nearest_expiry = None
    min_diff = float('inf')
    
    for expiry in expiry_dates:
        expiry_date = datetime.strptime(expiry, "%d%b%Y").date()
        diff = (expiry_date - current_date).days
        if diff < min_diff:
            min_diff = diff
            nearest_expiry = expiry
    
    return nearest_expiry

def get_strike_tokens(data, symbol, expiry_date, strike_price):
    strike_tokens = []
    for item in data:
        if item['name'] == symbol and item['expiry'] == expiry_date and item['strike'] == f"{strike_price}00.000000":
            if item['instrumenttype'] == 'OPTIDX' and (item['symbol'].endswith('CE') or item['symbol'].endswith('PE')):
                strike_tokens.append((item['token'], item['symbol']))
    return strike_tokens

def round_to_nearest_strike(price):
    return round(price / STRIKE_DIFFERENCE) * STRIKE_DIFFERENCE

def calculate_time_to_expiry(current_date, expiry_date):
    # Set the expiry time to 15:30:00
    expiry_datetime = datetime.combine(expiry_date.date(), datetime.strptime("15:30:00", "%H:%M:%S").time())
    time_diff = expiry_datetime - current_date
    days = time_diff.total_seconds() / (24 * 60 * 60)
    return max(days / 365.0, 1e-10)  # Ensure time to expiry is not zero

def calculate_greeks(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, option_price):
    try:
        option_type = 'c' if option_type.lower() == 'ce' else 'p'
        
        implied_vol = implied_volatility.implied_volatility(
            option_price, underlying_price, strike, time_to_expiry, risk_free_rate, option_type
        )
        
        greek_values = {
            'implied_volatility': implied_vol,
            'delta': delta(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, implied_vol),
            'gamma': gamma(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, implied_vol),
            'vega': vega(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, implied_vol),
            'theta': theta(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, implied_vol),
            'rho': rho(option_type, underlying_price, strike, time_to_expiry, risk_free_rate, implied_vol)
        }
        return greek_values
    except Exception as e:
        print(f"Error calculating Greeks: {e}")
        return {k: np.nan for k in ['implied_volatility', 'delta', 'gamma', 'vega', 'theta', 'rho']}

# Usage Example
auth_token, feed_token = login()

thirty_days_ago = datetime.now() - timedelta(days=1)
from_date = thirty_days_ago.strftime("%Y-%m-%d 09:15")
to_date = datetime.now().strftime("%Y-%m-%d %H:%M")

timeperiod = "ONE_MINUTE"

# Fetch historical data for the underlying (NSE)
exchange = "BSE"
token = "99919000" #99926000 nifty 99926074 MIDCAP 99919000 SENSEX 99926009 BANKNIFTY 99926037 FINNIFTY 99919012 BANKEX
df_underlying = historical_data(exchange, token, from_date, to_date, timeperiod)


# Save the underlying data to a CSV file with the symbol name
underlying_file_path = os.path.join(r'C:\Users\prana\Desktop\code\0fut-ca', f"{SYMBOL}.csv")
if os.path.exists(underlying_file_path):
    os.remove(underlying_file_path)  # Remove the file if it exists
df_underlying.to_csv(underlying_file_path)

# Read the underlying close price from the CSV file
df_underlying = pd.read_csv(underlying_file_path)

# Load the local JSON file
json_data = load_json_file(r'C:\Users\prana\Desktop\code\0fut-ca\OpenAPIScripMaster.json')


# Find the nearest expiry date
nearest_expiry = find_nearest_expiry(json_data, SYMBOL)

# Convert nearest_expiry to a datetime object with time set to 15:30:00
expiry_date = datetime.strptime(nearest_expiry + " 15:30:00", "%d%b%Y %H:%M:%S")

# Cache for fetched data
data_cache = defaultdict(dict)

# Calculate the synthetic futures close price for each row and save it in a CSV file
synthetic_futures_data = []

risk_free_rate = 0.0  # As per your requirement

for i in range(len(df_underlying)):
    close_price = df_underlying['Close'].iloc[i]
    open_price = df_underlying['Open'].iloc[i]
    rounded_strike_open = round_to_nearest_strike(open_price)
    rounded_strike_close = round_to_nearest_strike(close_price)
    timestamp = df_underlying['T'].iloc[i]  # Extract the timestamp from the underlying data
    
    # Fetch historical data for the rounded strike token (NFO)
    if rounded_strike_open not in data_cache:
        strike_tokens = get_strike_tokens(json_data, SYMBOL, nearest_expiry, rounded_strike_open)
        call_token = None
        put_token = None
        
        for token, option_type in strike_tokens:
            if option_type.endswith('CE'):
                call_token = token
            elif option_type.endswith('PE'):
                put_token = token
        
        if call_token and put_token:
            call_df = rate_limited_request(historical_data, EXG, call_token, from_date, to_date, timeperiod)
            put_df = rate_limited_request(historical_data, EXG, put_token, from_date, to_date, timeperiod)
            
            if call_df is not None and put_df is not None:
                data_cache[rounded_strike_open]['call'] = call_df
                data_cache[rounded_strike_open]['put'] = put_df
            else:
                print(f"Failed to fetch data for strike {rounded_strike_open} at index {i}")
        else:
            print(f"No tokens found for strike {rounded_strike_open} at index {i}")
    
    if rounded_strike_close not in data_cache:
        strike_tokens = get_strike_tokens(json_data, SYMBOL, nearest_expiry, rounded_strike_close)
        call_token = None
        put_token = None
        
        for token, option_type in strike_tokens:
            if option_type.endswith('CE'):
                call_token = token
            elif option_type.endswith('PE'):
                put_token = token
        
        if call_token and put_token:
            call_df = rate_limited_request(historical_data, EXG, call_token, from_date, to_date, timeperiod)
            put_df = rate_limited_request(historical_data, EXG, put_token, from_date, to_date, timeperiod)
            
            if call_df is not None and put_df is not None:
                data_cache[rounded_strike_close]['call'] = call_df
                data_cache[rounded_strike_close]['put'] = put_df
            else:
                print(f"Failed to fetch data for strike {rounded_strike_close} at index {i}")
        else:
            print(f"No tokens found for strike {rounded_strike_close} at index {i}")
    
    if rounded_strike_open in data_cache and rounded_strike_close in data_cache:
        call_df_open = data_cache[rounded_strike_open]['call']
        put_df_open = data_cache[rounded_strike_open]['put']
        call_df_close = data_cache[rounded_strike_close]['call']
        put_df_close = data_cache[rounded_strike_close]['put']
        
        # Check if the index exists in all DataFrames
        if i < len(call_df_open) and i < len(put_df_open) and i < len(call_df_close) and i < len(put_df_close):
            # Extract the open and close prices
            spot_open = open_price
            spot_close = close_price
            call_open = call_df_open['Open'].iloc[i]
            call_close = call_df_close['Close'].iloc[i]
            put_open = put_df_open['Open'].iloc[i]
            put_close = put_df_close['Close'].iloc[i]
            
            # Calculate the synthetic futures open and close prices for the current row
            synthetic_futures_open = rounded_strike_open + (call_open - put_open)
            synthetic_futures_close = rounded_strike_close + (call_close - put_close)
            
            # Calculate additional metrics
            synthetic_spot_open_difference = synthetic_futures_open - spot_open
            synthetic_spot_close_difference = synthetic_futures_close - spot_close
            straddle_open = call_open + put_open
            straddle_close = call_close + put_close
            
            # Calculate time to expiry
            current_date = pd.to_datetime(timestamp)
            time_to_expiry = calculate_time_to_expiry(current_date, expiry_date)
            
            # Calculate Greeks for call and put options at open
            call_greeks_open = calculate_greeks('CE', spot_open, rounded_strike_open, time_to_expiry, risk_free_rate, call_open)
            put_greeks_open = calculate_greeks('PE', spot_open, rounded_strike_open, time_to_expiry, risk_free_rate, put_open)
            
            # Calculate Greeks for call and put options at close
            call_greeks_close = calculate_greeks('CE', spot_close, rounded_strike_close, time_to_expiry, risk_free_rate, call_close)
            put_greeks_close = calculate_greeks('PE', spot_close, rounded_strike_close, time_to_expiry, risk_free_rate, put_close)
            
            synthetic_futures_data.append({
                'Timestamp': timestamp,
                'Spot Open': spot_open,
                'Spot Close': spot_close,
                'Rounded Strike Open': rounded_strike_open,
                'Rounded Strike Close': rounded_strike_close,
                'Call Open': call_open,
                'Call Close': call_close,
                'Put Open': put_open,
                'Put Close': put_close,
                'Synthetic Futures Open': synthetic_futures_open.round(2),
                'Synthetic Futures Close': synthetic_futures_close.round(2),
                'Synthetic Spot Open Difference': synthetic_spot_open_difference.round(2),
                'Synthetic Spot Close Difference': synthetic_spot_close_difference.round(2),
                'Straddle Open': straddle_open.round(2),
                'Straddle Close': straddle_close.round(2),
                'Call IV Close': call_greeks_close['implied_volatility'] * 100,  # Multiplied by 100
                'Call Delta Close': call_greeks_close['delta'],
                'Put IV Close': put_greeks_close['implied_volatility'] * 100,  # Multiplied by 100
                'Put Delta Close': put_greeks_close['delta'],
                'IV Difference (Call - Put)': (call_greeks_close['implied_volatility'] - put_greeks_close['implied_volatility']) * 100,  # Multiplied by 100
            })
        else:
            print(f"Skipping index {i} due to missing data")
    else:
        print(f"Data not available for strike {rounded_strike_open} or {rounded_strike_close} at index {i}")

# Convert the list of dictionaries to a DataFrame
synthetic_futures_df = pd.DataFrame(synthetic_futures_data)

# Save the synthetic futures data to a CSV file
synthetic_futures_file_path = os.path.join(r'C:\Users\prana\Desktop\code\0fut-ca', f"{SYMBOL}_synthetic_futures.csv")
if os.path.exists(synthetic_futures_file_path):
    os.remove(synthetic_futures_file_path)  # Remove the file if it exists

synthetic_futures_df.to_csv(synthetic_futures_file_path, index=False)

print(f"Synthetic Futures Open and Close data saved to {synthetic_futures_file_path}")
