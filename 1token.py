import os
import requests
import json
from tqdm import tqdm
import psycopg2
from datetime import datetime

TOKEN_DB_NAME = 'token_database'
TOKEN_DB_USER = 'postgres'
TOKEN_DB_PASSWORD = 'postgres'
TOKEN_DB_HOST = 'localhost'
TOKEN_DB_PORT = '5432'

# URL to download the JSON data
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'

# Stream the download and show progress
response = requests.get(url, stream=True)
total_size = int(response.headers.get('content-length', 0))  # Get total size

# Download the JSON data
with open('OpenAPIScripMaster.json', 'wb') as json_file:
    for data in tqdm(response.iter_content(chunk_size=1024), total=total_size // 1024, unit='KB'):
        json_file.write(data)  # Write data to file

# Load the JSON data
with open('OpenAPIScripMaster.json', 'r') as json_file:
    data = json.load(json_file)

# PostgreSQL connection details
db_params = {
    'dbname': TOKEN_DB_NAME,
    'user': TOKEN_DB_USER,
    'password': TOKEN_DB_PASSWORD,
    'host': TOKEN_DB_HOST,
    'port': TOKEN_DB_PORT
}

# Establish connection to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Clear all data from the table
clear_table_query = "TRUNCATE TABLE instrument_data;"
cursor.execute(clear_table_query)
conn.commit()
print("Existing data cleared from the table.")

# Insert data into the table
insert_query = '''
INSERT INTO instrument_data (name, instrumenttype, expiry, strike, token, symbol, option_type, exch_seg)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
'''

# Use tqdm to show progress
for item in tqdm(data, desc="Inserting data", unit="record"):
    name = item.get('name')
    instrumenttype = item.get('instrumenttype')
    expiry = item.get('expiry')
    if expiry == "":
        expiry = None
    else:
        expiry = datetime.strptime(expiry, '%d%b%Y').date()
    
    strike = item.get('strike')
    if strike in ['1', '-1', '0']:
        strike = None
    else:
        try:
            strike = float(strike)
            if strike == -0.01:
                strike = None
            else:
                strike = strike / 100
        except (ValueError, TypeError):
            strike = None
    
    token = item.get('token')
    symbol = item.get('symbol')
    exch_seg = item.get('exch_seg')
    
    # Extract option_type from symbol
    option_type = None
    if symbol.endswith('CE'):
        option_type = 'CE'
    elif symbol.endswith('PE'):
        option_type = 'PE'
    
    try:
        cursor.execute(insert_query, (name, instrumenttype, expiry, strike, token, symbol, option_type, exch_seg))
    except psycopg2.Error as e:
        print(f"Error inserting record: {e}")
        print(f"Problematic data: name={name}, instrumenttype={instrumenttype}, expiry={expiry}, strike={strike}, token={token}, symbol={symbol}, option_type={option_type}, exch_seg={exch_seg}")
        conn.rollback()  # Rollback the transaction
    else:
        conn.commit()  # Commit if successful

# Close connection
cursor.close()
conn.close()

print("Data insertion completed.")
