# Real-time Stock Analysis and Option Data Processing

This project provides real-time stock analysis, option data processing, and synthetic futures calculation for the NIFTY index.

## Prerequisites

- Python 3.7+
- PostgreSQL 12+

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements_dev.txt
   ```

## Database Setup

1. Create the necessary PostgreSQL databases:
   ```sql
   CREATE DATABASE candlestick_data;
   CREATE DATABASE token_database;
   CREATE DATABASE websocket;
   ```

2. Connect to each database and create the required tables:

   For `candlestick_data`:
   ```sql
   \c candlestick_data

   CREATE TABLE candlesticks (
       timestamp TIMESTAMP PRIMARY KEY,
       open FLOAT,
       high FLOAT,
       low FLOAT,
       close FLOAT
   );
   ```

   For `token_database`:
   ```sql
   \c token_database

   CREATE TABLE instrument_data (
       name VARCHAR(255),
       instrumenttype VARCHAR(50),
       expiry DATE,
       strike FLOAT,
       token VARCHAR(50),
       symbol VARCHAR(255),
       option_type VARCHAR(2),
       exch_seg VARCHAR(50)
   );
   ```

   For `websocket`:
   ```sql
   \c websocket

   CREATE TABLE live_data (
       token VARCHAR(50),
       ltp FLOAT,
       exchange_timestamp TIMESTAMP
   );

   CREATE TABLE option_data (
       token VARCHAR(50),
       strike FLOAT,
       option_type VARCHAR(2),
       ltp FLOAT,
       timestamp TIMESTAMP
   );
   ```

## Configuration

1. Create a `config.py` file in the project root directory with your API credentials:
   ```python
   API_KEY = 'your_api_key'
   USERNAME = 'your_username'
   PIN = 'your_pin'
   TOKEN = 'your_token'
   CORRELATION_ID = 'your_correlation_id'
   FEED_MODE = 'your_feed_mode'
   ```

## Usage

1. Run the token data updater:
   ```
   python 1token.py
   ```

2. Start the historical data fetcher:
   ```
   python 2hiv4.py
   ```

3. Launch the real-time data collector:
   ```
   python 2undv2.py
   ```

4. Start the option data processor:
   ```
   python 3options.py
   ```

5. Run the synthetic futures calculator:
   ```
   python 4ca.py
   ```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.