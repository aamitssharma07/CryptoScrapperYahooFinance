from datetime import datetime
import time
import json
import pandas as pd
import ssl
import certifi
import urllib.request
import io

def construct_download_url(ticker, period1, period2, interval='monthly'):
    """
    Construct the Yahoo Finance URL for downloading historical data.
    
    :param ticker: str: Ticker symbol of the asset.
    :param period1: str: Start date in 'yyyy-mm-dd' format.
    :param period2: str: End date in 'yyyy-mm-dd' format.
    :param interval: str: Data interval ('daily', 'weekly', 'monthly').
    :return: str: Constructed URL or None if there's an error.
    """
    def convert_to_seconds(period):
        datetime_value = datetime.strptime(period, '%Y-%m-%d')
        total_seconds = int(time.mktime(datetime_value.timetuple()))
        return total_seconds

    try:
        interval_reference = {'daily': '1d', 'weekly': '1wk', 'monthly': '1mo'}
        _interval = interval_reference.get(interval)
        if _interval is None:
            print('Interval code is incorrect')
            return None
        p1 = convert_to_seconds(period1)
        p2 = convert_to_seconds(period2)
        url = (f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}'
               f'?period1={p1}&period2={p2}&interval={_interval}&events=history')
        return url
    except Exception as e:
        print(f"Error in constructing URL: {e}")
        return None

# Retrieve dataset
query_url = construct_download_url('AR-USD', '2015-01-01', '2024-04-03', 'daily')
if query_url:
    try:
        # Add SSL context with certifi certificates
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        with urllib.request.urlopen(query_url, context=ssl_context) as response:
            data = response.read()

        # Read the CSV data from the response
        df = pd.read_csv(io.StringIO(data.decode('utf-8')))
        df.set_index('Date', inplace=True)

        # Save dataset as a CSV
        df.to_csv('Arweave-USD_Historical_Data.csv')

        # Save dataset as a JSON file
        with open('Arweave-USD_Historical_Data.json', 'w') as f:
            json.dump(df.T.to_dict(), f, indent=4)
        
        print("Data has been successfully saved as CSV and JSON.")
    except Exception as e:
        print(f"Error in retrieving or saving data: {e}")
else:
    print("Failed to construct a valid URL.")
