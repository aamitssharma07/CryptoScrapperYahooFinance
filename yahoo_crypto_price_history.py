from datetime import datetime
import time
import json
import pandas as pd
import ssl
import certifi
import urllib.request
import io
import os

def construct_download_url(ticker, period1, period2, interval='daily'):
    def convert_to_seconds(period):
        datetime_value = datetime.strptime(period, '%Y-%m-%d')
        total_seconds = int(time.mktime(datetime_value.timetuple()))
        return total_seconds

    try:
        interval_reference = {'daily': '1d', 'weekly': '1wk', 'monthly': '1mo'}
        _interval = interval_reference.get(interval, '1d')
        p1 = convert_to_seconds(period1)
        p2 = convert_to_seconds(period2)
        return f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={p1}&period2={p2}&interval={_interval}&events=history'
    except Exception as e:
        print(f"Error in constructing URL: {e}")
        return None

def download_and_save_data(ticker, crypto_name, idx):
    base_output_folder = 'Yahoo_Crypto_Data'
    csv_folder = os.path.join(base_output_folder, 'CSVs')
    json_folder = os.path.join(base_output_folder, 'JSONs')

    # Create directories if they do not exist
    os.makedirs(csv_folder, exist_ok=True)
    os.makedirs(json_folder, exist_ok=True)

    query_url = construct_download_url(ticker, '2017-01-01', '2024-05-13')
    if query_url:
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            with urllib.request.urlopen(query_url, context=ssl_context) as response:
                data = response.read()

            df = pd.read_csv(io.StringIO(data.decode('utf-8')))
            df.set_index('Date', inplace=True)

            csv_file_path = os.path.join(csv_folder, f'{idx}.{crypto_name}.csv')
            json_file_path = os.path.join(json_folder, f'{idx}.{crypto_name}.json')

            df.to_csv(csv_file_path)
            with open(json_file_path, 'w') as f:
                json.dump(df.T.to_dict(), f, indent=4)

            print(f"Data for {crypto_name} has been saved as CSV and JSON in separate folders.")
        except Exception as e:
            print(f"Error retrieving or saving data for {ticker}: {e}")
    else:
        print(f"Failed to construct a valid URL for {ticker}.")

def main():
    with open('crypto_tickers.text', 'r') as file:
        for idx, line in enumerate(file, start=1):
            parts = line.strip().split(' | ')
            if len(parts) == 2:
                ticker, crypto_name = parts
                download_and_save_data(ticker, crypto_name, idx)

if __name__ == '__main__':
    main()
