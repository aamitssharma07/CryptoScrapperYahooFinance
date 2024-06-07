# Crypto Data Scraper

This script downloads historical cryptocurrency data from Yahoo Finance and saves it in CSV and JSON formats. Users can specify the date range for the data they want to download.

## Prerequisites

- Python 3.x
- Required Python libraries:
  - pandas
  - certifi

You can install the necessary libraries using pip:

```sh
pip install pandas certifi
```

1. Prepare the crypto_tickers.txt file

Create a file named crypto_tickers.txt in the same directory as the script. The file should contain cryptocurrency tickers in the following format:

1. BTC-USD
2. ETH-USD
3. XRP-USD

The index here represents the rank of the crypto

2. Run the Script
   python yahoo_crypto_price_history.py
   You will be prompted to enter the start and end dates for the data download in the YYYY-MM-DD format.
   Example:
   Enter the start date (YYYY-MM-DD): 2017-01-01
   Enter the end date (YYYY-MM-DD): 2024-05-27

3. Output

The script will download and save the data in two separate folders: CSV and JSON. Each folder will contain files named with the format rank.crypto_name.csv and rank.crypto_name.json.

Notes
Please make sure your crypto_tickers.txt file is formatted correctly.
The script creates CSV and JSON folders if they do not already exist.
The data download depends on the availability and correctness of data on Yahoo Finance.
