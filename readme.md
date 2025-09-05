# Yahoo Crypto Price History — Quick Start

Fetch **daily** crypto price history from Yahoo Finance for tickers listed in a text file.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install yfinance pandas
```

## Tickers file (`crypto_tickers.txt`)

Supports numbered/bulleted lines:

```
1. BTC-USD
2) ETH-USD
• USDT-USD
- BNB-USD
SOL-USD
```

## Run (daily data)

```bash
python yahoo_crypto_price_history.py \
  --tickers-file crypto_tickers.txt \
  --outdir Yahoo_Crypto_Data \
  --start 2021-01-01 --interval 1d \
  --auto-adjust \
  --csv --json --merged-json
```

## Output

- `Yahoo_Crypto_Data/CSV/<TICKER>.csv`
- `Yahoo_Crypto_Data/JSONs/<TICKER>.json`
- `Yahoo_Crypto_Data/JSONs/merged/merged_<start>_to_<end>_1d.json`

> Notes:
>
> - Change `--start` as needed; `--end` defaults to today.
> - “empty” messages mean Yahoo has no data for that symbol/interval.

## Usage & Citation

Use this script for getting the stock or crypto price history data.

If you use this script for getting data  in your research, please cite our paper:

```bibtex
@INPROCEEDINGS{10982029,
  author={Kumar, Amit and Ji, Taoran},
  booktitle={2024 IEEE International Conference on Big Data (BigData)}, 
  title={CryptoPulse: Short-Term Cryptocurrency Forecasting with Dual-Prediction and Cross-Correlated Market Indicators}, 
  year={2024},
  volume={},
  number={},
  pages={1-8},
  keywords={},
  doi={10.1109/BigData62323.2024.10982029}}
