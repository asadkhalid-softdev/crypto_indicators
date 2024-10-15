# Binance Data Analyser

## Overview
This bot fetches data for all cyrptos on Binance and generates sets of indicators.

1. Bollinger Bands
2. RSI + Stochastic RSI
3. MACD
4. Candlesticks

Based on these indicators, you can filter out the cryptos based on whether a crypto shows:

1. Breakout Ups and Pullback Downs
2. Breakout Downs and Pullback Ups
3. Reversal Ups and Downs
4. Bullish Candle Scores
5. Bearish Candle Scores
6. Uptrending or downtrending MACD/Signal/MACD-Signal-Change

All filters are available in the API documentation below for testing:

[API DOCUMENTATION](https://documenter.getpostman.com/view/12104204/2sAXxTcAsi)

## STEP 1:
Install the requirements `pip install -r requirements.txt`

## STEP 2:
Rename the *keys_bkp.json* to *keys.json*. Update the keys inside, obtained from binance. [Binance API Keys](https://www.binance.com/en/support/faq/how-to-create-api-keys-on-binance-360002502072)

## STEP 3:
Run `python main.py`

## STEP 4:
Use the API documentation to load the APIs to postman for testing.

## OPTIONS:

1. Individual Analysis
You can analyze single or myltiple symbols by choice.

2. Fetch Desired Cryptos
You can run a fetcher which gather data for all symbols on binance, prepares their indicators which can then be used to filter out desired symbols.