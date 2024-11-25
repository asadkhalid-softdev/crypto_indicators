# KUCOIN Data Analyser

## Overview
This bot fetches data for all cyrptos on Binance and can generates sets of indicators as follows:

1. Bollinger Bands
2. RSI + Stochastic RSI
3. MACD
4. Candlestick analysis

Based on these indicators, you can filter out the cryptos based on whether a crypto shows:

1. Breakout Ups and Pullback Downs
2. Breakout Downs and Pullback Ups
3. Reversal Ups and Downs
4. Bullish Candle Scores
5. Bearish Candle Scores
6. Uptrending or downtrending MACD/Signal/MACD-Signal-Change

Currently **1hour**, **1day** and **1week** intervals are being used. More intervals can be added by editing **default_configs** in the *main.py* file.

## STEP 1:
Install the requirements `pip install -r requirements.txt`

## STEP 2:
Rename the **keys_bkp.json** to **keys.json**. Update the keys inside, obtained from binance. 
[KUCOIN API Keys](https://www.kucoin.com/support/360015102174)

## STEP 3:
Run `python main.py`

## STEP 4:
Use the API documentation to load the APIs to postman for testing.

All filters are available in the API documentation below for testing:
[API DOCUMENTATION](https://documenter.getpostman.com/view/25081135/2sAYBVgBZu)

## OPTIONS:

**Fetch Desired Cryptos:**
You can run a fetcher which gather data for all symbols on kucoin, prepares their indicators which can then be used to filter out desired symbols.