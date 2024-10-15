from binance import Client

import pandas as pd
pd.options.mode.chained_assignment = None

from datetime import datetime, timedelta
import time
import math
import logging
import requests
import numpy as np
from scipy.signal import argrelextrema
import requests
import pytz
import dateparser
from typing import Optional
from binance.exceptions import UnknownDateFormat
import copy 

from urllib.parse import urlencode
import threading
from functools import reduce

import os
import scipy.stats as stats

import talib

import logging
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(filename=f'app.log', filemode='a', format='%(asctime)s >>>> %(process)d - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

max_duration_days = 14

#region Indicators
bullish_trends = ["Bullish", "Hammer", "Inverted Hammer", "Dragonfly Doji", "Bullish Spinning Top", "Bullish Kicker", "Bullish Engulfing", 
                  "Bullish Harami", "Piercing Line", "Tweezer Bottom", "Morning Star", "Morning Doji Star", "Bullish Abandoned Baby",
                  "Three White Soldiers", "Three Line Strike (Bullish)", "Three Inside Up", "Three Outside Up"]
bearish_trends = ["Bearish", "Hanging Man", "Shooting Star", "Gravestone Doji", "Bearish Spinning Top", "Bearish Kicker", "Bearish Engulfing", "Bearish Harami",
                  "Dark Cloud Cover", "Tweezer Top", "Evening Star", "Evening Doji Star", "Bearish Abandoned Baby", "Three Black Crows", "Three Line Strike (Bearish)",
                  "Three Inside Down", "Three Outside Down"]
neutral = ["Doji"]
#endregion

#region Functions
def prepare_url(url, parameters, api_url):

    url_appendix = ""
    for k in parameters:
        url_appendix+=f"{k}={parameters[k]}&"
    url_appendix = url_appendix[:-1]

    return api_url+url+f"?{url_appendix}"

def convert_ts_str(ts_str):
    if ts_str is None:
        return ts_str
    if type(ts_str) == int:
        return ts_str
    return date_to_milliseconds(ts_str)

def date_to_milliseconds(date_str: str) -> int:
    """Convert UTC date to milliseconds

    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/

    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    """
    # get epoch value in UTC
    epoch: datetime = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d: Optional[datetime] = dateparser.parse(date_str, settings={'TIMEZONE': "UTC"})
    if not d:
        raise UnknownDateFormat(date_str)

    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)
#endregion

class binance_api:
    def __init__(self,
                api_type="test",
                keys={}
                ):
        
        self.api_type = api_type
        self.keys = keys
        
        self.load_api()

    def load_api(self):
        try:
            logging.info(f"{datetime.now().isoformat()} >>> Loading Binance API...")

            self.api_key = self.keys['api_key']
            self.api_secret = self.keys['api_secret']

            # Instantiate a Binance API client
            self.client = Client(self.api_key, self.api_secret)

            if self.api_type == "test":
                self.client.API_URL = 'https://testnet.binance.vision/api'
                logging.info(f"{datetime.now().isoformat()} >>> You are using TEST network...")
            else:
                logging.info(f"{datetime.now().isoformat()} >>> You are using LIVE network...")

                self.time_res = self.client.get_server_time()
                self.time_res_datetime = datetime.fromtimestamp(self.time_res['serverTime']/1e3)
                logging.info(f"{datetime.now().isoformat()} >>> Connected to Binance. Server time: {self.time_res_datetime}")
        except Exception as e:
            logging.error(e, exc_info=True)

class binance_fetch_promising(threading.Thread):
    def __init__(self,
                app,
                binance_api_obj,
                default_configs,
                delay=60,
                rsi_low=30,
                rsi_high=70):
        
        super().__init__()
        
        self.app = app
        self.delay = delay
        self.default_configs = default_configs
        self.binance_api = binance_api_obj
        self.client = self.binance_api.client
        self.rsi_low = rsi_low
        self.rsi_high = rsi_high

        self.running = True
        self.refresh_time = datetime.now().timestamp() # used to delay thread functions
        self.symbols_info = []
        self.skip_unavailable = True
        self.ignore_symbols = ["EURUSDT", "EURIUSDT", "AEURUSDT"]
        self.macd_indicator = pd.DataFrame()

        if os.path.exists("df_indicator_promising.csv"):
            self.indicator_info = pd.read_csv("df_indicator_promising.csv")
            self.indicator_info = self.indicator_info.sort_values("symbol", ascending=True)
        else:
            self.indicator_info = pd.DataFrame()

        logging.info(f"{datetime.now().isoformat()} >>> Initialized fetcher for promising cryptos...")

    def run(self):
        logging.info(f"{datetime.now().isoformat()} >>> Running fetcher...")

        while self.running:
            self.fetch_exchange_info()
            self.fetch_indicator()

            while self.running and datetime.now().timestamp()-self.refresh_time < self.delay:
                pass
            self.refresh_time = datetime.now().timestamp()

    def stop(self):
        logging.info(f"{datetime.now().isoformat()} >>> Stopping fetcher...")
        self.running = False
    
    def fetch_exchange_info(self):
        exchange_info = self.client.get_exchange_info()
        self.symbols_info = exchange_info['symbols']
        self.symbols_info = [i for i in self.symbols_info if i["quoteAsset"]=="USDT"]
        self.symbols_info = sorted(self.symbols_info, key=lambda x: x['symbol'])
    
    def fetch_indicator(self):
        now = datetime.now()
        how_much_ago = now - timedelta(days=7)
        how_much_ago = how_much_ago.isoformat()

        store_df_time = datetime.now().timestamp()

        for sym in self.symbols_info:
            try:

                if sym["symbol"] in self.ignore_symbols:
                    continue

                if not self.running:
                    break
                
                if not self.indicator_info.empty:
                    if sym["symbol"] in self.indicator_info['symbol'].values.tolist():
                        old_df = self.indicator_info[self.indicator_info["symbol"]==sym["symbol"]]
                        is_data = old_df["is_data"].values.tolist()[0]
                        last_check = old_df["last_check"].values.tolist()[0]
                    
                        if not is_data and self.skip_unavailable:
                            logging.info(f"{datetime.now().isoformat()} >>> Skipping {sym['symbol']}...")
                            continue
                        elif datetime.now().timestamp() - last_check < 5*60:
                            logging.info(f"{datetime.now().isoformat()} >>> Already checked recently. Skipping {sym['symbol']}...")
                            continue
                
                # start binance data fetching for symbol
                sym_data = {
                    "baseAsset": sym["baseAsset"],
                    "quoteAsset": sym["quoteAsset"],
                    "symbol": sym["symbol"]
                    }

                local_thread = binance_data(
                    app=self.app, 
                    binance_api_obj=self.binance_api, 
                    symbol=sym["symbol"], 
                    base_asset=sym["baseAsset"], 
                    quote_asset=sym["quoteAsset"], 
                    default_configs=copy.deepcopy(self.default_configs), 
                    delay=1
                    )
                
                local_thread.get_multiple_klines_data()
                local_thread.generate_indicator()

                if local_thread.combined_indicators.empty:
                    raise Exception(f"Data unavailable for {sym['symbol']}")
                
                sym_data["is_data"] = True
                sym_data["last_check"] = datetime.now().timestamp()

                # extract indicators for the symbol, get buys and sell times
                indicator = local_thread.combined_indicators
                if indicator.empty:
                    raise Exception(f"Data unavailable for {sym['symbol']}")

                latest_sample = indicator.head(1).to_dict('records')[0]
                # sym_data['latest_sample'] = latest_sample

                for inter in self.default_configs:
                    #region breakdowns and pullbacks
                    # find breakout downs
                    is_BreakoutDown = indicator[(indicator[f'breakout_down_{inter}']==True)]
                    is_BreakoutDown = is_BreakoutDown[(is_BreakoutDown['Open time'] >= how_much_ago)]
                    is_BreakoutDown['Open time'] = is_BreakoutDown['Open time'].astype(str)
                    if not is_BreakoutDown.empty:
                        sym_data[f"is_BreakoutDown_{inter}"] = True
                        sym_data[f"breakoutDown_times_{inter}"] = str(is_BreakoutDown['Open time'].values.tolist())
                    else:
                        sym_data[f"is_BreakoutDown_{inter}"] = False
                        sym_data[f"breakoutDown_times_{inter}"] = "[]"

                    # find breakout ups
                    is_BreakoutUp = indicator[(indicator[f'breakout_up_{inter}']==True)]
                    is_BreakoutUp = is_BreakoutUp[(is_BreakoutUp['Open time'] >= how_much_ago)]
                    is_BreakoutUp['Open time'] = is_BreakoutUp['Open time'].astype(str)
                    if not is_BreakoutUp.empty:
                        sym_data[f"is_BreakoutUp_{inter}"] = True
                        sym_data[f"breakoutUp_times_{inter}"] = str(is_BreakoutUp['Open time'].values.tolist())
                    else:
                        sym_data[f"is_BreakoutUp_{inter}"] = False
                        sym_data[f"breakoutUp_times_{inter}"] = "[]"

                    # find pullback downs
                    is_PullbackDown = indicator[(indicator[f'pullback_down_{inter}']==True)]
                    is_PullbackDown = is_PullbackDown[(is_PullbackDown['Open time'] >= how_much_ago)]
                    is_PullbackDown['Open time'] = is_PullbackDown['Open time'].astype(str)
                    if not is_PullbackDown.empty:
                        sym_data[f"is_PullbackDown_{inter}"] = True
                        sym_data[f"pullbackDown_times_{inter}"] = str(is_PullbackDown['Open time'].values.tolist())
                    else:
                        sym_data[f"is_PullbackDown_{inter}"] = False
                        sym_data[f"pullbackDown_times_{inter}"] = "[]"

                    # find pullback ups
                    is_PullbackUp = indicator[(indicator[f'pullback_up_{inter}']==True)]
                    is_PullbackUp = is_PullbackUp[(is_PullbackUp['Open time'] >= how_much_ago)]
                    is_PullbackUp['Open time'] = is_PullbackUp['Open time'].astype(str)
                    if not is_PullbackUp.empty:
                        sym_data[f"is_PullbackUp_{inter}"] = True
                        sym_data[f"pullbackUp_times_{inter}"] = str(is_PullbackUp['Open time'].values.tolist())
                    else:
                        sym_data[f"is_PullbackUp_{inter}"] = False
                        sym_data[f"pullbackUp_times_{inter}"] = "[]"
                    #endregion

                    #region reversals
                    # find reversal downs
                    is_ReversalDown = indicator[(indicator[f'reversal_down_{inter}']==True)]
                    is_ReversalDown = is_ReversalDown[(is_ReversalDown['Open time'] >= how_much_ago)]
                    is_ReversalDown['Open time'] = is_ReversalDown['Open time'].astype(str)
                    if not is_ReversalDown.empty:
                        sym_data[f"is_ReversalDown_{inter}"] = True
                        sym_data[f"reversalDown_times_{inter}"] = str(is_ReversalDown['Open time'].values.tolist())
                    else:
                        sym_data[f"is_ReversalDown_{inter}"] = False
                        sym_data[f"reversalDown_times_{inter}"] = "[]"

                    # find reversal ups
                    is_ReversalUp = indicator[(indicator[f'reversal_up_{inter}']==True)]
                    is_ReversalUp = is_ReversalUp[(is_ReversalUp['Open time'] >= how_much_ago)]
                    is_ReversalUp['Open time'] = is_ReversalUp['Open time'].astype(str)
                    if not is_ReversalUp.empty:
                        sym_data[f"is_ReversalUp_{inter}"] = True
                        sym_data[f"reversalUp_times_{inter}"] = str(is_ReversalUp['Open time'].values.tolist())
                    else:
                        sym_data[f"is_ReversalUp_{inter}"] = False
                        sym_data[f"reversalUp_times_{inter}"] = "[]"
                    #endregion

                    #region stoch rsi
                    # find rsi low
                    is_stochrsi_low = indicator[(indicator[f'stochastic_rsi_k_{inter}'] < self.rsi_low)]
                    is_stochrsi_low = is_stochrsi_low[(is_stochrsi_low['Open time'] >= how_much_ago)]
                    is_stochrsi_low['Open time'] = is_stochrsi_low['Open time'].astype(str)
                    if not is_stochrsi_low.empty:
                        sym_data[f"is_stochrsi_low_{inter}"] = True
                        sym_data[f"stochrsi_low_times_{inter}"] = str(is_stochrsi_low['Open time'].values.tolist())
                    else:
                        sym_data[f"is_stochrsi_low_{inter}"] = False
                        sym_data[f"stochrsi_low_times_{inter}"] = "[]"

                    # find rsi Zero
                    is_stochrsi_zero = indicator[(indicator[f'stochastic_rsi_k_{inter}'] < 1)]
                    is_stochrsi_zero = is_stochrsi_zero[(is_stochrsi_zero['Open time'] >= how_much_ago)]
                    is_stochrsi_zero['Open time'] = is_stochrsi_zero['Open time'].astype(str)
                    if not is_stochrsi_zero.empty:
                        sym_data[f"is_stochrsi_zero_{inter}"] = True
                        sym_data[f"stochrsi_zero_times_{inter}"] = str(is_stochrsi_zero['Open time'].values.tolist())
                    else:
                        sym_data[f"is_stochrsi_zero_{inter}"] = False
                        sym_data[f"stochrsi_zero_times_{inter}"] = "[]"

                    # find rsi Uptrend
                    is_stochrsi_uptrend = indicator[(indicator[f'trend_stochastic_rsi_k_{inter}'] == "Uptrend")]
                    is_stochrsi_uptrend = is_stochrsi_uptrend[(is_stochrsi_uptrend['Open time'] >= how_much_ago)]
                    is_stochrsi_uptrend['Open time'] = is_stochrsi_uptrend['Open time'].astype(str)
                    if not is_stochrsi_uptrend.empty:
                        sym_data[f"is_stochrsi_uptrend_{inter}"] = True
                        sym_data[f"stochrsi_uptrend_times_{inter}"] = str(is_stochrsi_uptrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_stochrsi_uptrend_{inter}"] = False
                        sym_data[f"stochrsi_uptrend_times_{inter}"] = "[]"

                    # find rsi downtrend
                    is_stochrsi_downtrend = indicator[(indicator[f'trend_stochastic_rsi_k_{inter}'] == "Downtrend")]
                    is_stochrsi_downtrend = is_stochrsi_downtrend[(is_stochrsi_downtrend['Open time'] >= how_much_ago)]
                    is_stochrsi_downtrend['Open time'] = is_stochrsi_downtrend['Open time'].astype(str)
                    if not is_stochrsi_downtrend.empty:
                        sym_data[f"is_stochrsi_downtrend_{inter}"] = True
                        sym_data[f"stochrsi_downtrend_times_{inter}"] = str(is_stochrsi_downtrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_stochrsi_downtrend_{inter}"] = False
                        sym_data[f"stochrsi_downtrend_times_{inter}"] = "[]"
                    #endregion

                    #region candle score
                    # find bullish
                    is_candle_bullish = indicator[(indicator[f'candle_score_{inter}'] >= 0)]
                    is_candle_bullish = is_candle_bullish[(is_candle_bullish['Open time'] >= how_much_ago)]
                    is_candle_bullish['Open time'] = is_candle_bullish['Open time'].astype(str)
                    if not is_candle_bullish.empty:
                        sym_data[f"is_candle_bullish_{inter}"] = True
                        sym_data[f"candle_bullish_times_{inter}"] = str(is_candle_bullish['Open time'].values.tolist())
                    else:
                        sym_data[f"is_candle_bullish_{inter}"] = False
                        sym_data[f"candle_bullish_times_{inter}"] = "[]"

                    # find bearish
                    is_candle_bearish = indicator[(indicator[f'candle_score_{inter}'] < 0)]
                    is_candle_bearish = is_candle_bearish[(is_candle_bearish['Open time'] >= how_much_ago)]
                    is_candle_bearish['Open time'] = is_candle_bearish['Open time'].astype(str)
                    if not is_candle_bearish.empty:
                        sym_data[f"is_candle_bearish_{inter}"] = True
                        sym_data[f"candle_bearish_times_{inter}"] = str(is_candle_bearish['Open time'].values.tolist())
                    else:
                        sym_data[f"is_candle_bearish_{inter}"] = False
                        sym_data[f"candle_bearish_times_{inter}"] = "[]"
                    #endregion

                    #region macd
                    # find macd positive
                    is_macd_positive = indicator[(indicator[f'macd_{inter}'] > 0)]
                    is_macd_positive = is_macd_positive[(is_macd_positive['Open time'] >= how_much_ago)]
                    is_macd_positive['Open time'] = is_macd_positive['Open time'].astype(str)
                    if not is_macd_positive.empty:
                        sym_data[f"is_macd_positive_{inter}"] = True
                        sym_data[f"macd_positive_times_{inter}"] = str(is_macd_positive['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_positive_{inter}"] = False
                        sym_data[f"macd_positive_times_{inter}"] = "[]"

                    # find macd negatives
                    is_macd_negative = indicator[(indicator[f'macd_{inter}'] < 0)]
                    is_macd_negative = is_macd_negative[(is_macd_negative['Open time'] >= how_much_ago)]
                    is_macd_negative['Open time'] = is_macd_negative['Open time'].astype(str)
                    if not is_macd_negative.empty:
                        sym_data[f"is_macd_negative_{inter}"] = True
                        sym_data[f"macd_negative_times_{inter}"] = str(is_macd_negative['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_negative_{inter}"] = False
                        sym_data[f"macd_negative_times_{inter}"] = "[]"

                    # find macd uptrend
                    is_macd_uptrend = indicator[(indicator[f'trend_macd_{inter}'] == "Uptrend")]
                    is_macd_uptrend = is_macd_uptrend[(is_macd_uptrend['Open time'] >= how_much_ago)]
                    is_macd_uptrend['Open time'] = is_macd_uptrend['Open time'].astype(str)
                    if not is_macd_uptrend.empty:
                        sym_data[f"is_macd_uptrend_{inter}"] = True
                        sym_data[f"macd_uptrend_times_{inter}"] = str(is_macd_uptrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_uptrend_{inter}"] = False
                        sym_data[f"macd_uptrend_times_{inter}"] = "[]"

                    # find macd downtrend
                    is_macd_downtrend = indicator[(indicator[f'trend_macd_{inter}'] == "Downtrend")]
                    is_macd_downtrend = is_macd_downtrend[(is_macd_downtrend['Open time'] >= how_much_ago)]
                    is_macd_downtrend['Open time'] = is_macd_downtrend['Open time'].astype(str)
                    if not is_macd_downtrend.empty:
                        sym_data[f"is_macd_downtrend_{inter}"] = True
                        sym_data[f"macd_downtrend_times_{inter}"] = str(is_macd_downtrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_downtrend_{inter}"] = False
                        sym_data[f"macd_downtrend_times_{inter}"] = "[]"
                    
                    # find signal uptrend
                    is_signal_uptrend = indicator[(indicator[f'trend_signal_line_{inter}'] == "Uptrend")]
                    is_signal_uptrend = is_signal_uptrend[(is_signal_uptrend['Open time'] >= how_much_ago)]
                    is_signal_uptrend['Open time'] = is_signal_uptrend['Open time'].astype(str)
                    if not is_signal_uptrend.empty:
                        sym_data[f"is_signal_uptrend_{inter}"] = True
                        sym_data[f"signal_uptrend_times_{inter}"] = str(is_signal_uptrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_signal_uptrend_{inter}"] = False
                        sym_data[f"signal_uptrend_times_{inter}"] = "[]"

                    # find signal downtrend
                    is_signal_downtrend = indicator[(indicator[f'trend_signal_line_{inter}'] == "Downtrend")]
                    is_signal_downtrend = is_signal_downtrend[(is_signal_downtrend['Open time'] >= how_much_ago)]
                    is_signal_downtrend['Open time'] = is_signal_downtrend['Open time'].astype(str)
                    if not is_signal_downtrend.empty:
                        sym_data[f"is_signal_downtrend_{inter}"] = True
                        sym_data[f"signal_downtrend_times_{inter}"] = str(is_signal_downtrend['Open time'].values.tolist())
                    else:
                        sym_data[f"is_signal_downtrend_{inter}"] = False
                        sym_data[f"signal_downtrend_times_{inter}"] = "[]"

                    # find whether macd on top
                    is_macd_on_top = indicator[(indicator[f'macd_signal_change_direction_{inter}'] > 0)]
                    is_macd_on_top = is_macd_on_top[(is_macd_on_top['Open time'] >= how_much_ago)]
                    is_macd_on_top['Open time'] = is_macd_on_top['Open time'].astype(str)
                    if not is_macd_on_top.empty:
                        sym_data[f"is_macd_on_top_{inter}"] = True
                        sym_data[f"macd_on_top_times_{inter}"] = str(is_macd_on_top['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_on_top_{inter}"] = False
                        sym_data[f"macd_on_top_times_{inter}"] = "[]"

                    # find whether signal on top
                    is_signal_on_top = indicator[(indicator[f'macd_signal_change_direction_{inter}'] < 0)]
                    is_signal_on_top = is_signal_on_top[(is_signal_on_top['Open time'] >= how_much_ago)]
                    is_signal_on_top['Open time'] = is_signal_on_top['Open time'].astype(str)
                    if not is_signal_on_top.empty:
                        sym_data[f"is_signal_on_top_{inter}"] = True
                        sym_data[f"signal_on_top_times_{inter}"] = str(is_signal_on_top['Open time'].values.tolist())
                    else:
                        sym_data[f"is_signal_on_top_{inter}"] = False
                        sym_data[f"signal_on_top_times_{inter}"] = "[]"

                    # find macd percentage macd uptrending
                    is_macd_signal_change_increase = indicator[(indicator[f'trend_macd_signal_change_intensity_{inter}'] == "Uptrend")]
                    is_macd_signal_change_increase = is_macd_signal_change_increase[(is_macd_signal_change_increase['Open time'] >= how_much_ago)]
                    is_macd_signal_change_increase['Open time'] = is_macd_signal_change_increase['Open time'].astype(str)
                    if not is_macd_signal_change_increase.empty:
                        sym_data[f"is_macd_signal_change_increase_{inter}"] = True
                        sym_data[f"macd_signal_change_increase_times_{inter}"] = str(is_macd_signal_change_increase['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_signal_change_increase_{inter}"] = False
                        sym_data[f"macd_signal_change_increase_times_{inter}"] = "[]"

                    # find macd percentage macd uptrending
                    is_macd_signal_change_decrease = indicator[(indicator[f'trend_macd_signal_change_intensity_{inter}'] == "Downtrend")]
                    is_macd_signal_change_decrease = is_macd_signal_change_decrease[(is_macd_signal_change_decrease['Open time'] >= how_much_ago)]
                    is_macd_signal_change_decrease['Open time'] = is_macd_signal_change_decrease['Open time'].astype(str)
                    if not is_macd_signal_change_decrease.empty:
                        sym_data[f"is_macd_signal_change_decrease_{inter}"] = True
                        sym_data[f"macd_signal_change_decrease_times_{inter}"] = str(is_macd_signal_change_decrease['Open time'].values.tolist())
                    else:
                        sym_data[f"is_macd_signal_change_decrease_{inter}"] = False
                        sym_data[f"macd_signal_change_decrease_times_{inter}"] = "[]"
                    #endregion
                        
                    sym_data[f'latest_volume_{inter}'] = latest_sample[f'Volume_{inter}']
                    sym_data[f'latest_rsi_{inter}'] = latest_sample[f'rsi_{inter}']
                    sym_data[f'latest_stoch_rsi_{inter}'] = latest_sample[f'stochastic_rsi_k_{inter}']
                    sym_data[f'latest_candle_score_{inter}'] = latest_sample[f'candle_score_{inter}']
                    sym_data[f'latest_macd_signal_change_{inter}'] = latest_sample[f'macd_signal_change_direction_{inter}']
                    sym_data[f'latest_macd_signal_intensity_{inter}'] = latest_sample[f'macd_signal_change_intensity_{inter}']
                    sym_data[f'latest_macd_{inter}'] = latest_sample[f'macd_{inter}']

                # store indicators
                if self.indicator_info.empty:
                    self.indicator_info = pd.DataFrame([sym_data])
                elif sym["symbol"] in self.indicator_info['symbol'].values.tolist():
                    self.indicator_info = self.indicator_info.drop(self.indicator_info[self.indicator_info["symbol"] == sym["symbol"]].index)
                    self.indicator_info = pd.concat([self.indicator_info, pd.DataFrame([sym_data])], ignore_index=True)
                else:
                    self.indicator_info = pd.concat([self.indicator_info, pd.DataFrame([sym_data])], ignore_index=True)

                # save the df after every minute
                if datetime.now().timestamp() - store_df_time > 60:
                    logging.info(f"{datetime.now().isoformat()} >>> Storing promising symbols...")
                    self.indicator_info = self.indicator_info.sort_values("symbol", ascending=True)
                    self.indicator_info.to_csv("df_indicator_promising.csv", index=False)
                    store_df_time = datetime.now().timestamp()
            except Exception as e:
                logging.error(e, exc_info=True)
                sym_data["is_data"] = False
                self.indicator_info = pd.concat([self.indicator_info, pd.DataFrame([sym_data])], ignore_index=True)

class binance_data(threading.Thread):
    def __init__(self,
                app,
                binance_api_obj,
                symbol,
                base_asset,
                quote_asset,
                default_configs,
                delay=0,
                max_duration_days=max_duration_days):
        
        super().__init__()

        self.app = app
        self.binance_api = binance_api_obj
        self.client = self.binance_api.client
        self.symbol = symbol
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.kline_configs = default_configs
        self.delay = delay
        self.max_duration_days = max_duration_days

        self.informing_time = datetime.now().timestamp()-300 # used to delay thread functions
        self.informing_delay = 300

        self.keep_columns = ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]

        self.running = True
        self.current_price = None
        self.timestamp = None
        self.rsi_supports = []
        self.indicator = pd.DataFrame()
        self.combined_indicators = pd.DataFrame()
        self.refresh_time = datetime.now().timestamp() # used to delay thread functions
        self.wait_time = 0
        self.time_capture = {}

    def run(self):
        while self.running:
            # self.time_capture['get_current_price'] = self.time_count(self.get_current_price)
            self.time_capture['get_multiple_klines_data'] = self.time_count(self.get_multiple_klines_data)
            self.time_capture['generate_indicator'] = self.time_count(self.generate_indicator)
            self.time_capture['total'] = sum([self.time_capture[i] for i in self.time_capture if i!="total"])

            while self.running and datetime.now().timestamp()-self.refresh_time < self.delay:
                pass
            self.refresh_time = datetime.now().timestamp()

    def time_count(self, fc):
        start_time = datetime.now().timestamp()
        fc()
        return datetime.now().timestamp() - start_time

    def stop(self):
        self.running = False

    def get_current_price(self, url="/v3/ticker/price", delay=0):
        try:
            api_response = requests.get(self.client.API_URL+url+f"?symbol={self.symbol}")
            json_response = api_response.json()

            self.current_price = json_response['price']

            if delay>0:
                time.sleep(delay)

            return json_response
        except Exception as e:
            logging.error(e, exc_info=True)
            return None
    
    #region Acquire Data and Generate Indicators
    def get_multiple_klines_data(self):
        for interval in self.kline_configs:
            self.get_klines_data(
                                    startTime=self.kline_configs[interval]['startTime'], 
                                    endTime=self.kline_configs[interval]['endTime'], 
                                    interval=self.kline_configs[interval]['interval'], 
                                    configs = self.kline_configs[interval]
                                )

    def get_trend(self, df):

        def analyze_trend(numbers):
            # Generate a sequence of indices to pair with the numbers

            indices = list(range(len(numbers)))

            # Perform linear regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(indices, numbers)

            # Analyze the slope for trend
            if slope > 0:
                return "Downtrend"
            elif slope < 0:
                return "Uptrend"
            else:
                return "No clear trend"
        
        def apply_trend_analysis(df, column_name, window=5, offset=0):
            trends = []
            for i in range(len(df)):
                # Ensure we have at least 5 subsequent values
                if i+offset+window <= len(df):
                    subset = df[column_name].iloc[i+offset:i+offset+window]
                    trend = analyze_trend(subset)
                    trends.append(trend)
                else:
                    trends.append("Insufficient data")
            return trends
        
        
        df = df.sort_values('Open time', ascending=False)
        exclude_features = ['1_candle_pattern', '2_candle_pattern', 'macd_signal_change_direction', 'candle_score', 'middle_bolinger_band', 'upper_bolinger_band', 'lower_bolinger_band', 'breakout_up', 'breakout_down', 'reversal_up', 'reversal_down', 'pullback_up', 'pullback_down', 'Volume']
        for c in df.columns:
            if c!="Open time":
                interval = c.split("_")[-1]
                feature = "_".join(c.split("_")[0:-1])
                if feature in exclude_features:
                    continue

                window_offsets = [3, 0]

                window = window_offsets[0] #* self.kline_configs[interval]["indicator_multiplier"]
                offset = window_offsets[1] #* self.kline_configs[interval]["indicator_multiplier"]
                df[f'trend_{feature}_{interval}'] = apply_trend_analysis(df, c, window=window, offset=offset)
        df = df.sort_values('Open time', ascending=True)

        return df
    
    def get_klines_data(self, url="/v3/klines",  startTime="1 hour ago", endTime="now", interval="1m", delay=0, configs={}):
        try:
            parameters = {
                "symbol": self.symbol,
                "interval": interval,
                "startTime": convert_ts_str(startTime),
                "endTime": convert_ts_str(endTime)
            }

            api_response = requests.get(prepare_url(url, parameters, self.client.API_URL), headers={"X-MBX-APIKEY": self.binance_api.api_key})
            json_response = api_response.json()
            
            df_klines = pd.DataFrame(json_response, columns=["Open time", "Open", "High", "Low", "Close", "Volume", "Close time", "Quote asset volume", "Number of trades", "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"])
            df_klines = df_klines[self.keep_columns] # filter columns

            if df_klines.empty:
                logging.error(f"{datetime.now().isoformat()} >>> No Data in JSON Response with params: {parameters}")
                return 

            if not configs['get_volume']:
                df_klines = df_klines[[i for i in df_klines.columns if 'volume' not in i.lower()]]

            df_klines = df_klines.astype(float)

            # process time features
            df_klines['Open time'] = df_klines['Open time'].apply(lambda x:datetime.fromtimestamp(float(x) / 1e3))
            df_klines['Open time'] = pd.to_datetime(df_klines['Open time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_klines['Close time'] = df_klines['Close time'].apply(lambda x:datetime.fromtimestamp(float(x) / 1e3))
            df_klines['Close time'] = pd.to_datetime(df_klines['Close time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_klines = df_klines.sort_values("Open time", ascending=True)

            # store dataframe
            self.kline_configs[interval]['df_kline'] = df_klines

            if configs['get_volume']:
                self.kline_configs[interval]['df_volume'] = df_klines[['Open time', 'Volume']]
                self.kline_configs[interval]['df_volume'].rename(columns={'Volume': f'Volume_{interval}'}, inplace=True)
                self.kline_configs[interval]['df_volume'] = self.get_trend(self.kline_configs[interval]['df_volume'])

            if configs['get_stochastic']:
                df_stochs = self.get_stochastic(df_klines, interval=interval)
                self.kline_configs[interval]['df_stochs'] = df_stochs[['Open time', f'stochastic_k_{interval}']]
                self.kline_configs[interval]['df_stochs'] = self.get_trend(self.kline_configs[interval]['df_stochs'])

            if configs['get_rsi']:
                df_rsi = self.get_rsi(df_klines, interval=interval)
                self.kline_configs[interval]['df_rsi'] = df_rsi[['Open time', 
                                                                f'rsi_{interval}', 
                                                                f'stochastic_rsi_k_{interval}'
                                                                ]]
                self.kline_configs[interval]['df_rsi'] = self.get_trend(self.kline_configs[interval]['df_rsi'])

            if configs['get_bollinger']:
                df_bollinger = self.get_bollinger(df_klines, interval=interval)
                self.kline_configs[interval]['df_bollinger'] = df_bollinger[['Open time', f'middle_bolinger_band_{interval}',
                                                                            f'upper_bolinger_band_{interval}', f'lower_bolinger_band_{interval}',
                                                                            f'breakout_up_{interval}', f'breakout_down_{interval}',
                                                                            f'reversal_up_{interval}', f'reversal_down_{interval}',
                                                                            f'pullback_up_{interval}', f'pullback_down_{interval}'
                                                                            ]]
                self.kline_configs[interval]['df_bollinger'] = self.get_trend(self.kline_configs[interval]['df_bollinger'])

            if configs['get_macd']:
                df_macd = self.get_macd(df_klines, interval=interval)
                self.kline_configs[interval]['df_macd'] = df_macd[['Open time', f'macd_{interval}',
                                                                            f'signal_line_{interval}', f'macd_signal_change_intensity_{interval}',
                                                                            f'macd_signal_change_direction_{interval}'
                                                                            ]]
                self.kline_configs[interval]['df_macd'] = self.get_trend(self.kline_configs[interval]['df_macd'])
            
            if configs['get_score']:
                df_score = df_klines.copy()
                df_score[f'1_candle_pattern_{interval}'] = df_score.apply(lambda row: self.one_candle_pattern(row), axis=1)

                past_1 = df_score.shift(1)
                df_score[f'2_candle_pattern_{interval}'] = df_score.apply(lambda row: self.two_candle_pattern(past_1.loc[row.name], row), axis=1)
                
                # past_2 = df_score.shift(2)
                # df_score[f'3_candle_pattern_{interval}'] = df_score.apply(lambda row: self.three_plus_candle_pattern(past_2.loc[row.name], past_1.loc[row.name], row), axis=1)
                
                df_score = df_score[['Open time', 
                                    f'1_candle_pattern_{interval}',
                                    f'2_candle_pattern_{interval}'
                                    ]]

                self.kline_configs[interval]['df_score'] = self.get_score(df_score, interval=interval)    
                self.kline_configs[interval]['df_score'] = self.get_trend(self.kline_configs[interval]['df_score'])

            if delay > 0:
                time.sleep(delay)

            return json_response
        except Exception as e:
            logging.error(e, exc_info=True)
            return None

    def get_stochastic(self, df, lengthK=14, k_period=1, d_period=3, interval='1h'):
        """
        Calculate the Stochastic Oscillator for a DataFrame.
        :param df: DataFrame with 'High', 'Low', and 'Close' columns.
        :param n: Number of periods to consider (typically 14).
        :return: DataFrame with Stochastic Oscillator values.
        """

        lowestlow = df[f'Low'].rolling(window=lengthK, center=False).min()
        highesthigh = df[f'High'].rolling(window=lengthK, center=False).max()
        df[f'stochastic_k_{interval}'] = 100 * ((df[f'rsi_{interval}'] - lowestlow) / (highesthigh - lowestlow)).rolling(window=k_period).mean()
        df[f'stochastic_k_{interval}'] = df[f'stochastic_k_{interval}'].rolling(window=d_period).mean()

        return df

    def get_rsi(self, df, lengthrsi=14, lengthStoch=14, k_period=3, d_period=3, interval='1h'):
        
        # Calculate the rsi
        df[f'rsi_{interval}'] = talib.RSI(df['Close'], timeperiod=lengthrsi)

        lowestlow = df[f'rsi_{interval}'].rolling(window=lengthStoch, center=False).min()
        highesthigh = df[f'rsi_{interval}'].rolling(window=lengthStoch, center=False).max()
        stoch_rsi_k = 100 * ((df[f'rsi_{interval}'] - lowestlow) / (highesthigh - lowestlow)).rolling(window=k_period).mean()
        stoch_rsi_d = stoch_rsi_k.rolling(window=d_period).mean()

        # Adding columns to the dataframe
        df[f'stochastic_rsi_k_{interval}'] = stoch_rsi_k
        df[f'stochastic_rsi_d_{interval}'] = stoch_rsi_d

        return df

    def get_bollinger(self, df, interval='1h', period=20, std_dev=2):
        """
        Calculate Bollinger Bands for a given DataFrame.
        
        Parameters:
            df (pd.DataFrame): DataFrame containing OHLCV data with columns ["Open time", "Open", "High", "Low", "Close"].
            interval (str): Interval for resampling data if necessary.
            period (int): Period for calculating the moving average and standard deviation.
            std_dev (int): Standard deviation multiplier for the bands.
        
        Returns:
            pd.DataFrame: Original DataFrame with added columns for the middle band, upper band, and lower band.
        """
        
        # Calculate the middle band (SMA)
        df[f'middle_bolinger_band_{interval}'] = df['Close'].rolling(window=period, min_periods=1).mean()
        
        # Calculate the standard deviation
        std_dev_df = df['Close'].rolling(window=period, min_periods=1).std()
        
        # Calculate upper and lower Bollinger Bands
        df[f'upper_bolinger_band_{interval}'] = df[f'middle_bolinger_band_{interval}'] + (std_dev_df * std_dev)
        df[f'lower_bolinger_band_{interval}'] = df[f'middle_bolinger_band_{interval}'] - (std_dev_df * std_dev)

        # Breakout above the upper band
        df[f'breakout_up_{interval}'] = df['Close'] > df[f'upper_bolinger_band_{interval}']
        
        # Breakout below the lower band
        df[f'breakout_down_{interval}'] = df['Close'] < df[f'lower_bolinger_band_{interval}']
        
        # Potential reversal from the upper band to the downside
        df[f'reversal_down_{interval}'] = (df['Close'].shift(1) > df[f'upper_bolinger_band_{interval}']) & (df['Close'] < df[f'middle_bolinger_band_{interval}'])
        
        # Potential reversal from the lower band to the upside
        df[f'reversal_up_{interval}'] = (df['Close'].shift(1) < df[f'lower_bolinger_band_{interval}']) & (df['Close'] > df[f'middle_bolinger_band_{interval}'])

        # Pullback after breakout above upper band (price pulls back towards middle band)
        df[f'pullback_down_{interval}'] = (df[f'breakout_up_{interval}'].shift(1)) & (df['Close'] < df[f'upper_bolinger_band_{interval}'])
        
        # Pullback after breakout below lower band (price pulls back towards middle band)
        df[f'pullback_up_{interval}'] = (df[f'breakout_down_{interval}'].shift(1)) & (df['Close'] > df[f'lower_bolinger_band_{interval}'])
    
        return df
    
    def get_macd(self, df, short_period=12, long_period=26, signal_period=9, interval='1h'):
        """
        Function to calculate the macd, Signal Line, and macd Histogram.
        
        Parameters:
        df (pd.DataFrame): DataFrame containing the 'Close' price data.
        short_period (int): Period for the short-term EMA (default 12).
        long_period (int): Period for the long-term EMA (default 26).
        signal_period (int): Period for the signal line EMA (default 9).
        
        Returns:
        pd.DataFrame: The original DataFrame with added columns for macd, Signal Line, and macd Histogram.
        """
        # Calculate short-term and long-term EMAs
        short_ema = df['Close'].ewm(span=short_period, adjust=False).mean()
        long_ema = df['Close'].ewm(span=long_period, adjust=False).mean()
        
        # Calculate macd line
        df[f'macd_{interval}'] = short_ema - long_ema
        
        # Calculate Signal line (9-period EMA of macd)
        df[f'signal_line_{interval}'] = df[f'macd_{interval}'].ewm(span=signal_period, adjust=False).mean()
        
        # Calculate macd Histogram (macd - Signal line)
        # df[f'macd_Histogram_{interval}'] = df[f'macd_{interval}'] - df[f'signal_line_{interval}']

        # df[f'macd_signal_change_direction_{interval}'] = ((df[f'macd_{interval}'] - df[f'signal_line_{interval}']) / ( abs(df[f'macd_{interval}']) + abs(df[f'signal_line_{interval}']) )) * 100
        df[f'macd_signal_change_direction_{interval}'] = 100 * (df[f'macd_{interval}'] - df[f'signal_line_{interval}']) / abs(df[f'signal_line_{interval}']) 
        df[f'macd_signal_change_intensity_{interval}'] = abs(df[f'macd_signal_change_direction_{interval}'])
        return df

    def get_score(self, df, interval='1h'):
        try:
            df_orig = df.copy()
            df = df.fillna(np.nan)
            df = df.replace("No Pattern", 0)
            df = df.replace(neutral, 0)
            df = df.replace(bullish_trends, 1)
            df = df.replace(bearish_trends, -1)

            df_orig[f'candle_score_{interval}'] = df.apply(lambda x: sum(x[i]/int(i.split("_candle_pattern")[0]) for i in x.keys() if "candle_" in i), axis=1)
        except Exception as e:
            logging.error(e, exc_info=True)

        return df_orig
    
    #endregion

    def generate_indicator(self, delay=0):

        def analyze_trend(numbers):
            # Generate a sequence of indices to pair with the numbers

            indices = list(range(len(numbers)))

            # Perform linear regression
            slope, intercept, r_value, p_value, std_err = stats.linregress(indices, numbers)

            # Analyze the slope for trend
            if slope > 0:
                return "Downtrend"
            elif slope < 0:
                return "Uptrend"
            else:
                return "No clear trend"
        
        def apply_trend_analysis(df, column_name, window=5, offset=0):
            trends = []
            for i in range(len(df)):
                # Ensure we have at least 5 subsequent values
                if i+offset+window <= len(df):
                    subset = df[column_name].iloc[i+offset:i+offset+window]
                    trend = analyze_trend(subset)
                    trends.append(trend)
                else:
                    trends.append("Insufficient data")
            return trends
        
        try:
            #~~~~~~~~~ Merge to combine stocks columns and fill NaNs
            dataframes = []
            for key, value in self.kline_configs.items():
                df = value['df_volume'].copy()
                if not df.empty:
                    dataframes.append(df)

                df = value['df_stochs'].copy()
                if not df.empty:
                    dataframes.append(df)

                df = value['df_rsi'].copy()
                if not df.empty:
                    dataframes.append(df)
                
                df = value['df_bollinger'].copy()
                if not df.empty:
                    dataframes.append(df)
                
                df = value['df_macd'].copy()
                if not df.empty:
                    dataframes.append(df)
                
                df = value['df_score'].copy()
                if not df.empty:
                    dataframes.append(df)

            if dataframes==[]:
                return None
            
            # prepare max data days but hourly
            now = datetime.now().replace(minute=0, second=0, microsecond=0)
            days_ago = now - timedelta(days=self.max_duration_days)
            time_range = pd.date_range(start=days_ago, end=now, freq='H')
            df_time = pd.DataFrame(time_range, columns=['Open time'])
            df_time = df_time.sort_values('Open time', ascending=False)
            df_time['Open time'] = df_time['Open time'].astype(str)
            dataframes.insert(0, df_time)

            self.combined_indicators = reduce(lambda left, right: pd.merge(left, right, on='Open time', how='left'), dataframes)
            self.combined_indicators = self.combined_indicators.ffill().bfill()

            if delay>0:
                time.sleep(delay)
        except Exception as e:
            logging.error(e, exc_info=True)

    #region score patterns
    def one_candle_pattern(self, candle):
        
        open_price, high_price, low_price, close_price = candle['Open'], candle['High'], candle['Low'], candle['Close']

        body = abs(close_price - open_price)
        upper_shadow = high_price - max(open_price, close_price)
        lower_shadow = min(open_price, close_price) - low_price
        total_length = high_price - low_price

        # Bullish or Bearish
        bullish = close_price > open_price
        bearish = close_price < open_price

        # Define thresholds
        small_body_threshold = total_length * 0.1 # Decimal(0.1)  # small body is less than 10% of the total length
        long_shadow_threshold = total_length * 0.7 # Decimal(0.7)  # long shadow is more than 70% of the total length

        # Hammer: Small body at the upper end, long lower shadow
        if bullish and body <= small_body_threshold and lower_shadow >= long_shadow_threshold:
            return "Hammer"
        
        # Inverted Hammer: Small body at the lower end, long upper shadow
        elif bullish and body <= small_body_threshold and upper_shadow >= long_shadow_threshold:
            return "Inverted Hammer"

        # Dragonfly Doji: Very small body, long lower shadow, no upper shadow
        elif bullish and body <= small_body_threshold and upper_shadow <= small_body_threshold and lower_shadow >= long_shadow_threshold:
            return "Dragonfly Doji"

        # Bullish Spinning Top: Small body, upper and lower shadows are not significantly long
        elif bullish and body <= small_body_threshold and lower_shadow < long_shadow_threshold and upper_shadow < long_shadow_threshold:
            return "Bullish Spinning Top"

        # Hanging Man: Similar to Hammer but occurs in an uptrend
        elif bearish and body <= small_body_threshold and lower_shadow >= long_shadow_threshold:
            return "Hanging Man"

        # Shooting Star: Small body at the lower end, long upper shadow
        elif bearish and body <= small_body_threshold and upper_shadow >= long_shadow_threshold:
            return "Shooting Star"

        # Gravestone Doji: Very small body, long upper shadow, no lower shadow
        elif bearish and body <= small_body_threshold and lower_shadow <= small_body_threshold and upper_shadow >= long_shadow_threshold:
            return "Gravestone Doji"

        # Bearish Spinning Top: Small body, upper and lower shadows are not significantly long
        elif bearish and body <= small_body_threshold and lower_shadow < long_shadow_threshold and upper_shadow < long_shadow_threshold:
            return "Bearish Spinning Top"
        
        elif body <= small_body_threshold:
            return 'Doji'

        elif bullish:
            return 'Bullish'
        
        elif bearish:
            return 'Bearish'
        
        else:
            return "No Pattern"

    def two_candle_pattern(self, candle1=None, candle2=None):

        try:
            if candle1==None:
                return 'No Pattern'
        except:
            pass

        high_price1, low_price1 = candle1['High'], candle1['Low']
        high_price2, low_price2 = candle2['High'], candle2['Low']

        open_price1, close_price1 = candle1['Open'], candle1['Close']
        open_price2, close_price2 = candle2['Open'], candle2['Close']

        # Bullish or Bearish
        bullish1 = close_price1 > open_price1
        bullish2 = close_price2 > open_price2

        bearish1 = close_price1 < open_price1
        bearish2 = close_price2 < open_price2

        # Bullish Kicker: A bearish candle followed by a bullish candle with a gap
        if bearish1 and bullish2 and open_price2 > close_price1:
            return "Bullish Kicker"

        # Bullish Engulfing: A small bearish candle fully engulfed by a large bullish candle
        elif bearish1 and bullish2 and open_price2 < close_price1 and close_price2 > open_price1:
            return "Bullish Engulfing"

        # Bullish Harami: A large bearish candle followed by a small bullish candle
        elif bearish1 and bullish2 and open_price2 > close_price1 and close_price2 < open_price1:
            return "Bullish Harami"

        # Piercing Line: A bearish candle followed by a bullish candle, opening below the previous low and closing above the midpoint of the first candle
        elif bearish1 and bullish2 and open_price2 < low_price1 and close_price2 > (open_price1 + close_price1) / 2:
            return "Piercing Line"

        # Tweezer Bottom: Two adjacent candles with the same low point, where the first is bearish and the second is bullish
        elif bearish1 and bullish2 and candle1['Low'] == candle2['Low']:
            return "Tweezer Bottom"

        # Bearish Kicker: A bullish candle followed by a bearish candle with a gap
        elif bullish1 and bearish2 and open_price2 < close_price1:
            return "Bearish Kicker"

        # Bearish Engulfing: A small bullish candle fully engulfed by a large bearish candle
        elif bullish1 and bearish2 and open_price2 > close_price1 and close_price2 < open_price1:
            return "Bearish Engulfing"

        # Bearish Harami: A large bullish candle followed by a small bearish candle
        elif bullish1 and bearish2 and open_price2 < close_price1 and close_price2 > open_price1:
            return "Bearish Harami"

        # Dark Cloud Cover: A bullish candle followed by a bearish candle, opening above the previous high and closing below the midpoint of the first candle
        elif bullish1 and bearish2 and open_price2 > high_price1 and close_price2 < (open_price1 + close_price1) / 2:
            return "Dark Cloud Cover"

        # Tweezer Top: Two adjacent candles with the same high point, where the first is bullish and the second is bearish
        elif bullish1 and bearish2 and candle1['High'] == candle2['High']:
            return "Tweezer Top"

        else:
            return "No Pattern"

    def three_plus_candle_pattern(self, candle1, candle2, candle3):
        # Extract open, high, low, close for each candle
        op1, hp1, lp1, cp1 = candle1['Open'], candle1['High'], candle1['Low'], candle1['Close']
        op2, hp2, lp2, cp2 = candle2['Open'], candle2['High'], candle2['Low'], candle2['Close']
        op3, hp3, lp3, cp3 = candle3['Open'], candle3['High'], candle3['Low'], candle3['Close']

        # Define bullish or bearish for each candle
        bullish1, bearish1 = cp1 > op1, cp1 < op1
        bullish2, bearish2 = cp2 > op2, cp2 < op2
        bullish3, bearish3 = cp3 > op3, cp3 < op3

        # Morning Star
        if bearish1 and self.is_small_body(op2, hp2, lp2, cp2) and bullish3 and cp3 > cp1:
            return "Morning Star"
        
        # Morning Doji Star
        elif bearish1 and self.is_doji(op2, hp2, lp2, cp2) and bullish3 and cp3 > cp1:
            return "Morning Doji Star"

        # Bullish Abandoned Baby
        elif bearish1 and self.is_doji(op2, hp2, lp2, cp2) and bullish3 and lp2 > hp1 and op3 > cp2:
            return "Bullish Abandoned Baby"

        # Three White Soldiers
        elif bullish1 and bullish2 and bullish3 and cp1 > op1 and cp2 > op2 and cp3 > op3:
            return "Three White Soldiers"

        # Three Line Strike (Bullish)
        elif bullish1 and bullish2 and bullish3 and bearish3 and cp3 < op1:
            return "Three Line Strike (Bullish)"

        # Three Inside Up
        elif bearish1 and bullish2 and cp2 > op1 and bullish3 and cp3 > cp2:
            return "Three Inside Up"

        # Three Outside Up
        elif bearish1 and bullish2 and cp2 > cp1 and bullish3 and cp3 > cp2:
            return "Three Outside Up"

        # Evening Star
        elif bullish1 and self.is_small_body(op2, hp2, lp2, cp2) and bearish3 and cp3 < cp1:
            return "Evening Star"
        
        # Evening Doji Star
        elif bullish1 and self.is_doji(op2, hp2, lp2, cp2) and bearish3 and cp3 < cp1:
            return "Evening Doji Star"

        # Bearish Abandoned Baby
        elif bullish1 and self.is_doji(op2, hp2, lp2, cp2) and bearish3 and hp2 < lp1 and cp3 < op2:
            return "Bearish Abandoned Baby"

        # Three Black Crows
        elif bearish1 and bearish2 and bearish3 and cp1 < op1 and cp2 < op2 and cp3 < op3:
            return "Three Black Crows"

        # Three Line Strike (Bearish)
        elif bearish1 and bearish2 and bearish3 and bullish3 and cp3 > op1:
            return "Three Line Strike (Bearish)"

        # Three Inside Down
        elif bullish1 and bearish2 and cp2 < cp1 and bearish3 and cp3 < cp2:
            return "Three Inside Down"

        # Three Outside Down
        elif bullish1 and bearish2 and cp2 < cp1 and bearish3 and cp3 < cp2:
            return "Three Outside Down"

        else:
            return "No Pattern"
    
    def is_small_body(self, open_price, high_price, low_price, close_price):
    # Small body can be defined as a small difference between open and close
        
        body = abs(close_price - open_price)
        total_length = high_price - low_price

        small_body_threshold = total_length * 0.3 # Decimal(0.3)

        return body <= small_body_threshold

    def is_doji(self, open_price, high_price, low_price, close_price):
        
        body = abs(close_price - open_price)
        total_length = high_price - low_price

        small_body_threshold = total_length * 0.1 # Decimal(0.1)

        return body <= small_body_threshold
    #endregion
