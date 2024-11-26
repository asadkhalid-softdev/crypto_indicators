from flask import Flask, jsonify, make_response, request
import json
from datetime import datetime
import pandas as pd
import logging
import os
import pandas as pd
import ast
from data_class import kucoin_api, kucoin_fetch_promising
import logging
logging.basicConfig(filename=f'app.log', filemode='a', format='%(asctime)s >>>> %(process)d - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

current_directory = os.getcwd()
print(f"Current Directory: {current_directory}")

max_duration_days = 14

app = Flask(__name__)

def check_date_in_list(date_list, target_date_obj):
    """
    Whether target_date_obj is greater than any element in the list.
    """
    date_list = ast.literal_eval(date_list)
    return any(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") == target_date_obj for dt in date_list)

#region FLASK API
def get_indicator_cryptos(type="long-term", when_last=None, enable=['1week', '1day']):
    try:

        data = params['thread_fetchie'].indicator_info
                    
        if data.empty:
            raise Exception("No Data found.")

        data = data[data["is_data"]==True]

        if type=="long-term":

            if when_last!=None:
                target_date_obj = datetime.strptime(when_last, "%Y-%m-%d %H:%M:%S")
            else:
                target_date_obj = datetime.now()
                target_date_obj = target_date_obj.replace(hour=0, minute=0, second=0, microsecond=0)

            logging.info(f"Target date: {target_date_obj}")

            timeframe = '1week'
            if timeframe in enable:

                data = data[data[f'is_macd_histogram_pct_uptrend_{timeframe}']==True]
                data = data[data[f'macd_histogram_pct_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_uptrend_{timeframe}']==True]
                data = data[data[f'stochrsi_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_high_{timeframe}']==True]
                data = data[data[f'stochrsi_high_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
                
                logging.info(f"For interval {timeframe}, got {len(data)} symbols.")
            
            timeframe = '1day'
            if timeframe in enable:
                # data = data[data[f'latest_candle_diff_{timeframe}'] > 0]

                # data = data.sort_values(f'latest_stoch_rsi_{timeframe}', ascending=True)
                data = data.sort_values(f'latest_volume_{timeframe}', ascending=False)
                # data = data.sort_values(f'latest_candle_diff_{timeframe}', ascending=True)

                # data[f'latest_scoring_{timeframe}'] = data.apply(lambda x: x[f'latest_volume_{timeframe}']/x[f'latest_candle_diff_{timeframe}'], axis=1)
                # data = data.sort_values(f'latest_scoring_{timeframe}', ascending=False)

                data = data[data[f'is_candle_positive_{timeframe}']==True]
                data = data[data[f'candle_positive_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_uptrend_{timeframe}']==True]
                data = data[data[f'stochrsi_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_high_{timeframe}']==True]
                data = data[data[f'stochrsi_high_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_not_too_high_{timeframe}']==True]
                data = data[data[f'stochrsi_not_too_high_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                logging.info(f"For interval {timeframe}, got remaining {len(data)} symbols.")

            if when_last!=None:
                target_date_obj = datetime.strptime(when_last, "%Y-%m-%d %H:%M:%S")
            else:
                target_date_obj = datetime.now()
                target_date_obj = target_date_obj.replace(minute=0, second=0, microsecond=0)

            timeframe = '1hour'
            if timeframe in enable:
                # data = data[(data[f'latest_slope_stoch_rsi_{timeframe}'] < -1)] 

                data = data[data[f'is_stochrsi_uptrend_{timeframe}']==True]
                data = data[data[f'stochrsi_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                data = data[data[f'is_stochrsi_not_too_high_{timeframe}']==True]
                data = data[data[f'stochrsi_not_too_high_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                # data = data[data[f'is_stochrsi_uptrend_{timeframe}']==True]
                # data = data[data[f'stochrsi_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

                logging.info(f"For interval {timeframe}, got remaining {len(data)} symbols.")

        return data[['symbol']]
    except Exception as e:
        logging.error(e, exc_info=True)
        return pd.DataFrame()

@app.route('/start_fetcher', methods=['GET'])
def start_fetcher():
    """
    This function starts data analysis on all kucoin symbols.
    """
    try:
        if params['thread_fetchie_status']:
            response = jsonify({'status': params['thread_fetchie'].fetcher_status, 'message': "Already running."})
        else:
            params['thread_fetchie'] = kucoin_fetch_promising(app, kucoin_rest_api, default_configs, 600)
            params['thread_fetchie'].start()
            params['thread_fetchie_status'] = True
            response = jsonify({'status': params['thread_fetchie'].fetcher_status, 'message': "Started fetcher."})

        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)
    
@app.route('/stop_fetcher', methods=['GET'])
def stop_fetcher():
    """
    This function stops data analysis on all kucoin symbols.
    """
    try:
        if params['thread_fetchie_status']:
            params['thread_fetchie'].stop()
            params['thread_fetchie'].join()
            response = jsonify({'message': "Stopped fetcher."})
            params["thread_fetchie_status"] = False
        else:
            response = jsonify({'message': "Not running."})

        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)

@app.route('/get_magic', methods=['POST'])
def get_magic():
    try:
        type = request.args.get('type', 'long-term')
        timeframes_enable = request.args.get('timeframes_enable','1week,1day,4hour')
        timeframes_enable = timeframes_enable.split(',')
        when_last = request.args.get('when_last', None)

        data = get_indicator_cryptos(type=type, when_last=when_last, enable=timeframes_enable)

        data = data['symbol'].values.tolist()

        x_com_string = ""
        for i in data:
            x_com_string += f"${i.split('-')[0]} OR "
        x_com_string = f"{x_com_string[:-4]} min_replies:5 min_retweets:10"

        response = jsonify({'status': params['thread_fetchie'].fetcher_status,'n_symbols': len(data),'list': data, 'x_com_string':x_com_string})
        return make_response(response, 200)
    
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'status': params['thread_fetchie'].fetcher_status, 'message': str(e)})
        return make_response(response, 400)

if __name__ == '__main__':
    try:
        # initialize kucoin api
        keys = json.load(open(f"{current_directory}/keys.json","rb"))
        api_type = 'live'
        kucoin_rest_api = kucoin_api(api_type, keys[api_type])

        # define configuration which will be used to fetch data and generate indicators
        default_configs = {
            "1hour":{
                "interval": "1hour",
                "startTime": "3 days ago",
                "endTime": "now",
                "df_kline": pd.DataFrame(),

                "get_rsi": True,
                "df_rsi": pd.DataFrame(),

                "get_score": False,
                "df_score": pd.DataFrame(),

                "get_volume": True,
                "df_volume": pd.DataFrame(),

                "get_stochastic": False,
                "df_stochs": pd.DataFrame(),

                "get_bollinger": False,
                "df_bollinger": pd.DataFrame(),

                "get_macd": True,
                "df_macd": pd.DataFrame()
            },
            "1day":{
                "interval": "1day",
                "startTime": "60 days ago",
                "endTime": "now",
                "df_kline": pd.DataFrame(),

                "get_rsi": True,
                "df_rsi": pd.DataFrame(),

                "get_score": False,
                "df_score": pd.DataFrame(),

                "get_volume": True,
                "df_volume": pd.DataFrame(),

                "get_stochastic": False,
                "df_stochs": pd.DataFrame(),

                "get_bollinger": False,
                "df_bollinger": pd.DataFrame(),

                "get_macd": True,
                "df_macd": pd.DataFrame()
            },
            "1week":{
                "interval": "1week",
                "startTime": "1 year ago",
                "endTime": "now",
                "df_kline": pd.DataFrame(),

                "get_rsi": True,
                "df_rsi": pd.DataFrame(),

                "get_score": False,
                "df_score": pd.DataFrame(),

                "get_volume": True,
                "df_volume": pd.DataFrame(),

                "get_stochastic": False,
                "df_stochs": pd.DataFrame(),

                "get_bollinger": False,
                "df_bollinger": pd.DataFrame(),

                "get_macd": True,
                "df_macd": pd.DataFrame()
            }
        }

        params = {} # global params
        params['thread_fetchie_status'] = False # initialize fetch all
        params['thread_fetchie'] = kucoin_fetch_promising(app, kucoin_rest_api, default_configs, 600)

        symbol_threads = {} # initialize symbol_threads

        # start app
        app.run(debug=True, use_reloader=False)
    except Exception as e:
        logging.error(e, exc_info=True)
    finally:
        # Stop the symbol_threads
        [symbol_threads[thr].stop() for thr in symbol_threads]
        [symbol_threads[thr].join() for thr in symbol_threads]

        if params['thread_fetchie_status']:
            params['thread_fetchie'].stop()
            params['thread_fetchie'].join()