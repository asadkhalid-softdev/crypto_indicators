from flask import Flask, jsonify, make_response, request
import json
from datetime import datetime
import traceback
import pandas as pd
import logging
import os
import copy 
import pandas as pd
import ast
from data_class import binance_data, binance_api, binance_fetch_promising
from functools import reduce
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
    return any(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") >= target_date_obj for dt in date_list)

#region FLASK API
@app.route('/get_assets', methods=['GET'])
def get_assets():
    """
    Get list of added symbols for data access.
    """
    try:
        res = [{"quote_asset": symbol_threads[thr].quote_asset, "base_asset": symbol_threads[thr].base_asset} for thr in symbol_threads]
        response = jsonify({'message': res, 'length': len(res)})
        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)
    
@app.route('/add_assets', methods=['POST'])
def add_assets():
    """
    Add symbols and start extracting their data.
    """
    try:
        data = request.json

        for ass in data['assets']:
            quote_asset = ass['quote_asset']
            base_asset = ass['base_asset']
            symbol = base_asset+quote_asset

            if symbol not in symbol_threads:
                symbol_threads[symbol] = binance_data(
                    app=app, 
                    binance_api_obj=binance_rest_api, 
                    symbol=symbol, 
                    base_asset=base_asset, 
                    quote_asset=quote_asset, 
                    default_configs=copy.deepcopy(default_configs), 
                    delay=1, 
                    max_duration_days=max_duration_days
                    )
                symbol_threads[symbol].start()

        response = jsonify({'message': f"{len(data['assets'])} assets added..."})
        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)

@app.route('/remove_assets', methods=['POST'])
def remove_assets():
    """
    Remove any or all assets
    """
    try:
        remove_all = request.args.get('remove_all',"false")

        if remove_all=="false":
            data = request.json
            for ass in data['assets']:
                quote_asset = ass['quote_asset']
                base_asset = ass['base_asset']
                symbol = base_asset+quote_asset

                if symbol in symbol_threads:
                    symbol_threads[symbol].stop()
                    symbol_threads[symbol].join()
                    symbol_threads.pop(symbol, None)

            response = jsonify({'message': f"{len(data['assets'])} assets removed..."})
        elif remove_all=="true":
            data = list(symbol_threads.keys())
            for symbol in data:
                symbol_threads[symbol].stop()
                symbol_threads[symbol].join()
                symbol_threads.pop(symbol, None)
            response = jsonify({'message': f"all assets removed..."})

        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        app.logger.error(f"{traceback.print_exc()}")
        return make_response(response, 400)

@app.route('/get_specific_datetime', methods=['GET'])
def get_specific_datetime():
    """
    Return data for a symbol between the provided times.
    """
    try:
        datetimes={}

        start_datetime = request.args.get('start_datetime', datetime.now().isoformat())
        end_datetime = request.args.get('end_datetime', datetime.now().isoformat())
        spec_symbol = request.args.get('spec_symbol', "")

        for thr in symbol_threads:
            if spec_symbol=="" or symbol_threads[thr].symbol==spec_symbol:
                if not symbol_threads[thr].combined_indicators.empty:
                    indicator = symbol_threads[thr].combined_indicators
                    if not indicator.empty:
                        datetimes[thr] = indicator[(indicator['Open time'] >= start_datetime) & (indicator['Open time'] <= end_datetime)].to_dict('index')
                    else:
                        datetimes[thr] = []

        response = jsonify({'message': datetimes})
        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)

@app.route('/get_time_captures', methods=['GET'])
def get_time_captures():
    """
    Capture time taken for data collection.
    """
    try:
        res = [{"name": thr, "value": symbol_threads[thr].time_capture} for thr in symbol_threads]

        response = jsonify({'message': res})
        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)

@app.route('/start_fetcher', methods=['GET'])
def start_fetcher():
    """
    This function starts data analysis on all binance symbols.
    """
    try:
        if params['thread_fetchie_status']:
            response = jsonify({'message': "Already running."})
        else:
            params['thread_fetchie'] = binance_fetch_promising(app, binance_rest_api, default_configs, 600)
            params['thread_fetchie'].start()
            params['thread_fetchie_status'] = True
            response = jsonify({'message': "Started fetcher."})

        return make_response(response, 200)
    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)
    
@app.route('/stop_fetcher', methods=['GET'])
def stop_fetcher():
    """
    This function stops data analysis on all binance symbols.
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

@app.route('/fetch_indicators', methods=['POST'])
def fetch_indicators():
    try:
        #region fetch params
        timeframes = ast.literal_eval(request.args.get('timeframes', "['12h']"))
        get_symbols_only = eval(request.args.get('get_symbols_only', 'False'))
        sort_by = request.args.get('sort_by', None)

        when_last = request.args.get('when_last', None)
        if when_last!=None:
            target_date_obj = datetime.strptime(when_last, "%Y-%m-%d %H:%M:%S")

        #region filters
        symbol_name = request.args.get('symbol_name', None)
        latest_rsi_lower_than = request.args.get('latest_rsi_lower_than', None)
        latest_stoch_rsi_lower_than = request.args.get('latest_stoch_rsi_lower_than', None)
        latest_candle_score_positive = eval(request.args.get('latest_candle_score_positive', 'None'))
        latest_macd_positive = eval(request.args.get('latest_macd_positive', 'None'))
        latest_macd_signal_change = request.args.get('latest_macd_signal_change', None)
        #endregion

        #region indicators
        is_BreakoutUp = eval(request.args.get('is_BreakoutUp', 'None'))
        is_BreakoutDown = eval(request.args.get('is_BreakoutDown', 'None'))

        is_PullbackUp = eval(request.args.get('is_PullbackUp', 'None'))
        is_PullbackDown = eval(request.args.get('is_PullbackDown', 'None'))

        is_ReversalUp = eval(request.args.get('is_ReversalUp', 'None'))
        is_ReversalDown = eval(request.args.get('is_ReversalDown', 'None'))

        is_stochrsi_low = eval(request.args.get('is_stochrsi_low', 'None'))
        is_stochrsi_zero = eval(request.args.get('is_stochrsi_zero', 'None'))
        is_stochrsi_uptrend = eval(request.args.get('is_stochrsi_uptrend', 'None'))
        is_stochrsi_downtrend = eval(request.args.get('is_stochrsi_downtrend', 'None'))
        
        is_candle_bullish = eval(request.args.get('is_candle_bullish', 'None'))
        is_candle_bearish = eval(request.args.get('is_candle_bearish', 'None'))

        is_macd_positive = eval(request.args.get('is_macd_positive', 'None'))
        is_macd_negative = eval(request.args.get('is_macd_negative', 'None'))

        is_macd_uptrend = eval(request.args.get('is_macd_uptrend', 'None'))
        is_macd_downtrend = eval(request.args.get('is_macd_downtrend', 'None'))

        is_signal_uptrend = eval(request.args.get('is_signal_uptrend', 'None'))
        is_signal_downtrend = eval(request.args.get('is_signal_downtrend', 'None'))

        is_macd_signal_change_increase = eval(request.args.get('is_macd_signal_change_increase', 'None'))
        is_macd_signal_change_decrease = eval(request.args.get('is_macd_signal_change_decrease', 'None'))

        is_macd_on_top = eval(request.args.get('is_macd_on_top', 'None'))
        is_signal_on_top = eval(request.args.get('is_signal_on_top', 'None'))
        #endregion 

        datae = {}
        for timeframe in timeframes:

            # get data
            data = params['thread_fetchie'].indicator_info
                        
            if data.empty:
                raise Exception("No Data found.")

            data = data[data["is_data"]==True]

            if sort_by=="symbol":
                data = data.sort_values('symbol', ascending=True)
            elif sort_by=="volume":
                data = data.sort_values(f'latest_volume_{timeframe}', ascending=False)
            elif sort_by=="stochrsi":
                data = data.sort_values(f'latest_stoch_rsi_{timeframe}', ascending=True)
            elif sort_by=="macd_signal_change":
                data = data.sort_values(f'latest_macd_signal_change_{timeframe}', ascending=True)

            #region filters
            if symbol_name != None:
                data = data[data[f'symbol'] == symbol_name]

            if latest_rsi_lower_than != None:
                data = data[data[f'latest_rsi_{timeframe}'] < float(latest_rsi_lower_than)]
                
            if latest_stoch_rsi_lower_than != None:
                data = data[data[f'latest_stoch_rsi_{timeframe}'] < float(latest_stoch_rsi_lower_than)]

            if latest_candle_score_positive != None:
                data = data[data[f'latest_candle_score_{timeframe}'] > 0]

            if latest_macd_positive==True:
                data = data[data[f'latest_macd_{timeframe}'] > 0]
            elif latest_macd_positive==False:
                data = data[data[f'latest_macd_{timeframe}'] < 0]

            if latest_macd_signal_change != None:
                splits = latest_macd_signal_change.split("_")

                if len(splits) == 2:
                    arithm_symbol = splits[0]
                    value = float(splits[1])
                    if arithm_symbol==">":
                        data = data[data[f'latest_macd_signal_change_{timeframe}'] > value]
                    if arithm_symbol==">=":
                        data = data[data[f'latest_macd_signal_change_{timeframe}'] >= value]
                    elif arithm_symbol=="<":
                        data = data[data[f'latest_macd_signal_change_{timeframe}'] < value]
                    elif arithm_symbol=="<=":
                        data = data[data[f'latest_macd_signal_change_{timeframe}'] <= value]
                elif len(splits) == 4:
                    arithm_symbols = [splits[0], splits[2]]
                    values = [splits[1], splits[3]]
                    for ind, arithm_symbol in enumerate(arithm_symbols):
                        if arithm_symbol==">":
                            data = data[data[f'latest_macd_signal_change_{timeframe}'] > float(values[ind])]
                        elif arithm_symbol==">=":
                            data = data[data[f'latest_macd_signal_change_{timeframe}'] >= float(values[ind])]
                        elif arithm_symbol=="<":
                            data = data[data[f'latest_macd_signal_change_{timeframe}'] < float(values[ind])]
                        elif arithm_symbol=="<=":
                            data = data[data[f'latest_macd_signal_change_{timeframe}'] <= float(values[ind])]
            #endregion 
            
            #region filters
            if is_BreakoutUp != None:
                data = data[data[f'is_BreakoutUp_{timeframe}']==is_BreakoutUp]
                if when_last!=None: data = data[data[f'breakoutUp_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_BreakoutDown != None:
                data = data[data[f'is_BreakoutDown_{timeframe}']==is_BreakoutDown]
                if when_last!=None: data = data[data[f'breakoutDown_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_PullbackUp != None:
                data = data[data[f'is_PullbackUp_{timeframe}']==is_PullbackUp]
                if when_last!=None: data = data[data[f'pullbackUp_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_PullbackDown:
                data = data[data[f'is_PullbackDown_{timeframe}']==is_PullbackDown]
                if when_last!=None: data = data[data[f'pullbackDown_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_ReversalUp != None:
                data = data[data[f'is_ReversalUp_{timeframe}']==is_ReversalUp]
                if when_last!=None: data = data[data[f'reversalUp_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_ReversalDown != None:
                data = data[data[f'is_ReversalDown_{timeframe}']==is_ReversalDown]
                if when_last!=None: data = data[data[f'reversalDown_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_stochrsi_low != None:
                data = data[data[f'is_stochrsi_low_{timeframe}']==is_stochrsi_low]
                if when_last!=None: data = data[data[f'stochrsi_low_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_stochrsi_zero != None:
                data = data[data[f'is_stochrsi_zero_{timeframe}']==is_stochrsi_zero]
                if when_last!=None: data = data[data[f'stochrsi_zero_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_stochrsi_uptrend != None:
                data = data[data[f'is_stochrsi_uptrend_{timeframe}']==is_stochrsi_uptrend]
                if when_last!=None: data = data[data[f'stochrsi_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_stochrsi_downtrend != None:
                data = data[data[f'is_stochrsi_downtrend_{timeframe}']==is_stochrsi_downtrend]
                if when_last!=None: data = data[data[f'stochrsi_downtrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_candle_bullish != None:
                data = data[data[f'is_candle_bullish_{timeframe}']==is_candle_bullish]
                if when_last!=None: data = data[data[f'candle_bullish_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_candle_bearish != None:
                data = data[data[f'is_candle_bearish_{timeframe}']==is_candle_bearish]
                if when_last!=None: data = data[data[f'candle_bearish_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_macd_positive != None:
                data = data[data[f'is_macd_positive_{timeframe}']==is_macd_positive]
                if when_last!=None: data = data[data[f'macd_positive_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_macd_negative != None:
                data = data[data[f'is_macd_negative_{timeframe}']==is_macd_negative]
                if when_last!=None: data = data[data[f'macd_negative_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_macd_uptrend != None:
                data = data[data[f'is_macd_uptrend_{timeframe}']==is_macd_uptrend]
                if when_last!=None: data = data[data[f'macd_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_macd_downtrend != None:
                data = data[data[f'is_macd_downtrend_{timeframe}']==is_macd_downtrend]
                if when_last!=None: data = data[data[f'macd_downtrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_signal_uptrend != None:
                data = data[data[f'is_signal_uptrend_{timeframe}']==is_signal_uptrend]
                if when_last!=None: data = data[data[f'signal_uptrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_signal_downtrend != None:
                data = data[data[f'is_signal_downtrend_{timeframe}']==is_signal_downtrend]
                if when_last!=None: data = data[data[f'signal_downtrend_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_macd_on_top != None:
                data = data[data[f'is_macd_on_top_{timeframe}']==is_macd_on_top]
                if when_last!=None: data = data[data[f'macd_on_top_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            if is_signal_on_top != None:
                data = data[data[f'is_signal_on_top_{timeframe}']==is_signal_on_top]
                if when_last!=None: data = data[data[f'signal_on_top_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_macd_signal_change_increase != None:
                data = data[data[f'is_macd_signal_change_increase_{timeframe}']==is_macd_signal_change_increase]
                if when_last!=None: data = data[data[f'macd_signal_change_increase_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]

            if is_macd_signal_change_decrease != None:
                data = data[data[f'is_macd_signal_change_decrease_{timeframe}']==is_macd_signal_change_decrease]
                if when_last!=None: data = data[data[f'macd_signal_change_decrease_times_{timeframe}'].apply(check_date_in_list, args=(target_date_obj,))]
            #endregion
            
            datae[timeframe] = data

        if len(timeframes) > 1:        
            logging.info(f"Two timeframes, finding common symbols...")
            common_symbols = reduce(lambda x, y: set(x).intersection(set(y)), 
                            [df['symbol'] for df in datae.values()])

            # Step 2: Filter each DataFrame by the common symbols
            data = [df[df['symbol'].isin(common_symbols)] for df in datae.values()]
            data = data[0]

            # Step 3: Concatenate the filtered DataFrames (if needed)
            # data = pd.concat(filtered_dfs)

        time_cols = [i for i in data.columns if 'times' in i]
        for col in time_cols:
            data[col] = data[col].apply(lambda x: str(x)[:67])
                
        if get_symbols_only:
            data = data['symbol'].values.tolist()
            response = jsonify({'n_symbols': len(data),'list': data})
        else:
            response = jsonify({'n_symbols': len(data),'list': data.to_dict("records")})
        return make_response(response, 200)

    except Exception as e:
        logging.error(e, exc_info=True)
        response = jsonify({'message': str(e)})
        return make_response(response, 400)

#endregion

if __name__ == '__main__':
    try:
        params = {} # global params
        params['thread_fetchie_status'] = False # initialize fetch all
        symbol_threads = {} # initialize symbol_threads

        # define configuration which will be used to fetch data and generate indicators
        default_configs = {
            "1h":{
                "interval": "1h",
                "startTime": "3 days ago",
                "endTime": "now",
                "df_kline": pd.DataFrame(),

                "get_rsi": True,
                "df_rsi": pd.DataFrame(),

                "get_score": True,
                "df_score": pd.DataFrame(),

                "get_volume": True,
                "df_volume": pd.DataFrame(),

                "get_stochastic": False,
                "df_stochs": pd.DataFrame(),

                "get_bollinger": True,
                "df_bollinger": pd.DataFrame(),

                "get_macd": True,
                "df_macd": pd.DataFrame(),

                "indicator_multiplier": 1,
            },
            "1d":{
                "interval": "1d",
                "startTime": "70 days ago",
                "endTime": "now",
                "df_kline": pd.DataFrame(),

                "get_rsi": True,
                "df_rsi": pd.DataFrame(),

                "get_score": True,
                "df_score": pd.DataFrame(),

                "get_volume": True,
                "df_volume": pd.DataFrame(),

                "get_stochastic": False,
                "df_stochs": pd.DataFrame(),

                "get_bollinger": True,
                "df_bollinger": pd.DataFrame(),

                "get_macd": True,
                "df_macd": pd.DataFrame(),

                "indicator_multiplier": 24,
            }
        }

        # initialize binance api
        keys = json.load(open(f"{current_directory}/keys.json","rb"))
        api_type = 'live'
        binance_rest_api = binance_api(api_type, keys[api_type])

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