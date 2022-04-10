# -*- coding: utf-8 -*-

#Core libraries
import pandas as pd
import numpy as np 
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

#Custom Impports
import yfinance as yf
import ta


def clean_dataframe_column_names(): 
    '''
    TO DO - Clean dataframe column names - a generic function
    '''
    return None


def pull_yahoo_data(ticker: str, verbose=True) -> pd.DataFrame:
    '''
    Function to get data from yahoo finance initially, and format as a dataframe. Automatically
    gets the maxiumum amount of data available for that stock. 
    args: 
        ticker: the stock ticker you want to pull data for
        verbose: choose whether to add print statements and logs
    rtypes: 
        stock_dataframe: dataframe for a given stock based on the ticker selected. Note that 
        the date is represented by a string here for specific reasons...
    '''
    if verbose: print('\nGetting yahoo finance data for ticker {}...'.format(ticker))
    stock_dataframe = yf.Ticker(ticker)
    stock_dataframe = stock_dataframe.history(period='max')
    stock_dataframe = stock_dataframe.reset_index()
    stock_dataframe['Date'] = stock_dataframe['Date'].astype(str)
    stock_dataframe = stock_dataframe.sort_values(by='Date', ascending=False)
    stock_dataframe = stock_dataframe.reset_index(drop=True)
    if verbose: print('Data loaded into memory for ticker {}.'.format(ticker))
    return stock_dataframe



def create_target_feature(stock_dataframe: pd.DataFrame, shift_periods: int,\
     move_value: float, strategy: str, verbose=True) -> pd.DataFrame:
    '''
    Create a target feature determining whether you should have 1) bought or sold today, based on...
    ...the % move a certain number of days into the future. 
    args: 
        shift_periods: how many days in the future you want to look to determine what to do today
        move_value: what percent the stock needs to move to create a "buy" or "sell" signal
        strategy: whether you're trying to run a buy, sell, or "both" strategy - note that this...
        ...will determine if your problem is a binary classification problem or multi-class
            for this choose either "buy", "sell", or "both"
        verbose: choose whether to add print statements and logs
    rtypes: 
        buy_dataframe: dataframe with buy target created
        sell_dataframe: dataframe with sell target created
        both_dataframe: dataframe with both buy and sell targets crreated
    dependencies: 
        Requires data specifically from the "pull_yahoo_data" function, otherwise
        it will create breaking changes in the data formatting
    '''
    if verbose: print('\nEngineering target variable: looking {} periods ahead for a {}% move...'.\
        format(shift_periods, move_value))
    stock_dataframe['later_value'] = stock_dataframe['Close'].shift(periods=shift_periods)
    stock_dataframe['percent_difference'] = (-(stock_dataframe['Close']\
         - stock_dataframe['later_value']) / stock_dataframe['Close']) * 100

    #Split out two dataframes, one for buying, and one for selling
    buy_dataframe = stock_dataframe.copy()
    sell_dataframe = stock_dataframe.copy()
    both_dataframe = stock_dataframe.copy()

    #Engineer the target
    if strategy == 'buy':
        buy_dataframe.loc[buy_dataframe['percent_difference'] >= move_value, 'TARGET'] = 'BUY'
        buy_dataframe.loc[buy_dataframe['percent_difference'] <= -move_value, 'TARGET'] = 'HOLD'
        buy_dataframe.loc[(buy_dataframe['percent_difference'] <= move_value) &\
            (buy_dataframe['percent_difference'] >= -move_value), 'TARGET'] = 'HOLD'
        buy_dataframe = buy_dataframe.drop(['later_value', 'percent_difference'], axis=1)
        if verbose: print('Target variable for strategy {} has been engineered.'.format(strategy))
        return buy_dataframe


    #Engineer the target
    if strategy == 'sell':
        sell_dataframe.loc[sell_dataframe['percent_difference'] >= move_value, 'TARGET'] = 'HOLD'
        sell_dataframe.loc[sell_dataframe['percent_difference'] <= -move_value, 'TARGET'] = 'SELL'
        sell_dataframe.loc[(sell_dataframe['percent_difference'] <= move_value) &\
            (sell_dataframe['percent_difference'] >= -move_value), 'TARGET'] = 'HOLD'
        sell_dataframe = sell_dataframe.drop(['later_value', 'percent_difference'], axis=1)
        if verbose: print('Target variable for strategy {} has been engineered.'.format(strategy))
        return sell_dataframe


    #Engineer the target
    if strategy == 'both':
        both_dataframe.loc[both_dataframe['percent_difference'] >= move_value, 'TARGET'] = 'BUY'
        both_dataframe.loc[both_dataframe['percent_difference'] <= -move_value, 'TARGET'] = 'SELL'
        both_dataframe.loc[(both_dataframe['percent_difference'] <= move_value) &\
            (both_dataframe['percent_difference'] >= -move_value), 'TARGET'] = 'HOLD'
        both_dataframe = both_dataframe.drop(['later_value', 'percent_difference'], axis=1)
        if verbose: print('Target variable for strategy {} has been engineered.'.format(strategy))
        return both_dataframe



def engineer_technical_indicators(dataframe: pd.DataFrame, verbose=True) -> pd.DataFrame: 
    '''
    Engineer technical indicator features for a stock dataframe.
    args: 
        dataframe: dataframe with stock data and all the relevant columns (Opoen, High, Low, Close, Volume) 
        verbose: choose whether to add prints and logs 
    rtypes: 
        dataframe: dataframe with all the technical indicators as engineered features
    dependencies: 
        Requires data output from either the "pull_yahoo_data" or "create_target_feature" functions
    '''
    if verbose: print('\nEngineering technical indicators...')
    #Engineer technical indicators
    dataframe = ta.add_all_ta_features(dataframe, open="Open", high="High", low="Low",\
         close = "Close", volume="Volume", fillna=True)
    
    #Create a list of features to drop - these features weren't present for the most recent day, and thus...
    #...were not considered useful 
    drop_feature_list = ['volume_fi', 'volume_em', 'volume_sma_em', 'volume_mfi',\
    'volume_nvi', 'volatility_bbw', 'volatility_bbp', 'volatility_bbhi', 'volatility_bbli',\
    'volatility_kchi', 'volatility_kcli', 'volatility_atr', 'volatility_ui', 'trend_macd', 'trend_macd_signal',\
    'trend_macd_diff', 'trend_vortex_ind_pos', 'trend_vortex_ind_neg', 'trend_vortex_ind_diff', \
    'trend_trix', 'trend_mass_index', 'trend_kst_diff', 'trend_stc', 'trend_adx', 'trend_adx_pos', 'trend_adx_neg',\
    'trend_cci', 'trend_aroon_up', 'trend_aroon_down', 'trend_aroon_ind', 'trend_psar_up', 'trend_psar_down',\
    'trend_psar_up_indicator', 'trend_psar_down_indicator', 'momentum_rsi', 'momentum_stoch_rsi', \
    'momentum_stoch_rsi_k', 'momentum_stoch_rsi_d', 'momentum_tsi', 'momentum_uo',\
    'momentum_stoch', 'momentum_stoch_signal', 'momentum_ao', 'momentum_roc', 'momentum_ppo',\
        'momentum_ppo_signal', 'momentum_ppo_hist', 'momentum_pvo', 'momentum_pvo_signal', 'momentum_pvo_hist',\
            'momentum_kama', 'others_dr', 'others_dlr', 'others_cr']
    dataframe = dataframe.drop(drop_feature_list, axis=1)
    if verbose: print('\nTechnical indicators created.')
    return dataframe
    