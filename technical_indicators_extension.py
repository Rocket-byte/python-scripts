import numpy
from scipy.signal import argrelextrema
from stockstats import StockDataFrame

# Author: Ruslana Kruk
# This module extends OHLC data with additional technical indicators like ATR, TR, and others.

def add_technical_indicators(ohlc_df):
    """
    Extends OHLC dataframe with technical indicators like ATR, TR, etc.
    Args:
    ohlc_df (DataFrame): DataFrame with columns OPEN, CLOSE, HIGH, LOW, VOLUME.
    Returns:
    DataFrame: Extended OHLC dataframe with additional technical indicators.
    """
    # Handle Volume zero values and NaNs
    ohlc_df = ohlc_df.replace(0, numpy.nan).dropna(subset=["VOLUME"]).replace(numpy.nan, 0)
    
    # Add additional columns for calculations
    data_length = len(ohlc_df)
    ohlc_df['ROW_NUM_ACS'] = numpy.arange(data_length) + 1
    ohlc_df['ROW_NUM_DESC'] = data_length - numpy.arange(data_length)
    ohlc_df[['PREV_OPEN', 'PREV_CLOSE', 'PREV_HIGH', 'PREV_LOW', 'PREV_VOLUME']] = ohlc_df[['OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME']].shift(1)
    ohlc_df['CHANGE_OPEN_PRICE'] = ohlc_df['OPEN'] - ohlc_df['PREV_CLOSE']
    ohlc_df['CHANGE_OPEN_PERCENT'] = (ohlc_df['CHANGE_OPEN_PRICE'] / ohlc_df['CLOSE']) * 100
    ohlc_df['CHANGE_PRICE'] = ohlc_df['CLOSE'] - ohlc_df['PREV_CLOSE']
    ohlc_df['CHANGE_PERCENT'] = (ohlc_df['CHANGE_PRICE'] / ohlc_df['PREV_CLOSE']) * 100

    # Retype data for stockstats
    stocks = StockDataFrame.retype(ohlc_df[['OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME']])
    
    # Calculate True Range and Average True Range
    ohlc_df['TR'] = ohlc_df['HIGH'] - ohlc_df['LOW']
    for period in [5, 12, 24]:
        ohlc_df[f'ATR_{period}'] = stocks[f'atr_{period}'].fillna(0)

    # Calculate Volume Averages
    for period in [5, 12, 24]:
        ohlc_df[f'VOL_AVG_{period}'] = stocks[f'volume_{period}_sma'].fillna(0).round(0)

    # Find Extremes Points
    bar_count = 4
    ohlc_df['EXTREME_LOW'] = ohlc_df.iloc[argrelextrema(ohlc_df.LOW.values, numpy.less_equal, order=bar_count)[0]]['LOW']
    ohlc_df['EXTREME_HIGH'] = ohlc_df.iloc[argrelextrema(ohlc_df.HIGH.values, numpy.greater_equal, order=bar_count)[0]]['HIGH']

    # Mark Last Row
    ohlc_df['IS_LAST'] = 0
    if data_length > 0:
        ohlc_df.at[ohlc_df.index[-1], 'IS_LAST'] = 1

    # Finalize DataFrame
    ohlc_df = ohlc_df.replace(numpy.nan, 0).round(2)

    return ohlc_df
