from datetime import datetime
import pandas as pd
import yfinance as yf
from tqdm import tqdm
import oracle_connect
from technical_indicators import add_technical_indicators

# Author: Ruslana Kruk 2022
# This module imports stock data from Yahoo Finance for database tickers and stores it in database.

def read_tickers_from_database(connection, partition_size, query):
    print(f"{datetime.now().time()} - Import Stocks - Start load Tickers")
    ora_cursor = connection.cursor()

    ticker_parts, ticker_list, row_num, ticker_count = [], [], 0, 0
    for row in ora_cursor.execute(query):
        row_num += 1
        ticker_count += 1
        ticker_list.append(row[0])
        if row_num % partition_size == 0:
            ticker_parts.append(ticker_list)
            ticker_list = []

    ticker_parts.append(ticker_list)
    ora_cursor.close()
    print(f"{datetime.now().time()} - Import Stocks - End Tickers load. Loaded - {ticker_count}. Parts - {len(ticker_parts)} by {partition_size} per part.")
    return ticker_parts

def read_prices_from_yahoo(tickers, period, interval):
    ticker_prices = yf.download(tickers, period=period, interval=interval, group_by='ticker', auto_adjust=False, prepost=False, threads=True).T
    ticker_prices.sort_index(inplace=True)

    tickers_prices = []
    for ticker in tickers:
        ticker_df = ticker_prices.loc[(ticker,),].T
        ticker_df.columns = ticker_df.columns.str.upper()
        ticker_df['TICKER'] = ticker
        ticker_df['INTERVAL'] = interval
        ticker_df['DATE_TIME'] = ticker_df.index.tz_localize(None)
        ticker_df.drop(columns=['ADJ CLOSE'], inplace=True)
        ticker_df = ticker_df[['TICKER', 'INTERVAL', 'DATE_TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
        tickers_prices.append(ticker_df)

    return tickers_prices

def delete_ohlc_from_database(connection, ticker, interval):
    cur_delete = connection.cursor()
    cur_delete.execute("DELETE /*+NOLOGGING*/ FROM PRICES t WHERE t.SYMBOL = :ticker AND t.Interval = :interval", [ticker, interval])
    cur_delete.close()
    connection.commit()

def write_ohlc_to_database(connection, ohlc_df):
    cur_write = connection.cursor()
    stocks_to_db = ohlc_df[['TICKER', 'INTERVAL', 'DATE_TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'ROW_NUM_ACS', 'ROW_NUM_DESC', 'PREV_OPEN', 'PREV_CLOSE', 'PREV_HIGH', 'PREV_LOW', 'PREV_VOLUME', 'CHANGE_OPEN_PRICE', 'CHANGE_OPEN_PERCENT', 'CHANGE_PRICE', 'CHANGE_PERCENT', 'TR', 'ATR_5', 'ATR_12', 'ATR_24', 'VOL_AVG_5', 'VOL_AVG_12', 'VOL_AVG_24', 'IS_LAST', 'EXTREME_LOW', 'EXTREME_HIGH']].reset_index(drop=True)
    data = list(stocks_to_db.itertuples(index=False, name=None))

    query_add_stocks = """
    INSERT /*+ APPEND PARALLEL NOLOGGING */ INTO PRICES (SYMBOL, INTERVAL, Interval_Date_Time, OPEN, High, Low, CLOSE, Volume, Row_num_Asc, Row_num_Desc, Prev_Open, Prev_Close, Prev_High, Prev_Low, Prev_Volume, Change_Open_Price, Change_Open_Percent, Change_Price, Change_Percent, Ti_Tr, Ti_Atr_5, Ti_Atr_12, Ti_Atr_24, VOL_AVG_5, VOL_AVG_12, VOL_AVG_24, Is_Last, EXTREME_LOW, EXTREME_HIGH) VALUES (:Ticker, :INTERVAL, :Interval_Date_Time, :OPEN, :High, :Low, :CLOSE, :Volume, :row_num_asc, :Row_Num_Desc, :Prev_Open, :Prev_Close, :Prev_High, :Prev_Low, :Prev_Volume, :Change_Open_Price, :Change_Open_Percent, :Change_Price, :Change_Percent, :TR, :Atr_5, :Atr_12, :Atr_24, :VOL_AVG_5, :VOL_AVG_12, :VOL_AVG_24, :Is_Last, :EXTREME_LOW, :EXTREME_HIGH)
    """
    cur_write.executemany(query_add_stocks, data)
    cur_write.close()
    connection.commit()

# Example function for one of the data worker types
def stock_data_worker(interval, period, partition_size, query):
    ora_connection = oracle_connect.connect_to_oracle()
    tickers_partitions = read_tickers_from_database(ora_connection, partition_size, query)
    for i, tickers in enumerate(tickers_partitions, 1):
        print(f'\nWork on {i}/{len(tickers_partitions)} partition. Start {interval} timeframe')
        df = read_prices_from_yahoo(tickers, period, interval)
        pbar = tqdm(df)
        for x in pbar:
            current_ticker = x['TICKER'].iloc[0]
            df_ti = add_technical_indicators(x)
            delete_ohlc_from_database(ora_connection, current_ticker, interval)
            write_ohlc_to_database(ora_connection, df_ti)
            pbar.set_description('Write to database')

