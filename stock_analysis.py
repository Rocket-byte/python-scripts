# Author: Ruslana Kruk
# Language: Python
# Functionality: This script analyzes stock data using yfinance, numpy, matplotlib, and pandas. It plots local minima and maxima.

import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from scipy.signal import argrelextrema

# Configuration for grid line style in plots
GRID_LINE_STYLE = '--'
GRID_LINE_WIDTH = 0.1
GRID_LINE_COLOR = 'blue'

# Download stock data for a specified stock over a given period and interval
stock_data = yf.download('PLTR', interval="1d", period="180d")

# Identifying local minima and maxima using shifts
stock_data['local_min'] = stock_data.Close[(stock_data.Close.shift(1) > stock_data.Close) & (stock_data.Close.shift(-1) > stock_data.Close)]
stock_data['local_max'] = stock_data.Close[(stock_data.Close.shift(1) < stock_data.Close) & (stock_data.Close.shift(-1) < stock_data.Close)]

# Function to plot stock data with local minima and maxima
def plot_stock_data(stock_df, order_for_extrema, ax, label):
    local_minima = stock_df.iloc[argrelextrema(stock_df.Close.values, np.less_equal, order=order_for_extrema)[0]]['Close']
    local_maxima = stock_df.iloc[argrelextrema(stock_df.Close.values, np.greater_equal, order=order_for_extrema)[0]]['Close']

    ax.scatter(stock_df.index, local_minima, c='r')
    ax.scatter(stock_df.index, local_maxima, c='g')
    ax.plot(stock_df.index, stock_df['Close'])
    ax.set_ylabel(label)
    ax.grid(color=GRID_LINE_COLOR, linestyle=GRID_LINE_STYLE, linewidth=GRID_LINE_WIDTH)

# Creating subplots
fig, axes = plt.subplots(4, 1)

# Plotting different layers with respective configurations
plot_stock_data(stock_data, 5, axes[0], 'Undamped')
plot_stock_data(stock_data, 5, axes[1], 'Second')
plot_stock_data(stock_data, 3, axes[2], 'Third')
plot_stock_data(stock_data, 3, axes[3], 'Fourth')

# Adjusting figure size and saving
fig.set_size_inches(15.5, 7.5, forward=True)
fig.savefig('stock_analysis_plot.png', dpi=300)
