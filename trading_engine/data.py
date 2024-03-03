import numpy as np
import os
import pandas as pd
from abc import ABCMeta, abstractmethod
from trading_engine.event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for all inherited data handlers (live and historic).
    The goal of a (derived) DataHandler object is to output a generated set of bars (OHLCVI) for each symbol requested.
    This will replicate how a live strategy would function as current market data would be sent "down the pipe".
    Thus, a historic and live system will be treated identically by the rest of the backtesting suite.
    """

    # abstract base class (ABC)
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """

        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, n=1):
        """
        Returns the last n bars updated.
        """

        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar of the symbol.
        """

        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI from the last bar.
        """

        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, n=1):
        """
        Returns the last n bar values for the symbol, or all the latest bars if less available.
        """

        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol in a tuple format:
        (datetime, open, high, low, close, volume, open interest).
        This strictly prohibits look-ahead bias.
        """

        raise NotImplementedError("Should implement update_bars()")


class TsetmcHistoricCSVDataHandler(DataHandler):
    """
    TsetmcHistoricCSVDataHandler is designed to read CSV files downloaded from tsetmc.com for each requested symbol
    from disk and provide an interface to obtain the "latest" bar in a manner identical to a live trading interface.
    """

    def __init__(self, events, csv_dir, symbol_list, start_date):
        """
        Initialises the historic data handler by requesting the location of the CSV files and a list of symbols.
        It will be assumed that all files are of the form 'symbol.csv', where symbol is a string in the list.
        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """

        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.start_date = start_date
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self._open_csv_files()
        self._adjust_prices()
        self._reindex()

    def _open_csv_files(self):
        """
        Opens the CSV files from the data directory, converting them into pandas DataFrames within a symbol dictionary.
        """

        for s in self.symbol_list:
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.csv_dir, f'{s}.csv'),
                header=1,
                usecols=[1, 2, 3, 4, 5, 6, 7, 8, 10, 11],
                index_col=0,
                parse_dates=True,
                names=['date', 'open', 'high', 'low', 'close', 'value', 'volume', 'oi', 'yesterday', 'last']
            ).sort_index()
            self.symbol_data[s].drop(self.symbol_data[s].loc[: self.start_date].index, inplace=True)

    def _adjust_prices(self):
        """
        Adjusts closing price using yesterday price.
        """

        for s in self.symbol_list:
            # Times we have capital raising or DPS distribution
            adj_index = []
            i = 0
            while i < len(self.symbol_data[s]['close']) - 1:
                if self.symbol_data[s]['close'][i] != self.symbol_data[s]['yesterday'][i + 1]:
                    adj_index.append(self.symbol_data[s].index[i + 1])
                i += 1
            # Adjustment rate
            adj_rates = []
            for i in adj_index:
                adj_rates.append(
                    self.symbol_data[s]['yesterday'][i] / self.symbol_data[s]['close'].shift()[i])
            # Adjusting
            self.symbol_data[s]['adj_close'] = self.symbol_data[s]['close']
            for i in range(len(adj_index)):
                self.symbol_data[s].loc[: adj_index[i],  ['adj_close']] = (
                        self.symbol_data[s]['adj_close'][: adj_index[i]] * adj_rates[i])

    def _reindex(self):
        """
        Reindex the dataframes when there are multi symbols traded in different dates.
        """

        # Combine the index to pad forward values
        comb_index = None
        for s in self.symbol_list:
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """

        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, n=1):
        """
        Returns the last n bars from the latest_symbol list, or all the latest bars if less available.
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-n:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the open, high, low, close, volume, value, last, yesterday, oi values.
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, n=1):
        """
        Returns the last n bar values from the latest_symbol list.
        """

        try:
            bars_list = self.get_latest_bars(symbol, n)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure for all symbols in the symbol list.
        """

        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
