import pandas as pd
from trading_engine.event import OrderEvent
from trading_engine.performance import create_sharpe_ratio, create_drawdown


class Portfolio(object):
    """
    The Portfolio class handles the positions and market value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    The positions DataFrame stores a time-index of the quantity of positions held.
    The holdings DataFrame stores the cash and total market holdings value of each symbol for a particular
    time-index, as well as the percentage change in portfolio total across bars.
    """

    def __init__(self, bars, events, start_date, initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital.
        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital.
        """

        self.equity_curve = None
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        self.all_positions = self.construct_all_positions()
        self.current_positions = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date to determine when the time index will begin.
        This stores a list of all previous positions recorded at the timestamp of a market data event.
        A position is simply the quantity of the asset held. Negative positions mean the asset has been shorted.
        """

        d = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        d['datetime'] = self.start_date

        return [d]

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date to determine when the time index will begin.
        This describes the market value of all positions held.
        """

        d = dict((k, v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital

        return [d]

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous value of the portfolio across all symbols.
        """

        d = dict((k, v) for k, v in [(s, 0.0) for s in self.symbol_list])
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital

        return d

    def update_timeindex(self):
        """
        Adds a new record to the positions matrix for the current market data bar.
        This reflects the PREVIOUS bar, i.e. all current market data at this stage is known (OHLCV).
        Makes use of a MarketEvent from the events queue.
        """

        latest_datetime = self.bars.get_latest_bar_datetime(
            self.symbol_list[0]
        )

        # Update positions
        dp = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        dp['datetime'] = latest_datetime
        for s in self.symbol_list:
            dp[s] = self.current_positions[s]
        self.all_positions.append(dp)

        # Update holdings
        dh = dict((k, v) for k, v in [(s, 0) for s in self.symbol_list])
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']
        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * self.bars.get_latest_bar_value(s, "adj_close")
            dh[s] = market_value
            dh['total'] += market_value
        self.all_holdings.append(dh)

    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix to reflect the new position.
        Parameters:
        fill - The Fill object to update the positions with.
        """

        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        self.current_positions[fill.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to reflect the holdings value.
        Parameters:
        fill - The Fill object to update the holdings with.
        """

        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        fill_cost = self.bars.get_latest_bar_value(fill.symbol, "adj_close")
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings from a FillEvent.
        """

        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
        Simply files an Order object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.
        Parameters:
        signal - The tuple containing Signal information.
        """

        order = None
        symbol = signal.symbol
        direction = signal.direction
        mkt_quantity = 100
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'BUY' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SELL' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')

        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders based on the portfolio logic.
        """

        # if event.type == 'SIGNAL':
        order_event = self.generate_naive_order(event)
        self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings list of dictionaries.
        """

        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """

        total_return = self.equity_curve['equity_curve'][-1]

        returns = self.equity_curve['returns']
        daily_risk_free_rate = 0.25 / 252
        sharpe_ratio = create_sharpe_ratio(returns-daily_risk_free_rate, periods=252)

        pnl = self.equity_curve['equity_curve']
        drawdown, max_dd, dd_duration = create_drawdown(pnl)
        self.equity_curve['drawdown'] = drawdown

        stats = [("Total Return", (total_return - 1.0) * 100),
                 ("Annualized Sharpe Ratio", sharpe_ratio),
                 ("Max Drawdown", (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('equity.csv')

        return stats

