import pandas as pd
import random
from trading_engine.performance import create_drawdown
from unittest import TestCase


class TestPortfolio(TestCase):
    def test_construct_all_positions(self):
        symbol_list = ['Dashtestan Ce.', 'Khalij Fars Trans']
        start_date = 123
        initial_capital = 100000
        d = dict((k, v) for k, v in [(s, 0.0) for s in symbol_list])
        d['datetime'] = start_date
        d['cash'] = initial_capital
        d['commission'] = 0.0
        d['total'] = initial_capital

    def test_create_drawdown(self):
        pnl = pd.Series(data=random.sample(range(1200, 3400), 100), index=list(range(100)))
        duration, drawdown, drawdown_max, duration_max = create_drawdown(pnl)
