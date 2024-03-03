import queue
from trading_engine.data import TsetmcHistoricCSVDataHandler
from unittest import TestCase


class TestHistoricCSVDataHandler(TestCase):
    def test_get_latest_bar(self):
        csv_dir = '~/Downloads'
        ticker = ['Dashtestan Ce.', 'Khalij Fars Trans']
        data = TsetmcHistoricCSVDataHandler(None, csv_dir, ticker)
        data.update_bars()
        latest_bar = data.get_latest_bar('Dashtestan Ce.')

    def test_get_latest_bar_value(self):
        csv_dir = '~/Downloads'
        ticker = ['Dashtestan Ce.', 'Khalij Fars Trans']
        data = TsetmcHistoricCSVDataHandler(None, csv_dir, ticker)
        data.update_bars()
        latest_close = data.get_latest_bar_value('Dashtestan Ce.', 'close')

    def test_update_bars(self):
        csv_dir = '~/Downloads'
        ticker = ['Dashtestan Ce.', 'Khalij Fars Trans']
        data = TsetmcHistoricCSVDataHandler(queue.Queue(), csv_dir, ticker)
        data.update_bars()
        latest_close = data.get_latest_bar_value('Dashtestan Ce.', 'close')
