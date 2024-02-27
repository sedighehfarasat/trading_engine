from trading_engine.data import HistoricCSVDataHandler
from unittest import TestCase


class TestHistoricCSVDataHandler(TestCase):
    def test_get_latest_bar(self):
        csv_dir = '~/Downloads'
        ticker = ['Dashtestan Ce.', 'Khalij Fars Trans']
        data = HistoricCSVDataHandler(None, csv_dir, ticker)
        data.update_bars()
        latest_bar = data.get_latest_bar('Dashtestan Ce.')

        pass

    def test_get_latest_bar_value(self):
        csv_dir = '~/Downloads'
        ticker = ['Dashtestan Ce.', 'Khalij Fars Trans']
        data = HistoricCSVDataHandler(None, csv_dir, ticker)
        data.update_bars()
        latest_close = data.get_latest_bar_value('Dashtestan Ce.', 'close')

        pass
