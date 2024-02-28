import datetime
from trading_engine.event import *
from unittest import TestCase


class TestEvent(TestCase):
    def test_initialize_market_event(self):
        event = MarketEvent()
        event_type = event.type

        self.assertEqual(event_type, 'MARKET')

    def test_initialize_signal_event(self):
        signal_time = datetime.datetime.now().timestamp()
        event = SignalEvent(123, 'Arian', signal_time, 'BUY')
        event_type = event.type

        self.assertEqual(event_type, 'SIGNAL')

    def test_initialize_order_event(self):
        event = OrderEvent('Arian', 'LMT', 5000, 'BUY')
        event_type = event.type
        event.print_order()

        self.assertEqual(event_type, 'ORDER')

    def test_initialize_fill_event(self):
        fill_time = datetime.datetime.now().timestamp()
        event = FillEvent(fill_time, 'Arian', 'TSE', 1000, 'BUY', 0, 0)
        event_type = event.type

        self.assertEqual(event_type, 'FILL')
