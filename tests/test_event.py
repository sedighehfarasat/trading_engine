from trading_engine.event import *
from unittest import TestCase


class TestEvent(TestCase):
    def test_initialize_market_event(self):
        event = MarketEvent()
        event_type = event.type

        self.assertEqual(event_type, 'MARKET')

    def test_initialize_signal_event(self):
        event = SignalEvent(123, 'Arian', 123456789, 'BUY')
        event_type = event.type

        self.assertEqual(event_type, 'SIGNAL')
