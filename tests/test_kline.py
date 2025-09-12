import unittest
import pandas as pd
from datetime import datetime

# lightweight fake PoolManager for testing
class FakePoolManager:
    def get_price(self, stock_code, start_date, end_date, frequency='1d'):
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df = pd.DataFrame({
            'open': [1.0 + i*0.01 for i in range(len(dates))],
            'close': [1.0 + i*0.02 for i in range(len(dates))],
            'high': [1.0 + i*0.03 for i in range(len(dates))],
            'low': [1.0 + i*0.005 for i in range(len(dates))],
            'volume': [1000 + i*10 for i in range(len(dates))]
        }, index=dates)
        df.index.name = 'date'
        return df
    @property
    def logger(self):
        import logging
        return logging.getLogger('fakepool')

# Import StockPoolGUI from stockpool.py
from stockpool import StockPoolGUI

class KlineTest(unittest.TestCase):
    def test_get_kline_data(self):
        pm = FakePoolManager()
        gui = StockPoolGUI(pm)
        df = gui._get_kline_data('FAKE', 'daily')
        self.assertIsNotNone(df)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    def test_draw_kline_chart(self):
        pm = FakePoolManager()
        gui = StockPoolGUI(pm)
        df = gui._get_kline_data('FAKE', 'daily')
        # ensure drawing does not raise
        gui.fig = __import__('matplotlib').figure.Figure()
        gui._draw_kline_chart('FAKE', df)

if __name__ == '__main__':
    unittest.main()
