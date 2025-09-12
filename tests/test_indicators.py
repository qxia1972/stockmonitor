import sys
from pathlib import Path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import unittest
import pandas as pd
from stockpool import StockPoolGUI

class FakePM:
    def __init__(self):
        self._logger = __import__('logging').getLogger('fakepm')
    @property
    def logger(self):
        return self._logger
    def get_all_pools(self):
        return {'core_pool': pd.DataFrame({'code': ['C1']})}
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

class IndicatorTest(unittest.TestCase):
    def test_indicator_draw(self):
        pm = FakePM()
        gui = StockPoolGUI(pm)
        gui._populate_pool_sections()
        # set figure for draw
        import matplotlib
        gui.fig = matplotlib.figure.Figure()
        # enable indicators
        gui.indicator_vars['MA'].set(True)
        gui.indicator_vars['BOLL'].set(True)
        gui.indicator_vars['VOL'].set(True)
        # update chart (should not raise)
        gui._update_chart('C1')

if __name__ == '__main__':
    unittest.main()
