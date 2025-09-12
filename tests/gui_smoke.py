"""
Smoke test for GUI interactions without running the Tk mainloop.
Creates a StockPoolGUI with a FakePoolManager, populates the tree, simulates selection,
and calls chart update to make sure code paths run without a display.
"""
import logging
import sys
from pathlib import Path
# ensure project root in path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from stockpool import StockPoolGUI
import pandas as pd
from datetime import date

class FakePM:
    def __init__(self):
        self._data = {
            'basic_pool': pd.DataFrame({'code': ['FAKE1', 'FAKE2']}),
            'watch_pool': pd.DataFrame({'code': ['W1', 'W2']}),
            'core_pool': pd.DataFrame({'code': ['C1']})
        }
        self._logger = logging.getLogger('fakepm')
    @property
    def logger(self):
        return self._logger
    def get_all_pools(self):
        return self._data
    def get_price(self, stock_code, start_date, end_date, frequency='1d'):
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df = pd.DataFrame({
            'open': [1.0 + i*0.1 for i in range(len(dates))],
            'close': [1.2 + i*0.1 for i in range(len(dates))],
            'high': [1.3 + i*0.1 for i in range(len(dates))],
            'low': [0.9 + i*0.05 for i in range(len(dates))],
            'volume': [1000 + i*10 for i in range(len(dates))]
        }, index=dates)
        df.index.name = 'date'
        return df


def run_smoke():
    pm = FakePM()
    gui = StockPoolGUI(pm)

    # populate pools
    gui._populate_pool_sections()
    print('Tree children counts:')
    for parent in ('pool_core', 'pool_watch', 'pool_basic'):
        print(parent, len(gui.pool_tree.get_children(parent)))

    # simulate selecting first child of core pool
    children = gui.pool_tree.get_children('pool_core')
    if children:
        iid = children[0]
        # simulate selection event by calling handler directly
        gui.pool_tree.selection_set(iid)
        gui._on_pool_tree_selection(None)
        print('Selected', iid)

    # set a figure to allow drawing without embedding
    import matplotlib
    gui.fig = matplotlib.figure.Figure()
    # update chart for a sample code
    gui._update_chart('C1')
    print('Chart update completed')

if __name__ == '__main__':
    run_smoke()
