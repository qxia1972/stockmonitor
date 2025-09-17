from typing import Any

import pandas as pd

def init(*args: Any, **kwargs: Any) -> None: ...

class DataService:
    def get_indicators(self, symbol: str) -> pd.DataFrame: ...

def get_price(symbol: str, start: str, end: str) -> pd.DataFrame: ...
