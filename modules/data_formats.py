"""
数据格式约定和标准定义
Data Format Conventions and Standards

这个模块定义了整个系统中数据格式的标准约定，确保所有组件使用统一的DataFrame格式。
This module defines the standard conventions for data formats throughout the system,
ensuring all components use unified DataFrame formats.
"""

import pandas as pd
from typing import Dict, List, Optional, Union, Any
from datetime import datetime

# ============================================================================
# 标准数据格式定义
# ============================================================================

# 标准OHLCV列名定义
STANDARD_OHLCV_COLUMNS = {
    'open': 'open',           # 开盘价
    'high': 'high',           # 最高价
    'low': 'low',             # 最低价
    'close': 'close',         # 收盘价
    'volume': 'volume'        # 成交量
}

# 扩展市场数据列名定义
EXTENDED_MARKET_COLUMNS = {
    'total_turnover': 'total_turnover',     # 成交额
    'limit_up': 'limit_up',                 # 涨停价
    'limit_down': 'limit_down',             # 跌停价
    'num_trades': 'num_trades',             # 成交笔数
    'vwap': 'vwap',                         # 成交均价
    'adj_factor': 'adj_factor'              # 复权因子
}

# 所有标准K线数据列
STANDARD_KLINE_COLUMNS = {**STANDARD_OHLCV_COLUMNS, **EXTENDED_MARKET_COLUMNS}

# ============================================================================
# 数据类型定义
# ============================================================================

# 标准数据类型映射
STANDARD_DTYPES = {
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'int64',
    'total_turnover': 'float64',
    'limit_up': 'float64',
    'limit_down': 'float64',
    'num_trades': 'int64',
    'vwap': 'float64',
    'adj_factor': 'float64'
}

# ============================================================================
# 时间粒度定义
# ============================================================================

# 支持的时间粒度配置
TIMEFRAME_CONFIG = {
    'daily': {
        'frequency': '1d',
        'periods': 100,
        'description': '日K线',
        'cache_days': 150
    },
    '60min': {
        'frequency': '60m',
        'periods': 400,
        'description': '60分钟K线',
        'cache_days': 110
    },
    '15min': {
        'frequency': '15m',
        'periods': 1600,
        'description': '15分钟K线',
        'cache_days': 110
    },
    '5min': {
        'frequency': '5m',
        'periods': 4800,
        'description': '5分钟K线',
        'cache_days': 110
    },
    '1min': {
        'frequency': '1m',
        'periods': 24000,
        'description': '1分钟K线',
        'cache_days': 110
    }
}

# ============================================================================
# 指标命名约定和处理函数映射
# ============================================================================

# 基础技术指标命名约定和处理函数映射
INDICATOR_CONFIG = {
    # 移动平均线指标
    'SMA': {
        'function': 'calculate_sma',
        'description': '简单移动平均线',
        'parameters': {'period': int, 'price_column': str},
        'default_params': {'period': 20, 'price_column': 'close'},
        'output_columns': ['SMA'],
        'category': 'trend'
    },
    'EMA': {
        'function': 'calculate_ema',
        'description': '指数移动平均线',
        'parameters': {'period': int, 'price_column': str},
        'default_params': {'period': 20, 'price_column': 'close'},
        'output_columns': ['EMA'],
        'category': 'trend'
    },
    'TEMA': {
        'function': 'calculate_tema',
        'description': '三重指数移动平均线',
        'parameters': {'period': int, 'price_column': str},
        'default_params': {'period': 20, 'price_column': 'close'},
        'output_columns': ['TEMA'],
        'category': 'trend'
    },
    'WMA': {
        'function': 'calculate_wma',
        'description': '加权移动平均线',
        'parameters': {'period': int, 'price_column': str},
        'default_params': {'period': 20, 'price_column': 'close'},
        'output_columns': ['WMA'],
        'category': 'trend'
    },

    # 动量指标
    'RSI': {
        'function': 'calculate_rsi',
        'description': '相对强弱指标',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['RSI'],
        'category': 'momentum'
    },
    'STOCH': {
        'function': 'calculate_stochastic',
        'description': '随机指标',
        'parameters': {'k_period': int, 'd_period': int, 'slowing': int},
        'default_params': {'k_period': 14, 'd_period': 3, 'slowing': 3},
        'output_columns': ['STOCH_K', 'STOCH_D'],
        'category': 'momentum'
    },
    'CCI': {
        'function': 'calculate_cci',
        'description': '顺势指标',
        'parameters': {'period': int},
        'default_params': {'period': 20},
        'output_columns': ['CCI'],
        'category': 'momentum'
    },
    'MACD': {
        'function': 'calculate_macd',
        'description': 'MACD指标',
        'parameters': {'fast_period': int, 'slow_period': int, 'signal_period': int},
        'default_params': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
        'output_columns': ['MACD', 'MACD_SIGNAL', 'MACD_HIST'],
        'category': 'momentum'
    },
    'WILLR': {
        'function': 'calculate_williams_r',
        'description': '威廉指标',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['WILLR'],
        'category': 'momentum'
    },
    'KDJ': {
        'function': 'calculate_kdj',
        'description': 'KDJ随机指标',
        'parameters': {'k_period': int, 'd_period': int, 'j_period': int},
        'default_params': {'k_period': 14, 'd_period': 3, 'j_period': 3},
        'output_columns': ['KDJ_K', 'KDJ_D', 'KDJ_J'],
        'category': 'momentum'
    },
    'ROC': {
        'function': 'calculate_roc',
        'description': '变动率指标',
        'parameters': {'period': int},
        'default_params': {'period': 12},
        'output_columns': ['ROC'],
        'category': 'momentum'
    },

    # 波动率指标
    'ATR': {
        'function': 'calculate_atr',
        'description': '平均真实波幅',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['ATR'],
        'category': 'volatility'
    },
    'BB': {
        'function': 'calculate_bollinger_bands',
        'description': '布林带',
        'parameters': {'period': int, 'std_dev': float},
        'default_params': {'period': 20, 'std_dev': 2.0},
        'output_columns': ['BB_UPPER', 'BB_MIDDLE', 'BB_LOWER'],
        'category': 'volatility'
    },
    'DMI': {
        'function': 'calculate_dmi',
        'description': '方向运动指标',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['DMI_PDI', 'DMI_MDI', 'DMI_ADX', 'DMI_ADXR'],
        'category': 'trend'
    },

    # 成交量指标
    'OBV': {
        'function': 'calculate_obv',
        'description': '能量潮指标',
        'parameters': {},
        'default_params': {},
        'output_columns': ['OBV'],
        'category': 'volume'
    },
    'VOLUME_SMA': {
        'function': 'calculate_volume_sma',
        'description': '成交量简单移动平均',
        'parameters': {'period': int},
        'default_params': {'period': 20},
        'output_columns': ['VOLUME_SMA'],
        'category': 'volume'
    },
    'VWAP': {
        'function': 'calculate_vwap',
        'description': '成交量加权平均价格',
        'parameters': {},
        'default_params': {},
        'output_columns': ['VWAP'],
        'category': 'volume'
    },
    'MFI': {
        'function': 'calculate_mfi',
        'description': '资金流量指标',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['MFI'],
        'category': 'volume'
    },

    # 价格指标
    'AD': {
        'function': 'calculate_adline',
        'description': '累积/派发线',
        'parameters': {},
        'default_params': {},
        'output_columns': ['AD'],
        'category': 'price'
    },
    'CHAIKIN_OSC': {
        'function': 'calculate_chaikin_oscillator',
        'description': '蔡金震荡指标',
        'parameters': {'fast_period': int, 'slow_period': int},
        'default_params': {'fast_period': 3, 'slow_period': 10},
        'output_columns': ['CHAIKIN_OSC'],
        'category': 'price'
    },
    'EOM': {
        'function': 'calculate_ease_of_movement',
        'description': '简易波动指标',
        'parameters': {'period': int},
        'default_params': {'period': 14},
        'output_columns': ['EOM'],
        'category': 'price'
    },

    # 高级量能指标
    'VROC': {
        'function': 'calculate_vroc',
        'description': '成交量变动率',
        'parameters': {'period': int},
        'default_params': {'period': 12},
        'output_columns': ['VROC'],
        'category': 'volume'
    },
    'CMF': {
        'function': 'calculate_cmf',
        'description': '蔡金资金流量',
        'parameters': {'period': int},
        'default_params': {'period': 20},
        'output_columns': ['CMF'],
        'category': 'volume'
    },
    'FORCE_INDEX': {
        'function': 'calculate_force_index',
        'description': '强力指数',
        'parameters': {'period': int},
        'default_params': {'period': 13},
        'output_columns': ['FORCE_INDEX'],
        'category': 'volume'
    },
    'VOLUME_OSCILLATOR': {
        'function': 'calculate_volume_oscillator',
        'description': '成交量震荡器',
        'parameters': {'short_period': int, 'long_period': int},
        'default_params': {'short_period': 5, 'long_period': 10},
        'output_columns': ['VOLUME_OSCILLATOR'],
        'category': 'volume'
    },
    'NVI': {
        'function': 'calculate_nvi',
        'description': '负成交量指数',
        'parameters': {},
        'default_params': {},
        'output_columns': ['NVI'],
        'category': 'volume'
    },
    'PVI': {
        'function': 'calculate_pvi',
        'description': '正成交量指数',
        'parameters': {},
        'default_params': {},
        'output_columns': ['PVI'],
        'category': 'volume'
    },

    # 量价背离指标
    'PVT': {
        'function': 'calculate_pvt',
        'description': '价量趋势指标',
        'parameters': {},
        'default_params': {},
        'output_columns': ['PVT'],
        'category': 'price_volume'
    },
    'VPT': {
        'function': 'calculate_vpt',
        'description': '成交量价格趋势',
        'parameters': {},
        'default_params': {},
        'output_columns': ['VPT'],
        'category': 'price_volume'
    },
    'KLINGER': {
        'function': 'calculate_klinger_oscillator',
        'description': '克林格震荡器',
        'parameters': {'fast_period': int, 'slow_period': int, 'signal_period': int},
        'default_params': {'fast_period': 34, 'slow_period': 55, 'signal_period': 13},
        'output_columns': ['KLINGER', 'KLINGER_SIGNAL'],
        'category': 'price_volume'
    },
    'BW_MFI': {
        'function': 'calculate_bw_mfi',
        'description': '市场便利指数',
        'parameters': {},
        'default_params': {},
        'output_columns': ['BW_MFI'],
        'category': 'price_volume'
    },

    # 复合指标
    'VOLATILITY_ADVANCED': {
        'function': 'calculate_volatility_advanced',
        'description': '高级波动率指标',
        'parameters': {'period': int, 'method': str},
        'default_params': {'period': 20, 'method': 'close'},
        'output_columns': ['VOLATILITY', 'VOLATILITY_RATIO'],
        'category': 'volatility'
    },
    'VOLUME_ENHANCED_VOLATILITY': {
        'function': 'calculate_volume_enhanced_volatility',
        'description': '成交量增强波动率',
        'parameters': {'volatility_period': int, 'volume_period': int},
        'default_params': {'volatility_period': 20, 'volume_period': 20},
        'output_columns': ['VOLATILITY', 'VOLUME_RATIO', 'COMBINED_SCORE'],
        'category': 'volatility'
    }
}

# 指标类别定义
INDICATOR_CATEGORIES = {
    'trend': '趋势指标',
    'momentum': '动量指标',
    'volatility': '波动率指标',
    'volume': '成交量指标',
    'price': '价格指标',
    'price_volume': '量价背离指标'
}

# 标准指标列名映射（用于复合指标）
STANDARD_INDICATOR_COLUMNS = {
    # 移动平均线
    'SMA': 'SMA',
    'EMA': 'EMA',
    'TEMA': 'TEMA',
    'WMA': 'WMA',

    # 动量指标
    'RSI': 'RSI',
    'STOCH_K': 'STOCH_K',
    'STOCH_D': 'STOCH_D',
    'CCI': 'CCI',
    'MACD': 'MACD',
    'MACD_SIGNAL': 'MACD_SIGNAL',
    'MACD_HIST': 'MACD_HIST',
    'WILLR': 'WILLR',
    'KDJ_K': 'KDJ_K',
    'KDJ_D': 'KDJ_D',
    'KDJ_J': 'KDJ_J',
    'ROC': 'ROC',

    # 波动率指标
    'ATR': 'ATR',
    'BB_UPPER': 'BB_UPPER',
    'BB_MIDDLE': 'BB_MIDDLE',
    'BB_LOWER': 'BB_LOWER',
    'DMI_PDI': 'DMI_PDI',
    'DMI_MDI': 'DMI_MDI',
    'DMI_ADX': 'DMI_ADX',
    'DMI_ADXR': 'DMI_ADXR',
    'VOLATILITY': 'VOLATILITY',
    'VOLATILITY_RATIO': 'VOLATILITY_RATIO',

    # 成交量指标
    'OBV': 'OBV',
    'VOLUME_SMA': 'VOLUME_SMA',
    'VWAP': 'VWAP',
    'MFI': 'MFI',
    'VOLUME_RATIO': 'VOLUME_RATIO',

    # 高级量能指标
    'VROC': 'VROC',
    'CMF': 'CMF',
    'FORCE_INDEX': 'FORCE_INDEX',
    'VOLUME_OSCILLATOR': 'VOLUME_OSCILLATOR',
    'NVI': 'NVI',
    'PVI': 'PVI',

    # 量价背离指标
    'PVT': 'PVT',
    'VPT': 'VPT',
    'KLINGER': 'KLINGER',
    'KLINGER_SIGNAL': 'KLINGER_SIGNAL',
    'BW_MFI': 'BW_MFI',

    # 价格指标
    'AD': 'AD',
    'CHAIKIN_OSC': 'CHAIKIN_OSC',
    'EOM': 'EOM',

    # 复合指标
    'COMBINED_SCORE': 'COMBINED_SCORE'
}

# ============================================================================
# 数据验证函数
# ============================================================================

def validate_kline_dataframe(df: pd.DataFrame,
                           required_columns: Optional[List[str]] = None) -> bool:
    """
    验证K线DataFrame是否符合标准格式

    Args:
        df: 要验证的DataFrame
        required_columns: 必需的列名列表，默认使用STANDARD_OHLCV_COLUMNS

    Returns:
        bool: 是否符合标准格式
    """
    if df is None or df.empty:
        return False

    if required_columns is None:
        required_columns = list(STANDARD_OHLCV_COLUMNS.keys())

    # 检查必需列是否存在
    for col in required_columns:
        if col not in df.columns:
            return False

    # 检查索引是否为datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    return True

def standardize_kline_dataframe(df: pd.DataFrame,
                              fill_missing: bool = True) -> pd.DataFrame:
    """
    将DataFrame标准化为标准K线格式

    Args:
        df: 输入的DataFrame
        fill_missing: 是否填充缺失的列

    Returns:
        pd.DataFrame: 标准化的DataFrame
    """
    if df is None or df.empty:
        return df

    df_copy = df.copy()

    # 确保索引是datetime类型
    if not isinstance(df_copy.index, pd.DatetimeIndex):
        try:
            df_copy.index = pd.to_datetime(df_copy.index)
        except:
            # 如果转换失败，创建新的datetime索引
            df_copy.index = pd.date_range(start=datetime.now(), periods=len(df_copy), freq='D')

    # 确保所有标准列都存在
    for col in STANDARD_KLINE_COLUMNS.keys():
        if col not in df_copy.columns:
            if fill_missing:
                if col in STANDARD_DTYPES:
                    dtype = STANDARD_DTYPES[col]
                    if 'int' in dtype:
                        df_copy[col] = pd.Series(dtype='int64', index=df_copy.index)
                    else:
                        df_copy[col] = pd.Series(dtype='float64', index=df_copy.index)
                else:
                    df_copy[col] = pd.Series(dtype='float64', index=df_copy.index)
            else:
                continue

    # 重新排列列的顺序
    existing_columns = [col for col in STANDARD_KLINE_COLUMNS.keys() if col in df_copy.columns]
    other_columns = [col for col in df_copy.columns if col not in STANDARD_KLINE_COLUMNS]

    df_copy = df_copy[existing_columns + other_columns]

    return df_copy

def create_empty_kline_dataframe(index: Optional[pd.DatetimeIndex] = None,
                               periods: int = 100) -> pd.DataFrame:
    """
    创建空的标准化K线DataFrame

    Args:
        index: 日期时间索引，如果为None则创建新的
        periods: 如果index为None，要创建的周期数

    Returns:
        pd.DataFrame: 空的标准化K线DataFrame
    """
    if index is None:
        index = pd.date_range(start=datetime.now(), periods=periods, freq='D')

    # 创建空DataFrame
    df = pd.DataFrame(index=index)

    # 添加所有标准列
    for col, dtype in STANDARD_DTYPES.items():
        if 'int' in dtype:
            df[col] = pd.Series(dtype='int64', index=index)
        else:
            df[col] = pd.Series(dtype='float64', index=index)

    return df

# ============================================================================
# 数据转换函数
# ============================================================================

def convert_series_to_dataframe(series: pd.Series,
                               column_name: str) -> pd.DataFrame:
    """
    将Series转换为标准DataFrame格式

    Args:
        series: 输入的Series
        column_name: 列名

    Returns:
        pd.DataFrame: 标准化的DataFrame
    """
    if series is None or series.empty:
        return pd.DataFrame()

    return pd.DataFrame({column_name: series})

def merge_indicator_dataframes(*dfs: pd.DataFrame) -> pd.DataFrame:
    """
    合并多个指标DataFrame

    Args:
        *dfs: 要合并的DataFrame

    Returns:
        pd.DataFrame: 合并后的DataFrame
    """
    if not dfs:
        return pd.DataFrame()

    # 过滤掉None和空DataFrame
    valid_dfs = [df for df in dfs if df is not None and not df.empty]

    if not valid_dfs:
        return pd.DataFrame()

    # 使用outer join合并所有DataFrame
    result = valid_dfs[0]
    for df in valid_dfs[1:]:
        result = result.join(df, how='outer')

    return result

# ============================================================================
# 缓存键生成函数
# ============================================================================

def generate_kline_cache_key(stock_code: str, timeframe: str) -> str:
    """
    生成K线数据缓存键

    Args:
        stock_code: 股票代码
        timeframe: 时间粒度

    Returns:
        str: 缓存键
    """
    return f"{stock_code}_{timeframe}"

def generate_indicator_cache_key(stock_code: str,
                               timeframe: str,
                               indicator_type: str,
                               period: Union[int, str]) -> str:
    """
    生成指标数据缓存键

    Args:
        stock_code: 股票代码
        timeframe: 时间粒度
        indicator_type: 指标类型
        period: 周期参数

    Returns:
        str: 缓存键
    """
    return f"{indicator_type}_{stock_code}_{timeframe}_{period}"

# ============================================================================
# 数据质量检查函数
# ============================================================================

def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    检查DataFrame数据质量

    Args:
        df: 要检查的DataFrame

    Returns:
        dict: 数据质量报告
    """
    if df is None or df.empty:
        return {'valid': False, 'message': 'DataFrame is None or empty'}

    report = {
        'valid': True,
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': {},
        'data_types': {},
        'issues': []
    }

    # 检查每列的缺失值
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        report['missing_values'][col] = missing_count
        report['data_types'][col] = str(df[col].dtype)

        if missing_count > 0:
            report['issues'].append(f"Column '{col}' has {missing_count} missing values")

    # 检查数据类型是否符合标准
    for col in STANDARD_DTYPES:
        if col in df.columns:
            expected_dtype = STANDARD_DTYPES[col]
            actual_dtype = str(df[col].dtype)

            if expected_dtype not in actual_dtype:
                report['issues'].append(f"Column '{col}' dtype mismatch: expected {expected_dtype}, got {actual_dtype}")

    # 检查是否有重复索引
    if df.index.duplicated().any():
        duplicate_count = df.index.duplicated().sum()
        report['issues'].append(f"Found {duplicate_count} duplicate index values")

    return report

# ============================================================================
# 便捷函数
# ============================================================================

def get_supported_timeframes() -> List[str]:
    """获取支持的时间粒度列表"""
    return list(TIMEFRAME_CONFIG.keys())

def get_timeframe_config(timeframe: str) -> Optional[Dict[str, Any]]:
    """获取指定时间粒度的配置"""
    return TIMEFRAME_CONFIG.get(timeframe)

def get_indicator_config(indicator_name: str) -> Optional[Dict[str, Any]]:
    """
    获取指定指标的配置信息

    Args:
        indicator_name: 指标名称

    Returns:
        dict: 指标配置信息，如果不存在返回None
    """
    return INDICATOR_CONFIG.get(indicator_name)

def get_indicators_by_category(category: str) -> List[str]:
    """
    获取指定类别的所有指标名称

    Args:
        category: 指标类别

    Returns:
        list: 指标名称列表
    """
    return [name for name, config in INDICATOR_CONFIG.items()
            if config.get('category') == category]

def get_all_indicators() -> List[str]:
    """
    获取所有支持的指标名称

    Returns:
        list: 所有指标名称列表
    """
    return list(INDICATOR_CONFIG.keys())

def validate_indicator_dataframe(df: pd.DataFrame,
                               indicator_name: str) -> bool:
    """
    验证指标DataFrame是否符合标准格式

    Args:
        df: 要验证的DataFrame
        indicator_name: 指标名称

    Returns:
        bool: 是否符合标准格式
    """
    if df is None or df.empty:
        return False

    config = get_indicator_config(indicator_name)
    if config is None:
        return False

    # 检查必需的输出列是否存在
    required_columns = config.get('output_columns', [])
    for col in required_columns:
        if col not in df.columns:
            return False

    # 检查索引是否为datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        return False

    return True

def standardize_indicator_dataframe(df: pd.DataFrame,
                                  indicator_name: str,
                                  fill_missing: bool = True) -> pd.DataFrame:
    """
    将指标DataFrame标准化为标准格式

    Args:
        df: 输入的DataFrame
        indicator_name: 指标名称
        fill_missing: 是否填充缺失的列

    Returns:
        pd.DataFrame: 标准化的DataFrame
    """
    if df is None or df.empty:
        return df

    config = get_indicator_config(indicator_name)
    if config is None:
        return df

    df_copy = df.copy()

    # 确保索引是datetime类型
    if not isinstance(df_copy.index, pd.DatetimeIndex):
        try:
            df_copy.index = pd.to_datetime(df_copy.index)
        except:
            # 如果转换失败，创建新的datetime索引
            df_copy.index = pd.date_range(start=datetime.now(), periods=len(df_copy), freq='D')

    # 确保所有标准指标列都存在
    expected_columns = config.get('output_columns', [])
    for col in expected_columns:
        if col not in df_copy.columns:
            if fill_missing:
                df_copy[col] = pd.Series(dtype='float64', index=df_copy.index)
            else:
                continue

    return df_copy

def create_indicator_cache_key(stock_code: str,
                             timeframe: str,
                             indicator_name: str,
                             **params) -> str:
    """
    创建指标缓存键

    Args:
        stock_code: 股票代码
        timeframe: 时间粒度
        indicator_name: 指标名称
        **params: 指标参数

    Returns:
        str: 缓存键
    """
    # 获取默认参数
    config = get_indicator_config(indicator_name)
    if config:
        default_params = config.get('default_params', {})
        # 合并参数，使用提供的参数覆盖默认参数
        merged_params = {**default_params, **params}
    else:
        merged_params = params

    # 创建参数字符串
    param_str = '_'.join([f"{k}_{v}" for k, v in sorted(merged_params.items())])

    if param_str:
        return f"{indicator_name}_{stock_code}_{timeframe}_{param_str}"
    else:
        return f"{indicator_name}_{stock_code}_{timeframe}"

def get_indicator_function_name(indicator_name: str) -> Optional[str]:
    """
    获取指标对应的处理函数名称

    Args:
        indicator_name: 指标名称

    Returns:
        str: 函数名称，如果不存在返回None
    """
    config = get_indicator_config(indicator_name)
    if config:
        return config.get('function')
    return None

def get_indicator_description(indicator_name: str) -> str:
    """
    获取指标的描述信息

    Args:
        indicator_name: 指标名称

    Returns:
        str: 指标描述
    """
    config = get_indicator_config(indicator_name)
    if config:
        return config.get('description', indicator_name)
    return indicator_name

def get_indicator_parameters(indicator_name: str) -> Dict[str, Any]:
    """
    获取指标的参数信息

    Args:
        indicator_name: 指标名称

    Returns:
        dict: 参数信息
    """
    config = get_indicator_config(indicator_name)
    if config:
        return config.get('parameters', {})
    return {}

def get_indicator_default_params(indicator_name: str) -> Dict[str, Any]:
    """
    获取指标的默认参数

    Args:
        indicator_name: 指标名称

    Returns:
        dict: 默认参数
    """
    config = get_indicator_config(indicator_name)
    if config:
        return config.get('default_params', {})
    return {}
