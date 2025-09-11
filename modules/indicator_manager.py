"""
统一指标计算引擎 (Unified Indicator Calculation Engine)
合并指标管理器和算法模块的核心功能

职责：
- 技术指标的计算和管理
- 统一的指标计算接口
- 指标结果缓存和管理
- 批量指标计算支持
- 直接集成所有计算算法
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Union
import sys
import os
import warnings
import talib

# 禁用警告
warnings.filterwarnings('ignore')

from .data_formats import (
    STANDARD_OHLCV_COLUMNS,
    EXTENDED_MARKET_COLUMNS,
    STANDARD_INDICATOR_COLUMNS,
    get_indicator_config,
    validate_kline_dataframe,
    validate_indicator_dataframe
)

# 标准字段定义
STANDARD_FIELDS = list(STANDARD_OHLCV_COLUMNS.keys()) + list(EXTENDED_MARKET_COLUMNS.keys())
OHLCV_FIELDS = list(STANDARD_OHLCV_COLUMNS.keys())

# 导入配置
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.unified_config import *
except ImportError:
    # 提供默认配置
    VOLATILITY_MODE = "tonghuashun"
    TONGHUASHUN_CONFIG = {
        'enable_volume_enhancement': True,
        'volume_weight_limit': [0.1, 5.0],
        'N': 10,
    }
    ADVANCED_CONFIG = {
        "N": 10,
        "M": 5,
        "Extra": 20,
        "trend_type": 1,
    }


class UnifiedIndicatorEngine:
    """
    统一指标计算引擎
    合并指标管理和算法计算的核心功能
    """

    # 指标计算配置
    INDICATOR_CONFIG = {
        # 移动平均线周期
        "ma_period": [5, 10, 20, 60],
        # RSI配置
        "rsi_period": 14,
        "rsi_oversold": 30,
        "rsi_overbought": 70,
        "rsi_extreme_low": 10,
        "rsi_extreme_high": 90,
        # 数据处理配置
        "history_days": 100,
        "volume_avg_days": 20,
        "volume_ratio_multiplier": 2,
    }

    def __init__(self, datastore):
        """
        初始化统一指标计算引擎

        Args:
            datastore: 数据存储实例
        """
        self.ds = datastore
        self.data_processor = datastore  # 直接使用 datastore，它现在包含了 processor 功能

        # 缓存和统计
        self._result_cache = {}
        self._calculation_count = 0

        # 指标注册表
        self.indicators = self._build_indicator_registry()

        print("🚀 统一指标计算引擎初始化完成")
        print(f"   数据处理器: ✅")
        print(f"   指标注册表: ✅ ({len(self.indicators)} 个指标)")
        print(f"   TA-Lib状态: ✅")

    def _build_indicator_registry(self) -> Dict[str, Dict[str, Any]]:
        """构建指标注册表"""
        return {
            'SMA': {
                'name': '简单移动平均',
                'category': '趋势指标',
                'default_params': {'period': 20},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算指定周期的简单移动平均值'
            },
            'EMA': {
                'name': '指数移动平均',
                'category': '趋势指标',
                'default_params': {'period': 20},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算指定周期的指数移动平均值'
            },
            'RSI': {
                'name': '相对强弱指数',
                'category': '动量指标',
                'default_params': {'period': 14},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算相对强弱指数，范围0-100'
            },
            'MACD': {
                'name': 'MACD指标',
                'category': '动量指标',
                'default_params': {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
                'required_columns': ['close'],
                'output_type': 'dict',
                'description': '计算MACD指标，包括MACD线、信号线和柱状图'
            },
            'BOLLINGER_BANDS': {
                'name': '布林带',
                'category': '波动率指标',
                'default_params': {'period': 20, 'std_dev': 2.0},
                'required_columns': ['close'],
                'output_type': 'dict',
                'description': '计算布林带，包括上轨、中轨和下轨'
            },
            'ATR': {
                'name': '平均真实波幅',
                'category': '波动率指标',
                'default_params': {'period': 14},
                'required_columns': ['high', 'low', 'close'],
                'output_type': 'series',
                'description': '计算平均真实波幅'
            },
            'STOCHASTIC': {
                'name': '随机指标',
                'category': '动量指标',
                'default_params': {'k_period': 14, 'd_period': 3},
                'required_columns': ['high', 'low', 'close'],
                'output_type': 'dict',
                'description': '计算随机指标KDJ'
            },
            'OBV': {
                'name': '能量潮指标',
                'category': '成交量指标',
                'default_params': {},
                'required_columns': ['close', 'volume'],
                'output_type': 'series',
                'description': '计算能量潮指标'
            },
            'CCI': {
                'name': '顺势指标',
                'category': '动量指标',
                'default_params': {'period': 20},
                'required_columns': ['high', 'low', 'close'],
                'output_type': 'series',
                'description': '计算顺势指标'
            },
            'ROC': {
                'name': '变动率指标',
                'category': '动量指标',
                'default_params': {'period': 10},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算变动率指标'
            },
            'TEMA': {
                'name': '三重指数移动平均',
                'category': '趋势指标',
                'default_params': {'period': 20},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算三重指数移动平均'
            },
            'WMA': {
                'name': '加权移动平均',
                'category': '趋势指标',
                'default_params': {'period': 20},
                'required_columns': ['close'],
                'output_type': 'series',
                'description': '计算加权移动平均'
            },
            'DMI': {
                'name': '方向运动指标',
                'category': '趋势指标',
                'default_params': {'period': 14},
                'required_columns': ['high', 'low', 'close'],
                'output_type': 'dict',
                'description': '计算方向运动指标'
            }
        }

    def calculate_indicator(self, key: str, indicator: str,
                          start_index: Optional[int] = None,
                          count: Optional[int] = None,
                          **params) -> Union[pd.Series, Dict[str, pd.Series], None]:
        """
        计算单个指标

        Args:
            key: 数据键值
            indicator: 指标名称
            start_index: 起始索引
            count: 数据点数量
            **params: 指标参数

        Returns:
            计算结果
        """
        try:
            indicator = indicator.upper()

            # 检查指标是否支持
            indicator_info = self.indicators.get(indicator)
            if indicator_info is None:
                print(f"❌ 不支持的指标: {indicator}")
                return None

            # 获取数据
            required_columns = indicator_info['required_columns']
            if len(required_columns) == 1 and required_columns[0] == 'close':
                # 单列数据
                data = self.data_processor.get_data_with_range(key, start_index, count)
            else:
                # 多列数据（OHLCV等）
                data = self.data_processor.get_ohlcv_data(key, start_index, count)

            if data is None or data.empty:
                print(f"❌ 无法获取数据进行{indicator}计算")
                return None

            # 验证必需列
            missing_cols = [col for col in required_columns if col not in data.columns]
            if missing_cols:
                print(f"❌ {indicator}计算缺少必需列: {missing_cols}")
                return None

            # 验证数据质量
            if not validate_kline_dataframe(data):
                print(f"⚠️ {indicator}计算数据质量不符合标准格式")
                # 尝试标准化数据
                try:
                    from .data_formats import standardize_kline_dataframe
                    data = standardize_kline_dataframe(data)
                    print(f"✅ 数据已标准化")
                except Exception as e:
                    print(f"❌ 数据标准化失败: {e}")
                    return None

            # 合并参数
            calc_params = indicator_info['default_params'].copy()
            calc_params.update(params)

            # 直接调用内部计算方法
            result = self._calculate_indicator_internal(data, indicator, calc_params)

            if result is not None:
                self._calculation_count += 1

                # 验证指标结果格式
                if isinstance(result, pd.Series):
                    # 单值指标验证
                    if not validate_indicator_dataframe(pd.DataFrame({indicator: result}), indicator):
                        print(f"⚠️ {indicator}结果格式不符合标准")
                elif isinstance(result, dict):
                    # 多值指标验证
                    result_df = pd.DataFrame(result)
                    for col in result_df.columns:
                        if col in STANDARD_INDICATOR_COLUMNS:
                            if not validate_indicator_dataframe(result_df[[col]], col):
                                print(f"⚠️ {indicator}结果列{col}格式不符合标准")

                # 如果请求了特定数量，截取结果
                if count is not None and count > 0:
                    result = self._truncate_result(result, count)

            return result

        except Exception as e:
            print(f"❌ {indicator}计算失败: {e}")
            return None

    def _calculate_indicator_internal(self, data: pd.DataFrame, indicator: str,
                                    params: Dict[str, Any]) -> Union[pd.Series, Dict[str, pd.Series], None]:
        """内部指标计算方法"""
        try:
            if indicator == 'SMA':
                return self._calculate_sma(data, **params)
            elif indicator == 'EMA':
                return self._calculate_ema(data, **params)
            elif indicator == 'RSI':
                return self._calculate_rsi(data, **params)
            elif indicator == 'MACD':
                return self._calculate_macd(data, **params)
            elif indicator == 'BOLLINGER_BANDS':
                return self._calculate_bollinger_bands(data, **params)
            elif indicator == 'ATR':
                return self._calculate_atr(data, **params)
            elif indicator == 'STOCHASTIC':
                return self._calculate_stochastic(data, **params)
            elif indicator == 'OBV':
                return self._calculate_obv(data, **params)
            elif indicator == 'CCI':
                return self._calculate_cci(data, **params)
            elif indicator == 'ROC':
                return self._calculate_roc(data, **params)
            elif indicator == 'TEMA':
                return self._calculate_tema(data, **params)
            elif indicator == 'WMA':
                return self._calculate_wma(data, **params)
            elif indicator == 'DMI':
                return self._calculate_dmi(data, **params)
            else:
                print(f"❌ 未实现的指标: {indicator}")
                return None

        except Exception as e:
            print(f"❌ {indicator}内部计算失败: {e}")
            return None

    # ============================================================================
    # 核心算法实现
    # ============================================================================

    def _calculate_sma(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算简单移动平均"""
        prices = data['close']

        if period <= 0:
            raise ValueError("周期必须大于0")

        if len(prices) < period:
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['SMA'])

        try:
            result = talib.SMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['SMA'])
        except Exception as e:
            print(f"❌ SMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['SMA'])

    def _calculate_ema(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算指数移动平均"""
        prices = data['close']

        if period <= 0:
            raise ValueError("周期必须大于0")

        try:
            result = talib.EMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['EMA'])
        except Exception as e:
            print(f"❌ EMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['EMA'])

    def _calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算相对强弱指数"""
        prices = data['close']

        if period <= 0:
            raise ValueError("周期必须大于0")

        try:
            result = talib.RSI(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['RSI'])
        except Exception as e:
            print(f"❌ RSI计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['RSI'])

    def _calculate_macd(self, data: pd.DataFrame, fast_period: int = 12,
                       slow_period: int = 26, signal_period: int = 9) -> Dict[str, pd.Series]:
        """计算MACD指标"""
        prices = data['close']

        try:
            # 计算快速和慢速EMA
            fast_ema = talib.EMA(prices.values.astype(float), timeperiod=fast_period)
            slow_ema = talib.EMA(prices.values.astype(float), timeperiod=slow_period)

            # 计算MACD线
            macd_line = fast_ema - slow_ema

            # 计算信号线
            signal_line = talib.EMA(macd_line, timeperiod=signal_period)

            # 计算MACD柱状图
            macd_hist = macd_line - signal_line

            return {
                STANDARD_INDICATOR_COLUMNS['MACD']: pd.Series(macd_line, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['MACD']),
                STANDARD_INDICATOR_COLUMNS['MACD_SIGNAL']: pd.Series(signal_line, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['MACD_SIGNAL']),
                STANDARD_INDICATOR_COLUMNS['MACD_HIST']: pd.Series(macd_hist, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['MACD_HIST'])
            }

        except Exception as e:
            print(f"❌ MACD计算失败: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return {
                STANDARD_INDICATOR_COLUMNS['MACD']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['MACD_SIGNAL']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['MACD_HIST']: empty_series.copy()
            }

    def _calculate_bollinger_bands(self, data: pd.DataFrame, period: int = 20,
                                 std_dev: float = 2.0) -> Dict[str, pd.Series]:
        """计算布林带"""
        prices = data['close']

        try:
            # 计算中轨（简单移动平均）
            middle_band = talib.SMA(prices.values.astype(float), timeperiod=period)

            # 计算标准差
            std = talib.STDDEV(prices.values.astype(float), timeperiod=period, nbdev=1)

            # 计算上轨和下轨
            upper_band = middle_band + (std * std_dev)
            lower_band = middle_band - (std * std_dev)

            return {
                STANDARD_INDICATOR_COLUMNS['BB_UPPER']: pd.Series(upper_band, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['BB_UPPER']),
                STANDARD_INDICATOR_COLUMNS['BB_MIDDLE']: pd.Series(middle_band, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['BB_MIDDLE']),
                STANDARD_INDICATOR_COLUMNS['BB_LOWER']: pd.Series(lower_band, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['BB_LOWER'])
            }

        except Exception as e:
            print(f"❌ 布林带计算失败: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return {
                STANDARD_INDICATOR_COLUMNS['BB_UPPER']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['BB_MIDDLE']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['BB_LOWER']: empty_series.copy()
            }

    def _calculate_atr(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算平均真实波幅"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            result = talib.ATR(high.values.astype(float), low.values.astype(float),
                             close.values.astype(float), timeperiod=period)
            return pd.Series(result, index=close.index, name=STANDARD_INDICATOR_COLUMNS['ATR'])
        except Exception as e:
            print(f"❌ ATR计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['ATR'])

    def _calculate_stochastic(self, data: pd.DataFrame, k_period: int = 14,
                            d_period: int = 3) -> Dict[str, pd.Series]:
        """计算随机指标"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            k_values, d_values = talib.STOCH(high.values.astype(float), low.values.astype(float),
                                           close.values.astype(float), fastk_period=k_period,
                                           slowk_period=3, slowd_period=d_period)

            return {
                STANDARD_INDICATOR_COLUMNS['STOCH_K']: pd.Series(k_values, index=close.index, name=STANDARD_INDICATOR_COLUMNS['STOCH_K']),
                STANDARD_INDICATOR_COLUMNS['STOCH_D']: pd.Series(d_values, index=close.index, name=STANDARD_INDICATOR_COLUMNS['STOCH_D'])
            }

        except Exception as e:
            print(f"❌ 随机指标计算失败: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return {
                STANDARD_INDICATOR_COLUMNS['STOCH_K']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['STOCH_D']: empty_series.copy()
            }

    def _calculate_cci(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算顺势指标"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            result = talib.CCI(high.values.astype(float), low.values.astype(float),
                             close.values.astype(float), timeperiod=period)
            return pd.Series(result, index=close.index, name=STANDARD_INDICATOR_COLUMNS['CCI'])
        except Exception as e:
            print(f"❌ CCI计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['CCI'])

    def _calculate_roc(self, data: pd.DataFrame, period: int = 10) -> pd.Series:
        """计算变动率指标"""
        prices = data['close']

        try:
            result = talib.ROC(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['ROC'])
        except Exception as e:
            print(f"❌ ROC计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['ROC'])

    def _calculate_tema(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算三重指数移动平均"""
        prices = data['close']

        try:
            result = talib.TEMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['TEMA'])
        except Exception as e:
            print(f"❌ TEMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['TEMA'])

    def _calculate_wma(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算加权移动平均"""
        prices = data['close']

        try:
            result = talib.WMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=STANDARD_INDICATOR_COLUMNS['WMA'])
        except Exception as e:
            print(f"❌ WMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['WMA'])

    def _calculate_dmi(self, data: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
        """计算方向运动指标"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            # 计算DMI指标
            plus_di = talib.PLUS_DI(high.values.astype(float), low.values.astype(float),
                                  close.values.astype(float), timeperiod=period)
            minus_di = talib.MINUS_DI(high.values.astype(float), low.values.astype(float),
                                    close.values.astype(float), timeperiod=period)
            adx = talib.ADX(high.values.astype(float), low.values.astype(float),
                          close.values.astype(float), timeperiod=period)
            adxr = talib.ADXR(high.values.astype(float), low.values.astype(float),
                            close.values.astype(float), timeperiod=period)

            return {
                STANDARD_INDICATOR_COLUMNS['DMI_PDI']: pd.Series(plus_di, index=close.index, name=STANDARD_INDICATOR_COLUMNS['DMI_PDI']),
                STANDARD_INDICATOR_COLUMNS['DMI_MDI']: pd.Series(minus_di, index=close.index, name=STANDARD_INDICATOR_COLUMNS['DMI_MDI']),
                STANDARD_INDICATOR_COLUMNS['DMI_ADX']: pd.Series(adx, index=close.index, name=STANDARD_INDICATOR_COLUMNS['DMI_ADX']),
                STANDARD_INDICATOR_COLUMNS['DMI_ADXR']: pd.Series(adxr, index=close.index, name=STANDARD_INDICATOR_COLUMNS['DMI_ADXR'])
            }

        except Exception as e:
            print(f"❌ DMI计算失败: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return {
                STANDARD_INDICATOR_COLUMNS['DMI_PDI']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['DMI_MDI']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['DMI_ADX']: empty_series.copy(),
                STANDARD_INDICATOR_COLUMNS['DMI_ADXR']: empty_series.copy()
            }

    def _calculate_obv(self, data: pd.DataFrame) -> pd.Series:
        """计算能量潮指标"""
        close = data['close']
        volume = data['volume']

        try:
            result = talib.OBV(close.values.astype(float), volume.values.astype(float))
            return pd.Series(result, index=close.index, name=STANDARD_INDICATOR_COLUMNS['OBV'])
        except Exception as e:
            print(f"❌ OBV计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['OBV'])

    # ============================================================================
    # 批量计算和工具方法
    # ============================================================================

    def batch_calculate(self, key: str, indicators: Dict[str, Dict[str, Any]],
                       start_index: Optional[int] = None, count: Optional[int] = None) -> Dict[str, Any]:
        """
        批量计算指标

        Args:
            key: 数据键值
            indicators: 指标配置字典
            start_index: 起始索引
            count: 数据点数量

        Returns:
            计算结果字典
        """
        results = {}

        for indicator_name, params in indicators.items():
            try:
                # 解析指标名称和参数
                if '_' in indicator_name:
                    parts = indicator_name.split('_')
                    indicator_type = parts[0]
                    if len(parts) > 1 and parts[1].isdigit():
                        period = int(parts[1])
                        params['period'] = period
                else:
                    indicator_type = indicator_name

                result = self.calculate_indicator(key, indicator_type, start_index, count, **params)
                if result is not None:
                    results[indicator_name] = result

            except Exception as e:
                print(f"❌ 批量计算{indicator_name}失败: {e}")
                continue

        return results

    def calculate_range_indicators(self, key: str, start_index: Optional[int] = None,
                                 count: Optional[int] = None,
                                 indicators_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算范围指标

        Args:
            key: 数据键值
            start_index: 起始索引
            count: 数据点数量
            indicators_list: 指标列表

        Returns:
            计算结果字典
        """
        if indicators_list is None:
            indicators_list = ['SMA_20', 'EMA_12', 'RSI_14', 'MACD', 'BOLLINGER_BANDS_20']

        # 转换指标列表为批量计算格式
        indicators_dict = {}
        for indicator_spec in indicators_list:
            if '_' in indicator_spec:
                parts = indicator_spec.split('_')
                indicator_name = parts[0]
                if len(parts) > 1 and parts[1].isdigit():
                    period = int(parts[1])
                    indicators_dict[indicator_spec] = {indicator_name: {'period': period}}
                else:
                    indicators_dict[indicator_spec] = {indicator_name: {}}
            else:
                indicators_dict[indicator_spec] = {indicator_spec: {}}

        return self.batch_calculate(key, indicators_dict, start_index, count)

    def _truncate_result(self, result: Union[pd.Series, Dict[str, pd.Series]],
                        count: int) -> Union[pd.Series, Dict[str, pd.Series]]:
        """截取结果到指定数量"""
        if isinstance(result, pd.Series):
            if len(result) > count:
                return result.tail(count)
            return result
        elif isinstance(result, dict):
            truncated = {}
            for key, series in result.items():
                if isinstance(series, pd.Series) and len(series) > count:
                    truncated[key] = series.tail(count)
                else:
                    truncated[key] = series
            return truncated
        return result

    # ============================================================================
    # 信息查询方法
    # ============================================================================

    def get_supported_indicators(self) -> List[str]:
        """获取支持的指标列表"""
        return list(self.indicators.keys())

    def get_indicator_info(self, indicator: str) -> Optional[Dict[str, Any]]:
        """获取指标信息"""
        return self.indicators.get(indicator.upper())

    def get_manager_stats(self) -> Dict[str, Any]:
        """获取管理器统计信息"""
        processor_stats = self.data_processor.get_processor_stats()

        return {
            'calculation_count': self._calculation_count,
            'cache_size': len(self._result_cache),
            'supported_indicators': len(self.indicators),
            'processor_stats': processor_stats,
            'talib_available': True
        }

    def clear_cache(self) -> None:
        """清空缓存"""
        self._result_cache.clear()
        self.data_processor.cleanup_processor()
        print("🧹 指标计算引擎缓存已清空")


# ============================================================================
# 便捷函数
# ============================================================================

def create_indicator_manager(datastore) -> UnifiedIndicatorEngine:
    """创建指标管理器实例"""
    return UnifiedIndicatorEngine(datastore)


# ============================================================================
# 向后兼容的静态方法
# ============================================================================

class IndicatorCalculator:
    """指标计算器，提供向后兼容的静态方法"""

    @staticmethod
    def calculate_sma(data: pd.DataFrame, period: int = 20, price_column: str = 'close') -> pd.Series:
        """计算简单移动平均（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        if period <= 0:
            raise ValueError("周期必须大于0")

        try:
            result = talib.SMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["SMA"]}_{period}')
        except Exception as e:
            print(f"❌ SMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["SMA"]}_{period}')

    @staticmethod
    def calculate_ema(data: pd.DataFrame, period: int = 20, price_column: str = 'close') -> pd.Series:
        """计算指数移动平均（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        if period <= 0:
            raise ValueError("周期必须大于0")

        try:
            result = talib.EMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["EMA"]}_{period}')
        except Exception as e:
            print(f"❌ EMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["EMA"]}_{period}')

    @staticmethod
    def calculate_rsi(data: pd.DataFrame, period: int = 14, price_column: str = 'close') -> pd.Series:
        """计算相对强弱指数（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        if period <= 0:
            raise ValueError("周期必须大于0")

        try:
            result = talib.RSI(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["RSI"]}_{period}')
        except Exception as e:
            print(f"❌ RSI计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["RSI"]}_{period}')
    @staticmethod
    def calculate_cci(data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算顺势指标（向后兼容）"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            result = talib.CCI(high.values.astype(float), low.values.astype(float),
                             close.values.astype(float), timeperiod=period)
            return pd.Series(result, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["CCI"]}_{period}')
        except Exception as e:
            print(f"❌ CCI计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["CCI"]}_{period}')

    @staticmethod
    def calculate_roc(data: pd.DataFrame, period: int = 10, price_column: str = 'close') -> pd.Series:
        """计算变动率指标（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        try:
            result = talib.ROC(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["ROC"]}_{period}')
        except Exception as e:
            print(f"❌ ROC计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["ROC"]}_{period}')

    @staticmethod
    def calculate_tema(data: pd.DataFrame, period: int = 20, price_column: str = 'close') -> pd.Series:
        """计算三重指数移动平均（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        try:
            result = talib.TEMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["TEMA"]}_{period}')
        except Exception as e:
            print(f"❌ TEMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["TEMA"]}_{period}')

    @staticmethod
    def calculate_wma(data: pd.DataFrame, period: int = 20, price_column: str = 'close') -> pd.Series:
        """计算加权移动平均（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        try:
            result = talib.WMA(prices.values.astype(float), timeperiod=period)
            return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["WMA"]}_{period}')
        except Exception as e:
            print(f"❌ WMA计算失败: {e}")
            return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["WMA"]}_{period}')

    @staticmethod
    def calculate_dmi(data: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
        """计算方向运动指标（向后兼容）"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            # 计算DMI指标
            plus_di = talib.PLUS_DI(high.values.astype(float), low.values.astype(float),
                                  close.values.astype(float), timeperiod=period)
            minus_di = talib.MINUS_DI(high.values.astype(float), low.values.astype(float),
                                    close.values.astype(float), timeperiod=period)
            adx = talib.ADX(high.values.astype(float), low.values.astype(float),
                          close.values.astype(float), timeperiod=period)
            adxr = talib.ADXR(high.values.astype(float), low.values.astype(float),
                            close.values.astype(float), timeperiod=period)

            return {
                'PDI': pd.Series(plus_di, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_PDI"]}_{period}'),
                'MDI': pd.Series(minus_di, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_MDI"]}_{period}'),
                'ADX': pd.Series(adx, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_ADX"]}_{period}'),
                'ADXR': pd.Series(adxr, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_ADXR"]}_{period}')
            }

        except Exception as e:
            print(f"❌ DMI计算失败: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return {
                'PDI': empty_series.copy(),
                'MDI': empty_series.copy(),
                'ADX': empty_series.copy(),
                'ADXR': empty_series.copy()
            }

    @staticmethod
    def calculate_stochastic(data: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> Dict[str, pd.Series]:
        """计算随机指标（向后兼容）"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            k_values, d_values = talib.STOCH(high.values.astype(float), low.values.astype(float),
                                           close.values.astype(float), fastk_period=k_period,
                                           slowk_period=3, slowd_period=d_period)

            return {
                'K': pd.Series(k_values, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["STOCH_K"]}_{k_period}'),
                'D': pd.Series(d_values, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["STOCH_D"]}_{d_period}')
            }

        except Exception as e:
            print(f"❌ 随机指标计算失败: {e}")
            empty_series = pd.Series(index=close.index, dtype=float)
            return {
                'K': empty_series.copy(),
                'D': empty_series.copy()
            }

    @staticmethod
    def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算平均真实波幅（向后兼容）"""
        high = data['high']
        low = data['low']
        close = data['close']

        try:
            result = talib.ATR(high.values.astype(float), low.values.astype(float),
                             close.values.astype(float), timeperiod=period)
            return pd.Series(result, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["ATR"]}_{period}')
        except Exception as e:
            print(f"❌ ATR计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["ATR"]}_{period}')

    @staticmethod
    def calculate_obv(data: pd.DataFrame) -> pd.Series:
        """计算能量潮指标（向后兼容）"""
        close = data['close']
        volume = data['volume']

        try:
            result = talib.OBV(close.values.astype(float), volume.values.astype(float))
            return pd.Series(result, index=close.index, name=STANDARD_INDICATOR_COLUMNS['OBV'])
        except Exception as e:
            print(f"❌ OBV计算失败: {e}")
            return pd.Series(index=close.index, dtype=float, name=STANDARD_INDICATOR_COLUMNS['OBV'])

    @staticmethod
    def calculate_macd(data: pd.DataFrame, fast_period: int = 12, slow_period: int = 26,
                      signal_period: int = 9, price_column: str = 'close') -> Dict[str, pd.Series]:
        """计算MACD指标（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        try:
            # 计算快速和慢速EMA
            fast_ema = talib.EMA(prices.values.astype(float), timeperiod=fast_period)
            slow_ema = talib.EMA(prices.values.astype(float), timeperiod=slow_period)

            # 计算MACD线
            macd_line = fast_ema - slow_ema

            # 计算信号线
            signal_line = talib.EMA(macd_line, timeperiod=signal_period)

            # 计算MACD柱状图
            macd_hist = macd_line - signal_line

            return {
                'MACD': pd.Series(macd_line, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["MACD"]}_{fast_period}_{slow_period}_{signal_period}'),
                'SIGNAL': pd.Series(signal_line, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["MACD_SIGNAL"]}_{fast_period}_{slow_period}_{signal_period}'),
                'HIST': pd.Series(macd_hist, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["MACD_HIST"]}_{fast_period}_{slow_period}_{signal_period}')
            }

        except Exception as e:
            print(f"❌ MACD计算失败: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return {
                'MACD': empty_series.copy(),
                'SIGNAL': empty_series.copy(),
                'HIST': empty_series.copy()
            }

    @staticmethod
    def calculate_bollinger_bands(data: pd.DataFrame, period: int = 20, std_dev: float = 2.0,
                                price_column: str = 'close') -> Dict[str, pd.Series]:
        """计算布林带（向后兼容）"""
        if isinstance(data, pd.Series):
            prices = data
        else:
            prices = data[price_column]

        try:
            # 计算中轨（简单移动平均）
            middle_band = talib.SMA(prices.values.astype(float), timeperiod=period)

            # 计算标准差
            std = talib.STDDEV(prices.values.astype(float), timeperiod=period, nbdev=1)

            # 计算上轨和下轨
            upper_band = middle_band + (std * std_dev)
            lower_band = middle_band - (std * std_dev)

            return {
                'UPPER': pd.Series(upper_band, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["BB_UPPER"]}_{period}_{std_dev}'),
                'MIDDLE': pd.Series(middle_band, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["BB_MIDDLE"]}_{period}_{std_dev}'),
                'LOWER': pd.Series(lower_band, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["BB_LOWER"]}_{period}_{std_dev}')
            }

        except Exception as e:
            print(f"❌ 布林带计算失败: {e}")
            empty_series = pd.Series(index=prices.index, dtype=float)
            return {
                'UPPER': empty_series.copy(),
                'MIDDLE': empty_series.copy(),
                'LOWER': empty_series.copy()
            }


# ============================================================================
# 导出兼容函数
# ============================================================================

# 为了向后兼容，提供别名
IndicatorManager = UnifiedIndicatorEngine

# 导出静态方法作为模块级函数（简化版本）
def calculate_sma(data, period=20, price_column='close'):
    """计算简单移动平均"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.SMA(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["SMA"]}_{period}')
    except Exception as e:
        print(f"❌ SMA计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["SMA"]}_{period}')

def calculate_ema(data, period=20, price_column='close'):
    """计算指数移动平均"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.EMA(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["EMA"]}_{period}')
    except Exception as e:
        print(f"❌ EMA计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["EMA"]}_{period}')

def calculate_rsi(data, period=14, price_column='close'):
    """计算相对强弱指数"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.RSI(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["RSI"]}_{period}')
    except Exception as e:
        print(f"❌ RSI计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["RSI"]}_{period}')

def calculate_cci(data, period=20):
    """计算顺势指标"""
    high = data['high']
    low = data['low']
    close = data['close']
    try:
        result = talib.CCI(high.values.astype(float), low.values.astype(float),
                         close.values.astype(float), timeperiod=period)
        return pd.Series(result, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["CCI"]}_{period}')
    except Exception as e:
        print(f"❌ CCI计算失败: {e}")
        return pd.Series(index=close.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["CCI"]}_{period}')

def calculate_roc(data, period=10, price_column='close'):
    """计算变动率指标"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.ROC(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["ROC"]}_{period}')
    except Exception as e:
        print(f"❌ ROC计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["ROC"]}_{period}')

def calculate_tema(data, period=20, price_column='close'):
    """计算三重指数移动平均"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.TEMA(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["TEMA"]}_{period}')
    except Exception as e:
        print(f"❌ TEMA计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["TEMA"]}_{period}')

def calculate_wma(data, period=20, price_column='close'):
    """计算加权移动平均"""
    if isinstance(data, pd.Series):
        prices = data
    else:
        prices = data[price_column]
    try:
        result = talib.WMA(prices.values.astype(float), timeperiod=period)
        return pd.Series(result, index=prices.index, name=f'{STANDARD_INDICATOR_COLUMNS["WMA"]}_{period}')
    except Exception as e:
        print(f"❌ WMA计算失败: {e}")
        return pd.Series(index=prices.index, dtype=float, name=f'{STANDARD_INDICATOR_COLUMNS["WMA"]}_{period}')

def calculate_dmi(data, period=14):
    """计算方向运动指标"""
    high = data['high']
    low = data['low']
    close = data['close']
    try:
        plus_di = talib.PLUS_DI(high.values.astype(float), low.values.astype(float),
                              close.values.astype(float), timeperiod=period)
        minus_di = talib.MINUS_DI(high.values.astype(float), low.values.astype(float),
                                close.values.astype(float), timeperiod=period)
        adx = talib.ADX(high.values.astype(float), low.values.astype(float),
                      close.values.astype(float), timeperiod=period)
        adxr = talib.ADXR(high.values.astype(float), low.values.astype(float),
                        close.values.astype(float), timeperiod=period)
        return {
            'PDI': pd.Series(plus_di, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_PDI"]}_{period}'),
            'MDI': pd.Series(minus_di, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_MDI"]}_{period}'),
            'ADX': pd.Series(adx, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_ADX"]}_{period}'),
            'ADXR': pd.Series(adxr, index=close.index, name=f'{STANDARD_INDICATOR_COLUMNS["DMI_ADXR"]}_{period}')
        }
    except Exception as e:
        print(f"❌ DMI计算失败: {e}")
        empty_series = pd.Series(index=close.index, dtype=float)
        return {
            'PDI': empty_series.copy(),
            'MDI': empty_series.copy(),
            'ADX': empty_series.copy(),
            'ADXR': empty_series.copy()
        }

# 其他兼容函数（占位符）
def calculate_volatility_advanced(data, **kwargs):
    """高级波动率指标（待实现）"""
    print("⚠️ 高级波动率指标暂未实现")
    return pd.Series(index=data.index, dtype=float, name='VOLATILITY_ADVANCED')

def calculate_kdj(data, **kwargs):
    """KDJ指标（待实现）"""
    print("⚠️ KDJ指标暂未实现")
    return pd.Series(index=data.index, dtype=float, name='KDJ')

# 导出类内部的静态方法为模块级别的函数
calculate_stochastic = IndicatorCalculator.calculate_stochastic
calculate_atr = IndicatorCalculator.calculate_atr
calculate_cci = IndicatorCalculator.calculate_cci
calculate_obv = IndicatorCalculator.calculate_obv
