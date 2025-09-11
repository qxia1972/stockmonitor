"""
DataStore - 统一数据存储和管理中心
DataStore - Unified Data Storage and Management Center

基于DataFrame的统一数据存储系统，直接使用标准格式存储所有数据。
DataFrame-based unified data storage system that directly uses standard formats for all data.
"""

import os
import sys
import pickle
import json
import threading
import time
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import numpy as np

# Import configuration
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.unified_config import *
except ImportError:
    # Provide default configuration
    CACHE_EXPIRE_HOURS = 24
    MAX_CACHE_SIZE = 1000
    API_BATCH_SIZE = 50
    API_RETRY_TIMES = 3
    API_TIMEOUT = 30

# 导入数据格式约定
from .data_formats import (
    STANDARD_OHLCV_COLUMNS,
    STANDARD_KLINE_COLUMNS,
    STANDARD_DTYPES,
    TIMEFRAME_CONFIG,
    validate_kline_dataframe,
    standardize_kline_dataframe,
    generate_kline_cache_key,
    check_data_quality,
    get_supported_timeframes,
    get_timeframe_config
)

# Delayed import of rqdatac to avoid dependency issues during module loading
try:
    import rqdatac
    RQDATAC_AVAILABLE = True
except ImportError:
    RQDATAC_AVAILABLE = False
    print("⚠️ Warning: rqdatac module not installed, some features will be limited")

class DataStore:
    """
    DataStore singleton class - ensures only one data storage instance across the application
    """
    _instance = None
    _initialized = False
    
    def __new__(cls, cache_dir='cache'):
        if cls._instance is None:
            cls._instance = super(DataStore, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, cache_dir='cache'):
        # 避免重复初始化
        if DataStore._initialized:
            return
        
        DataStore._initialized = True
        
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        self.data = {}
        
        # Initialize indicators engine for unified calculation
        self._indicators_engine = None
        
        # 定时器相关属性
        self.timer_thread = None
        self.timer_running = False
        self.update_interval = 300  # 5分钟更新一次
        self.last_update_time = None
        
        # 统一时间粒度配置 - 使用标准配置
        self.timeframe_config = TIMEFRAME_CONFIG
        
        # 统一分层缓存系统
        self.kline_cache = {
            timeframe: {} for timeframe in self.timeframe_config.keys()
        }
        
        # 基础指标缓存（第二层）
        self.basic_indicators = {}    # {stock_code: {indicators: {...}, method: 'talib'|'pandas', ...}}
        
        # 高级指标缓存（第三层）
        self.advanced_indicators = {}
        
        # 缓存状态追踪
        self.cache_status = {
            'layer1_kline_ready': False,
            'layer2_basic_ready': False, 
            'layer3_advanced_ready': False,
            'cached_stocks': set(),
            'update_in_progress': False,
            'last_layer1_time': None,
            'last_layer2_time': None,
            'last_layer3_time': None
        }
        
        # 性能统计
        self.performance_stats = {
            'layer1_kline': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0},
            'layer2_basic': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0},
            'layer3_advanced': {'count': 0, 'total_time': 0.0, 'avg_time': 0.0},
            'talib_success': {'count': 0, 'total_time': 0.0},
            'talib_failure': {'count': 0, 'total_time': 0.0},
            'pandas_fallback': {'count': 0, 'total_time': 0.0}
        }
        
        # talib成功率统计
        self.talib_success_count = 0
        self.talib_total_count = 0
        self.total_calculations = 0
        
        # rqdatac初始化状态
        self.rqdatac_initialized = False
        
        # 股票池管理（简化为单一列表）
        self.stock_pool = []
        
        # 指标和事件缓存
        # indicators_cache: 轻量级增量缓存，只存储未完成K线的临时计算结果
        # 格式: {cache_key: {'data': pd.Series, 'timestamp': datetime, 'is_partial': bool}}
        self.indicators_cache = {}
        self.events_cache = {}
        
        # 支持的时间粒度列表
        self.supported_timeframes = list(self.timeframe_config.keys())
        
        # 文件股票池功能（仅读取股票代码列表）
        self.data_dir = Path("data")
        self._file_stock_codes_cache = None
        self._file_last_load_date = None
        
        # 缓存日期跟踪
        self.cache_date = None
    
    def clear_daily_cache(self):
        """清理日内缓存数据"""
        try:
            self.data.clear()
            self.indicators_cache.clear()
            self.events_cache.clear()
            print("日内缓存已清理")
        except Exception as e:
            print(f"清理日内缓存失败: {e}")
    
    def _get_indicators_engine(self):
        """
        Get indicators engine with lazy initialization

        Returns:
            IndicatorManager: Initialized indicator manager instance
        """
        if self._indicators_engine is None:
            from .indicator_manager import create_indicator_manager
            self._indicators_engine = create_indicator_manager(self)
        return self._indicators_engine
    
    def _get_latest_available_date(self) -> datetime:
        """
        Get the most recent date with available market data
        
        Returns:
            datetime: Latest date with confirmed data availability
        """
        try:
            if not RQDATAC_AVAILABLE:
                # Fallback to current date when rqdatac unavailable
                return datetime.now()
            
            import rqdatac
            
            # Query recent trading dates from market data
            trading_dates = rqdatac.get_trading_dates(
                start_date=(datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
                end_date=datetime.now().strftime('%Y-%m-%d')
            )
            
            if trading_dates is not None and len(trading_dates) > 0:
                # Test the most recent dates to find one with available data
                for i in range(len(trading_dates)-1, max(-1, len(trading_dates)-4), -1):
                    test_date = trading_dates[i]
                    
                    # Verify data availability with a quick test
                    try:
                        test_data = rqdatac.get_price(
                            '000001.XSHE',  # Use a reliable test stock
                            start_date=test_date,
                            end_date=test_date,
                            fields=['open', 'close', 'high', 'low', 'limit_up', 'limit_down', 'total_turnover', 'volume', 'num_trades']
                        )
                        
                        if test_data is not None and not test_data.empty:
                            return datetime.combine(test_date, datetime.min.time())
                        else:
                            continue
                            
                    except Exception:
                        continue
                
                # If all recent dates failed, use the second most recent as fallback
                fallback_date = trading_dates[-2] if len(trading_dates) >= 2 else trading_dates[-1]
                return datetime.combine(fallback_date, datetime.min.time())
            else:
                # Fallback when no trading dates returned
                return datetime.now()
                
        except Exception as e:
            print(f"❌ Error getting latest available date: {e}")
            return datetime.now()
    
    def get_supported_timeframes(self):
        """获取支持的时间粒度列表 - 使用统一的标准配置"""
        return get_supported_timeframes()
    
    def _standardize_dataframe(self, df):
        """
        标准化DataFrame格式 - 使用统一的数据格式标准

        Args:
            df: 输入的DataFrame

        Returns:
            DataFrame: 标准化的DataFrame
        """
        if df is None or df.empty:
            return df

        # 使用data_formats模块的标准化函数
        return standardize_kline_dataframe(df)
    
    def get_unified_kline_data(self, stock_code, timeframe='daily', use_cache=True):
        """
        统一的K线数据获取接口 - 确保返回标准DataFrame格式

        Args:
            stock_code: 股票代码
            timeframe: 时间粒度 ('daily', '60min', '15min', '5min', '1min')
            use_cache: 是否使用缓存

        Returns:
            DataFrame: 标准OHLCV格式的K线数据，包含标准列名
        """
        if timeframe not in self.timeframe_config:
            raise ValueError(f"不支持的时间粒度: {timeframe}，支持的粒度: {self.supported_timeframes}")

        # 检查缓存
        if use_cache and stock_code in self.kline_cache[timeframe]:
            cached_data = self.kline_cache[timeframe][stock_code]
            if cached_data is not None and not cached_data.empty:
                # 确保返回标准格式的DataFrame
                return self._standardize_dataframe(cached_data)

        # 如果缓存未命中，尝试更新缓存
        if self._update_single_kline_cache(stock_code, timeframe):
            raw_data = self.kline_cache[timeframe].get(stock_code)
            if raw_data is not None:
                # 标准化DataFrame格式
                return self._standardize_dataframe(raw_data)

        return None
    
    def update_kline_cache(self, stock_codes, timeframes=None):
        """
        统一的K线缓存更新接口
        
        Args:
            stock_codes: 股票代码列表
            timeframes: 时间粒度列表，None表示更新所有支持的粒度
        """
        if timeframes is None:
            timeframes = self.supported_timeframes
        
        if isinstance(timeframes, str):
            timeframes = [timeframes]
        
        # 验证时间粒度
        for tf in timeframes:
            if tf not in self.timeframe_config:
                raise ValueError(f"不支持的时间粒度: {tf}")
        
        start_time = time.time()
        print(f"开始更新K线缓存，股票数量: {len(stock_codes)}, 时间粒度: {timeframes}")
        
        self.cache_status['update_in_progress'] = True
        success_count = 0
        
        for i, stock_code in enumerate(stock_codes):
            try:
                stock_success = True
                for timeframe in timeframes:
                    if not self._update_single_kline_cache(stock_code, timeframe):
                        stock_success = False
                
                if stock_success:
                    success_count += 1
                
                if (i + 1) % 50 == 0:
                    print(f"缓存更新进度: {i + 1}/{len(stock_codes)}")
                    
            except Exception as e:
                print(f"更新{stock_code}的K线缓存失败: {e}")
        
        self.cache_status['update_in_progress'] = False
        end_time = time.time()
        
        print(f"K线缓存更新完成: {success_count}/{len(stock_codes)}只股票，耗时{end_time - start_time:.2f}秒")
        
        if success_count > 0:
            self.cache_status['layer1_kline_ready'] = True
        
        return success_count
    
    def get_cache_status(self, detailed=False):
        """
        获取缓存状态信息
        
        Args:
            detailed: 是否返回详细信息
            
        Returns:
            dict: 缓存状态信息
        """
        status = {
            'supported_timeframes': self.supported_timeframes,
            'cache_summary': {}
        }
        
        # 统计各时间粒度的缓存状态
        for timeframe in self.supported_timeframes:
            cache_data = self.kline_cache[timeframe]
            cached_stocks = len(cache_data)
            
            # 计算数据量
            total_records = 0
            if cache_data:
                for stock_code, df in cache_data.items():
                    if df is not None and not df.empty:
                        total_records += len(df)
            
            config = self.timeframe_config[timeframe]
            status['cache_summary'][timeframe] = {
                'description': config['description'],
                'cached_stocks': cached_stocks,
                'total_records': total_records,
                'avg_records_per_stock': total_records / cached_stocks if cached_stocks > 0 else 0,
                'max_periods': config['periods'],
                'frequency': config['frequency']
            }
        
        if detailed:
            status['detailed'] = {}
            for timeframe in self.supported_timeframes:
                cache_data = self.kline_cache[timeframe]
                status['detailed'][timeframe] = {}
                
                for stock_code, df in cache_data.items():
                    if df is not None and not df.empty:
                        status['detailed'][timeframe][stock_code] = {
                            'records': len(df),
                            'start_time': str(df.index[0]),
                            'end_time': str(df.index[-1]),
                            'columns': list(df.columns)
                        }
        
        # 添加整体状态
        all_cached_stocks = set()
        for cache in self.kline_cache.values():
            all_cached_stocks.update(cache.keys())
        
        status['overall'] = {
            'total_cached_stocks': len(all_cached_stocks),
            'cache_ready': self.cache_status['layer1_kline_ready'],
            'update_in_progress': self.cache_status['update_in_progress']
        }
        
        return status
    
    def clear_cache(self, timeframes=None, stock_codes=None):
        """
        清理缓存
        
        Args:
            timeframes: 要清理的时间粒度列表，None表示所有
            stock_codes: 要清理的股票代码列表，None表示所有
        """
        if timeframes is None:
            timeframes = self.supported_timeframes
        elif isinstance(timeframes, str):
            timeframes = [timeframes]
        
        for timeframe in timeframes:
            if timeframe not in self.kline_cache:
                continue
                
            if stock_codes is None:
                # 清理所有股票
                cleared_count = len(self.kline_cache[timeframe])
                self.kline_cache[timeframe].clear()
                print(f"已清理{timeframe}缓存: {cleared_count}只股票")
            else:
                # 清理指定股票
                cleared_count = 0
                for stock_code in stock_codes:
                    if stock_code in self.kline_cache[timeframe]:
                        del self.kline_cache[timeframe][stock_code]
                        cleared_count += 1
                print(f"已清理{timeframe}缓存中的{cleared_count}只股票")
    
    def get_pool_stocks(self, pool_type='basic'):
        """获取指定类型的股票池
        
        Args:
            pool_type: 股票池类型 ('basic', 'core', 'watch')
            
        Returns:
            list: 股票代码列表
        """
        try:
            # 根据池类型确定文件名
            pool_files = {
                'basic': 'basic_pool.json',
                'core': 'core_pool.json', 
                'watch': 'watch_pool.json'
            }
            
            if pool_type not in pool_files:
                print(f"不支持的股票池类型: {pool_type}，使用基础池")
                pool_type = 'basic'
            
            pool_file = self.data_dir / pool_files[pool_type]
            
            # 检查文件是否存在
            if not pool_file.exists():
                print(f"股票池文件不存在: {pool_file}")
                return []
            
            # 读取股票池文件
            with open(pool_file, 'r', encoding='utf-8') as f:
                pool_data = json.load(f)
            
            # 提取股票代码列表
            if 'stocks' in pool_data and isinstance(pool_data['stocks'], list):
                stock_codes = []
                for stock_info in pool_data['stocks']:
                    if isinstance(stock_info, dict) and 'stock_code' in stock_info:
                        stock_codes.append(stock_info['stock_code'])
                    elif isinstance(stock_info, str):
                        # 兼容直接存储股票代码的格式
                        stock_codes.append(stock_info)
                
                print(f"从{pool_type}股票池加载: {len(stock_codes)}只股票")
                return stock_codes
            else:
                print(f"股票池文件格式错误: {pool_file}")
                return []
                
        except Exception as e:
            print(f"读取股票池文件失败: {e}")
            return []

    def update_data(self, key, new_data):
        """更新内存中的数据"""
        self.data[key] = new_data

    def cache_data(self, key):
        """将数据缓存到本地"""
        if key in self.data:
            with open(os.path.join(self.cache_dir, f'{key}.pkl'), 'wb') as f:
                pickle.dump(self.data[key], f)

    def load_cache(self, key):
        """从本地缓存加载数据"""
        path = os.path.join(self.cache_dir, f'{key}.pkl')
        if os.path.exists(path):
            with open(path, 'rb') as f:
                self.data[key] = pickle.load(f)
            return self.data[key]
        return None

    def get_data(self, key):
        """获取内存中的数据，支持传统data格式和kline_cache格式

        Args:
            key: 数据键，支持两种格式：
                - 传统格式：直接从self.data获取
                - K线格式：如'000001.SZ_daily'，从kline_cache获取

        Returns:
            pandas.DataFrame: 标准化的DataFrame数据
        """
        # 优先从传统data字典获取
        if key in self.data:
            data = self.data[key]
            if isinstance(data, pd.DataFrame):
                return self._standardize_dataframe(data)
            return data

        # 尝试解析为K线格式 (stock_code_frequency)
        if '_' in key:
            parts = key.split('_')
            if len(parts) >= 2:
                stock_code = parts[0]
                frequency = parts[1]

                # 从kline_cache获取数据
                df = self.get_kline_from_cache(stock_code, frequency)
                if df is not None:
                    return self._standardize_dataframe(df)

        # 都没找到，返回None
        return None

    # === 第一层：全市场K线数据缓存 ===
    
    def _update_single_kline_cache(self, stock_code, frequency):
        """更新单只股票的K线缓存（统一时间粒度接口）"""
        try:
            if not RQDATAC_AVAILABLE:
                return False
            
            # 使用统一配置
            if frequency not in self.timeframe_config:
                print(f"不支持的时间粒度: {frequency}")
                return False
            
            config = self.timeframe_config[frequency]
            periods = config['periods']
            freq = config['frequency']
            days_back = config['cache_days']
            
            # Get data with reliable date handling
            end_date = self._get_latest_available_date()
            start_date = end_date - timedelta(days=days_back)
            
            import rqdatac
            df = rqdatac.get_price(
                stock_code,
                start_date=start_date,
                end_date=end_date,
                frequency=freq,
                fields=['open', 'close', 'high', 'low', 'limit_up', 'limit_down', 'total_turnover', 'volume', 'num_trades']
            )
            
            if df is not None and not df.empty:
                # 只保留最新的指定周期数
                df_trimmed = df.tail(periods)
                # 标准化DataFrame格式后存储
                standardized_df = self._standardize_dataframe(df_trimmed)
                self.kline_cache[frequency][stock_code] = standardized_df
                print(f"成功缓存{stock_code}的{config['description']}数据: {len(standardized_df)}条记录")
                return True
            else:
                print(f"{stock_code}的{config['description']}数据为空")
                return False
                
        except Exception as e:
            print(f"获取{stock_code}的{frequency}数据失败: {e}")
            return False
    
    def get_kline_from_cache(self, stock_code, frequency='daily', periods=None):
        """从第一层缓存获取K线数据 - 返回标准化的DataFrame"""
        if frequency not in self.kline_cache:
            return None

        if stock_code not in self.kline_cache[frequency]:
            return None

        df = self.kline_cache[frequency][stock_code]
        if df is None or df.empty:
            return None

        # 确保返回标准化的DataFrame
        standardized_df = self._standardize_dataframe(df)

        if periods is not None:
            return standardized_df.tail(periods)
        return standardized_df
    
    # ==================== 第二层缓存：基础技术指标 ====================
    
    def update_layer2_basic_indicators(self, stock_list=None):
        """更新第二层基础指标缓存
        
        Args:
            stock_list: 股票代码列表，如果为None则使用第一层缓存中的所有股票
            
        Returns:
            int: 成功计算指标的股票数量
        """
        start_time = time.time()
        
        # 确定要计算指标的股票列表
        if stock_list is None:
            # 从第一层缓存获取股票列表
            available_stocks = set()
            for freq in ['daily', '60min']:
                if freq in self.kline_cache:
                    available_stocks.update(self.kline_cache[freq].keys())
            stock_list = list(available_stocks)
        
        if not stock_list:
            print("⚠️  没有可用的股票列表进行指标计算")
            return 0
        
        print(f"开始计算第二层基础指标，股票数量: {len(stock_list)}")
        
        success_count = 0
        talib_success = 0
        pandas_fallback = 0
        
        for stock_code in stock_list:
            try:
                # 获取K线数据
                daily_data = self.get_kline_from_cache(stock_code, 'daily')
                min60_data = self.get_kline_from_cache(stock_code, '60min')
                
                if daily_data is None or len(daily_data) < 20:
                    continue
                
                # 为每个股票创建指标缓存
                if stock_code not in self.basic_indicators:
                    self.basic_indicators[stock_code] = {}
                
                # 计算基础技术指标
                indicators_result = self._calculate_basic_indicators(daily_data, min60_data, stock_code)
                
                # 记录计算方法统计
                if indicators_result['method'] == 'talib':
                    talib_success += 1
                else:
                    pandas_fallback += 1
                
                # 存储指标数据
                self.basic_indicators[stock_code] = {
                    'indicators': indicators_result['data'],
                    'method': indicators_result['method'],
                    'update_time': time.time(),
                    'data_length': len(daily_data)
                }
                
                success_count += 1
                
            except Exception as e:
                print(f"⚠️  {stock_code} 指标计算失败: {e}")
                continue
        
        # 更新缓存状态
        self.cache_status['layer2_basic_ready'] = True
        self.cache_status['update_in_progress'] = False
        
        # 记录性能统计
        end_time = time.time()
        elapsed = end_time - start_time
        
        self.performance_stats['layer2_basic']['count'] += 1
        self.performance_stats['layer2_basic']['total_time'] += elapsed
        self.performance_stats['layer2_basic']['avg_time'] = self.performance_stats['layer2_basic']['total_time'] / self.performance_stats['layer2_basic']['count']
        
        # 更新talib成功率统计
        total_calculations = talib_success + pandas_fallback
        if total_calculations > 0:
            self.talib_success_count += talib_success
            self.total_calculations += total_calculations
        
        print(f"第二层基础指标计算完成: {success_count}/{len(stock_list)}只股票，耗时{elapsed:.2f}秒")
        print(f"  talib计算: {talib_success}只股票")
        print(f"  pandas备用: {pandas_fallback}只股票")
        
        return success_count
    
    def _calculate_basic_indicators(self, daily_data, min60_data=None, stock_code=None):
        """计算基础技术指标（使用统一的IndicatorEngine和DataFrame API）

        Args:
            daily_data: 日K线DataFrame数据
            min60_data: 60分钟K线DataFrame数据（可选）
            stock_code: 股票代码，用于检查已有预计算数据

        Returns:
            dict: {'data': 指标数据字典, 'method': 计算方法}
        """
        try:
            # 确保输入数据是标准化的DataFrame
            daily_data = self._standardize_dataframe(daily_data)
            if daily_data is None or daily_data.empty:
                return {'data': {}, 'method': 'failed'}

            # 检查是否已有预计算指标，避免重复计算（lazy precomputation）
            if stock_code and stock_code in self.basic_indicators:
                existing_data = self.basic_indicators[stock_code].get('indicators', {})
                # 检查现有数据的时间覆盖范围
                if existing_data and len(existing_data) > 0:
                    # 获取一个指标作为代表检查数据长度和时间范围
                    sample_indicator = next(iter(existing_data.values()))
                    if hasattr(sample_indicator, 'index') and len(sample_indicator) >= len(daily_data):
                        # 检查时间范围是否覆盖
                        existing_end_date = sample_indicator.index[-1]
                        current_end_date = daily_data.index[-1]
                        if existing_end_date >= current_end_date:
                            print(f"  {stock_code}: 跳过批量计算（已有最新预计算数据）")
                            return {'data': existing_data, 'method': 'cached'}

            # 使用核心的IndicatorManager进行指标计算
            from .indicator_manager import create_indicator_manager
            indicator_manager = create_indicator_manager(self)

            # 构建数据键
            key = f"{stock_code}_daily" if stock_code else "unknown_daily"

            # 清除缓存，确保使用最新数据进行计算
            cache_keys_to_clear = [
                f"sma_{key}_5", f"sma_{key}_10", f"sma_{key}_20",
                f"ema_{key}_5", f"ema_{key}_10", f"ema_{key}_20",
                f"rsi_{key}_7", f"rsi_{key}_14",
                f"macd_{key}_12_26_9",
                f"bollinger_{key}_20_2",
                f"stochastic_{key}_14_3",
                f"atr_{key}_14",
                f"obv_{key}"
            ]
            for cache_key in cache_keys_to_clear:
                if cache_key in self.indicators_cache:
                    del self.indicators_cache[cache_key]

            # 使用IndicatorManager直接计算指标
            indicators_data = {}
            try:
                # 价格类指标 - 直接调用核心方法
                indicators_data['SMA_5'] = indicator_manager.calculate_indicator(key, 'SMA', period=5)
                indicators_data['SMA_10'] = indicator_manager.calculate_indicator(key, 'SMA', period=10)
                indicators_data['SMA_20'] = indicator_manager.calculate_indicator(key, 'SMA', period=20)
                indicators_data['EMA_5'] = indicator_manager.calculate_indicator(key, 'EMA', period=5)
                indicators_data['EMA_10'] = indicator_manager.calculate_indicator(key, 'EMA', period=10)
                indicators_data['EMA_20'] = indicator_manager.calculate_indicator(key, 'EMA', period=20)
                indicators_data['RSI_7'] = indicator_manager.calculate_indicator(key, 'RSI', period=7)
                indicators_data['RSI_14'] = indicator_manager.calculate_indicator(key, 'RSI', period=14)
                indicators_data['ATR_14'] = indicator_manager.calculate_indicator(key, 'ATR', period=14)

                # MACD指标
                macd_result = indicator_manager.calculate_indicator(key, 'MACD', fast_period=12, slow_period=26, signal_period=9)
                if isinstance(macd_result, dict):
                    indicators_data['MACD_LINE'] = macd_result.get('macd')
                    indicators_data['MACD_SIGNAL'] = macd_result.get('signal')
                    indicators_data['MACD_HISTOGRAM'] = macd_result.get('histogram')

                # 布林带指标
                bb_result = indicator_manager.calculate_indicator(key, 'BOLLINGER_BANDS', period=20, std_dev=2)
                if isinstance(bb_result, dict):
                    indicators_data['BOLL_UPPER'] = bb_result.get('upper')
                    indicators_data['BOLL_MIDDLE'] = bb_result.get('middle')
                    indicators_data['BOLL_LOWER'] = bb_result.get('lower')

                # 随机指标
                stoch_result = indicator_manager.calculate_indicator(key, 'STOCHASTIC', k_period=14, d_period=3)
                if isinstance(stoch_result, dict):
                    indicators_data['STOCH_K'] = stoch_result.get('k')
                    indicators_data['STOCH_D'] = stoch_result.get('d')

                # 成交量指标
                indicators_data['OBV'] = indicator_manager.calculate_indicator(key, 'OBV')

                # 成交量均线（使用DataFrame方法）
                volume_sma5 = self._calc_volume_sma_dataframe(daily_data, 5)
                if volume_sma5 is not None:
                    indicators_data['VOLUME_SMA_5'] = volume_sma5['VOLUME_SMA']

                volume_sma10 = self._calc_volume_sma_dataframe(daily_data, 10)
                if volume_sma10 is not None:
                    indicators_data['VOLUME_SMA_10'] = volume_sma10['VOLUME_SMA']

                volume_sma20 = self._calc_volume_sma_dataframe(daily_data, 20)
                if volume_sma20 is not None:
                    indicators_data['VOLUME_SMA_20'] = volume_sma20['VOLUME_SMA']

                volume_ratio = self._calc_volume_ratio_dataframe(daily_data)
                if volume_ratio is not None:
                    indicators_data['VOLUME_RATIO'] = volume_ratio['VOLUME_RATIO']

                # A/D线和CMF（使用DataFrame方法）
                ad_line = self._calc_ad_line_dataframe(daily_data)
                if ad_line is not None:
                    indicators_data['AD'] = ad_line['AD']

                cmf = self._calc_cmf_dataframe(daily_data, 20)
                if cmf is not None:
                    indicators_data['CMF'] = cmf['CMF']

                return {'data': indicators_data, 'method': 'indicator_manager'}

            except Exception as e:
                print(f"  IndicatorManager计算失败，使用pandas备用: {e}")
                return self._calculate_indicators_pandas_fallback(daily_data)

        except Exception as e:
            # 最终兜底：pandas备用方案
            print(f"  基础指标计算失败，使用pandas备用: {e}")
            return self._calculate_indicators_pandas_fallback(daily_data)

    def _calculate_indicators_pandas_fallback(self, daily_data):
        """pandas备用指标计算方案"""
        try:
            close = daily_data['close']
            high = daily_data['high']
            low = daily_data['low']
            volume = daily_data['volume']
            
            indicators_data = {
                # 价格类指标
                'SMA_5': close.rolling(5).mean(),
                'SMA_10': close.rolling(10).mean(),
                'SMA_20': close.rolling(20).mean(),
                'EMA_5': close.ewm(span=5).mean(),
                'EMA_10': close.ewm(span=10).mean(),
                'EMA_20': close.ewm(span=20).mean(),
                'RSI_7': self._calc_rsi_pandas(close, 7),
                'RSI_14': self._calc_rsi_pandas(close, 14),
                'MACD': self._calc_macd_pandas(close),
                'BOLL': self._calc_bollinger_pandas(close, 20, 2),
                'ATR_14': self._calc_atr_pandas(high, low, close, 14),
                'STOCH': self._calc_stoch_pandas(high, low, close),
                
                # 成交量类指标
                'OBV': self._calc_obv_pandas(close, volume),
                'AD': self._calc_ad_pandas(high, low, close, volume),
                'VOLUME_SMA_5': volume.rolling(5).mean(),
                'VOLUME_SMA_10': volume.rolling(10).mean(),
                'VOLUME_SMA_20': volume.rolling(20).mean(),
                'VOLUME_RATIO': volume / volume.rolling(20).mean()
            }

            # 单独计算CMF
            cmf_result = self._calc_cmf_dataframe(daily_data, 20)
            if cmf_result is not None:
                indicators_data['CMF'] = cmf_result['CMF']
            else:
                indicators_data['CMF'] = pd.Series(dtype=float, index=daily_data.index)
            
            return {'data': indicators_data, 'method': 'pandas'}
            
        except Exception as e:
            print(f"  pandas备用计算也失败: {e}")
            return {'data': {}, 'method': 'failed'}
    
    def _calc_rsi_pandas(self, close, period=14):
        """pandas计算RSI"""
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    def _calc_macd_pandas(self, close):
        """pandas计算MACD"""
        ema10 = close.ewm(span=10).mean()
        ema20 = close.ewm(span=20).mean()
        macd_line = ema10 - ema20
        signal_line = macd_line.ewm(span=6).mean()
        histogram = macd_line - signal_line
        return {'MACD': macd_line, 'Signal': signal_line, 'Histogram': histogram}
    
    def _calc_bollinger_pandas(self, close, period=20, std_dev=2):
        """pandas计算布林带"""
        sma = close.rolling(period).mean()
        std = close.rolling(period).std()
        return {
            'Upper': sma + (std * std_dev),
            'Middle': sma,
            'Lower': sma - (std * std_dev)
        }
    
    def _calc_atr_pandas(self, high, low, close, period=14):
        """pandas计算ATR"""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    def _calc_stoch_pandas(self, high, low, close, k_period=14, d_period=3):
        """pandas计算随机指标"""
        lowest_low = low.rolling(k_period).min()
        highest_high = high.rolling(k_period).max()
        k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(d_period).mean()
        return {'%K': k_percent, '%D': d_percent}
    
    def _calc_obv_pandas(self, close, volume):
        """pandas计算OBV (On Balance Volume)"""
        price_change = close.diff()
        obv = pd.Series(index=close.index, dtype=float)
        obv.iloc[0] = volume.iloc[0]
        
        for i in range(1, len(close)):
            if price_change.iloc[i] > 0:
                obv.iloc[i] = obv.iloc[i-1] + volume.iloc[i]
            elif price_change.iloc[i] < 0:
                obv.iloc[i] = obv.iloc[i-1] - volume.iloc[i]
            else:
                obv.iloc[i] = obv.iloc[i-1]
        
        return obv
    
    def _calc_ad_pandas(self, high, low, close, volume):
        """pandas计算A/D Line (Accumulation/Distribution Line)"""
        clv = ((close - low) - (high - close)) / (high - low)
        clv = clv.fillna(0)  # 处理high=low的情况
        mfv = clv * volume
        ad_line = mfv.cumsum()
        return ad_line
    
    def _calc_volume_sma_dataframe(self, data, period):
        """计算成交量简单移动平均线，返回DataFrame格式"""
        if 'volume' not in data.columns:
            return None
        volume = data['volume']
        sma = volume.rolling(period).mean()
        return pd.DataFrame({'VOLUME_SMA': sma}, index=data.index)

    def _calc_volume_ratio_dataframe(self, data):
        """计算成交量比率，返回DataFrame格式"""
        if 'volume' not in data.columns:
            return None
        volume = data['volume']
        volume_sma20 = volume.rolling(20).mean()
        ratio = volume / volume_sma20
        return pd.DataFrame({'VOLUME_RATIO': ratio}, index=data.index)

    def _calc_ad_line_dataframe(self, data):
        """计算累积/派发线，返回DataFrame格式"""
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            return None

        high = data['high']
        low = data['low']
        close = data['close']
        volume = data['volume']

        # 计算Money Flow Multiplier
        mfm = ((close - low) - (high - close)) / (high - low)
        mfm = mfm.fillna(0)

        # 计算Money Flow Volume
        mfv = mfm * volume

        # 计算A/D Line
        ad_line = mfv.cumsum()
        return pd.DataFrame({'AD': ad_line}, index=data.index)

    def _calc_cmf_dataframe(self, data, period=20):
        """计算Chaikin Money Flow，返回DataFrame格式"""
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            return None

        high = data['high']
        low = data['low']
        close = data['close']
        volume = data['volume']

        # 计算Money Flow Multiplier
        mfm = ((close - low) - (high - close)) / (high - low)
        mfm = mfm.fillna(0)

        # 计算Money Flow Volume
        mfv = mfm * volume

        # 计算CMF
        cmf = mfv.rolling(period).sum() / volume.rolling(period).sum()
        return pd.DataFrame({'CMF': cmf}, index=data.index)
    
    def get_basic_indicators_from_cache(self, stock_code, indicator_name=None):
        """从第二层缓存获取基础指标数据
        
        Args:
            stock_code: 股票代码
            indicator_name: 指标名称，如果为None则返回所有指标
            
        Returns:
            指标数据或None
        """
        if stock_code not in self.basic_indicators:
            return None
        
        stock_indicators = self.basic_indicators[stock_code]
        indicators_data = stock_indicators.get('indicators', {})
        
        if indicator_name is None:
            return indicators_data
        else:
            return indicators_data.get(indicator_name, None)
    
    def verify_time_alignment(self, stock_code):
        """验证指定股票的K线数据和指标数据时间对齐
        
        Args:
            stock_code: 股票代码
            
        Returns:
            dict: 时间对齐验证结果
        """
        result = {
            'stock_code': stock_code,
            'aligned': True,
            'issues': [],
            'kline_info': {},
            'indicators_info': {}
        }
        
        # 获取K线数据
        daily_kline = self.get_kline_from_cache(stock_code, 'daily')
        if daily_kline is None:
            result['aligned'] = False
            result['issues'].append('日K线数据不存在')
            return result
        
        # 获取指标数据
        indicators_data = self.get_basic_indicators_from_cache(stock_code)
        if indicators_data is None:
            result['aligned'] = False
            result['issues'].append('指标数据不存在')
            return result
        
        # 记录K线信息
        result['kline_info'] = {
            'length': len(daily_kline),
            'start_date': str(daily_kline.index[0]),
            'end_date': str(daily_kline.index[-1]),
            'index_type': str(type(daily_kline.index))
        }
        
        # 检查每个指标的时间对齐
        aligned_indicators = 0
        total_indicators = 0
        
        for ind_name, ind_data in indicators_data.items():
            if ind_data is None:
                continue
                
            total_indicators += 1
            
            if isinstance(ind_data, dict):
                # 复合指标（如MACD, BOLL, STOCH）
                for sub_name, sub_data in ind_data.items():
                    if hasattr(sub_data, 'index'):
                        if sub_data.index.equals(daily_kline.index):
                            aligned_indicators += 1
                        else:
                            result['issues'].append(f'{ind_name}.{sub_name}时间索引不对齐')
                            result['aligned'] = False
                    else:
                        result['issues'].append(f'{ind_name}.{sub_name}缺失时间索引')
                        result['aligned'] = False
            else:
                # 单一指标
                if hasattr(ind_data, 'index'):
                    if ind_data.index.equals(daily_kline.index):
                        aligned_indicators += 1
                        # 记录示例指标信息
                        if ind_name == 'SMA_20':
                            result['indicators_info']['SMA_20'] = {
                                'length': len(ind_data),
                                'start_date': str(ind_data.index[0]),
                                'end_date': str(ind_data.index[-1]),
                                'index_type': str(type(ind_data.index)),
                                'latest_value': float(ind_data.iloc[-1]) if not pd.isna(ind_data.iloc[-1]) else None
                            }
                    else:
                        result['issues'].append(f'{ind_name}时间索引不对齐')
                        result['aligned'] = False
                else:
                    result['issues'].append(f'{ind_name}缺失时间索引')
                    result['aligned'] = False
        
        result['alignment_ratio'] = aligned_indicators / total_indicators if total_indicators > 0 else 0
        
        return result
    
    def verify_cache_consistency(self, stock_list=None):
        """验证缓存数据的一致性和时间对齐
        
        Args:
            stock_list: 要验证的股票列表，如果为None则验证所有缓存的股票
            
        Returns:
            dict: 整体验证结果
        """
        if stock_list is None:
            # 获取所有已缓存的股票
            cached_stocks = set()
            for freq in ['daily', '60min']:
                if freq in self.kline_cache:
                    cached_stocks.update(self.kline_cache[freq].keys())
            stock_list = list(cached_stocks)
        
        overall_result = {
            'total_stocks': len(stock_list),
            'aligned_stocks': 0,
            'failed_stocks': 0,
            'alignment_issues': [],
            'summary': {}
        }
        
        for stock_code in stock_list:
            verification = self.verify_time_alignment(stock_code)
            
            if verification['aligned']:
                overall_result['aligned_stocks'] += 1
            else:
                overall_result['failed_stocks'] += 1
                overall_result['alignment_issues'].extend([
                    f"{stock_code}: {issue}" for issue in verification['issues']
                ])
        
        overall_result['alignment_rate'] = (
            overall_result['aligned_stocks'] / overall_result['total_stocks'] 
            if overall_result['total_stocks'] > 0 else 0
        )
        
        overall_result['summary'] = {
            'cache_layers_ready': [
                self.cache_status['layer1_kline_ready'],
                self.cache_status['layer2_basic_ready'],
                self.cache_status['layer3_advanced_ready']
            ],
            'performance_stats': {
                'layer1_avg_time': self.performance_stats['layer1_kline']['avg_time'],
                'layer2_avg_time': self.performance_stats['layer2_basic']['avg_time'],
                'talib_success_rate': self.talib_success_count / self.total_calculations if self.total_calculations > 0 else 0
            }
        }
        
        return overall_result
    
    # ==================== 第三层缓存：高级指标和组合策略 ====================
    
    def update_layer3_advanced_indicators(self, stock_list=None):
        """更新第三层高级指标缓存
        优先使用基础指标进行计算，避免重复计算
        
        Args:
            stock_list: 股票代码列表，如果为None则使用第二层缓存中的所有股票
            
        Returns:
            int: 成功计算高级指标的股票数量
        """
        start_time = time.time()
        
        # 确定要计算高级指标的股票列表
        if stock_list is None:
            stock_list = list(self.basic_indicators.keys())
        
        if not stock_list:
            print("⚠️  没有可用的基础指标数据进行高级指标计算")
            return 0
        
        print(f"开始计算第三层高级指标，股票数量: {len(stock_list)}")
        
        success_count = 0
        calculation_stats = {
            'reuse_basic': 0,     # 复用基础指标次数
            'new_calculation': 0,  # 新计算次数
            'failed': 0           # 失败次数
        }
        
        for stock_code in stock_list:
            try:
                # 获取基础指标数据
                basic_indicators = self.get_basic_indicators_from_cache(stock_code)
                if basic_indicators is None:
                    print(f"⚠️  {stock_code} 缺少基础指标数据，跳过高级指标计算")
                    calculation_stats['failed'] += 1
                    continue
                
                # 获取K线数据（用于需要原始数据的高级指标）
                daily_kline = self.get_kline_from_cache(stock_code, 'daily')
                if daily_kline is None:
                    print(f"⚠️  {stock_code} 缺少K线数据，跳过高级指标计算")
                    calculation_stats['failed'] += 1
                    continue
                
                # 为每个股票创建高级指标缓存
                if stock_code not in self.advanced_indicators:
                    self.advanced_indicators[stock_code] = {}
                
                # 计算高级指标
                advanced_result = self._calculate_advanced_indicators(
                    basic_indicators, daily_kline, calculation_stats
                )
                
                # 存储高级指标数据
                self.advanced_indicators[stock_code] = {
                    'indicators': advanced_result['data'],
                    'dependencies': advanced_result['dependencies'],
                    'calculation_method': advanced_result['method'],
                    'update_time': time.time(),
                    'reuse_count': advanced_result['reuse_count']
                }
                
                success_count += 1
                
            except Exception as e:
                print(f"⚠️  {stock_code} 高级指标计算失败: {e}")
                calculation_stats['failed'] += 1
                continue
        
        # 更新缓存状态
        self.cache_status['layer3_advanced_ready'] = True
        self.cache_status['update_in_progress'] = False
        
        # 记录性能统计
        end_time = time.time()
        elapsed = end_time - start_time
        
        self.performance_stats['layer3_advanced']['count'] += 1
        self.performance_stats['layer3_advanced']['total_time'] += elapsed
        self.performance_stats['layer3_advanced']['avg_time'] = self.performance_stats['layer3_advanced']['total_time'] / self.performance_stats['layer3_advanced']['count']
        
        print(f"第三层高级指标计算完成: {success_count}/{len(stock_list)}只股票，耗时{elapsed:.2f}秒")
        print(f"  复用基础指标: {calculation_stats['reuse_basic']}次")
        print(f"  新增计算: {calculation_stats['new_calculation']}次")
        print(f"  计算失败: {calculation_stats['failed']}次")
        
        return success_count
    
    def _calculate_advanced_indicators(self, basic_indicators, daily_kline, stats):
        """计算高级指标，优先使用基础指标
        
        Args:
            basic_indicators: 基础指标数据
            daily_kline: 日K线数据
            stats: 统计信息字典
            
        Returns:
            dict: {'data': 高级指标数据, 'dependencies': 依赖关系, 'method': 计算方法, 'reuse_count': 复用次数}
        """
        advanced_data = {}
        dependencies = {}
        reuse_count = 0
        
        try:
            # 1. KDJ指标 - 基于STOCH基础指标计算
            if 'STOCH' in basic_indicators and basic_indicators['STOCH'] is not None:
                stoch_data = basic_indicators['STOCH']
                if '%K' in stoch_data and '%D' in stoch_data:
                    k_values = stoch_data['%K']
                    d_values = stoch_data['%D']
                    
                    # KDJ的J值 = 3K - 2D
                    j_values = 3 * k_values - 2 * d_values
                    
                    advanced_data['KDJ'] = {
                        'K': k_values,  # 复用基础指标
                        'D': d_values,  # 复用基础指标
                        'J': j_values   # 新计算
                    }
                    dependencies['KDJ'] = ['STOCH']
                    reuse_count += 2  # K和D都是复用的
                    stats['reuse_basic'] += 2
                    stats['new_calculation'] += 1
            
            # 2. Williams %R - 基于STOCH计算或独立计算
            if 'STOCH' in basic_indicators and basic_indicators['STOCH'] is not None:
                # 优先基于STOCH数据计算Williams %R
                stoch_k = basic_indicators['STOCH']['%K']
                williams_r = stoch_k - 100  # Williams %R = %K - 100
                advanced_data['WILLIAMS_R'] = williams_r
                dependencies['WILLIAMS_R'] = ['STOCH']
                reuse_count += 1
                stats['reuse_basic'] += 1
            else:
                # 备用：直接计算Williams %R
                high = daily_kline['high']
                low = daily_kline['low']
                close = daily_kline['close']
                williams_r = self._calc_williams_r(high, low, close, 14)
                advanced_data['WILLIAMS_R'] = williams_r
                dependencies['WILLIAMS_R'] = ['raw_data']
                stats['new_calculation'] += 1
            
            # 3. CCI指标 - 需要原始价格数据
            high = daily_kline['high']
            low = daily_kline['low']
            close = daily_kline['close']
            cci = self._calc_cci(high, low, close, 20)
            advanced_data['CCI'] = cci
            dependencies['CCI'] = ['raw_data']
            stats['new_calculation'] += 1
            
            # 4. MACD信号分析 - 基于基础MACD指标
            if 'MACD' in basic_indicators and basic_indicators['MACD'] is not None:
                macd_data = basic_indicators['MACD']
                if all(key in macd_data for key in ['MACD', 'Signal', 'Histogram']):
                    macd_signals = self._analyze_macd_signals(macd_data)
                    advanced_data['MACD_SIGNALS'] = macd_signals
                    dependencies['MACD_SIGNALS'] = ['MACD']
                    reuse_count += 1
                    stats['reuse_basic'] += 1
            
            # 5. 布林带突破信号 - 基于基础布林带指标
            if 'BOLL' in basic_indicators and basic_indicators['BOLL'] is not None:
                boll_data = basic_indicators['BOLL']
                if all(key in boll_data for key in ['Upper', 'Middle', 'Lower']):
                    boll_signals = self._analyze_bollinger_signals(boll_data, close)
                    advanced_data['BOLL_SIGNALS'] = boll_signals
                    dependencies['BOLL_SIGNALS'] = ['BOLL']
                    reuse_count += 1
                    stats['reuse_basic'] += 1
            
            # 6. 均线交叉信号 - 基于基础移动平均线（包含5日均线）
            sma_indicators = ['SMA_5', 'SMA_10', 'SMA_20']
            ema_indicators = ['EMA_5', 'EMA_10', 'EMA_20']
            if all(ind in basic_indicators for ind in sma_indicators + ema_indicators):
                ma_cross_signals = self._analyze_ma_cross_signals({
                    **{ind: basic_indicators[ind] for ind in sma_indicators},
                    **{ind: basic_indicators[ind] for ind in ema_indicators}
                })
                advanced_data['MA_CROSS_SIGNALS'] = ma_cross_signals
                dependencies['MA_CROSS_SIGNALS'] = sma_indicators + ema_indicators
                reuse_count += len(sma_indicators + ema_indicators)
                stats['reuse_basic'] += len(sma_indicators + ema_indicators)
            
            # 7. 成交量价格趋势分析 - 基于成交量基础指标
            volume_indicators = ['OBV', 'AD', 'CMF']
            if all(ind in basic_indicators for ind in volume_indicators):
                vpt_analysis = self._analyze_volume_price_trend({
                    ind: basic_indicators[ind] for ind in volume_indicators
                }, close)
                advanced_data['VOLUME_PRICE_TREND'] = vpt_analysis
                dependencies['VOLUME_PRICE_TREND'] = volume_indicators
                reuse_count += len(volume_indicators)
                stats['reuse_basic'] += len(volume_indicators)
            
            # 8. 成交量突破信号 - 基于成交量均线（包含5日均线）
            volume_sma_indicators = ['VOLUME_SMA_5', 'VOLUME_SMA_10', 'VOLUME_SMA_20', 'VOLUME_RATIO']
            if all(ind in basic_indicators for ind in volume_sma_indicators):
                volume_signals = self._analyze_volume_breakout_signals({
                    ind: basic_indicators[ind] for ind in volume_sma_indicators
                }, daily_kline['volume'])
                advanced_data['VOLUME_BREAKOUT_SIGNALS'] = volume_signals
                dependencies['VOLUME_BREAKOUT_SIGNALS'] = volume_sma_indicators
                reuse_count += len(volume_sma_indicators)
                stats['reuse_basic'] += len(volume_sma_indicators)
            
            # 9. 价量背离分析 - 结合价格和成交量指标
            price_volume_indicators = ['RSI_14', 'OBV', 'AD']
            if all(ind in basic_indicators for ind in price_volume_indicators):
                divergence_analysis = self._analyze_price_volume_divergence({
                    ind: basic_indicators[ind] for ind in price_volume_indicators
                }, close)
                advanced_data['PRICE_VOLUME_DIVERGENCE'] = divergence_analysis
                dependencies['PRICE_VOLUME_DIVERGENCE'] = price_volume_indicators
                reuse_count += len(price_volume_indicators)
                stats['reuse_basic'] += len(price_volume_indicators)
            
            # 10. 短期动量分析 - 基于RSI_7和5日移动平均线的高敏感性
            short_momentum_indicators = ['RSI_7', 'RSI_14', 'ATR_14', 'EMA_5', 'SMA_5', 'VOLUME_SMA_5']
            if all(ind in basic_indicators for ind in short_momentum_indicators):
                short_momentum_analysis = self._analyze_short_term_momentum({
                    ind: basic_indicators[ind] for ind in short_momentum_indicators
                }, close)
                advanced_data['SHORT_MOMENTUM'] = short_momentum_analysis
                dependencies['SHORT_MOMENTUM'] = short_momentum_indicators
                reuse_count += len(short_momentum_indicators)
                stats['reuse_basic'] += len(short_momentum_indicators)
            
            # 11. 综合强度指标 - 基于多个基础指标
            strength_indicators = ['RSI_14', 'MACD', 'ATR_14']
            if all(ind in basic_indicators for ind in strength_indicators):
                composite_strength = self._calc_composite_strength({
                    ind: basic_indicators[ind] for ind in strength_indicators
                })
                advanced_data['COMPOSITE_STRENGTH'] = composite_strength
                dependencies['COMPOSITE_STRENGTH'] = strength_indicators
                reuse_count += len(strength_indicators)
                stats['reuse_basic'] += len(strength_indicators)
            
            return {
                'data': advanced_data,
                'dependencies': dependencies,
                'method': 'advanced_composite',
                'reuse_count': reuse_count
            }
            
        except Exception as e:
            print(f"  高级指标计算异常: {e}")
            return {
                'data': {},
                'dependencies': {},
                'method': 'failed',
                'reuse_count': 0
            }
    
    def _calc_williams_r(self, high, low, close, period=14):
        """计算Williams %R指标"""
        highest_high = high.rolling(period).max()
        lowest_low = low.rolling(period).min()
        williams_r = -100 * (highest_high - close) / (highest_high - lowest_low)
        return williams_r
    
    def _calc_cci(self, high, low, close, period=20):
        """计算CCI指标"""
        typical_price = (high + low + close) / 3
        sma_tp = typical_price.rolling(period).mean()
        
        # 计算平均绝对偏差（替代已被移除的mad方法）
        def calc_mad(x):
            return (x - x.mean()).abs().mean()
        
        mad = typical_price.rolling(period).apply(calc_mad, raw=False)
        cci = (typical_price - sma_tp) / (0.015 * mad)
        return cci
    
    def _analyze_macd_signals(self, macd_data):
        """分析MACD信号"""
        macd_line = macd_data['MACD']
        signal_line = macd_data['Signal']
        histogram = macd_data['Histogram']
        
        # MACD穿越信号
        macd_above_signal = macd_line > signal_line
        macd_cross_up = macd_above_signal & (~macd_above_signal.shift(1).fillna(False))
        macd_cross_down = (~macd_above_signal) & macd_above_signal.shift(1).fillna(False)
        
        # 直方图变化
        hist_increasing = histogram > histogram.shift(1)
        hist_decreasing = histogram < histogram.shift(1)
        
        signals = pd.DataFrame({
            'macd_above_signal': macd_above_signal,
            'macd_cross_up': macd_cross_up,
            'macd_cross_down': macd_cross_down,
            'histogram_increasing': hist_increasing,
            'histogram_decreasing': hist_decreasing
        }, index=macd_line.index)
        
        return signals
    
    def _analyze_bollinger_signals(self, boll_data, close):
        """分析布林带信号"""
        upper = boll_data['Upper']
        middle = boll_data['Middle']
        lower = boll_data['Lower']
        
        # 布林带突破信号
        above_upper = close > upper
        below_lower = close < lower
        break_upper = above_upper & (~above_upper.shift(1).fillna(False))
        break_lower = below_lower & (~below_lower.shift(1).fillna(False))
        
        # 布林带宽度
        bb_width = (upper - lower) / middle
        bb_squeeze = bb_width < bb_width.rolling(20).quantile(0.1)
        
        signals = pd.DataFrame({
            'above_upper': above_upper,
            'below_lower': below_lower,
            'break_upper': break_upper,
            'break_lower': break_lower,
            'bb_width': bb_width,
            'bb_squeeze': bb_squeeze
        }, index=close.index)
        
        return signals
    
    def _analyze_ma_cross_signals(self, ma_data):
        """分析均线交叉信号"""
        sma5 = ma_data['SMA_5']
        sma10 = ma_data['SMA_10']
        sma20 = ma_data['SMA_20']
        
        # 金叉死叉信号
        golden_cross_5_10 = (sma5 > sma10) & (sma5.shift(1) <= sma10.shift(1))
        death_cross_5_10 = (sma5 < sma10) & (sma5.shift(1) >= sma10.shift(1))
        
        golden_cross_10_20 = (sma10 > sma20) & (sma10.shift(1) <= sma20.shift(1))
        death_cross_10_20 = (sma10 < sma20) & (sma10.shift(1) >= sma20.shift(1))
        
        # 多头排列
        bullish_alignment = (sma5 > sma10) & (sma10 > sma20)
        bearish_alignment = (sma5 < sma10) & (sma10 < sma20)
        
        signals = pd.DataFrame({
            'golden_cross_5_10': golden_cross_5_10,
            'death_cross_5_10': death_cross_5_10,
            'golden_cross_10_20': golden_cross_10_20,
            'death_cross_10_20': death_cross_10_20,
            'bullish_alignment': bullish_alignment,
            'bearish_alignment': bearish_alignment
        }, index=sma5.index)
        
        return signals
    
    def _calc_composite_strength(self, indicators):
        """计算综合强度指标"""
        rsi = indicators['RSI_14']
        macd_data = indicators['MACD']
        atr = indicators['ATR_14']
        
        # 标准化各个指标到0-100区间
        rsi_norm = rsi  # RSI already 0-100
        
        # MACD强度（基于MACD线和信号线的关系）
        macd_strength = pd.Series(50, index=rsi.index)  # 默认中性
        if 'MACD' in macd_data and 'Signal' in macd_data:
            macd_diff = macd_data['MACD'] - macd_data['Signal']
            macd_strength = 50 + (macd_diff / macd_diff.rolling(20).std()) * 10
            macd_strength = macd_strength.clip(0, 100)
        
        # ATR强度（波动率强度）
        atr_norm = atr / atr.rolling(20).max() * 100
        
        # 综合强度（加权平均）
        composite = (rsi_norm * 0.4 + macd_strength * 0.4 + atr_norm * 0.2)
        composite = composite.clip(0, 100)
        
        return composite
    
    def _analyze_volume_price_trend(self, volume_indicators, close):
        """分析成交量价格趋势
        
        Args:
            volume_indicators: 包含OBV, AD, CMF的字典
            close: 收盘价序列
            
        Returns:
            DataFrame: 成交量价格趋势分析结果
        """
        obv = volume_indicators['OBV']
        ad = volume_indicators['AD']
        cmf = volume_indicators['CMF']
        
        # 价格趋势
        price_trend = close.pct_change(5)  # 5日价格变化率
        
        # OBV趋势
        obv_trend = obv.pct_change(5)
        obv_divergence = (price_trend > 0) & (obv_trend < 0) | (price_trend < 0) & (obv_trend > 0)
        
        # A/D Line趋势
        ad_trend = ad.pct_change(5)
        ad_divergence = (price_trend > 0) & (ad_trend < 0) | (price_trend < 0) & (ad_trend > 0)
        
        # CMF信号
        cmf_bullish = cmf > 0.1
        cmf_bearish = cmf < -0.1
        cmf_neutral = (cmf >= -0.1) & (cmf <= 0.1)
        
        # 综合成交量强度
        volume_strength = pd.Series(0, index=close.index)
        volume_strength += (cmf > 0).astype(int) * 1  # CMF正值+1
        volume_strength += (obv_trend > 0).astype(int) * 1  # OBV上升+1
        volume_strength += (ad_trend > 0).astype(int) * 1  # A/D上升+1
        volume_strength -= obv_divergence.astype(int) * 1  # OBV背离-1
        volume_strength -= ad_divergence.astype(int) * 1   # A/D背离-1
        
        analysis = pd.DataFrame({
            'price_trend': price_trend,
            'obv_trend': obv_trend,
            'ad_trend': ad_trend,
            'obv_divergence': obv_divergence,
            'ad_divergence': ad_divergence,
            'cmf_bullish': cmf_bullish,
            'cmf_bearish': cmf_bearish,
            'cmf_neutral': cmf_neutral,
            'volume_strength': volume_strength
        }, index=close.index)
        
        return analysis
    
    def _analyze_volume_breakout_signals(self, volume_sma_indicators, volume):
        """分析成交量突破信号
        
        Args:
            volume_sma_indicators: 包含成交量均线的字典
            volume: 成交量序列
            
        Returns:
            DataFrame: 成交量突破信号分析结果
        """
        volume_sma_10 = volume_sma_indicators['VOLUME_SMA_10']
        volume_sma_20 = volume_sma_indicators['VOLUME_SMA_20']
        volume_ratio = volume_sma_indicators['VOLUME_RATIO']
        
        # 成交量突破信号
        volume_breakout_10 = volume > volume_sma_10 * 2  # 超过10日均量2倍
        volume_breakout_20 = volume > volume_sma_20 * 1.5  # 超过20日均量1.5倍
        
        # 成交量萎缩信号
        volume_shrink = volume < volume_sma_20 * 0.5  # 低于20日均量50%
        
        # 成交量比率信号
        high_volume_ratio = volume_ratio > 2.0  # 成交量比率大于2
        low_volume_ratio = volume_ratio < 0.5   # 成交量比率小于0.5
        
        # 成交量均线交叉
        volume_golden_cross = (volume_sma_10 > volume_sma_20) & (volume_sma_10.shift(1) <= volume_sma_20.shift(1))
        volume_death_cross = (volume_sma_10 < volume_sma_20) & (volume_sma_10.shift(1) >= volume_sma_20.shift(1))
        
        # 异常成交量检测
        volume_spike = volume > volume.rolling(20).quantile(0.95)  # 成交量达到20日95%分位数
        
        signals = pd.DataFrame({
            'volume_breakout_10': volume_breakout_10,
            'volume_breakout_20': volume_breakout_20,
            'volume_shrink': volume_shrink,
            'high_volume_ratio': high_volume_ratio,
            'low_volume_ratio': low_volume_ratio,
            'volume_golden_cross': volume_golden_cross,
            'volume_death_cross': volume_death_cross,
            'volume_spike': volume_spike
        }, index=volume.index)
        
        return signals
    
    def _analyze_price_volume_divergence(self, price_volume_indicators, close):
        """分析价量背离
        
        Args:
            price_volume_indicators: 包含RSI, OBV, AD的字典
            close: 收盘价序列
            
        Returns:
            DataFrame: 价量背离分析结果
        """
        rsi = price_volume_indicators['RSI_14']
        obv = price_volume_indicators['OBV']
        ad = price_volume_indicators['AD']
        
        # 价格动量（使用RSI变化）
        price_momentum = rsi.diff(5)  # 5日RSI变化
        
        # 成交量动量
        obv_momentum = obv.pct_change(5)
        ad_momentum = ad.pct_change(5)
        
        # 顶背离：价格创新高，但成交量指标未创新高
        price_high = close == close.rolling(20).max()
        obv_high = obv == obv.rolling(20).max()
        ad_high = ad == ad.rolling(20).max()
        
        top_divergence_obv = price_high & ~obv_high
        top_divergence_ad = price_high & ~ad_high
        
        # 底背离：价格创新低，但成交量指标未创新低
        price_low = close == close.rolling(20).min()
        obv_low = obv == obv.rolling(20).min()
        ad_low = ad == ad.rolling(20).min()
        
        bottom_divergence_obv = price_low & ~obv_low
        bottom_divergence_ad = price_low & ~ad_low
        
        # 动量背离
        momentum_divergence_obv = (price_momentum > 0) & (obv_momentum < 0) | (price_momentum < 0) & (obv_momentum > 0)
        momentum_divergence_ad = (price_momentum > 0) & (ad_momentum < 0) | (price_momentum < 0) & (ad_momentum > 0)
        
        # 综合背离信号强度
        divergence_strength = pd.Series(0, index=close.index)
        divergence_strength += top_divergence_obv.astype(int) * 2    # 顶背离权重较高
        divergence_strength += top_divergence_ad.astype(int) * 2
        divergence_strength += bottom_divergence_obv.astype(int) * 2
        divergence_strength += bottom_divergence_ad.astype(int) * 2
        divergence_strength += momentum_divergence_obv.astype(int) * 1  # 动量背离权重较低
        divergence_strength += momentum_divergence_ad.astype(int) * 1
        
        analysis = pd.DataFrame({
            'price_momentum': price_momentum,
            'obv_momentum': obv_momentum,
            'ad_momentum': ad_momentum,
            'top_divergence_obv': top_divergence_obv,
            'top_divergence_ad': top_divergence_ad,
            'bottom_divergence_obv': bottom_divergence_obv,
            'bottom_divergence_ad': bottom_divergence_ad,
            'momentum_divergence_obv': momentum_divergence_obv,
            'momentum_divergence_ad': momentum_divergence_ad,
            'divergence_strength': divergence_strength
        }, index=close.index)
        
        return analysis
    
    def _analyze_short_term_momentum(self, momentum_indicators, close):
        """分析短期动量（基于RSI_7和5日移动平均线的高敏感性）
        
        Args:
            momentum_indicators: 包含RSI_7, RSI_14, ATR_14, EMA_5, SMA_5, VOLUME_SMA_5的字典
            close: 收盘价序列
            
        Returns:
            DataFrame: 短期动量分析结果
        """
        rsi7 = momentum_indicators['RSI_7']
        rsi14 = momentum_indicators['RSI_14']
        atr14 = momentum_indicators['ATR_14']
        ema5 = momentum_indicators['EMA_5']
        sma5 = momentum_indicators['SMA_5']
        volume_sma5 = momentum_indicators['VOLUME_SMA_5']
        
        # RSI_7短期信号
        rsi7_overbought = rsi7 > 70
        rsi7_oversold = rsi7 < 30
        rsi7_neutral = (rsi7 >= 30) & (rsi7 <= 70)
        
        # RSI_7快速反转信号
        rsi7_quick_reversal_up = (rsi7 > 50) & (rsi7.shift(1) <= 40)  # 快速从低位反弹
        rsi7_quick_reversal_down = (rsi7 < 50) & (rsi7.shift(1) >= 60)  # 快速从高位回落
        
        # RSI_7与RSI_14的背离
        rsi_divergence = ((rsi7.diff() > 0) & (rsi14.diff() < 0)) | ((rsi7.diff() < 0) & (rsi14.diff() > 0))
        
        # 5日移动平均线信号
        price_above_ema5 = close > ema5
        price_above_sma5 = close > sma5
        ema5_rising = ema5 > ema5.shift(1)
        sma5_rising = sma5 > sma5.shift(1)
        
        # EMA5与SMA5交叉信号（极短期趋势变化）
        ema5_above_sma5 = ema5 > sma5
        ema5_cross_up_sma5 = ema5_above_sma5 & (~ema5_above_sma5.shift(1).fillna(False))
        ema5_cross_down_sma5 = (~ema5_above_sma5) & ema5_above_sma5.shift(1).fillna(False)
        
        # 5日成交量趋势
        volume_above_avg5 = close.index.to_series().apply(lambda x: True)  # 占位，实际需要成交量数据
        try:
            # 获取当前K线中的成交量数据用于比较
            volume_increasing = volume_sma5 > volume_sma5.shift(1)
            volume_surge = volume_sma5 > volume_sma5.rolling(10).mean() * 1.5
        except:
            volume_increasing = pd.Series(False, index=close.index)
            volume_surge = pd.Series(False, index=close.index)
        
        # 基于RSI_7的动量强度
        rsi7_momentum = rsi7.diff(3)  # 3日RSI变化
        momentum_strength = pd.Series(0, index=close.index)
        momentum_strength += (rsi7_momentum > 5).astype(int) * 2    # 强上升动量
        momentum_strength += (rsi7_momentum > 2).astype(int) * 1    # 中等上升动量
        momentum_strength -= (rsi7_momentum < -5).astype(int) * 2   # 强下降动量
        momentum_strength -= (rsi7_momentum < -2).astype(int) * 1   # 中等下降动量
        
        # 价格动量与RSI_7一致性
        price_momentum = close.pct_change(3)  # 3日价格变化率
        momentum_consistency = ((price_momentum > 0) & (rsi7_momentum > 0)) | ((price_momentum < 0) & (rsi7_momentum < 0))
        
        # 基于ATR的波动率调整信号
        atr_normalized = atr14 / close  # ATR标准化
        high_volatility = atr_normalized > atr_normalized.rolling(20).quantile(0.8)
        low_volatility = atr_normalized < atr_normalized.rolling(20).quantile(0.2)
        
        # 短期突破信号（结合RSI_7和5日均线）
        price_breakout_up = close > close.rolling(5).max().shift(1)  # 突破5日高点
        price_breakout_down = close < close.rolling(5).min().shift(1)  # 跌破5日低点
        
        # RSI_7和5日均线确认的突破信号
        confirmed_breakout_up = price_breakout_up & (rsi7 > 50) & price_above_ema5 & ema5_rising
        confirmed_breakout_down = price_breakout_down & (rsi7 < 50) & (~price_above_ema5) & (~ema5_rising)
        
        # 5日均线支撑/阻力信号
        ema5_support = price_above_ema5 & (close.shift(1) <= ema5.shift(1))  # 价格重新站上EMA5
        ema5_resistance = (~price_above_ema5) & (close.shift(1) >= ema5.shift(1))  # 价格跌破EMA5
        
        # 短期交易信号生成（增强版）
        # 买入信号：RSI_7从超卖区域反弹且价格站上5日均线
        buy_signal = (rsi7_oversold.shift(1) & (rsi7 > 35)) & momentum_consistency & (ema5_support | ema5_cross_up_sma5)
        
        # 卖出信号：RSI_7从超买区域回落且价格跌破5日均线
        sell_signal = (rsi7_overbought.shift(1) & (rsi7 < 65)) & momentum_consistency & (ema5_resistance | ema5_cross_down_sma5)
        
        # 综合短期动量评分（增强版）
        momentum_score = pd.Series(50, index=close.index)  # 基础分50
        momentum_score += rsi7_quick_reversal_up.astype(int) * 10   # 快速反转上升+10
        momentum_score -= rsi7_quick_reversal_down.astype(int) * 10  # 快速反转下降-10
        momentum_score += confirmed_breakout_up.astype(int) * 15     # 确认突破上升+15
        momentum_score -= confirmed_breakout_down.astype(int) * 15   # 确认突破下降-15
        momentum_score += (rsi7 > rsi14).astype(int) * 5            # RSI_7>RSI_14为正面+5
        momentum_score -= (rsi7 < rsi14).astype(int) * 5            # RSI_7<RSI_14为负面-5
        momentum_score += (price_above_ema5 & ema5_rising).astype(int) * 8    # 价格在EMA5上方且EMA5上升+8
        momentum_score -= ((~price_above_ema5) & (~ema5_rising)).astype(int) * 8  # 价格在EMA5下方且EMA5下降-8
        momentum_score += ema5_cross_up_sma5.astype(int) * 6        # EMA5上穿SMA5+6
        momentum_score -= ema5_cross_down_sma5.astype(int) * 6      # EMA5下穿SMA5-6
        momentum_score += volume_surge.astype(int) * 5              # 成交量放大+5
        momentum_score = momentum_score.clip(0, 100)                # 限制在0-100范围

        analysis = pd.DataFrame({
            'rsi7_overbought': rsi7_overbought,
            'rsi7_oversold': rsi7_oversold,
            'rsi7_neutral': rsi7_neutral,
            'rsi7_quick_reversal_up': rsi7_quick_reversal_up,
            'rsi7_quick_reversal_down': rsi7_quick_reversal_down,
            'rsi_divergence': rsi_divergence,
            'rsi7_momentum': rsi7_momentum,
            'momentum_strength': momentum_strength,
            'momentum_consistency': momentum_consistency,
            'high_volatility': high_volatility,
            'low_volatility': low_volatility,
            'confirmed_breakout_up': confirmed_breakout_up,
            'confirmed_breakout_down': confirmed_breakout_down,
            'buy_signal': buy_signal,
            'sell_signal': sell_signal,
            'momentum_score': momentum_score,
            # 新增5日移动平均线相关字段
            'price_above_ema5': price_above_ema5,
            'price_above_sma5': price_above_sma5,
            'ema5_rising': ema5_rising,
            'sma5_rising': sma5_rising,
            'ema5_cross_up_sma5': ema5_cross_up_sma5,
            'ema5_cross_down_sma5': ema5_cross_down_sma5,
            'ema5_support': ema5_support,
            'ema5_resistance': ema5_resistance,
            'volume_increasing': volume_increasing,
            'volume_surge': volume_surge
        }, index=close.index)
        
        return analysis
    
    def get_advanced_indicators_from_cache(self, stock_code, indicator_name=None):
        """从第三层缓存获取高级指标数据
        
        Args:
            stock_code: 股票代码
            indicator_name: 指标名称，如果为None则返回所有指标
            
        Returns:
            指标数据或None
        """
        if stock_code not in self.advanced_indicators:
            return None
        
        stock_indicators = self.advanced_indicators[stock_code]
        indicators_data = stock_indicators.get('indicators', {})
        
        if indicator_name is None:
            return indicators_data
        else:
            return indicators_data.get(indicator_name, None)
    
    def get_indicator_dependencies(self, stock_code, indicator_name):
        """获取指标的依赖关系
        
        Args:
            stock_code: 股票代码
            indicator_name: 指标名称
            
        Returns:
            list: 依赖的基础指标列表
        """
        if stock_code not in self.advanced_indicators:
            return []
        
        dependencies = self.advanced_indicators[stock_code].get('dependencies', {})
        return dependencies.get(indicator_name, [])
    
    def _calculate_talib_success_rate(self):
        """计算talib成功率"""
        total_talib = (self.performance_stats['talib_success']['count'] + 
                      self.performance_stats['talib_failure']['count'])
        if total_talib > 0:
            return self.performance_stats['talib_success']['count'] / total_talib
        return 0.0

    def initialize_rqdatac(self):
        """初始化rqdatac连接"""
        try:
            if not RQDATAC_AVAILABLE:
                print("rqdatac模块未安装，无法初始化")
                print("请使用以下命令安装：pip install rqdatac")
                return False
                
            if self.rqdatac_initialized:
                print("rqdatac已经初始化")
                return True
                
            import rqdatac
            
            # 检查是否已经初始化
            try:
                # 尝试调用一个简单的API来检查初始化状态
                rqdatac.get_trading_dates('2024-01-01', '2024-01-01')
                print("rqdatac已经初始化")
                self.rqdatac_initialized = True
                return True
            except:
                # 如果调用失败，说明未初始化，继续初始化流程
                pass
                
            # 尝试初始化
            rqdatac.init()
            
            # 验证连接
            test_result = rqdatac.get_trading_dates('2024-01-01', '2024-01-02')
            if test_result is not None:
                print("rqdatac初始化成功")
                self.rqdatac_initialized = True
                return True
            else:
                print("rqdatac初始化失败：无法获取交易日数据")
                return False
                
        except Exception as e:
            print(f"rqdatac初始化失败: {e}")
            print("请确保：")
            print("1. 已安装rqdatac: pip install rqdatac")
            print("2. 已注册米筐账户并配置认证信息")
            print("3. 网络连接正常")
            return False
            
    def start_auto_update(self):
        """启动自动更新定时器"""
        if not self.timer_running:
            self.timer_running = True
            self.timer_thread = threading.Thread(target=self._update_loop, daemon=True)
            self.timer_thread.start()
            print("DataStore定时器已启动，每5分钟自动更新")
            
    def stop_auto_update(self):
        """停止自动更新定时器"""
        self.timer_running = False
        if self.timer_thread:
            self.timer_thread.join(timeout=5)
        print("DataStore定时器已停止")
        
    def _update_loop(self):
        """定时器主循环"""
        while self.timer_running:
            try:
                current_time = datetime.now()
                
                # 检查是否需要更新（工作日9:00-15:30）
                if self._is_trading_time(current_time):
                    if (self.last_update_time is None or 
                        (current_time - self.last_update_time).seconds >= self.update_interval):
                        
                        print(f"[{current_time.strftime('%H:%M:%S')}] 开始自动更新数据...")
                        self._auto_update_data()
                        self.last_update_time = current_time
                        
                time.sleep(60)  # 每分钟检查一次
                
            except Exception as e:
                print(f"定时器更新异常: {e}")
                time.sleep(60)
                
    def _is_trading_time(self, dt):
        """判断是否为交易时间"""
        # 周一到周五，9:00-15:30
        if dt.weekday() >= 5:  # 周末
            return False
        
        time_now = dt.time()
        morning_start = time_now >= dt.replace(hour=9, minute=0, second=0).time()
        morning_end = time_now <= dt.replace(hour=11, minute=30, second=0).time()
        afternoon_start = time_now >= dt.replace(hour=13, minute=0, second=0).time()
        afternoon_end = time_now <= dt.replace(hour=15, minute=30, second=0).time()
        
        return (morning_start and morning_end) or (afternoon_start and afternoon_end)
        
    def _auto_update_data(self):
        """自动更新所有缓存数据"""
        try:
            if not RQDATAC_AVAILABLE:
                print("rqdatac不可用，跳过数据更新")
                return
                
            if not self.rqdatac_initialized:
                if not self.initialize_rqdatac():
                    return
                
            # 1. 更新股票池
            self._update_stock_pool()
            
            # 2. 批量更新K线数据
            self._batch_update_kline_data()
            
            # 3. 计算和更新指标
            self._update_all_indicators()
            
            # 4. 更新事件
            self._update_events()
            
            print("数据自动更新完成")
            
        except Exception as e:
            print(f"自动更新数据失败: {e}")
            
    def _update_stock_pool(self):
        """更新股票池（直接从基础股票池文件加载）"""
        try:
            # 从基础股票池文件获取股票列表
            basic_stocks = self.get_pool_stocks('basic')
            if basic_stocks:
                self.stock_pool = basic_stocks
                print(f"股票池更新完成，共{len(self.stock_pool)}只股票")
            else:
                print("基础股票池为空，保持原有股票池")
        except Exception as e:
            print(f"更新股票池失败: {e}")
            
    def _batch_update_kline_data(self):
        """批量更新K线数据"""
        if not self.stock_pool:
            return
            
        print(f"开始更新{len(self.stock_pool)}只股票的K线数据...")
        
        # 使用统一的K线缓存更新方法
        self.update_kline_cache(self.stock_pool, ['daily', '60min', '15min'])
                
    def _update_all_indicators(self):
        """更新所有股票的指标"""
        if not self.stock_pool:
            return
            
        print("开始计算指标...")
        # 这里会被indicators模块调用，先预留接口
        for stock_code in self.stock_pool:
            try:
                # 为每只股票计算波动性指标
                indicator_key = f"volatility_{stock_code}"
                # 指标计算逻辑会在indicators模块中实现
                self.indicators_cache[indicator_key] = None  # 占位符
            except Exception as e:
                print(f"计算{stock_code}指标失败: {e}")
                
    def _update_events(self):
        """更新事件信息"""
        try:
            # 事件更新逻辑会在indicators模块中实现
            print("事件信息更新完成")
        except Exception as e:
            print(f"更新事件失败: {e}")
            
    # 新增的数据访问接口方法
    def get_60min_kline(self, stock_code, periods=60):
        """获取60分钟K线数据（使用统一接口）

        Args:
            stock_code: 股票代码
            periods: 返回的周期数

        Returns:
            DataFrame: 标准化的60分钟K线数据
        """
        try:
            # 使用统一的K线数据获取接口
            df = self.get_unified_kline_data(stock_code, timeframe='60min', use_cache=True)
            if df is not None and not df.empty:
                return df.tail(periods)
            else:
                return None
        except Exception as e:
            print(f"获取60分钟K线数据失败: {e}")
            return None
        
    def get_15min_kline(self, stock_code, periods=100):
        """获取15分钟K线数据（使用统一接口）

        Args:
            stock_code: 股票代码
            periods: 返回的周期数

        Returns:
            DataFrame: 标准化的15分钟K线数据
        """
        try:
            # 使用统一的K线数据获取接口
            df = self.get_unified_kline_data(stock_code, timeframe='15min', use_cache=True)
            if df is not None and not df.empty:
                return df.tail(periods)
            else:
                return None
        except Exception as e:
            print(f"获取15分钟K线数据失败: {e}")
            return None
        
    def get_daily_kline(self, stock_code, periods=100):
        """获取日K线数据（使用统一接口）

        Args:
            stock_code: 股票代码
            periods: 返回的周期数

        Returns:
            DataFrame: 标准化的日K线数据
        """
        try:
            # 使用统一的K线数据获取接口
            df = self.get_unified_kline_data(stock_code, timeframe='daily', use_cache=True)
            if df is not None and not df.empty:
                return df.tail(periods)
            else:
                return None
        except Exception as e:
            print(f"获取日K线数据失败: {e}")
            return None
        
    def get_stock_pool(self):
        """获取当前股票池"""
        return self.stock_pool.copy()
        
    def get_indicators_cache(self, key, check_latest=True):
        """获取指标缓存，优先从basic_indicators获取完整数据
        
        Args:
            key: 缓存键
            check_latest: 是否检查数据时效性（True则只返回最新数据）
        """
        # 1. 检查增量缓存
        cache_entry = self.indicators_cache.get(key)
        if cache_entry is not None:
            return cache_entry.get('data')
            
        # 2. 尝试从basic_indicators获取完整预计算数据
        complete_data = self._get_from_basic_indicators(key)
        if complete_data is not None:
            # 如果需要检查时效性
            if check_latest:
                # 获取对应的原始数据检查时间范围
                try:
                    # 解析key获取股票代码和频率
                    parts = key.split('_')
                    if len(parts) >= 4:
                        stock_code = parts[1]
                        frequency = parts[2]
                        
                        # 获取原始数据
                        raw_data_key = f"{stock_code}_{frequency}"
                        raw_data = self.get_data(raw_data_key)
                        
                        if raw_data is not None:
                            raw_end_date = raw_data.index[-1]
                            complete_end_date = complete_data.index[-1]
                            
                            # 只有当预计算数据覆盖到最新时间才直接返回
                            if complete_end_date >= raw_end_date:
                                return complete_data
                            # 否则返回None，让调用方进行增量计算
                            else:
                                return None
                except Exception:
                    pass
            else:
                # 不检查时效性，直接返回
                return complete_data
            
        return None
    
    def _get_from_basic_indicators(self, cache_key):
        """从basic_indicators中获取对应的指标数据"""
        try:
            # 解析cache_key: 格式类似 "rsi_000001.SZ_daily_14"
            parts = cache_key.split('_')
            if len(parts) < 4:
                return None
                
            indicator_type = parts[0].upper()  # RSI, SMA, EMA等
            stock_code = parts[1]
            frequency = parts[2]
            period = parts[3]
            
            # 构建指标名称
            indicator_name = f"{indicator_type}_{period}"
            
            # 从basic_indicators获取数据
            if stock_code in self.basic_indicators:
                indicators_data = self.basic_indicators[stock_code].get('indicators', {})
                return indicators_data.get(indicator_name)
                
        except Exception:
            pass
            
        return None
        
    def set_indicators_cache(self, key, value, is_partial=False):
        """设置指标缓存，智能决定存储位置

        Args:
            key: 缓存键
            value: 指标数据（可以是Series或DataFrame）
            is_partial: 是否为部分数据
        """
        from datetime import datetime

        if is_partial:
            # 部分数据存储在轻量级增量缓存中
            self.indicators_cache[key] = {
                'data': value,
                'timestamp': datetime.now(),
                'is_partial': True
            }
        else:
            # 完整数据优先存储在basic_indicators中
            self._store_in_basic_indicators(key, value)

            # 清理对应的增量缓存
            if key in self.indicators_cache:
                del self.indicators_cache[key]
    
    def _store_in_basic_indicators(self, cache_key, value):
        """将完整指标数据存储到basic_indicators中

        Args:
            cache_key: 缓存键
            value: 指标数据（可以是Series或DataFrame）
        """
        from datetime import datetime
        try:
            # 解析cache_key并存储到basic_indicators
            parts = cache_key.split('_')
            if len(parts) < 4:
                return

            indicator_type = parts[0].upper()
            stock_code = parts[1]
            frequency = parts[2]
            period = parts[3]

            indicator_name = f"{indicator_type}_{period}"

            # 确保股票的存储结构存在
            if stock_code not in self.basic_indicators:
                self.basic_indicators[stock_code] = {'indicators': {}}
            elif 'indicators' not in self.basic_indicators[stock_code]:
                self.basic_indicators[stock_code]['indicators'] = {}

            # 处理DataFrame数据，提取相应的列
            if isinstance(value, pd.DataFrame):
                # 如果是DataFrame，提取第一列作为指标数据
                if not value.empty and len(value.columns) > 0:
                    indicator_data = value.iloc[:, 0]  # 取第一列
                else:
                    indicator_data = pd.Series(dtype=float)
            else:
                # 如果是Series，直接使用
                indicator_data = value

            # 存储指标数据
            self.basic_indicators[stock_code]['indicators'][indicator_name] = indicator_data

        except Exception as e:
            print(f"存储指标数据失败: {e}")
            # 如果解析失败，降级到传统缓存
            self.indicators_cache[cache_key] = {
                'data': value,
                'timestamp': datetime.now(),
                'is_partial': False
            }
        
    def get_events_cache(self, key):
        """获取事件缓存"""
        return self.events_cache.get(key)
        
    def set_events_cache(self, key, value):
        """设置事件缓存"""
        self.events_cache[key] = value
    
    # 文件股票池功能（仅读取股票代码列表）
    def _load_stock_codes_from_file(self):
        """从文件读取股票代码列表"""
        try:
            file_path = self.data_dir / "filtered_stocks.json"
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                stocks = data.get('stocks', [])
                return [stock['code'] for stock in stocks if 'code' in stock]
            return []
        except Exception as e:
            print(f"从文件读取股票代码失败: {e}")
            return []
    
    def get_stock_codes_from_file(self):
        """获取文件中的股票代码列表"""
        today = date.today().isoformat()
        
        # 检查缓存是否需要更新
        if self._file_last_load_date != today or self._file_stock_codes_cache is None:
            self._file_stock_codes_cache = self._load_stock_codes_from_file()
            self._file_last_load_date = today
            
            if self._file_stock_codes_cache:
                print(f"从文件加载股票代码: {len(self._file_stock_codes_cache)}只")
            else:
                print("未找到文件中的股票代码")
        
        return self._file_stock_codes_cache or []
    
    def update_pool_from_file(self):
        """从文件更新股票池"""
        stock_codes = self.get_stock_codes_from_file()
        if stock_codes:
            # 更新股票池
            self.stock_pool = stock_codes
            print(f"股票池已从文件更新: {len(stock_codes)}只股票")
            return stock_codes
        else:
            print("文件中没有找到股票代码，保持原有股票池")
            return self.stock_pool.copy()
    
    def get_standardized_data(self, stock_code, data_type='kline', timeframe='daily', periods=None):
        """统一的标准化数据获取接口

        Args:
            stock_code: 股票代码
            data_type: 数据类型 ('kline', 'indicators')
            timeframe: 时间粒度 ('daily', '60min', '15min', '5min', '1min')
            periods: 返回的周期数，None表示全部

        Returns:
            DataFrame: 标准化的数据
        """
        try:
            if data_type == 'kline':
                # 获取K线数据
                df = self.get_unified_kline_data(stock_code, timeframe=timeframe, use_cache=True)
                if df is not None and periods is not None:
                    df = df.tail(periods)
                return df

            elif data_type == 'indicators':
                # 获取基础指标数据
                indicators = self.get_basic_indicators_from_cache(stock_code)
                if indicators is None:
                    return None

                # 将指标数据转换为DataFrame格式
                indicator_dfs = {}
                for ind_name, ind_data in indicators.items():
                    if hasattr(ind_data, 'index'):
                        if isinstance(ind_data, dict):
                            # 复合指标（如MACD, BOLL, STOCH）
                            for sub_name, sub_data in ind_data.items():
                                if hasattr(sub_data, 'index'):
                                    col_name = f"{ind_name}_{sub_name}"
                                    indicator_dfs[col_name] = sub_data
                        else:
                            # 单一指标
                            indicator_dfs[ind_name] = ind_data

                if indicator_dfs:
                    result_df = pd.DataFrame(indicator_dfs)
                    if periods is not None:
                        result_df = result_df.tail(periods)
                    return result_df

            return None

        except Exception as e:
            print(f"获取标准化数据失败: {e}")
            return None
    
    @classmethod
    def get_instance(cls, cache_dir='cache'):
        """获取DataStore单例实例的便捷方法"""
        return cls(cache_dir)
    
    @classmethod
    def reset_instance(cls):
        """重置单例实例（主要用于测试）"""
        cls._instance = None
        cls._initialized = False

# 创建全局DataStore实例
datastore = DataStore.get_instance()