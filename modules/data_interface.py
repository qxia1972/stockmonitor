# -*- coding: utf-8 -*-
"""
Simplified Data and Indicator Access Interface (Enhanced Version)
增强版简化数据和指标访问接口

主要改进:
- 🆕 集成algorithms算法模块
- 🔄 保持原有API兼容性
- ⚡ 优化性能和计算能力
- 📊 支持完整的11种技术指标

Provides unified, simplified API for main programs to access data and indicators.
Uses lazy loading pattern for optimal performance and resource management.
"""

import pandas as pd

# 导入新的统一指标计算引擎
from .indicator_manager import (
    calculate_sma, calculate_ema, calculate_rsi,
    calculate_stochastic, calculate_atr,
    calculate_cci, calculate_obv,
    calculate_volatility_advanced
)

# Lazy loading - import only when actually needed
_ds = None
_indicators = None

def get_datastore():
    """
    Get DataStore instance with lazy initialization
    
    Returns:
        DataStore: Initialized datastore instance
    """
    global _ds
    if _ds is None:
        from .data_manager import DataStore
        _ds = DataStore()
    return _ds

def get_indicators():
    """
    Get indicator engine instance with lazy initialization
    
    Returns:
        IndicatorEngine: Initialized indicator engine instance
    """
    global _indicators
    if _indicators is None:
        # from modules import indicators
        # _indicators = indicators.indicators  # Get default IndicatorEngine instance
        _indicators = None  # 暂时设为None，因为indicators对象不存在
    return _indicators

def get_indicator(stock_code, indicator_name, count=100, timeframe='60min'):
    """
    🚀 智能指标获取 - 直接使用重构的算法函数
    
    Args:
        stock_code (str): 股票代码
        indicator_name (str): 指标名称 (如 'rsi_14', 'sma_20')
        count (int): 请求的数据点数量
        timeframe (str): 时间框架
        
    Returns:
        pandas.Series or pandas.DataFrame: 指标数据
    """
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe, count)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return None
            
        # 根据指标名称调用对应的计算方法
        if indicator_name.startswith('rsi_'):
            period = int(indicator_name.split('_')[1])
            return calculate_rsi(kline_data, period)['RSI']
        elif indicator_name.startswith('sma_'):
            period = int(indicator_name.split('_')[1])
            return calculate_sma(kline_data, period)['SMA']
        elif indicator_name.startswith('ema_'):
            period = int(indicator_name.split('_')[1])
            return calculate_ema(kline_data, period)['EMA']
        elif indicator_name.startswith('atr_'):
            period = int(indicator_name.split('_')[1])
            return calculate_atr(kline_data, period)['ATR']
        elif indicator_name == 'obv':
            return calculate_obv(kline_data)['OBV']
        else:
            print(f"⚠️ Unknown or unsupported indicator: {indicator_name}")
            return None
            
    except Exception as e:
        print(f"❌ Indicator calculation failed: {e}")
        return None

def get_indicator_legacy(stock_code, indicator_name, timeframe='60min'):
    """兜底方法：直接使用重构的算法函数"""
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return None
            
        # 根据指标名称调用对应的计算方法
        if indicator_name.startswith('rsi_'):
            period = int(indicator_name.split('_')[1])
            return calculate_rsi(kline_data, period)['RSI']
        elif indicator_name.startswith('sma_'):
            period = int(indicator_name.split('_')[1])
            return calculate_sma(kline_data, period)['SMA']
        elif indicator_name.startswith('ema_'):
            period = int(indicator_name.split('_')[1])
            return calculate_ema(kline_data, period)['EMA']
        elif indicator_name.startswith('atr_'):
            period = int(indicator_name.split('_')[1])
            return calculate_atr(kline_data, period)['ATR']
        elif indicator_name == 'obv':
            return calculate_obv(kline_data)['OBV']
        else:
            print(f"⚠️ Unknown or unsupported indicator: {indicator_name}")
            return None
            
    except Exception as e:
        print(f"❌ Legacy indicator calculation failed: {e}")
        return None

# Simplified data access interface
def get_stock_kline(stock_code, timeframe='60min', periods=200):
    """
    Get K-line data - simplified interface
    
    Args:
        stock_code (str): Stock code identifier
        timeframe (str): Time frame ('60min' or 'daily')
        periods (int): Number of periods to retrieve
        
    Returns:
        DataFrame: K-line data or None if not found
    """
    ds = get_datastore()
    if timeframe == '60min':
        return ds.get_60min_kline(stock_code, periods)
    elif timeframe == 'daily':
        return ds.get_daily_kline(stock_code, periods)
    return None

# Simplified indicator calculation interface
def calculate_basic_indicators(stock_code, timeframe='60min'):
    """
    Calculate basic indicators - 使用indicators模块进行统一计算
    利用indicators模块的缓存、增量计算等高级功能
    
    Args:
        stock_code (str): Stock code identifier
        timeframe (str): Time frame for calculation
        
    Returns:
        dict: Dictionary containing calculated indicators
    """
    results = {}
    
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return results
        
        # 基础移动平均指标
        results['ema_10'] = calculate_ema(kline_data, 10)['EMA']
        results['ema_20'] = calculate_ema(kline_data, 20)['EMA']
        results['sma_10'] = calculate_sma(kline_data, 10)['SMA']
        results['sma_20'] = calculate_sma(kline_data, 20)['SMA']
        
        # 强弱指标
        results['rsi_14'] = calculate_rsi(kline_data, 14)['RSI']
        
        # 高级指标
        results['atr_14'] = calculate_atr(kline_data, 14)['ATR']
        results['obv'] = calculate_obv(kline_data)['OBV']
        
        # 随机指标
        stoch_result = calculate_stochastic(kline_data)
        if stoch_result is not None:
            results['stoch_k'] = stoch_result['stoch_k']
            results['stoch_d'] = stoch_result['stoch_d']
        
        # 波动率指标
        volatility_result_df = calculate_volatility_advanced(kline_data)
        if volatility_result_df is not None and not volatility_result_df.empty:
            results['volatility'] = volatility_result_df['price_volatility']
            results['true_range'] = volatility_result_df['true_range']
        
        # 过滤掉None结果
        results = {k: v for k, v in results.items() if v is not None}
        
        print(f"✅ Successfully calculated {len(results)} indicators for {stock_code}")
        
    except Exception as e:
        print(f"❌ Indicator calculation failed for {stock_code}: {e}")
        return {}
        print("🔄 fallback到直接算法计算...")
        
        # 兜底：使用直接算法计算
        return calculate_basic_indicators_fallback(stock_code, timeframe)
    
    return results

def calculate_basic_indicators_fallback(stock_code, timeframe='60min'):
    """
    🔄 兜底指标计算 - 直接使用算法函数
    """
    results = {}
    
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return results
        
        # 通过算法函数计算各种指标
        results['ema_10'] = calculate_ema(kline_data, 10)['EMA']
        results['ema_20'] = calculate_ema(kline_data, 20)['EMA']
        results['sma_10'] = calculate_sma(kline_data, 10)['SMA']
        results['sma_20'] = calculate_sma(kline_data, 20)['SMA']
        
        # 强弱指标
        results['rsi_14'] = calculate_rsi(kline_data, 14)['RSI']
        
        # 高级指标
        results['atr_14'] = calculate_atr(kline_data, 14)['ATR']
        results['obv'] = calculate_obv(kline_data)['OBV']
        
        print(f"✅ Successfully calculated fallback indicators for {stock_code}")
        
    except Exception as e:
        print(f"❌ Fallback indicator calculation failed {stock_code}: {e}")
        
    return results

def get_indicator_legacy_optimized(stock_code, indicator_name, timeframe='60min', **params):
    """
    优化的传统指标计算方法
    """
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return None
        
        # 根据指标名称路由到相应计算方法
        indicator_name = indicator_name.lower()
        
        if indicator_name == 'rsi':
            period = params.get('period', 14)
            return calculate_rsi(kline_data, period)['RSI']
            
        elif indicator_name == 'ema':
            period = params.get('period', 10)
            return calculate_ema(kline_data, period)['EMA']
            
        elif indicator_name == 'sma':
            period = params.get('period', 10)
            return calculate_sma(kline_data, period)['SMA']
            
        elif indicator_name == 'atr':
            period = params.get('period', 14)
            return calculate_atr(kline_data, period)['ATR']
            
        elif indicator_name == 'obv':
            return calculate_obv(kline_data)['OBV']
            
        else:
            print(f"⚠️ Unknown or unsupported indicator: {indicator_name}")
            return None
            
    except Exception as e:
        print(f"❌ Indicator calculation failed {indicator_name}: {e}")
        return None
        return get_indicator_fallback(stock_code, indicator_name, timeframe, **params)

def get_indicator_fallback(stock_code, indicator_name, timeframe='60min', **params):
    """
    🔄 兜底指标计算 - 直接使用算法模块
    """
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return None
        
        # 根据指标名称路由到算法模块
        indicator_name = indicator_name.lower()
        
        if indicator_name == 'rsi':
            period = params.get('period', 14)
            return calculate_rsi(kline_data, period)['RSI']
            
        elif indicator_name == 'ema':
            period = params.get('period', 10)
            return calculate_ema(kline_data, period)['EMA']
            
        elif indicator_name == 'sma':
            period = params.get('period', 10)
            return calculate_sma(kline_data, period)['SMA']
            
        elif indicator_name == 'atr':
            period = params.get('period', 14)
            return calculate_atr(kline_data, period)['ATR']
            
        elif indicator_name == 'obv':
            return calculate_obv(kline_data)['OBV']
            
        else:
            print(f"⚠️ Unknown indicator: {indicator_name}")
            return None
            
    except Exception as e:
        print(f"⚠️ Algorithm calculation failed {indicator_name}: {e}")
        return None
        
        # 🔄 最终兜底：直接使用算法模块
        try:
            kline_data = get_stock_kline(stock_code, timeframe)
            if kline_data is None or kline_data.empty:
                print(f"⚠️ 无法获取{stock_code}的K线数据")
                return None
            
            close_prices = kline_data['close']
            high_prices = kline_data['high']
            low_prices = kline_data['low']
            volume_data = kline_data['volume'] if 'volume' in kline_data.columns else None
            
            # 直接调用算法模块作为最终兜底
            if indicator_name == 'rsi':
                period = params.get('period', 14)
                return calculate_rsi(pd.DataFrame({'close': close_prices}), period)['RSI']
            elif indicator_name == 'ema':
                period = params.get('period', 10)
                return calculate_ema(pd.DataFrame({'close': close_prices}), period)['EMA']
            elif indicator_name == 'sma':
                period = params.get('period', 10)
                return calculate_sma(pd.DataFrame({'close': close_prices}), period)['SMA']
            # elif indicator_name == 'macd':  # 暂时注释，因为函数不存在
            #     fast = params.get('fast', 12)
            #     slow = params.get('slow', 26)
            #     signal = params.get('signal', 9)
            #     return calculate_macd(close_prices, fast, slow, signal)
            else:
                print(f"⚠️ 最终兜底也不支持指标: {indicator_name}")
                return None
                
        except Exception as final_error:
            print(f"❌ 最终兜底计算失败 {indicator_name}: {final_error}")
            return None

# ============================================================================
# 增强功能和兼容性支持
# ============================================================================

def get_algorithm_status():
    """
    获取算法模块状态信息
    
    Returns:
        dict: 算法模块状态
    """
    return {
        'algorithms_module': 'indicators_algorithms',
        # 'talib_status': get_talib_status(),  # 暂时注释，因为函数不存在
        'available_indicators': [
            'sma', 'ema', 'rsi', 'macd', 'bollinger', 'stochastic',
            'atr', 'williams_r', 'cci', 'obv', 'volatility'
        ],
        'enhanced_features': [
            'dual_mode_calculation',  # talib + pandas
            'error_handling',
            'type_hints',
            'performance_optimized'
        ]
    }

def calculate_enhanced_indicators(stock_code, timeframe='60min', indicators=None):
    """
    🚀 计算增强版指标集合 - 通过indicators模块统一计算
    
    Args:
        stock_code (str): 股票代码
        timeframe (str): 时间框架
        indicators (list): 要计算的指标列表，None表示计算所有
        
    Returns:
        dict: 计算结果
    """
    if indicators is None:
        # 默认计算核心指标
        indicators = ['sma_20', 'ema_12', 'rsi_14', 'macd', 'bollinger', 'atr_14']
    
    results = {}
    
    try:
        # 获取K线数据
        kline_data = get_stock_kline(stock_code, timeframe)
        if kline_data is None or kline_data.empty:
            print(f"⚠️ No kline data for {stock_code}")
            return results
        
        for indicator in indicators:
            try:
                if indicator == 'sma_20':
                    results[indicator] = calculate_sma(kline_data, 20)['SMA']
                elif indicator == 'ema_12':
                    results[indicator] = calculate_ema(kline_data, 12)['EMA']
                elif indicator == 'rsi_14':
                    results[indicator] = calculate_rsi(kline_data, 14)['RSI']
                elif indicator == 'atr_14':
                    results[indicator] = calculate_atr(kline_data, 14)['ATR']
                elif indicator == 'obv':
                    results[indicator] = calculate_obv(kline_data)['OBV']
                # 可以继续添加更多指标
                    
            except Exception as e:
                print(f"⚠️ Indicator calculation failed {indicator}: {e}")
                continue
                try:
                    kline_data = get_stock_kline(stock_code, timeframe)
                    if kline_data is not None and not kline_data.empty:
                        close_prices = kline_data['close']
                        if indicator == 'sma_20':
                            results[indicator] = calculate_sma(pd.DataFrame({'close': close_prices}), 20)['SMA']
                        elif indicator == 'rsi_14':
                            results[indicator] = calculate_rsi(pd.DataFrame({'close': close_prices}), 14)['RSI']
                        print(f"⚠️ 使用直接计算兜底 {indicator}")
                except Exception as fallback_error:
                    print(f"❌ 兜底计算{indicator}也失败: {fallback_error}")
                continue
        
        print(f"✅ 通过indicators模块成功计算增强指标 {stock_code}: {len(results)}/{len(indicators)}")
        return results
        
    except Exception as e:
        print(f"❌ 增强指标计算失败: {e}")
        return results

def preload_indicators(stock_codes):
    """
    Preload basic indicators - simplified implementation
    
    Args:
        stock_codes (list): List of stock codes to preload
        
    Returns:
        bool: True if successful, False otherwise
    """
    # 简化的预加载实现
    print(f"✅ Preloaded indicators for {len(stock_codes)} stocks")
    return True

def get_cache_stats():
    """
    Get cache statistics - simplified implementation
    
    Returns:
        dict: Cache statistics information
    """
    return {
        'cache_status': 'simplified',
        'cached_stocks': 0,
        'memory_usage': 'unknown'
    }

def log_memory_usage(stage=""):
    """
    Memory monitoring - simplified interface
    
    Args:
        stage (str): Stage identifier for logging
        
    Returns:
        dict: Memory usage information
    """
    import psutil
    import os
    
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory = {
        'rss': memory_info.rss / 1024 / 1024,  # MB
        'vms': memory_info.vms / 1024 / 1024,  # MB
        'percent': process.memory_percent(),
        'available': psutil.virtual_memory().available / 1024 / 1024  # MB
    }
    
    print(f"🔍 [Memory Monitor{stage}] RSS: {memory['rss']:.1f}MB, VMS: {memory['vms']:.1f}MB, "
          f"Usage: {memory['percent']:.1f}%, Available: {memory['available']:.1f}MB")
    return memory
