# -*- coding: utf-8 -*-
"""
Stock Selection Strategy System Unified Configuration File
Integrates all system-related configuration parameters
"""

# ========================================================
#                    Basic Configuration
# ========================================================



# Stock Pool Configuration
STOCK_POOL = []  # Please fill in stock codes to monitor, e.g., ['600000.XSHG', '000001.XSHE']
N_DAYS_REMOVE = 3  # Delayed removal days
PROTECT_ZONE_UP = [0.15, 0.30]  # Upper protection zone
PROTECT_ZONE_DOWN = [-0.15, -0.30]  # Lower protection zone

# ========================================================
#                  Volatility Calculation Configuration
# ========================================================

# Volatility calculation mode selection
VOLATILITY_MODE = "tonghuashun"  # "simple", "advanced", "tonghuashun"

# TongHuaShun formula configuration
TONGHUASHUN_CONFIG = {
    'enable_volume_enhancement': True,  # Enable volume enhancement
    'volume_weight_limit': [0.1, 5.0], # Volume weight limit range
    'N': 10,                           # Calculation period
}

# Advanced mode parameter configuration
ADVANCED_CONFIG = {
    # 基础周期参数
    "N": 10,        # 短期周期参数 (用于成交量标准化和波动成分计算)
    "M": 5,         # 中期周期参数 (用于趋势计算)
    "Extra": 20,    # 长期周期参数 (用于最终趋势线平滑)
    
    # 趋势计算类型
    "trend_type": 1,  # 1=双指数平滑, 2=小波分解, 3=移动平均
}

# 不同市场风格的预设配置
PRESET_CONFIGS = {
    "稳健型": {
        "N": 15,
        "M": 8, 
        "Extra": 30,
        "trend_type": 1
    },
    "激进型": {
        "N": 5,
        "M": 3,
        "Extra": 10,
        "trend_type": 2
    },
    "均衡型": {
        "N": 10,
        "M": 5,
        "Extra": 20,
        "trend_type": 1
    }
}

# ========================================================
#                   指标计算配置
# ========================================================

# 基础指标参数
INDICATOR_CONFIG = {
    'ma_periods': [5, 10, 20, 60],     # 移动平均线周期
    'rsi_period': 14,                   # RSI周期
    'macd_params': [12, 26, 9],        # MACD参数 [快线, 慢线, 信号线]
    'bb_period': 20,                    # 布林带周期
    'bb_std': 2,                        # 布林带标准差倍数
    'atr_period': 14,                   # ATR周期
    'volume_ma_period': 20,             # 成交量移动平均周期
}

# 评分权重配置
SCORE_WEIGHTS = {
    'technical_weight': 0.4,    # 技术指标权重
    'fundamental_weight': 0.3,  # 基本面权重
    'momentum_weight': 0.2,     # 动量因子权重
    'risk_weight': 0.1,         # 风险因子权重
}

# ========================================================
#                    数据源配置
# ========================================================

# rqdatac配置
RQDATAC_CONFIG = {
    'timeout': 30,              # 请求超时时间(秒)
    'retry_times': 3,           # 重试次数
    'retry_delay': 1,           # 重试间隔(秒)
    'batch_size': 100,          # 批处理大小
}

# 数据缓存配置
CACHE_CONFIG = {
    'enable_cache': True,        # 是否启用缓存
    'cache_dir': 'cache',        # 缓存目录
    'realtime_cache_dir': 'realtime_cache',  # 实时缓存目录
    'cache_expire_hours': 1,     # 缓存过期时间(小时)
    'max_cache_size_mb': 500,    # 最大缓存大小(MB)
}

# talib库可用性检查
ENABLE_TALIB = True  # 是否启用talib优化

# talib参数配置
TALIB_PARAMS = {
    'ATR_PERIOD': 14,      # ATR周期
    'RSI_PERIOD': 14,      # RSI周期
    'MACD_FAST': 12,       # MACD快线周期
    'MACD_SLOW': 26,       # MACD慢线周期
    'MACD_SIGNAL': 9,      # MACD信号线周期
    'BB_PERIOD': 20,       # 布林带周期
    'BB_NBDEV': 2,         # 布林带标准差倍数
}

# ========================================================
#                   日志配置
# ========================================================

# 日志级别
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# 日志文件配置
LOG_CONFIG = {
    'log_dir': 'logs',
    'event_log': 'event_log.md',
    'error_log': 'error.log',
    'performance_log': 'performance.log',
    'max_log_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5
}

# ========================================================
#                   Development Configuration
# ========================================================

# Debug mode
DEBUG_MODE = False

# Test configuration
TEST_CONFIG = {
    'enable_unit_tests': True,
    'enable_performance_tests': True,
    'test_data_size': 1000,
    'benchmark_iterations': 100
}

# Chart configuration
CHART_CONFIG = {
    'figure_size': (12, 8),
    'dpi': 100,
    'style': 'seaborn',
    'colors': ['blue', 'red', 'green', 'orange', 'purple']
}

# K-line chart configuration
KLINE_CONFIG = {
    'kline_count': 160,      # Number of K-lines
    'data_days': 160,        # Data acquisition days (ensure enough 60-minute K-lines)
    'frequency': '60m',      # K-line frequency
    'min_data_points': 20    # Minimum data points requirement
}
