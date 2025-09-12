"""
数据格式约定和标准定义
Data Format Conventions and Standards

这个模块定义了整个系统中数据格式的标准约定，确保所有组件使用统一的DataFrame格式。
This module defines the standard conventions for data formats throughout the system,
ensuring all components use unified DataFrame formats.
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Callable
import numpy as np
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import logging
import talib
import sys

# 获取logger
logger = logging.getLogger(__name__)

# ============================================================================
# RQDatac兼容字段定义
# ============================================================================

# RQDatac标准字段映射（扩展版，基于API探索结果）
RQDATAC_FIELD_MAPPING = {
    # 股票基本信息
    'order_book_id': 'str',                    # 股票代码 (RQDatac标准字段)
    'symbol': 'str',                          # 股票简称
    'display_name': 'str',                    # 显示名称
    'company_name': 'str',                    # 公司名称
    'sector_code': 'str',                     # 板块代码
    'industry_code': 'str',                   # 行业代码
    'industry_name': 'str',                   # 行业名称
    'area_code': 'str',                       # 地区代码
    'listed_date': 'datetime64[ns]',          # 上市日期

    # 价格数据 (扩展)
    'open': 'float32',                        # 开盘价
    'close': 'float32',                       # 收盘价
    'high': 'float32',                        # 最高价
    'low': 'float32',                         # 最低价
    'pre_close': 'float32',                   # 昨收价
    'change': 'float32',                      # 涨跌额
    'change_pct': 'float32',                  # 涨跌幅
    'amplitude': 'float32',                   # 振幅
    'volume': 'int64',                        # 成交量
    'total_turnover': 'float64',               # 成交额
    'turnover_ratio': 'float32',              # 换手率
    'vwap': 'float32',                        # 成交均价
    'adj_close': 'float32',                   # 后复权收盘价
    'adj_factor': 'float32',                  # 复权因子

    # 日期时间
    'date': 'datetime64[ns]',                 # 交易日期
    'datetime': 'datetime64[ns]',             # 交易时间戳

    # 股本数据 (扩展)
    'total_shares': 'int64',                  # 总股本
    'float_shares': 'int64',                  # 流通股本
    'circulation_shares': 'int64',            # 流通股本（另一种表示）
    'restricted_shares': 'int64',             # 限售股本
    'float_ratio': 'float32',                 # 流通比例

    # 市值数据 (扩展)
    'market_cap': 'float64',                  # 总市值
    'circulation_market_cap': 'float64',      # 流通市值
    'float_market_cap': 'float64',            # 自由流通市值
    'total_market_cap': 'float64',            # 总市值（另一种表示）

    # 估值指标
    'pe_ratio': 'float32',                    # 市盈率
    'pb_ratio': 'float32',                    # 市净率
    'ps_ratio': 'float32',                    # 市销率
    'pcf_ratio': 'float32',                   # 市现率

    # 财务指标 (扩展)
    'roe': 'float32',                         # 净资产收益率
    'roa': 'float32',                         # 总资产收益率
    'gross_profit_margin': 'float32',         # 毛利率
    'net_profit_margin': 'float32',           # 净利率
    'operating_profit_margin': 'float32',     # 营业利润率
    'eps': 'float32',                         # 每股收益
    'bps': 'float32',                         # 每股净资产
    'total_assets': 'float64',                # 总资产
    'total_liabilities': 'float64',           # 总负债
    'total_equity': 'float64',                # 股东权益
    'net_profit': 'float64',                  # 净利润
    'operating_revenue': 'float64',           # 营业收入
    'operating_cost': 'float64',              # 营业成本

    # 现金流指标
    'net_cash_flows_from_operating': 'float64',  # 经营活动现金流量净额
    'net_cash_flows_from_investing': 'float64',  # 投资活动现金流量净额
    'net_cash_flows_from_financing': 'float64',  # 融资活动现金流量净额
    'free_cash_flow': 'float64',              # 自由现金流

    # 成长能力指标
    'revenue_growth': 'float32',              # 营收增长率
    'profit_growth': 'float32',               # 利润增长率
    'eps_growth': 'float32',                  # 每股收益增长率
    'roe_growth': 'float32',                  # ROE增长率

    # 盈利能力指标
    'gross_profit': 'float64',                # 毛利润
    'operating_profit': 'float64',            # 营业利润
    'total_profit': 'float64',                # 利润总额
    'net_profit_to_parent': 'float64',        # 归母净利润

    # 营运能力指标
    'total_asset_turnover': 'float32',        # 总资产周转率
    'inventory_turnover': 'float32',          # 存货周转率
    'receivables_turnover': 'float32',        # 应收账款周转率
    'current_ratio': 'float32',               # 流动比率
    'quick_ratio': 'float32',                 # 速动比率

    # 因子数据 (扩展)
    'factor_name': 'category',                # 因子名称
    'factor_value': 'float32',                # 因子值
    'volume_ratio': 'float32',                # 量比
    'amount_ratio': 'float32',                # 金额比
    'advance_decline_ratio': 'float32',       # 涨跌比
    'up_down_ratio': 'float32',               # 涨跌家数比
    'volume_ma_ratio': 'float32',             # 成交量均线比
    'price_ma_ratio': 'float32',              # 价格均线比
    'momentum': 'float32',                    # 动量因子
    'volatility': 'float32',                  # 波动率因子
    'liquidity': 'float32',                   # 流动性因子
    'quality': 'float32',                     # 质量因子
    'value': 'float32',                       # 价值因子
    'growth': 'float32',                      # 成长因子
    'size': 'float32',                        # 规模因子

    # 行业数据 (扩展)
    'level': 'category',                      # 行业等级
    'parent_code': 'str',                     # 父级行业代码
    'source': 'category',                     # 数据来源
    'version': 'str',                         # 版本号
    'start_date': 'datetime64[ns]',           # 开始日期
    'end_date': 'datetime64[ns]',             # 结束日期

    # 股本变动数据
    'change_reason': 'str',                   # 变动原因
    'announcement_date': 'datetime64[ns]',    # 公告日期

    # 技术指标 (RQDatac直接提供)
    # SMA简单移动平均系列 (RQDatac中叫MA)
    'sma_3': 'float32',                       # 3日简单移动平均
    'sma_5': 'float32',                       # 5日简单移动平均
    'sma_10': 'float32',                      # 10日简单移动平均
    'sma_20': 'float32',                      # 20日简单移动平均
    'sma_30': 'float32',                      # 30日简单移动平均
    'sma_55': 'float32',                      # 55日简单移动平均
    'sma_60': 'float32',                      # 60日简单移动平均
    'sma_120': 'float32',                     # 120日简单移动平均
    'sma_250': 'float32',                     # 250日简单移动平均

    # EMA指数移动平均系列 (RQDatac可用)
    'ema_3': 'float32',                       # 3日指数移动平均
    'ema_5': 'float32',                       # 5日指数移动平均
    'ema_10': 'float32',                      # 10日指数移动平均
    'ema_12': 'float32',                      # 12日指数移动平均
    'ema_20': 'float32',                      # 20日指数移动平均
    'ema_26': 'float32',                      # 26日指数移动平均
    'ema_30': 'float32',                      # 30日指数移动平均
    'ema_55': 'float32',                      # 55日指数移动平均
    'ema_60': 'float32',                      # 60日指数移动平均

    # RSI相对强弱指数系列 (RQDatac可用)
    'rsi_6': 'float32',                       # 6日RSI指标
    'rsi_10': 'float32',                      # 10日RSI指标
    'rsi_14': 'float32',                      # 14日RSI指标
    'rsi_21': 'float32',                      # 21日RSI指标
    'marsi_6': 'float32',                     # 修正RSI 6日
    'marsi_10': 'float32',                    # 修正RSI 10日

    # MACD指标系列 (RQDatac可用)
    'macd': 'float32',                        # MACD指标 (DIFF线)
    'macd_signal': 'float32',                 # MACD信号线 (DEA线)
    'macd_hist': 'float32',                   # MACD柱状图 (HIST)

    # KDJ随机指标系列 (RQDatac可用)
    'stoch_k': 'float32',                     # 随机指标K值
    'stoch_d': 'float32',                     # 随机指标D值
    'stoch_j': 'float32',                     # 随机指标J值

    # CCI顺势指标 (RQDatac可用)
    'cci': 'float32',                         # 顺势指标
    'cci_14': 'float32',                      # 14日顺势指标
    'cci_20': 'float32',                      # 20日顺势指标

    # ADX平均趋向指数系列 (RQDatac可用)
    'adx': 'float32',                         # 平均趋向指数
    'adx_14': 'float32',                      # 14日平均趋向指数
    'adxr': 'float32',                        # 平均趋向指数评级

    # ATR平均真实波幅 (RQDatac可用)
    'atr': 'float32',                         # 平均真实波幅
    'atr_14': 'float32',                      # 14日平均真实波幅

    # 布林带系列 (RQDatac可用)
    'bb_upper': 'float32',                    # 布林带上轨
    'bb_middle': 'float32',                   # 布林带中轨
    'bb_lower': 'float32',                    # 布林带下轨
    'bbiboll_upper': 'float32',               # 布林带上轨 (BBI修正)
    'bbiboll_lower': 'float32',               # 布林带下轨 (BBI修正)

    # 其他技术指标 (RQDatac可用)
    'roc': 'float32',                         # 变动率指标
    'willr_14': 'float32',                    # 14日威廉指标
    'di_plus': 'float32',                     # 正向指标
    'di_minus': 'float32',                    # 负向指标
    'bbi': 'float32',                         # 多空指数
    'matrix': 'float32',                      # 矩阵指标

    # 系统字段
    'created_at': 'datetime64[ns]',           # 创建时间
    'updated_at': 'datetime64[ns]',           # 更新时间
    'data_source': 'str',                     # 数据来源
    'last_sync_time': 'datetime64[ns]'        # 最后同步时间
}

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
# RQDatac API字段名称映射
# ============================================================================

# 从内部字段名称到RQDatac API实际字段名称的映射
# 映射策略：
# - 'DIRECT': 直接映射，完全匹配
# - 'APPROXIMATE': 近似映射，使用最接近的参数
# - 'UNAVAILABLE': RQDatac中不可用，需要自己计算
RQDATAC_API_FIELD_MAPPING = {
    # 移动平均线系列 (SMA在RQDatac中叫MA)
    # ✅ 直接映射：RQDatac提供MA3-MA250
    'sma_3': 'MA3',                           # 3日简单移动平均 [DIRECT]
    'sma_5': 'MA5',                           # 5日简单移动平均 [DIRECT]
    'sma_10': 'MA10',                         # 10日简单移动平均 [DIRECT]
    'sma_20': 'MA20',                         # 20日简单移动平均 [DIRECT]
    'sma_30': 'MA30',                         # 30日简单移动平均 [DIRECT]
    'sma_55': 'MA55',                         # 55日简单移动平均 [DIRECT]
    'sma_60': 'MA60',                         # 60日简单移动平均 [DIRECT]
    'sma_120': 'MA120',                       # 120日简单移动平均 [DIRECT]
    'sma_250': 'MA250',                       # 250日简单移动平均 [DIRECT]

    # EMA指数移动平均系列
    # ✅ 直接映射：RQDatac提供EMA3-EMA60
    'ema_3': 'EMA3',                          # 3日指数移动平均 [DIRECT]
    'ema_5': 'EMA5',                          # 5日指数移动平均 [DIRECT]
    'ema_10': 'EMA10',                        # 10日指数移动平均 [DIRECT]
    'ema_12': 'EMA12',                        # 12日指数移动平均 [DIRECT]
    'ema_20': 'EMA20',                        # 20日指数移动平均 [DIRECT]
    'ema_26': 'EMA26',                        # 26日指数移动平均 [DIRECT]
    'ema_30': 'EMA30',                        # 30日指数移动平均 [DIRECT]
    'ema_55': 'EMA55',                        # 55日指数移动平均 [DIRECT]
    'ema_60': 'EMA60',                        # 60日指数移动平均 [DIRECT]

    # RSI指标系列
    # ✅ 部分直接映射：RQDatac提供RSI6, RSI10, MARSI6, MARSI10
    'rsi_6': 'RSI6',                          # 6日RSI指标 [DIRECT]
    'rsi_10': 'RSI10',                        # 10日RSI指标 [DIRECT]
    'rsi_14': 'RSI10',                        # 14日RSI指标 [APPROXIMATE: 使用RSI10]
    'rsi_21': 'RSI10',                        # 21日RSI指标 [APPROXIMATE: 使用RSI10]
    'marsi_6': 'MARSI6',                      # 修正RSI 6日 [DIRECT]
    'marsi_10': 'MARSI10',                    # 修正RSI 10日 [DIRECT]

    # MACD指标系列
    # ✅ 直接映射：RQDatac提供MACD_DIFF, MACD_DEA, MACD_HIST
    'macd': 'MACD_DIFF',                      # MACD指标 (DIFF线) [DIRECT]
    'macd_signal': 'MACD_DEA',                # MACD信号线 (DEA线) [DIRECT]
    'macd_hist': 'MACD_HIST',                 # MACD柱状图 (HIST) [DIRECT]

    # KDJ随机指标系列
    # ✅ 直接映射：RQDatac提供KDJ_K, KDJ_D, KDJ_J
    'stoch_k': 'KDJ_K',                       # 随机指标K值 [DIRECT]
    'stoch_d': 'KDJ_D',                       # 随机指标D值 [DIRECT]
    'stoch_j': 'KDJ_J',                       # 随机指标J值 [DIRECT]

    # CCI顺势指标
    # ⚠️ 近似映射：RQDatac只提供CCI（默认参数）
    'cci': 'CCI',                             # 顺势指标 [DIRECT: 默认参数]
    'cci_14': 'CCI',                          # 14日顺势指标 [APPROXIMATE: 使用默认CCI]
    'cci_20': 'CCI',                          # 20日顺势指标 [APPROXIMATE: 使用默认CCI]

    # ADX平均趋向指数系列
    # ⚠️ 近似映射：RQDatac只提供ADX, ADXR（默认参数）
    'adx': 'ADX',                             # 平均趋向指数 [DIRECT: 默认参数]
    'adx_14': 'ADX',                          # 14日平均趋向指数 [APPROXIMATE: 使用默认ADX]
    'adxr': 'ADXR',                           # 平均趋向指数评级 [DIRECT]

    # ATR平均真实波幅
    # ⚠️ 近似映射：RQDatac只提供ATR（默认参数）
    'atr': 'ATR',                             # 平均真实波幅 [DIRECT: 默认参数]
    'atr_14': 'ATR',                          # 14日平均真实波幅 [APPROXIMATE: 使用默认ATR]

    # 布林带系列
    # ✅ 直接映射：RQDatac提供完整的布林带指标
    'bb_upper': 'BOLL_UP',                    # 布林带上轨 [DIRECT]
    'bb_middle': 'BOLL',                      # 布林带中轨 [DIRECT]
    'bb_lower': 'BOLL_DOWN',                  # 布林带下轨 [DIRECT]
    'bbiboll_upper': 'BBIBOLL_UP',            # 布林带上轨 (BBI修正) [DIRECT]
    'bbiboll_lower': 'BBIBOLL_DOWN',          # 布林带下轨 (BBI修正) [DIRECT]

    # 其他技术指标
    # ✅ 直接映射：RQDatac提供这些指标
    'roc': 'ROC',                             # 变动率指标 [DIRECT]
    'willr_14': 'ROC',                        # 14日威廉指标 [APPROXIMATE: 使用ROC]
    'di_plus': 'DI1',                         # 正向指标 [DIRECT]
    'di_minus': 'DI2',                        # 负向指标 [DIRECT]
    'bbi': 'BBI',                             # 多空指数 [DIRECT]
    'matrix': 'MATRIX',                       # 矩阵指标 [DIRECT]

    # ❌ 不可用的技术指标（需要自己计算）
    # 以下字段在RQDatac中不存在，需要基于基础数据自行计算
    'vwma_5': None,                           # 成交量加权移动平均 [UNAVAILABLE]
    'vwma_10': None,                          # 成交量加权移动平均 [UNAVAILABLE]
    'vwma_20': None,                          # 成交量加权移动平均 [UNAVAILABLE]
    'tema_5': None,                           # 三重指数移动平均 [UNAVAILABLE]
    'tema_10': None,                          # 三重指数移动平均 [UNAVAILABLE]
    'tema_20': None,                          # 三重指数移动平均 [UNAVAILABLE]
    'dema_5': None,                           # 双重指数移动平均 [UNAVAILABLE]
    'dema_10': None,                          # 双重指数移动平均 [UNAVAILABLE]
    'dema_20': None,                          # 双重指数移动平均 [UNAVAILABLE]
    'kama_5': None,                           # 考夫曼自适应移动平均 [UNAVAILABLE]
    'kama_10': None,                          # 考夫曼自适应移动平均 [UNAVAILABLE]
    'kama_20': None,                          # 考夫曼自适应移动平均 [UNAVAILABLE]
    'wma_5': None,                            # 加权移动平均 [UNAVAILABLE]
    'wma_10': None,                           # 加权移动平均 [UNAVAILABLE]
    'wma_20': None,                           # 加权移动平均 [UNAVAILABLE]
    'hullma_5': None,                         # 赫尔移动平均 [UNAVAILABLE]
    'hullma_10': None,                        # 赫尔移动平均 [UNAVAILABLE]
    'hullma_20': None,                        # 赫尔移动平均 [UNAVAILABLE]
    'ichimoku_tenkan': None,                  # 一目均衡表转换线 [UNAVAILABLE]
    'ichimoku_kijun': None,                   # 一目均衡表基准线 [UNAVAILABLE]
    'ichimoku_senkou_a': None,                # 一目均衡表先行带A [UNAVAILABLE]
    'ichimoku_senkou_b': None,                # 一目均衡表先行带B [UNAVAILABLE]
    'ichimoku_chikou': None,                  # 一目均衡表延迟线 [UNAVAILABLE]
    'supertrend': None,                       # 超级趋势指标 [UNAVAILABLE]
    'parabolic_sar': None,                    # 抛物线SAR [UNAVAILABLE]
    'aroon_up': None,                         # 阿隆指标上升 [UNAVAILABLE]
    'aroon_down': None,                       # 阿隆指标下降 [UNAVAILABLE]
    'aroon_oscillator': None,                 # 阿隆振荡器 [UNAVAILABLE]
    'chaikin_money_flow': None,               # 蔡金货币流量 [UNAVAILABLE]
    'chaikin_oscillator': None,               # 蔡金振荡器 [UNAVAILABLE]
    'mfi': None,                              # 资金流量指标 [UNAVAILABLE]
    'trix': None,                             # TRIX指标 [UNAVAILABLE]
    'trix_signal': None,                      # TRIX信号线 [UNAVAILABLE]
    'ultimate_oscillator': None,              # 终极振荡器 [UNAVAILABLE]
    'williams_r': None,                       # 威廉指标 [UNAVAILABLE]
    'commodity_channel_index': None,          # 商品通道指数 [UNAVAILABLE]
    'stochastic_rsi': None,                   # 随机RSI [UNAVAILABLE]
    'elder_ray_index': None,                  # 埃尔德射线指数 [UNAVAILABLE]
    'force_index': None,                      # 强力指数 [UNAVAILABLE]
    'ease_of_movement': None,                 # 简易波动指标 [UNAVAILABLE]
    'volume_price_trend': None,               # 量价趋势指标 [UNAVAILABLE]
    'negative_volume_index': None,            # 负成交量指数 [UNAVAILABLE]
    'positive_volume_index': None,            # 正成交量指数 [UNAVAILABLE]
    'on_balance_volume': None,                # 平衡成交量 [UNAVAILABLE]
    'accumulation_distribution': None,        # 累积/派发线 [UNAVAILABLE]
    'market_facilitation_index': None,        # 市场促进指数 [UNAVAILABLE]
    'schaff_trend_cycle': None,               # 沙夫趋势循环 [UNAVAILABLE]
    'klinger_oscillator': None,               # 克林格振荡器 [UNAVAILABLE]
    'pretty_good_oscillator': None,           # 相当好的振荡器 [UNAVAILABLE]
    'balance_of_power': None,                 # 力量平衡指标 [UNAVAILABLE]
    'chande_momentum_oscillator': None,       # 钱德动量振荡器 [UNAVAILABLE]
    'dynamic_momentum_index': None,           # 动态动量指数 [UNAVAILABLE]
    'ralph_vincent_pvi': None,                # 拉尔夫·文森特PVI [UNAVAILABLE]
    'ralph_vincent_nvi': None,                # 拉尔夫·文森特NVI [UNAVAILABLE]
    'vertical_horizontal_filter': None,       # 垂直水平过滤器 [UNAVAILABLE]
    'random_walk_index': None,                # 随机游走指数 [UNAVAILABLE]
    'relative_vigor_index': None,             # 相对活力指数 [UNAVAILABLE]
    'relative_vigor_index_signal': None,      # 相对活力指数信号线 [UNAVAILABLE]
    'elder_force_index': None,                # 埃尔德强力指数 [UNAVAILABLE]
    'elder_thermometer': None,                # 埃尔德温度计 [UNAVAILABLE]
    'gopalakrishnan_range_index': None,       # 戈帕拉克里希南范围指数 [UNAVAILABLE]
    'prings_know_sure_thing': None,           # 普林斯知晓肯定之事 [UNAVAILABLE]
    'prings_special_k': None,                 # 普林斯特殊K [UNAVAILABLE]
    'psychological_line': None,               # 心理线 [UNAVAILABLE]
    'trend_intensity_index': None,            # 趋势强度指数 [UNAVAILABLE]
    'smoothed_rate_of_change': None,          # 平滑变动率 [UNAVAILABLE]
}

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
# RQDatac API字段映射函数
# ============================================================================

def get_rqdatac_api_field_name(internal_field_name: str) -> str:
    """
    获取RQDatac API中对应的字段名称

    Args:
        internal_field_name: 内部字段名称

    Returns:
        str: RQDatac API字段名称，如果不存在映射或不可用则返回原名称
    """
    api_field = RQDATAC_API_FIELD_MAPPING.get(internal_field_name)
    return api_field if api_field is not None else internal_field_name

def get_rqdatac_api_field_names(internal_field_names: List[str]) -> List[str]:
    """
    批量获取RQDatac API中对应的字段名称

    Args:
        internal_field_names: 内部字段名称列表

    Returns:
        List[str]: RQDatac API字段名称列表
    """
    return [get_rqdatac_api_field_name(field) for field in internal_field_names]

def is_rqdatac_api_field_available(internal_field_name: str, available_api_fields: List[str]) -> bool:
    """
    检查内部字段名称在RQDatac API中是否可用

    Args:
        internal_field_name: 内部字段名称
        available_api_fields: RQDatac API可用字段列表

    Returns:
        bool: 是否可用
    """
    api_field = RQDATAC_API_FIELD_MAPPING.get(internal_field_name)
    # 如果映射为None，表示该字段不可用
    if api_field is None:
        return False
    return api_field in available_api_fields

def get_rqdatac_field_mapping_info(internal_field_name: str) -> Dict[str, Any]:
    """
    获取字段映射的详细信息

    Args:
        internal_field_name: 内部字段名称

    Returns:
        Dict[str, Any]: 包含映射信息、可用性和类型的字典
    """
    api_field = RQDATAC_API_FIELD_MAPPING.get(internal_field_name)

    if api_field is None:
        return {
            'internal_field': internal_field_name,
            'api_field': None,
            'available': False,
            'mapping_type': 'UNAVAILABLE',
            'description': '该字段在RQDatac中不可用，需要自行计算'
        }

    # 分析映射类型
    mapping_type = 'DIRECT'
    if internal_field_name in ['rsi_14', 'rsi_21', 'cci_14', 'cci_20', 'adx_14', 'atr_14', 'willr_14']:
        mapping_type = 'APPROXIMATE'

    return {
        'internal_field': internal_field_name,
        'api_field': api_field,
        'available': True,
        'mapping_type': mapping_type,
        'description': f'映射到 {api_field} ({mapping_type})'
    }

def get_unavailable_rqdatac_fields() -> List[str]:
    """
    获取所有在RQDatac中不可用的字段列表

    Returns:
        List[str]: 不可用字段名称列表
    """
    return [field for field, api_field in RQDATAC_API_FIELD_MAPPING.items() if api_field is None]

def get_approximate_rqdatac_mappings() -> Dict[str, str]:
    """
    获取所有近似映射的字段

    Returns:
        Dict[str, str]: 近似映射字段字典 {内部字段: API字段}
    """
    approximate_fields = ['rsi_14', 'rsi_21', 'cci_14', 'cci_20', 'adx_14', 'atr_14', 'willr_14']
    return {field: RQDATAC_API_FIELD_MAPPING[field] for field in approximate_fields if field in RQDATAC_API_FIELD_MAPPING}

def get_direct_available_fields() -> List[str]:
    """
    获取所有可以直接从RQDatac获取的字段列表

    Returns:
        List[str]: 直接可用字段名称列表
    """
    return [field for field, api_field in RQDATAC_API_FIELD_MAPPING.items()
            if api_field is not None and field not in get_approximate_rqdatac_mappings()]

def get_computation_required_fields() -> List[str]:
    """
    获取所有需要自行计算的字段列表

    Returns:
        List[str]: 需要计算字段名称列表
    """
    return get_unavailable_rqdatac_fields()

# ============================================================================
# 统一指标配置表
# ============================================================================

INDICATORS_CONFIG = {
    # 复合指标
    'MACD': {
        'type': 'composite',
        'calculation_function': 'calculate_macd',
        'components': {
            'MACD': 'MACD',
            'MACD_SIGNAL': 'MACD_SIGNAL',
            'MACD_HIST': 'MACD_HIST'
        },
        'description': 'MACD指标 (DIFF线、信号线、柱状图)'
    },

    'BB': {
        'type': 'composite',
        'calculation_function': 'calculate_bollinger_bands',
        'components': {
            'BB_UPPER': 'BB_UPPER',
            'BB_MIDDLE': 'BB_MIDDLE',
            'BB_LOWER': 'BB_LOWER'
        },
        'description': '布林带 (上轨、中轨、下轨)'
    },

    'STOCH': {
        'type': 'composite',
        'calculation_function': 'calculate_stoch',
        'components': {
            'STOCH_K': 'STOCH_K',
            'STOCH_D': 'STOCH_D'
        },
        'description': '随机指标 (K值、D值)'
    },

    'AROON': {
        'type': 'composite',
        'calculation_function': 'calculate_aroon',
        'components': {
            'AROON_UP': 'AROON_UP',
            'AROON_DOWN': 'AROON_DOWN',
            'AROON_OSC': 'AROON_OSC'
        },
        'description': 'Aroon指标 (上升、下降、震荡)'
    },

    'ICHIMOKU': {
        'type': 'composite',
        'calculation_function': 'calculate_ichimoku',
        'components': {
            'TENKAN_SEN': 'TENKAN_SEN',
            'KIJUN_SEN': 'KIJUN_SEN',
            'SENKOU_SPAN_A': 'SENKOU_SPAN_A',
            'SENKOU_SPAN_B': 'SENKOU_SPAN_B',
            'CHIKOU_SPAN': 'CHIKOU_SPAN'
        },
        'description': '一目均衡表 (转换线、基准线、先行线A、先行线B、延迟线)'
    },

    'DMI': {
        'type': 'composite',
        'calculation_function': 'calculate_dmi',
        'components': {
            'PLUS_DI': 'PLUS_DI',
            'MINUS_DI': 'MINUS_DI',
            'ADX': 'ADX'
        },
        'description': '动向指标 (正向指标、负向指标、平均动向指数)'
    },

    # 参数化指标
    'SMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_sma',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '简单移动平均线'
    },

    'EMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_ema',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '指数移动平均线'
    },

    'RSI': {
        'type': 'parameterized',
        'calculation_function': 'calculate_rsi',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '相对强弱指数'
    },

    'VWMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_vwma',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '成交量加权移动平均'
    },

    'TEMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_tema',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '三重指数移动平均'
    },

    'DEMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_dema',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '双重指数移动平均'
    },

    'KAMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_kama',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '考夫曼自适应移动平均'
    },

    'WMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_wma',
        'parameter_type': 'period',
        'default_period': 5,
        'description': '加权移动平均'
    },

    'HULLMA': {
        'type': 'parameterized',
        'calculation_function': 'calculate_hullma',
        'parameter_type': 'period',
        'default_period': 5,
        'description': 'Hull移动平均'
    },

    'ATR': {
        'type': 'parameterized',
        'calculation_function': 'calculate_atr',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '平均真实波幅'
    },

    'CCI': {
        'type': 'parameterized',
        'calculation_function': 'calculate_cci',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '顺势指标'
    },

    'STOCH_RSI': {
        'type': 'parameterized',
        'calculation_function': 'calculate_stochastic_rsi',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '随机强弱指数'
    },

    'WILLR': {
        'type': 'parameterized',
        'calculation_function': 'calculate_willr',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '威廉指标'
    },

    'ROC': {
        'type': 'parameterized',
        'calculation_function': 'calculate_roc',
        'parameter_type': 'period',
        'default_period': 10,
        'description': '变动率指标'
    },

    'MFI': {
        'type': 'parameterized',
        'calculation_function': 'calculate_mfi',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '资金流量指标'
    },

    'TRIX': {
        'type': 'parameterized',
        'calculation_function': 'calculate_trix',
        'parameter_type': 'period',
        'default_period': 15,
        'description': '三重平滑平均线'
    },

    'ULTIMATE_OSCILLATOR': {
        'type': 'parameterized',
        'calculation_function': 'calculate_ultimate_oscillator',
        'parameter_type': 'period',
        'default_period': 14,
        'description': '终极震荡指标'
    },

    # 简单指标（无参数）
    'SUPERTREND': {
        'type': 'simple',
        'calculation_function': 'calculate_supertrend',
        'description': '超级趋势指标'
    },

    'PARABOLIC_SAR': {
        'type': 'simple',
        'calculation_function': 'calculate_parabolic_sar',
        'description': '抛物线转向指标'
    },

    'CHAIKIN_MONEY_FLOW': {
        'type': 'simple',
        'calculation_function': 'calculate_chaikin_money_flow',
        'description': '蔡金资金流量'
    },

    'CHAIKIN_OSCILLATOR': {
        'type': 'simple',
        'calculation_function': 'calculate_chaikin_oscillator',
        'description': '蔡金震荡指标'
    },

    'BBIBOLL_UPPER': {
        'type': 'simple',
        'calculation_function': 'calculate_bbi_bollinger_bands_upper',
        'description': 'BBI布林带上轨'
    },

    'BBIBOLL_LOWER': {
        'type': 'simple',
        'calculation_function': 'calculate_bbi_bollinger_bands_lower',
        'description': 'BBI布林带下轨'
    }
}

# 构建各种映射表
COMPOSITE_INDICATORS_CONFIG = {k: v for k, v in INDICATORS_CONFIG.items() if v['type'] == 'composite'}
PARAMETERIZED_INDICATORS_CONFIG = {k: v for k, v in INDICATORS_CONFIG.items() if v['type'] == 'parameterized'}
SIMPLE_INDICATORS_CONFIG = {k: v for k, v in INDICATORS_CONFIG.items() if v['type'] == 'simple'}

# 组件到父指标的映射
COMPONENT_TO_PARENT_MAP = {}
for parent_name, config in COMPOSITE_INDICATORS_CONFIG.items():
    for component in config['components'].keys():
        COMPONENT_TO_PARENT_MAP[component] = parent_name

def get_indicator_config(indicator_name: str) -> Optional[Dict]:
    """
    获取指标配置信息（统一接口）

    Args:
        indicator_name: 指标名称（可以是父指标、组件或参数化指标）

    Returns:
        Optional[Dict]: 指标配置信息
    """
    # 首先检查是否为复合指标组件
    parent = COMPONENT_TO_PARENT_MAP.get(indicator_name)
    if parent:
        return INDICATORS_CONFIG.get(parent)

    # 检查是否为父指标或简单指标
    config = INDICATORS_CONFIG.get(indicator_name.upper())
    if config:
        return config

    # 处理参数化指标：从名称中提取基础指标名
    if '_' in indicator_name:
        # 从后往前查找数字参数
        parts = indicator_name.split('_')
        for i in range(len(parts) - 1, -1, -1):
            if parts[i].isdigit():
                base_name = '_'.join(parts[:i]).upper()
                config = INDICATORS_CONFIG.get(base_name)
                if config:
                    return config
                break

    return None

def is_composite_indicator(indicator_name: str) -> bool:
    """
    检查指标是否为复合指标

    Args:
        indicator_name: 指标名称

    Returns:
        bool: 是否为复合指标
    """
    config = get_indicator_config(indicator_name)
    return config is not None and config.get('type') == 'composite'

def is_parameterized_indicator(indicator_name: str) -> bool:
    """
    检查指标是否为参数化指标

    Args:
        indicator_name: 指标名称

    Returns:
        bool: 是否为参数化指标
    """
    config = get_indicator_config(indicator_name)
    return config is not None and config.get('type') == 'parameterized'

def get_indicator_type(indicator_name: str) -> Optional[str]:
    """
    获取指标类型

    Args:
        indicator_name: 指标名称

    Returns:
        Optional[str]: 指标类型 ('composite', 'parameterized', 'simple')
    """
    config = get_indicator_config(indicator_name)
    return config.get('type') if config else None

def list_all_indicators() -> Dict[str, Dict]:
    """
    列出所有指标的配置信息

    Returns:
        Dict[str, Dict]: 指标配置字典
    """
    return INDICATORS_CONFIG.copy()

def list_composite_indicators() -> Dict[str, Dict]:
    """
    列出所有复合指标

    Returns:
        Dict[str, Dict]: 复合指标配置字典
    """
    return COMPOSITE_INDICATORS_CONFIG.copy()

def list_parameterized_indicators() -> Dict[str, Dict]:
    """
    列出所有参数化指标

    Returns:
        Dict[str, Dict]: 参数化指标配置字典
    """
    return PARAMETERIZED_INDICATORS_CONFIG.copy()

def list_simple_indicators() -> Dict[str, Dict]:
    """
    列出所有简单指标

    Returns:
        Dict[str, Dict]: 简单指标配置字典
    """
    return SIMPLE_INDICATORS_CONFIG.copy()

def expand_composite_indicators(indicator_names: List[str]) -> List[str]:
    """
    展开复合指标为具体字段名称

    Args:
        indicator_names: 原始指标名称列表

    Returns:
        List[str]: 展开后的具体字段名称列表
    """
    expanded_indicators = []

    for indicator_name in indicator_names:
        indicator_upper = indicator_name.upper()

        # 检查是否为复合指标
        if indicator_upper in COMPOSITE_INDICATORS_CONFIG:
            # 展开复合指标的所有组件
            config = COMPOSITE_INDICATORS_CONFIG[indicator_upper]
            expanded_indicators.extend(config['components'].keys())
        else:
            # 普通指标直接添加
            expanded_indicators.append(indicator_name.lower())

    return expanded_indicators

def calculate_indicators_batch(data: pd.DataFrame, indicator_names: List[str]) -> pd.DataFrame:
    """
    批量计算技术指标，返回包含所有指标的DataFrame

    Args:
        data: 包含OHLCV数据的DataFrame
        indicator_names: 要计算的指标名称列表（支持复合指标如'MACD', 'BB'等）

    Returns:
        pd.DataFrame: 包含原始数据和所有计算指标的DataFrame
    """
    if data is None or data.empty:
        return data.copy() if data is not None else pd.DataFrame()

    try:
        # 复制原始数据
        result_df = data.copy()

        # 展开复合指标
        expanded_indicators = expand_composite_indicators(indicator_names)

        # 用于缓存复合指标的计算结果，避免重复计算
        cached_results = {}

        for indicator_name in expanded_indicators:
            try:
                # 获取计算函数
                calc_function = get_indicator_calculation_function(indicator_name)

                if calc_function is None:
                    logger.warning(f"⚠️ 找不到指标计算函数: {indicator_name}")
                    continue

                # 检查是否为复合指标的组成部分
                parent_indicator = COMPONENT_TO_PARENT_MAP.get(indicator_name)
                composite_key = parent_indicator.lower() if parent_indicator else None

                if composite_key and composite_key in cached_results:
                    # 使用缓存的结果
                    composite_result = cached_results[composite_key]
                else:
                    # 计算复合指标或普通指标
                    if composite_key:
                        # 获取复合指标的计算函数
                        if parent_indicator:
                            parent_config = COMPOSITE_INDICATORS_CONFIG.get(parent_indicator)
                            if parent_config:
                                calc_func_name = parent_config['calculation_function']
                                calc_func = globals().get(calc_func_name)
                                if calc_func:
                                    composite_result = calc_func(data)
                                    cached_results[composite_key] = composite_result
                                else:
                                    logger.error(f"找不到复合指标计算函数: {calc_func_name}")
                                    continue
                            else:
                                logger.error(f"找不到复合指标配置: {parent_indicator}")
                                continue
                        else:
                            logger.error(f"复合指标父名称为空: {indicator_name}")
                            continue
                    else:
                        # 检查是否为带参数的指标
                        if '_' in indicator_name:
                            parts = indicator_name.split('_')
                            if len(parts) >= 2:
                                try:
                                    period = int(parts[-1])  # 取最后一个部分作为参数
                                    composite_result = calc_function(data, period)
                                except (ValueError, TypeError):
                                    # 如果无法解析参数，尝试无参数调用
                                    composite_result = calc_function(data)
                            else:
                                composite_result = calc_function(data)
                        else:
                            composite_result = calc_function(data)

                # 处理计算结果
                if isinstance(composite_result, dict):
                    # 复合指标结果 - 使用配置表格进行映射
                    if parent_indicator and parent_indicator in COMPOSITE_INDICATORS_CONFIG:
                        config = COMPOSITE_INDICATORS_CONFIG[parent_indicator]
                        components = config['components']
                        if indicator_name in components:
                            result_key = components[indicator_name]
                            result_df[indicator_name] = composite_result.get(result_key)
                        else:
                            logger.error(f"复合指标组件未找到: {indicator_name}")
                    else:
                        # 处理非配置表格中的复合指标（向后兼容）
                        if indicator_name == 'macd':
                            result_df['macd'] = composite_result.get('MACD')
                        elif indicator_name == 'macd_signal':
                            result_df['macd_signal'] = composite_result.get('MACD_SIGNAL')
                        elif indicator_name == 'macd_hist':
                            result_df['macd_hist'] = composite_result.get('MACD_HIST')
                        # ... 其他向后兼容的处理
                else:
                    # 单一指标结果
                    result_df[indicator_name] = composite_result

            except Exception as e:
                logger.error(f"❌ 计算指标失败 {indicator_name}: {e}")
                # 为失败的指标添加NaN列
                result_df[indicator_name] = np.nan

        return result_df

    except Exception as e:
        logger.error(f"❌ 批量计算指标失败: {e}")
        return data.copy() if data is not None else pd.DataFrame()

def get_indicator_calculation_function(indicator_name: str) -> Optional[Callable]:
    """
    根据指标名称获取对应的计算函数（统一架构）

    Args:
        indicator_name: 指标名称（如 'sma_5', 'rsi_14', 'macd', 'MACD'）

    Returns:
        Optional[Callable]: 计算函数，如果不存在则返回None
    """
    # 获取指标配置
    config = get_indicator_config(indicator_name)

    if config:
        indicator_type = config['type']
        calc_func_name = config['calculation_function']
        calc_func = getattr(sys.modules[__name__], calc_func_name, None)

        if not calc_func:
            logger.error(f"计算函数未找到: {calc_func_name}")
            return None

        if indicator_type == 'composite':
            # 复合指标：返回组件对应的结果
            components = config['components']
            if indicator_name in components:
                result_key = components[indicator_name]
                return lambda data: calc_func(data)[result_key]
            else:
                # 如果是父指标名称，返回整个结果字典
                return lambda data: calc_func(data)

        elif indicator_type == 'parameterized':
            # 参数化指标：解析参数并调用
            if '_' in indicator_name:
                parts = indicator_name.split('_')
                try:
                    period = int(parts[-1])
                    return lambda data: calc_func(data, period)
                except (ValueError, TypeError):
                    logger.error(f"无法解析参数化指标参数: {indicator_name}")
                    return None
            else:
                # 使用默认参数
                default_period = config.get('default_period', 14)
                return lambda data: calc_func(data, default_period)

        elif indicator_type == 'simple':
            # 简单指标：直接调用
            return lambda data: calc_func(data)

    # 如果都找不到，返回None
    return None

# ============================================================================
# 实际指标计算函数实现（使用TA-Lib，支持向量计算优化）
# ============================================================================

def _safe_to_numpy(series) -> np.ndarray:
    """
    安全地将pandas Series转换为numpy数组，支持向量计算优化

    Args:
        series: pandas Series或numpy数组

    Returns:
        numpy数组
    """
    try:
        if hasattr(series, 'values'):
            return np.array(series.values, dtype=float)
        elif hasattr(series, 'to_numpy'):
            return series.to_numpy().astype(float)
        else:
            return np.array(series, dtype=float)
    except Exception as e:
        logger.warning(f"转换到numpy数组失败: {e}")
        return np.array([], dtype=float)

def _ensure_length_match(result: np.ndarray, original_length: int) -> np.ndarray:
    """
    确保计算结果长度与原始数据匹配

    Args:
        result: 计算结果数组
        original_length: 原始数据长度

    Returns:
        长度匹配的数组
    """
    if len(result) < original_length:
        # 在前面填充NaN
        padding = np.full(original_length - len(result), np.nan)
        return np.concatenate([padding, result])
    elif len(result) > original_length:
        # 截取后段数据
        return result[-original_length:]
    return result

def calculate_sma(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算简单移动平均线 (SMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        SMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SMA_{period}')

        sma_result = talib.SMA(close_prices, timeperiod=period)
        sma_result = _ensure_length_match(sma_result, len(data))

        return pd.Series(sma_result, index=data.index, name=f'SMA_{period}')

    except Exception as e:
        logger.error(f"SMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'SMA_{period}')

def calculate_ema(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算指数移动平均线 (EMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        EMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EMA_{period}')

        ema_result = talib.EMA(close_prices, timeperiod=period)
        ema_result = _ensure_length_match(ema_result, len(data))

        return pd.Series(ema_result, index=data.index, name=f'EMA_{period}')

    except Exception as e:
        logger.error(f"EMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'EMA_{period}')

def calculate_rsi(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算相对强弱指数 (RSI) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: RSI周期，默认14

    Returns:
        RSI序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RSI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RSI_{period}')

        rsi_result = talib.RSI(close_prices, timeperiod=period)
        rsi_result = _ensure_length_match(rsi_result, len(data))

        return pd.Series(rsi_result, index=data.index, name=f'RSI_{period}')

    except Exception as e:
        logger.error(f"RSI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'RSI_{period}')

def calculate_macd(data: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, pd.Series]:
    """
    计算MACD指标 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        fast_period: 快线周期，默认12
        slow_period: 慢线周期，默认26
        signal_period: 信号线周期，默认9

    Returns:
        包含MACD、信号线、柱状图的字典
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'MACD': empty_series.rename('MACD'),
                'MACD_SIGNAL': empty_series.rename('MACD_SIGNAL'),
                'MACD_HIST': empty_series.rename('MACD_HIST')
            }

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'MACD': empty_series.rename('MACD'),
                'MACD_SIGNAL': empty_series.rename('MACD_SIGNAL'),
                'MACD_HIST': empty_series.rename('MACD_HIST')
            }

        macd, macdsignal, macdhist = talib.MACD(close_prices,
                                               fastperiod=fast_period,
                                               slowperiod=slow_period,
                                               signalperiod=signal_period)

        # 确保长度匹配
        macd = _ensure_length_match(macd, len(data))
        macdsignal = _ensure_length_match(macdsignal, len(data))
        macdhist = _ensure_length_match(macdhist, len(data))

        return {
            'MACD': pd.Series(macd, index=data.index, name='MACD'),
            'MACD_SIGNAL': pd.Series(macdsignal, index=data.index, name='MACD_SIGNAL'),
            'MACD_HIST': pd.Series(macdhist, index=data.index, name='MACD_HIST')
        }

    except Exception as e:
        logger.error(f"MACD计算失败: {e}")
        empty_series = pd.Series([np.nan] * len(data), index=data.index)
        return {
            'MACD': empty_series.rename('MACD'),
            'MACD_SIGNAL': empty_series.rename('MACD_SIGNAL'),
            'MACD_HIST': empty_series.rename('MACD_HIST')
        }

def calculate_bollinger_bands(data: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> Dict[str, pd.Series]:
    """
    计算布林带指标 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认20
        std_dev: 标准差倍数，默认2.0

    Returns:
        包含上轨、中轨、下轨的字典
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'BB_UPPER': empty_series.rename('BB_UPPER'),
                'BB_MIDDLE': empty_series.rename('BB_MIDDLE'),
                'BB_LOWER': empty_series.rename('BB_LOWER')
            }

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'BB_UPPER': empty_series.rename('BB_UPPER'),
                'BB_MIDDLE': empty_series.rename('BB_MIDDLE'),
                'BB_LOWER': empty_series.rename('BB_LOWER')
            }

        upper, middle, lower = talib.BBANDS(close_prices,
                                           timeperiod=period,
                                           nbdevup=std_dev,
                                           nbdevdn=std_dev)

        # 确保长度匹配
        upper = _ensure_length_match(upper, len(data))
        middle = _ensure_length_match(middle, len(data))
        lower = _ensure_length_match(lower, len(data))

        return {
            'BB_UPPER': pd.Series(upper, index=data.index, name='BB_UPPER'),
            'BB_MIDDLE': pd.Series(middle, index=data.index, name='BB_MIDDLE'),
            'BB_LOWER': pd.Series(lower, index=data.index, name='BB_LOWER')
        }

    except Exception as e:
        logger.error(f"布林带计算失败: {e}")
        empty_series = pd.Series([np.nan] * len(data), index=data.index)
        return {
            'BB_UPPER': empty_series.rename('BB_UPPER'),
            'BB_MIDDLE': empty_series.rename('BB_MIDDLE'),
            'BB_LOWER': empty_series.rename('BB_LOWER')
        }

def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算平均真实波幅 (ATR) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: ATR周期，默认14

    Returns:
        ATR序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ATR_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ATR_{period}')

        atr_result = talib.ATR(high_prices, low_prices, close_prices, timeperiod=period)
        atr_result = _ensure_length_match(atr_result, len(data))

        return pd.Series(atr_result, index=data.index, name=f'ATR_{period}')

    except Exception as e:
        logger.error(f"ATR计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'ATR_{period}')

def calculate_stoch(data: pd.DataFrame, fastk_period: int = 14, slowk_period: int = 3, slowd_period: int = 3) -> Dict[str, pd.Series]:
    """
    计算随机指标 (STOCH) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        fastk_period: 快线K周期，默认14
        slowk_period: 慢线K周期，默认3
        slowd_period: 慢线D周期，默认3

    Returns:
        包含K值、D值的字典
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'STOCH_K': empty_series.rename('STOCH_K'),
                'STOCH_D': empty_series.rename('STOCH_D')
            }

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'STOCH_K': empty_series.rename('STOCH_K'),
                'STOCH_D': empty_series.rename('STOCH_D')
            }

        slowk, slowd = talib.STOCH(high_prices, low_prices, close_prices,
                                  fastk_period=fastk_period,
                                  slowk_period=slowk_period,
                                  slowd_period=slowd_period)

        # 确保长度匹配
        slowk = _ensure_length_match(slowk, len(data))
        slowd = _ensure_length_match(slowd, len(data))

        return {
            'STOCH_K': pd.Series(slowk, index=data.index, name='STOCH_K'),
            'STOCH_D': pd.Series(slowd, index=data.index, name='STOCH_D')
        }

    except Exception as e:
        logger.error(f"STOCH计算失败: {e}")
        empty_series = pd.Series([np.nan] * len(data), index=data.index)
        return {
            'STOCH_K': empty_series.rename('STOCH_K'),
            'STOCH_D': empty_series.rename('STOCH_D')
        }

def calculate_cci(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算顺势指标 (CCI) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: CCI周期，默认14

    Returns:
        CCI序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'CCI_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'CCI_{period}')

        cci_result = talib.CCI(high_prices, low_prices, close_prices, timeperiod=period)
        cci_result = _ensure_length_match(cci_result, len(data))

        return pd.Series(cci_result, index=data.index, name=f'CCI_{period}')

    except Exception as e:
        logger.error(f"CCI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'CCI_{period}')

def calculate_roc(data: pd.DataFrame, period: int = 10) -> pd.Series:
    """
    计算变动率指标 (ROC) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: ROC周期，默认10

    Returns:
        ROC序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ROC_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ROC_{period}')

        roc_result = talib.ROC(close_prices, timeperiod=period)
        roc_result = _ensure_length_match(roc_result, len(data))

        return pd.Series(roc_result, index=data.index, name=f'ROC_{period}')

    except Exception as e:
        logger.error(f"ROC计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'ROC_{period}')

def calculate_wma(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算加权移动平均线 (WMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        WMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'WMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'WMA_{period}')

        wma_result = talib.WMA(close_prices, timeperiod=period)
        wma_result = _ensure_length_match(wma_result, len(data))

        return pd.Series(wma_result, index=data.index, name=f'WMA_{period}')

    except Exception as e:
        logger.error(f"WMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'WMA_{period}')

def calculate_tema(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算三重指数移动平均线 (TEMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        TEMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TEMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TEMA_{period}')

        tema_result = talib.TEMA(close_prices, timeperiod=period)
        tema_result = _ensure_length_match(tema_result, len(data))

        return pd.Series(tema_result, index=data.index, name=f'TEMA_{period}')

    except Exception as e:
        logger.error(f"TEMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'TEMA_{period}')

def calculate_mfi(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算资金流量指标 (MFI) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: MFI周期，默认14

    Returns:
        MFI序列
    """
    try:
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

        mfi_result = talib.MFI(high_prices, low_prices, close_prices, volume, timeperiod=period)
        mfi_result = _ensure_length_match(mfi_result, len(data))

        return pd.Series(mfi_result, index=data.index, name=f'MFI_{period}')

    except Exception as e:
        logger.error(f"MFI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

def calculate_willr(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算威廉指标 (WILLR) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: WILLR周期，默认14

    Returns:
        WILLR序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'WILLR_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'WILLR_{period}')

        willr_result = talib.WILLR(high_prices, low_prices, close_prices, timeperiod=period)
        willr_result = _ensure_length_match(willr_result, len(data))

        return pd.Series(willr_result, index=data.index, name=f'WILLR_{period}')

    except Exception as e:
        logger.error(f"WILLR计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'WILLR_{period}')

def calculate_obv(data: pd.DataFrame) -> pd.Series:
    """
    计算平衡成交量 (OBV) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame

    Returns:
        OBV序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name='OBV')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name='OBV')

        obv_result = talib.OBV(close_prices, volume)
        obv_result = _ensure_length_match(obv_result, len(data))

        return pd.Series(obv_result, index=data.index, name='OBV')

    except Exception as e:
        logger.error(f"OBV计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name='OBV')

def calculate_dmi(data: pd.DataFrame, period: int = 14) -> Dict[str, pd.Series]:
    """
    计算动向指标 (DMI) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: DMI周期，默认14

    Returns:
        包含PLUS_DI、MINUS_DI、ADX的字典
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'PLUS_DI': empty_series.rename('PLUS_DI'),
                'MINUS_DI': empty_series.rename('MINUS_DI'),
                'ADX': empty_series.rename('ADX')
            }

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            empty_series = pd.Series([np.nan] * len(data), index=data.index)
            return {
                'PLUS_DI': empty_series.rename('PLUS_DI'),
                'MINUS_DI': empty_series.rename('MINUS_DI'),
                'ADX': empty_series.rename('ADX')
            }

        plus_di = talib.PLUS_DI(high_prices, low_prices, close_prices, timeperiod=period)
        minus_di = talib.MINUS_DI(high_prices, low_prices, close_prices, timeperiod=period)
        adx = talib.ADX(high_prices, low_prices, close_prices, timeperiod=period)

        # 确保长度匹配
        plus_di = _ensure_length_match(plus_di, len(data))
        minus_di = _ensure_length_match(minus_di, len(data))
        adx = _ensure_length_match(adx, len(data))

        return {
            'PLUS_DI': pd.Series(plus_di, index=data.index, name='PLUS_DI'),
            'MINUS_DI': pd.Series(minus_di, index=data.index, name='MINUS_DI'),
            'ADX': pd.Series(adx, index=data.index, name='ADX')
        }

    except Exception as e:
        logger.error(f"DMI计算失败: {e}")
        empty_series = pd.Series([np.nan] * len(data), index=data.index)
        return {
            'PLUS_DI': empty_series.rename('PLUS_DI'),
            'MINUS_DI': empty_series.rename('MINUS_DI'),
            'ADX': empty_series.rename('ADX')
        }

def calculate_vwma(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算成交量加权移动平均线 (VWMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        VWMA序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VWMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VWMA_{period}')

        # VWMA = sum(price * volume) / sum(volume) over period
        # 使用向量化计算
        price_volume = close_prices * volume
        vwma_result = np.full(len(close_prices), np.nan)

        for i in range(period - 1, len(close_prices)):
            sum_pv = np.sum(price_volume[i - period + 1:i + 1])
            sum_vol = np.sum(volume[i - period + 1:i + 1])
            if sum_vol > 0:
                vwma_result[i] = sum_pv / sum_vol

        return pd.Series(vwma_result, index=data.index, name=f'VWMA_{period}')

    except Exception as e:
        logger.error(f"VWMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'VWMA_{period}')

def calculate_dema(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算双重指数移动平均线 (DEMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        DEMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'DEMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'DEMA_{period}')

        # DEMA = 2 * EMA(close, period) - EMA(EMA(close, period), period)
        ema1 = talib.EMA(close_prices, timeperiod=period)
        ema2 = talib.EMA(ema1, timeperiod=period)
        dema_result = 2 * ema1 - ema2

        dema_result = _ensure_length_match(dema_result, len(data))

        return pd.Series(dema_result, index=data.index, name=f'DEMA_{period}')

    except Exception as e:
        logger.error(f"DEMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'DEMA_{period}')

def calculate_kama(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算考夫曼自适应移动平均线 (KAMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        KAMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'KAMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'KAMA_{period}')

        kama_result = talib.KAMA(close_prices, timeperiod=period)
        kama_result = _ensure_length_match(kama_result, len(data))

        return pd.Series(kama_result, index=data.index, name=f'KAMA_{period}')

    except Exception as e:
        logger.error(f"KAMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'KAMA_{period}')

def calculate_hullma(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算赫尔移动平均线 (HULLMA) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 移动平均周期

    Returns:
        HULLMA序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'HULLMA_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'HULLMA_{period}')

        # HULLMA = WMA(2 * WMA(close, period/2) - WMA(close, period), sqrt(period))
        half_period = max(1, period // 2)
        sqrt_period = max(1, int(np.sqrt(period)))

        wma_half = talib.WMA(close_prices, timeperiod=half_period)
        wma_full = talib.WMA(close_prices, timeperiod=period)
        hull_temp = 2 * wma_half - wma_full
        hullma_result = talib.WMA(hull_temp, timeperiod=sqrt_period)

        hullma_result = _ensure_length_match(hullma_result, len(data))

        return pd.Series(hullma_result, index=data.index, name=f'HULLMA_{period}')

    except Exception as e:
        logger.error(f"HULLMA计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'HULLMA_{period}')

# 保留复合指标的占位符函数（暂时）
def calculate_modified_rsi(data, period):
    """修正RSI"""
    return pd.Series([None] * len(data), index=data.index, name=f'MARSI_{period}')

def calculate_bbi_bollinger_bands(data):
    """BBI修正布林带"""
    return {
        'BBI_UPPER': pd.Series([None] * len(data), index=data.index, name='BBI_UPPER'),
        'BBI_LOWER': pd.Series([None] * len(data), index=data.index, name='BBI_LOWER')
    }

def calculate_bbi_bollinger_bands_upper(data):
    """BBI修正布林带上轨"""
    return calculate_bbi_bollinger_bands(data)['BBI_UPPER']

def calculate_bbi_bollinger_bands_lower(data):
    """BBI修正布林带下轨"""
    return calculate_bbi_bollinger_bands(data)['BBI_LOWER']

def calculate_ichimoku(data):
    """一目均衡表"""
    return {
        'TENKAN': pd.Series([None] * len(data), index=data.index, name='TENKAN'),
        'KIJUN': pd.Series([None] * len(data), index=data.index, name='KIJUN'),
        'SENKOU_A': pd.Series([None] * len(data), index=data.index, name='SENKOU_A'),
        'SENKOU_B': pd.Series([None] * len(data), index=data.index, name='SENKOU_B'),
        'CHIKOU': pd.Series([None] * len(data), index=data.index, name='CHIKOU')
    }

def calculate_supertrend(data):
    """超级趋势"""
    return pd.Series([None] * len(data), index=data.index, name='SUPERTREND')

def calculate_parabolic_sar(data):
    """抛物线SAR"""
    return pd.Series([None] * len(data), index=data.index, name='SAR')

def calculate_aroon(data):
    """阿隆指标"""
    return {
        'AROON_UP': pd.Series([None] * len(data), index=data.index, name='AROON_UP'),
        'AROON_DOWN': pd.Series([None] * len(data), index=data.index, name='AROON_DOWN'),
        'AROON_OSCILLATOR': pd.Series([None] * len(data), index=data.index, name='AROON_OSCILLATOR')
    }

def calculate_chaikin_money_flow(data):
    """蔡金货币流量"""
    return pd.Series([None] * len(data), index=data.index, name='CMF')

def calculate_chaikin_oscillator(data):
    """蔡金振荡器"""
    return pd.Series([None] * len(data), index=data.index, name='CHO')

def calculate_trix(data: pd.DataFrame, period: int) -> pd.Series:
    """
    计算TRIX指标 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: TRIX周期

    Returns:
        TRIX序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TRIX_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TRIX_{period}')

        trix_result = talib.TRIX(close_prices, timeperiod=period)
        trix_result = _ensure_length_match(trix_result, len(data))

        return pd.Series(trix_result, index=data.index, name=f'TRIX_{period}')

    except Exception as e:
        logger.error(f"TRIX计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'TRIX_{period}')

def calculate_ultimate_oscillator(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算终极振荡器 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期参数（用于计算权重）

    Returns:
        终极振荡器序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ULTIMATE_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ULTIMATE_{period}')

        # 使用标准参数：7, 14, 28
        ult_result = talib.ULTOSC(high_prices, low_prices, close_prices,
                                 timeperiod1=7, timeperiod2=14, timeperiod3=28)
        ult_result = _ensure_length_match(ult_result, len(data))

        return pd.Series(ult_result, index=data.index, name=f'ULTIMATE_{period}')

    except Exception as e:
        logger.error(f"ULTIMATE计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'ULTIMATE_{period}')

def calculate_stochastic_rsi(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算随机RSI - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: RSI周期

    Returns:
        随机RSI序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'STOCH_RSI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'STOCH_RSI_{period}')

        # 计算RSI
        rsi = talib.RSI(close_prices, timeperiod=period)
        # 计算随机RSI（RSI的随机指标）
        fastk, fastd = talib.STOCHF(rsi, rsi, rsi, fastk_period=period, fastd_period=3)

        fastk = _ensure_length_match(fastk, len(data))

        return pd.Series(fastk, index=data.index, name=f'STOCH_RSI_{period}')

    except Exception as e:
        logger.error(f"STOCH_RSI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'STOCH_RSI_{period}')

def calculate_elder_ray(data: pd.DataFrame, period: int = 13) -> pd.Series:
    """
    计算埃尔德射线指数 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: EMA周期，默认13

    Returns:
        埃尔德射线指数序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ELDER_RAY_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ELDER_RAY_{period}')

        # 计算EMA
        ema = talib.EMA(close_prices, timeperiod=period)
        # 计算Bull Power和Bear Power的差值
        bull_power = high_prices - ema
        bear_power = low_prices - ema
        elder_ray = bull_power - bear_power

        elder_ray = _ensure_length_match(elder_ray, len(data))

        return pd.Series(elder_ray, index=data.index, name=f'ELDER_RAY_{period}')

    except Exception as e:
        logger.error(f"ELDER_RAY计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'ELDER_RAY_{period}')

def calculate_force_index(data: pd.DataFrame, period: int = 13) -> pd.Series:
    """
    计算强力指数 - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: EMA周期，默认13

    Returns:
        强力指数序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'FORCE_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'FORCE_{period}')

        # 计算价格变化
        price_change = np.diff(close_prices, prepend=close_prices[0])
        # 计算强力指数
        force_index = price_change * volume
        # 计算EMA
        force_ema = talib.EMA(force_index, timeperiod=period)

        force_ema = _ensure_length_match(force_ema, len(data))

        return pd.Series(force_ema, index=data.index, name=f'FORCE_{period}')

    except Exception as e:
        logger.error(f"FORCE计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'FORCE_{period}')

def calculate_ease_of_movement(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算简易波动指标 (EMV) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        EMV序列
    """
    try:
        required_cols = ['high', 'low', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EASE_OF_MOVEMENT_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        volume = _safe_to_numpy(data['volume'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EASE_OF_MOVEMENT_{period}')

        # 计算中点移动距离
        distance = ((high_prices + low_prices) / 2) - ((high_prices[1:] + low_prices[:-1]) / 2)
        distance = np.concatenate([[0], distance])  # 添加第一个元素

        # 计算盒子比例
        box_ratio = (volume / 100000000) / ((high_prices - low_prices) + 0.000001)  # 避免除零

        # 计算EMV
        emv = distance / box_ratio

        # 计算EMA
        emv_ema = talib.EMA(emv, timeperiod=period)
        emv_ema = _ensure_length_match(emv_ema, len(data))

        return pd.Series(emv_ema, index=data.index, name=f'EASE_OF_MOVEMENT_{period}')

    except Exception as e:
        logger.error(f"EASE_OF_MOVEMENT计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'EASE_OF_MOVEMENT_{period}')

def calculate_volume_price_trend(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算量价趋势指标 (VPT) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        VPT序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VPT_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VPT_{period}')

        # 计算价格变化率
        price_change = np.diff(close_prices, prepend=close_prices[0]) / close_prices
        # 计算VPT
        vpt = price_change * volume
        # 累积求和
        vpt_cumsum = np.cumsum(vpt)

        return pd.Series(vpt_cumsum, index=data.index, name=f'VPT_{period}')

    except Exception as e:
        logger.error(f"VPT计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'VPT_{period}')

# 为其他缺失的函数添加占位符实现
def calculate_negative_volume_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算负成交量指数 (Negative Volume Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        NVI序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'NVI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'NVI_{period}')

        # 计算NVI
        nvi = np.full(len(close_prices), np.nan)
        nvi[0] = 1000  # 起始值

        for i in range(1, len(close_prices)):
            if volume[i] < volume[i-1]:
                nvi[i] = nvi[i-1] + ((close_prices[i] - close_prices[i-1]) / close_prices[i-1]) * nvi[i-1]
            else:
                nvi[i] = nvi[i-1]

        return pd.Series(nvi, index=data.index, name=f'NVI_{period}')

    except Exception as e:
        logger.error(f"NVI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'NVI_{period}')

def calculate_positive_volume_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算正成交量指数 (Positive Volume Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        PVI序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PVI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PVI_{period}')

        # 计算PVI
        pvi = np.full(len(close_prices), np.nan)
        pvi[0] = 1000  # 起始值

        for i in range(1, len(close_prices)):
            if volume[i] > volume[i-1]:
                pvi[i] = pvi[i-1] + ((close_prices[i] - close_prices[i-1]) / close_prices[i-1]) * pvi[i-1]
            else:
                pvi[i] = pvi[i-1]

        return pd.Series(pvi, index=data.index, name=f'PVI_{period}')

    except Exception as e:
        logger.error(f"PVI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'PVI_{period}')

def calculate_accumulation_distribution(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算累积/派发线 (Accumulation/Distribution) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14（用于后续处理）

    Returns:
        AD序列
    """
    try:
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'AD_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'AD_{period}')

        ad_result = talib.AD(high_prices, low_prices, close_prices, volume)
        ad_result = _ensure_length_match(ad_result, len(data))

        return pd.Series(ad_result, index=data.index, name=f'AD_{period}')

    except Exception as e:
        logger.error(f"AD计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'AD_{period}')

def calculate_schaff_trend_cycle(data: pd.DataFrame, period: int = 10) -> pd.Series:
    """
    计算Schaff Trend Cycle - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认10

    Returns:
        STC序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SCHAFF_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SCHAFF_{period}')

        # 计算MACD
        macd_line, macd_signal, _ = talib.MACD(close_prices, fastperiod=23, slowperiod=50, signalperiod=9)

        # 计算KST
        kst_values = np.full(len(close_prices), np.nan)

        for i in range(period - 1, len(close_prices)):
            if not np.isnan(macd_line[i]) and not np.isnan(macd_signal[i]):
                # 计算周期内的最高和最低MACD
                macd_window = macd_line[max(0, i - period + 1):i + 1]
                macd_signal_window = macd_signal[max(0, i - period + 1):i + 1]

                if len(macd_window) > 0 and len(macd_signal_window) > 0:
                    macd_high = np.max(macd_window)
                    macd_low = np.min(macd_window)
                    macd_signal_high = np.max(macd_signal_window)
                    macd_signal_low = np.min(macd_signal_window)

                    # 计算K值
                    if macd_high != macd_low:
                        k_value = 100 * (macd_line[i] - macd_low) / (macd_high - macd_low)
                    else:
                        k_value = 50

                    if macd_signal_high != macd_signal_low:
                        d_value = 100 * (macd_signal[i] - macd_signal_low) / (macd_signal_high - macd_signal_low)
                    else:
                        d_value = 50

                    # 计算STC
                    if d_value != 0:
                        stc_value = 100 * (k_value - d_value) / d_value
                        kst_values[i] = stc_value

        # 平滑处理
        kst_values = talib.EMA(kst_values, timeperiod=3)
        kst_values = talib.EMA(kst_values, timeperiod=3)

        return pd.Series(kst_values, index=data.index, name=f'SCHAFF_{period}')

    except Exception as e:
        logger.error(f"STC计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'SCHAFF_{period}')

def calculate_klinger_oscillator(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算Klinger Oscillator - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        Klinger Oscillator序列
    """
    try:
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必要列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'KLINGER_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])
        volumes = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'KLINGER_{period}')

        # 计算DM (Daily Measurement)
        dm_values = np.zeros(len(close_prices))
        for i in range(1, len(close_prices)):
            move_up = high_prices[i] - high_prices[i-1]
            move_down = low_prices[i-1] - low_prices[i]
            dm_values[i] = move_up if move_up > move_down and move_up > 0 else (-move_down if move_down > move_up and move_down > 0 else 0)

        # 计算Volume Force
        vf_values = np.zeros(len(close_prices))
        for i in range(1, len(close_prices)):
            if close_prices[i] > close_prices[i-1]:
                vf_values[i] = volumes[i] * (2 - (close_prices[i-1] / close_prices[i]))
            elif close_prices[i] < close_prices[i-1]:
                vf_values[i] = volumes[i] * (2 - (close_prices[i] / close_prices[i-1]))
            else:
                vf_values[i] = 0

        # 计算Klinger Oscillator
        klinger_values = np.full(len(close_prices), np.nan)

        # 使用EMA计算趋势
        dm_ema34 = talib.EMA(dm_values, timeperiod=34)
        dm_ema55 = talib.EMA(dm_values, timeperiod=55)
        vf_ema34 = talib.EMA(vf_values, timeperiod=34)
        vf_ema55 = talib.EMA(vf_values, timeperiod=55)

        for i in range(len(close_prices)):
            if not np.isnan(dm_ema34[i]) and not np.isnan(dm_ema55[i]) and not np.isnan(vf_ema34[i]) and not np.isnan(vf_ema55[i]):
                klinger_values[i] = dm_ema34[i] * vf_ema34[i] - dm_ema55[i] * vf_ema55[i]

        return pd.Series(klinger_values, index=data.index, name=f'KLINGER_{period}')

    except Exception as e:
        logger.error(f"Klinger Oscillator计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'KLINGER_{period}')

def calculate_pretty_good_oscillator(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算相当好的振荡器 (Pretty Good Oscillator) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        PGO序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PGO_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PGO_{period}')

        # 计算价格动量
        price_momentum = np.diff(close_prices, prepend=close_prices[0])

        # 计算成交量变化率
        volume_change = np.diff(volume, prepend=volume[0])
        volume_change_rate = volume_change / volume

        # 计算PGO: 价格动量 * 成交量变化率
        pgo = price_momentum * volume_change_rate

        # 计算EMA平滑
        pgo_ema = talib.EMA(pgo, timeperiod=period)
        pgo_ema = _ensure_length_match(pgo_ema, len(data))

        return pd.Series(pgo_ema, index=data.index, name=f'PGO_{period}')

    except Exception as e:
        logger.error(f"PGO计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'PGO_{period}')

def calculate_balance_of_power(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算力量平衡指标 (Balance of Power) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        BOP序列
    """
    try:
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'BOP_{period}')

        open_prices = _safe_to_numpy(data['open'])
        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(open_prices) == 0 or len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'BOP_{period}')

        bop_result = talib.BOP(open_prices, high_prices, low_prices, close_prices)
        bop_result = _ensure_length_match(bop_result, len(data))

        return pd.Series(bop_result, index=data.index, name=f'BOP_{period}')

    except Exception as e:
        logger.error(f"BOP计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'BOP_{period}')

def calculate_chande_momentum_oscillator(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算钱德动量振荡器 (Chande Momentum Oscillator) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        CMO序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'CMO_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'CMO_{period}')

        cmo_result = talib.CMO(close_prices, timeperiod=period)
        cmo_result = _ensure_length_match(cmo_result, len(data))

        return pd.Series(cmo_result, index=data.index, name=f'CMO_{period}')

    except Exception as e:
        logger.error(f"CMO计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'CMO_{period}')

def calculate_dynamic_momentum_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算动态动量指数 (Dynamic Momentum Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: RSI周期，默认14

    Returns:
        DMI序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'DMI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'DMI_{period}')

        # 计算RSI
        rsi = talib.RSI(close_prices, timeperiod=period)

        # 计算动态动量指数：RSI的指数移动平均
        dmi = talib.EMA(rsi, timeperiod=period)
        dmi = _ensure_length_match(dmi, len(data))

        return pd.Series(dmi, index=data.index, name=f'DMI_{period}')

    except Exception as e:
        logger.error(f"DMI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'DMI_{period}')

def calculate_vertical_horizontal_filter(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算垂直水平过滤器 (Vertical Horizontal Filter) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        VHF序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VHF_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'VHF_{period}')

        # 计算VHF
        vhf_values = np.full(len(close_prices), np.nan)

        for i in range(period - 1, len(close_prices)):
            window = close_prices[i - period + 1:i + 1]
            if len(window) > 1:
                # 计算垂直变化（最高价-最低价）
                vertical_change = np.max(window) - np.min(window)
                # 计算水平变化（价格变化的总和）
                horizontal_change = np.sum(np.abs(np.diff(window)))
                # VHF = 垂直变化 / 水平变化
                if horizontal_change > 0:
                    vhf_values[i] = vertical_change / horizontal_change

        return pd.Series(vhf_values, index=data.index, name=f'VHF_{period}')

    except Exception as e:
        logger.error(f"VHF计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'VHF_{period}')

def calculate_random_walk_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算随机游走指数 (Random Walk Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        RWI序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RWI_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RWI_{period}')

        # 计算价格变动
        price_change = np.abs(close_prices - np.roll(close_prices, 1))
        price_change[0] = 0  # 第一天设为0

        # 计算真实波幅
        true_range = np.maximum(high_prices - low_prices,
                               np.maximum(np.abs(high_prices - np.roll(close_prices, 1)),
                                        np.abs(low_prices - np.roll(close_prices, 1))))
        true_range[0] = high_prices[0] - low_prices[0]

        # 计算RWI
        rwi = np.full(len(close_prices), np.nan)
        for i in range(period - 1, len(close_prices)):
            sum_change = np.sum(price_change[i - period + 1:i + 1])
            sum_range = np.sum(true_range[i - period + 1:i + 1])
            if sum_range > 0:
                rwi[i] = sum_change / sum_range

        return pd.Series(rwi, index=data.index, name=f'RWI_{period}')

    except Exception as e:
        logger.error(f"RWI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'RWI_{period}')

def calculate_relative_vigor_index(data: pd.DataFrame, period: int = 10) -> pd.Series:
    """
    计算相对活力指数 (Relative Vigor Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认10

    Returns:
        RVI序列
    """
    try:
        required_cols = ['open', 'high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RVI_{period}')

        open_prices = _safe_to_numpy(data['open'])
        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(open_prices) == 0 or len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'RVI_{period}')

        # 计算RVI: (close - open) / (high - low)
        # 这里使用简化的RVI计算，实际的RVI需要更复杂的计算
        numerator = close_prices - open_prices
        denominator = high_prices - low_prices
        # 避免除零
        denominator = np.where(denominator == 0, np.nan, denominator)
        rvi = numerator / denominator

        # 计算EMA
        rvi_ema = talib.EMA(rvi, timeperiod=period)
        rvi_ema = _ensure_length_match(rvi_ema, len(data))

        return pd.Series(rvi_ema, index=data.index, name=f'RVI_{period}')

    except Exception as e:
        logger.error(f"RVI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'RVI_{period}')

def calculate_elder_force_index(data: pd.DataFrame, period: int = 13) -> pd.Series:
    """
    计算埃尔德强力指数 (Elder Force Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: EMA周期，默认13

    Returns:
        EFI序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EFI_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'EFI_{period}')

        # 计算价格变化
        price_change = np.diff(close_prices, prepend=close_prices[0])

        # 计算强力指数
        force_index = price_change * volume

        # 计算EMA
        efi = talib.EMA(force_index, timeperiod=period)
        efi = _ensure_length_match(efi, len(data))

        return pd.Series(efi, index=data.index, name=f'EFI_{period}')

    except Exception as e:
        logger.error(f"EFI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'EFI_{period}')

def calculate_elder_thermometer(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算埃尔德温度计 (Elder Thermometer) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        ET序列
    """
    try:
        required_cols = ['high', 'low']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ET_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])

        if len(high_prices) == 0 or len(low_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'ET_{period}')

        # 计算真实波幅
        true_range = np.maximum(high_prices - low_prices,
                               np.maximum(np.abs(high_prices - np.roll(low_prices, 1)),
                                        np.abs(low_prices - np.roll(high_prices, 1))))
        true_range[0] = high_prices[0] - low_prices[0]

        # 计算EMA
        et = talib.EMA(true_range, timeperiod=period)
        et = _ensure_length_match(et, len(data))

        return pd.Series(et, index=data.index, name=f'ET_{period}')

    except Exception as e:
        logger.error(f"ET计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'ET_{period}')

def calculate_gopalakrishnan_range_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算戈帕拉克里希南范围指数 (Gopalakrishnan Range Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        GRI序列
    """
    try:
        required_cols = ['high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'GRI_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'GRI_{period}')

        # 计算价格范围
        price_range = high_prices - low_prices

        # 计算成交量加权价格范围
        volume_weighted_range = price_range * volume

        # 计算EMA
        gri = talib.EMA(volume_weighted_range, timeperiod=period)
        gri = _ensure_length_match(gri, len(data))

        return pd.Series(gri, index=data.index, name=f'GRI_{period}')

    except Exception as e:
        logger.error(f"GRI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'GRI_{period}')

def calculate_prings_know_sure_thing(data: pd.DataFrame, period: int = 10) -> pd.Series:
    """
    计算普林斯知晓肯定之事 (Pring's Know Sure Thing) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认10

    Returns:
        PKST序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PKST_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PKST_{period}')

        # 计算不同周期的ROC
        roc1 = talib.ROC(close_prices, timeperiod=10)
        roc2 = talib.ROC(close_prices, timeperiod=15)
        roc3 = talib.ROC(close_prices, timeperiod=20)
        roc4 = talib.ROC(close_prices, timeperiod=30)

        # 计算SMA
        sma1 = talib.SMA(roc1, timeperiod=10)
        sma2 = talib.SMA(roc2, timeperiod=10)
        sma3 = talib.SMA(roc3, timeperiod=10)
        sma4 = talib.SMA(roc4, timeperiod=15)

        # 计算KST
        kst = sma1 + 2 * sma2 + 3 * sma3 + 4 * sma4
        kst = _ensure_length_match(kst, len(data))

        return pd.Series(kst, index=data.index, name=f'PKST_{period}')

    except Exception as e:
        logger.error(f"PKST计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'PKST_{period}')

def calculate_prings_special_k(data: pd.DataFrame, period: int = 10) -> pd.Series:
    """
    计算普林斯特殊K (Pring's Special K) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认10

    Returns:
        PSK序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PSK_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PSK_{period}')

        # 计算不同周期的ROC
        roc1 = talib.ROC(close_prices, timeperiod=10)
        roc2 = talib.ROC(close_prices, timeperiod=15)
        roc3 = talib.ROC(close_prices, timeperiod=20)
        roc4 = talib.ROC(close_prices, timeperiod=30)

        # 计算SMA
        sma1 = talib.SMA(roc1, timeperiod=10)
        sma2 = talib.SMA(roc2, timeperiod=10)
        sma3 = talib.SMA(roc3, timeperiod=10)
        sma4 = talib.SMA(roc4, timeperiod=15)

        # 计算Special K (类似KST但权重不同)
        special_k = sma1 + sma2 + sma3 + sma4
        special_k = _ensure_length_match(special_k, len(data))

        return pd.Series(special_k, index=data.index, name=f'PSK_{period}')

    except Exception as e:
        logger.error(f"PSK计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'PSK_{period}')

def calculate_psychological_line(data: pd.DataFrame, period: int = 12) -> pd.Series:
    """
    计算心理线 (Psychological Line) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认12

    Returns:
        PSY序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PSY_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'PSY_{period}')

        # 计算心理线：上涨天数占比
        psy_values = np.full(len(close_prices), np.nan)

        for i in range(period - 1, len(close_prices)):
            # 计算过去period天中上涨的天数
            window = close_prices[i - period + 1:i + 1]
            if len(window) > 1:
                up_days = np.sum(np.diff(window) > 0)
                psy_values[i] = (up_days / (period - 1)) * 100

        return pd.Series(psy_values, index=data.index, name=f'PSY_{period}')

    except Exception as e:
        logger.error(f"PSY计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'PSY_{period}')

def calculate_trend_intensity_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算趋势强度指数 (Trend Intensity Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        TII序列
    """
    try:
        required_cols = ['high', 'low', 'close']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TII_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        close_prices = _safe_to_numpy(data['close'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'TII_{period}')

        # 计算ADX
        adx = talib.ADX(high_prices, low_prices, close_prices, timeperiod=period)

        # 计算趋势强度 (基于价格变动的标准差)
        price_change = np.diff(close_prices, prepend=close_prices[0])
        trend_intensity = np.full(len(close_prices), np.nan)

        for i in range(period - 1, len(close_prices)):
            window_changes = price_change[i - period + 1:i + 1]
            if len(window_changes) > 0:
                trend_intensity[i] = np.std(window_changes)

        # 结合ADX和趋势强度
        tii = adx * trend_intensity
        tii = _ensure_length_match(tii, len(data))

        return pd.Series(tii, index=data.index, name=f'TII_{period}')

    except Exception as e:
        logger.error(f"TII计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'TII_{period}')

def calculate_smoothed_rate_of_change(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算平滑变动率 (Smoothed Rate of Change) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        SROC序列
    """
    try:
        if 'close' not in data.columns:
            logger.warning("数据中缺少close列")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SROC_{period}')

        close_prices = _safe_to_numpy(data['close'])
        if len(close_prices) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'SROC_{period}')

        # 计算ROC
        roc = talib.ROC(close_prices, timeperiod=period)

        # 计算EMA平滑
        sroc = talib.EMA(roc, timeperiod=period)
        sroc = _ensure_length_match(sroc, len(data))

        return pd.Series(sroc, index=data.index, name=f'SROC_{period}')

    except Exception as e:
        logger.error(f"SROC计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'SROC_{period}')

def calculate_market_facilitation_index(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算市场便利化指数 (Market Facilitation Index) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14

    Returns:
        MFI序列
    """
    try:
        required_cols = ['high', 'low', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

        high_prices = _safe_to_numpy(data['high'])
        low_prices = _safe_to_numpy(data['low'])
        volume = _safe_to_numpy(data['volume'])

        if len(high_prices) == 0 or len(low_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

        # 计算MFI: (High - Low) / Volume
        mfi = (high_prices - low_prices) / volume
        # 计算EMA
        mfi_ema = talib.EMA(mfi, timeperiod=period)

        mfi_ema = _ensure_length_match(mfi_ema, len(data))

        return pd.Series(mfi_ema, index=data.index, name=f'MFI_{period}')

    except Exception as e:
        logger.error(f"MFI计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'MFI_{period}')

def calculate_on_balance_volume(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    计算平衡成交量 (On Balance Volume) - 向量计算优化

    Args:
        data: 包含OHLCV数据的DataFrame
        period: 周期，默认14（用于后续处理）

    Returns:
        OBV序列
    """
    try:
        required_cols = ['close', 'volume']
        if not all(col in data.columns for col in required_cols):
            logger.warning(f"数据中缺少必需列: {required_cols}")
            return pd.Series([np.nan] * len(data), index=data.index, name=f'OBV_{period}')

        close_prices = _safe_to_numpy(data['close'])
        volume = _safe_to_numpy(data['volume'])

        if len(close_prices) == 0 or len(volume) == 0:
            return pd.Series([np.nan] * len(data), index=data.index, name=f'OBV_{period}')

        # 计算OBV
        obv = talib.OBV(close_prices, volume)
        obv = _ensure_length_match(obv, len(data))

        return pd.Series(obv, index=data.index, name=f'OBV_{period}')

    except Exception as e:
        logger.error(f"OBV计算失败: {e}")
        return pd.Series([np.nan] * len(data), index=data.index, name=f'OBV_{period}')

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

# ============================================================================
# RQDatac字段标准化函数
# ============================================================================

def normalize_field_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    验证DataFrame字段名是否符合RQDatac标准格式

    Args:
        df: 输入DataFrame

    Returns:
        pd.DataFrame: 验证后的DataFrame（不进行字段名转换）
    """
    if df is None or df.empty:
        return df

    df_copy = df.copy()

    # 记录不符合RQDatac标准的字段（仅用于日志记录）
    non_standard_fields = []
    for col in df_copy.columns:
        if col not in RQDATAC_FIELD_MAPPING:
            non_standard_fields.append(col)

    if non_standard_fields:
        logger.warning(f"发现不符合RQDatac标准的字段: {non_standard_fields}")
        logger.info("建议使用RQDatac标准字段名以确保最佳兼容性")

    return df_copy

def standardize_column_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    根据RQDatac标准标准化DataFrame列的数据类型

    Args:
        df: 输入DataFrame

    Returns:
        pd.DataFrame: 数据类型标准化后的DataFrame
    """
    if df is None or df.empty:
        return df

    df_copy = df.copy()

    # 数据类型映射
    dtype_mapping = {
        'str': 'string',
        'float32': np.float32,
        'float64': np.float64,
        'int64': np.int64,
        'datetime64[ns]': 'datetime64[ns]',
        'category': 'category'
    }

    for column, expected_type in RQDATAC_FIELD_MAPPING.items():
        if column in df_copy.columns:
            try:
                pandas_dtype = dtype_mapping.get(expected_type, expected_type)

                if expected_type == 'str':
                    df_copy[column] = df_copy[column].astype('string')
                elif expected_type.startswith('float'):
                    df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce').astype(pandas_dtype)
                elif expected_type.startswith('int'):
                    df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce').astype(pandas_dtype)
                elif expected_type == 'datetime64[ns]':
                    df_copy[column] = pd.to_datetime(df_copy[column], errors='coerce')
                elif expected_type == 'category':
                    df_copy[column] = df_copy[column].astype('category')
            except (ValueError, TypeError):
                # 如果转换失败，保持原类型
                pass

    return df_copy

def validate_rqdatac_dataframe(df: pd.DataFrame, strict: bool = False) -> Dict[str, Any]:
    """
    验证DataFrame是否符合RQDatac标准

    Args:
        df: 待验证的DataFrame
        strict: 是否严格模式

    Returns:
        Dict: 验证结果
    """
    validation_result = {
        'is_valid': True,
        'missing_fields': [],
        'type_mismatches': [],
        'warnings': []
    }

    # 检查必需字段
    required_fields = ['order_book_id', 'symbol', 'date', 'open', 'close', 'high', 'low', 'volume']
    for field in required_fields:
        if field not in df.columns:
            validation_result['missing_fields'].append(field)
            validation_result['is_valid'] = False

    # 检查字段类型
    for field, expected_type in RQDATAC_FIELD_MAPPING.items():
        if field in df.columns:
            actual_dtype = str(df[field].dtype)

            # 类型匹配检查
            if expected_type == 'str' and actual_dtype not in ['object', 'string', 'category']:
                validation_result['type_mismatches'].append(f"{field}: 期望{expected_type}, 实际{actual_dtype}")
                if strict:
                    validation_result['is_valid'] = False
            elif expected_type.startswith('float') and not actual_dtype.startswith('float'):
                validation_result['type_mismatches'].append(f"{field}: 期望{expected_type}, 实际{actual_dtype}")
                if strict:
                    validation_result['is_valid'] = False
            elif expected_type.startswith('int') and not actual_dtype.startswith('int'):
                validation_result['type_mismatches'].append(f"{field}: 期望{expected_type}, 实际{actual_dtype}")
                if strict:
                    validation_result['is_valid'] = False
            elif expected_type == 'datetime64[ns]' and not actual_dtype.startswith('datetime'):
                validation_result['type_mismatches'].append(f"{field}: 期望{expected_type}, 实际{actual_dtype}")
                if strict:
                    validation_result['is_valid'] = False

    # 检查数据质量
    if 'volume' in df.columns:
        negative_volume = (df['volume'] < 0).sum()
        if negative_volume > 0:
            validation_result['warnings'].append(f"发现{negative_volume}条负数成交量记录")

    if 'close' in df.columns:
        negative_price = (df['close'] <= 0).sum()
        if negative_price > 0:
            validation_result['warnings'].append(f"发现{negative_price}条非正收盘价记录")

    return validation_result

def create_standardized_dataframe(data: Union[Dict, List[Dict], pd.DataFrame],
                                normalize_fields: bool = True) -> pd.DataFrame:
    """
    创建标准化的DataFrame

    Args:
        data: 输入数据
        normalize_fields: 是否标准化字段名

    Returns:
        pd.DataFrame: 标准化的DataFrame
    """
    # 转换为DataFrame
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError("不支持的数据类型")

    if df.empty:
        return df

    # 标准化字段名
    if normalize_fields:
        df = normalize_field_names(df)

    # 标准化数据类型
    df = standardize_column_dtypes(df)

    return df

def process_rqdatac_api_data(api_data: Union[pd.DataFrame, Dict, List],
                           api_endpoint: str = 'get_price') -> pd.DataFrame:
    """
    处理RQDatac API返回的数据，标准化为StockPool格式

    Args:
        api_data: RQDatac API返回的数据
        api_endpoint: API端点名称 ('get_price', 'get_basic_info', 'get_factor', 'get_industry', 'get_shares')

    Returns:
        pd.DataFrame: 标准化的DataFrame
    """
    try:
        # 转换为DataFrame
        if isinstance(api_data, pd.DataFrame):
            df = api_data.copy()
        elif isinstance(api_data, dict):
            df = pd.DataFrame([api_data])
        elif isinstance(api_data, list):
            df = pd.DataFrame(api_data)
        else:
            raise ValueError(f"不支持的RQDatac数据类型: {type(api_data)}")

        if df.empty:
            return df

        # 根据API端点进行特定处理
        if api_endpoint == 'get_price':
            # 价格数据处理
            df = _process_price_data(df)
        elif api_endpoint == 'get_basic_info':
            # 基本信息处理
            df = _process_basic_info_data(df)
        elif api_endpoint == 'get_factor':
            # 因子数据处理
            df = _process_factor_data(df)
        elif api_endpoint == 'get_industry':
            # 行业数据处理
            df = _process_industry_data(df)
        elif api_endpoint == 'get_shares':
            # 股本数据处理
            df = _process_shares_data(df)

        # 通用处理
        df = normalize_field_names(df)
        df = standardize_column_dtypes(df)

        # 添加数据源标识
        df['data_source'] = 'rqdatac'
        df['last_sync_time'] = pd.Timestamp.now()

        return df

    except Exception as e:
        logger.error(f"处理RQDatac {api_endpoint}数据时出错: {str(e)}")
        raise

def _process_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理价格数据"""
    # 确保必要的价格字段存在
    required_fields = ['order_book_id', 'date', 'open', 'close', 'high', 'low', 'volume']

    # 计算缺失的派生字段
    if 'change' not in df.columns and 'pre_close' in df.columns and 'close' in df.columns:
        df['change'] = df['close'] - df['pre_close']

    if 'change_pct' not in df.columns and 'change' in df.columns and 'pre_close' in df.columns:
        df['change_pct'] = (df['change'] / df['pre_close']) * 100

    if 'amplitude' not in df.columns and 'high' in df.columns and 'low' in df.columns and 'pre_close' in df.columns:
        df['amplitude'] = ((df['high'] - df['low']) / df['pre_close']) * 100

    return df

def _process_basic_info_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理基本信息数据"""
    # 基本信息数据通常包含公司基本面信息
    # 确保日期字段格式正确
    if 'listed_date' in df.columns:
        df['listed_date'] = pd.to_datetime(df['listed_date'], errors='coerce')

    return df

def _process_factor_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理因子数据"""
    # 因子数据可能包含多个因子值
    # 确保因子名称和值字段存在
    if 'factor_name' not in df.columns and 'factor' in df.columns:
        df = df.rename(columns={'factor': 'factor_name'})

    return df

def _process_industry_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理行业数据"""
    # 行业数据包含行业分类信息
    # 确保日期字段格式正确
    date_fields = ['start_date', 'end_date']
    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')

    return df

def _process_shares_data(df: pd.DataFrame) -> pd.DataFrame:
    """处理股本数据"""
    # 股本数据包含股权变动信息
    # 确保日期字段格式正确
    if 'announcement_date' in df.columns:
        df['announcement_date'] = pd.to_datetime(df['announcement_date'], errors='coerce')

    # 计算流通比例
    if 'float_ratio' not in df.columns and 'float_shares' in df.columns and 'total_shares' in df.columns:
        df['float_ratio'] = (df['float_shares'] / df['total_shares']) * 100

    return df

def get_rqdatac_field_info(field_name: str) -> Optional[Dict[str, Any]]:
    """
    获取RQDatac字段的详细信息

    Args:
        field_name: 字段名（必须是RQDatac标准字段名）

    Returns:
        Dict: 字段信息，包含类型、描述等
    """
    # 直接检查字段是否在RQDatac标准映射中
    if field_name in RQDATAC_FIELD_MAPPING:
        return {
            'field_name': field_name,
            'data_type': RQDATAC_FIELD_MAPPING[field_name],
            'is_alias': False,
            'original_name': None,
            'description': _get_field_description(field_name)
        }

    return None

def _get_field_description(field_name: str) -> str:
    """获取字段描述"""
    descriptions = {
        'order_book_id': '股票代码 (RQDatac标准字段)',
        'symbol': '股票简称',
        'company_name': '公司名称',
        'open': '开盘价',
        'close': '收盘价',
        'high': '最高价',
        'low': '最低价',
        'pre_close': '昨收价',
        'change': '涨跌额',
        'change_pct': '涨跌幅 (%)',
        'volume': '成交量',
        'total_turnover': '成交额',
        'vwap': '成交均价',
        'pe_ratio': '市盈率',
        'pb_ratio': '市净率',
        'roe': '净资产收益率 (%)',
        'market_cap': '总市值',
        'total_shares': '总股本',
        'float_shares': '流通股本',
        'factor_value': '因子值',
        'factor_name': '因子名称',
        'level': '行业等级',
        'source': '数据来源',
        'date': '交易日期',
        'datetime': '交易时间戳',
        'created_at': '创建时间',
        'updated_at': '更新时间',
        'data_source': '数据来源',
        'last_sync_time': '最后同步时间'
    }

    return descriptions.get(field_name, f'{field_name}字段')


# ============================================================================
# 指标数据验证函数
# ============================================================================

def validate_indicator_dataframe(df: pd.DataFrame, indicator_name: str) -> bool:
    """
    验证指标DataFrame的格式是否符合标准

    Args:
        df: 要验证的DataFrame
        indicator_name: 指标名称

    Returns:
        bool: 验证是否通过
    """
    try:
        # 检查DataFrame是否为空
        if df is None or df.empty:
            logger.warning(f"指标 {indicator_name} 的DataFrame为空")
            return False

        # 检查是否有NaN值
        if df.isnull().any().any():
            logger.warning(f"指标 {indicator_name} 包含NaN值")
            # 不直接返回False，因为某些指标可能在某些情况下有NaN

        # 检查数据类型
        if not isinstance(df, pd.DataFrame):
            logger.error(f"指标 {indicator_name} 不是DataFrame类型")
            return False

        # 检查列数
        if len(df.columns) != 1:
            logger.warning(f"指标 {indicator_name} 应该只有一列数据，当前有 {len(df.columns)} 列")

        # 检查索引
        if df.index is None or len(df.index) == 0:
            logger.warning(f"指标 {indicator_name} 的索引为空")
            return False

        # 检查数据类型是否为数值型
        col_name = df.columns[0]
        if not pd.api.types.is_numeric_dtype(df[col_name]):
            logger.warning(f"指标 {indicator_name} 的数据类型不是数值型: {df[col_name].dtype}")

        return True

    except Exception as e:
        logger.error(f"验证指标 {indicator_name} 时出错: {e}")
        return False
