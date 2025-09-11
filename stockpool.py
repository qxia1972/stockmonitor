# -*- coding: utf-8 -*-
"""
Stock Pool Manager - Enhanced Stock Pool Management System
增强版股票池管理系统

核心功能:
- �️ 三层股票池架构 (基础池 → 观察池 → 核心池)
- 📊 完整的评分算法 (基础/观察/核心层级)
- 🔧 集成技术指标引擎 (11种核心算法)
- 💾 独立数据存储 (避免与其他模块冲突)
- � 智能交易日确定 (基于交易时段判断)
- 🎯 数据质量评估 (多维度质量检查)

架构优势:
- 模块化设计: 每个组件独立可维护
- 高性能优化: 批量处理 + 智能缓存
- 错误处理: 完善的异常处理和日志记录
- 可扩展性: 易于添加新的指标和规则

使用说明:
- 自动环境检测和切换
- 支持完整的技术分析指标
- 独立缓存机制，避免文件冲突
- 详细的调试和错误日志
"""

# Standard library imports
import sys
import os
import json
import logging
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple, Callable, Any, Union
from pathlib import Path

# Third-party imports
import pandas as pd
import numpy as np
import talib

# Project imports
from modules.log_manager import get_stockpool_logger
from modules.data_formats import INDICATOR_CONFIG, STANDARD_INDICATOR_COLUMNS
from modules.python_manager import EnvironmentManager

def get_logger():
    """Get configured logger for stockpool module"""
    return get_stockpool_logger()

# Initialize logger
logger = get_logger()

# RQDatac initialization with proper error handling
rqdatac_available = False
rqdatac = None

try:
    import rqdatac
    rqdatac.init()
    rqdatac_available = True
    logger.info("✅ RQDatac initialized successfully")
except ImportError:
    logger.warning("⚠️ RQDatac not available - some features will be limited")
except Exception as e:
    logger.error(f"❌ RQDatac initialization failed: {e}")

# Use constant for availability check
RQDATAC_AVAILABLE = rqdatac_available

# Environment setup
def setup_environment():
    """Setup and validate Python environment"""
    logger.debug("🔧 Setting up Python environment")

    # Add project root to path
    project_root = Path(__file__).parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        logger.debug(f"📁 Added project root to sys.path: {project_root}")

    # Environment management
    env_manager = EnvironmentManager()
    env_manager.ensure_environment_with_fallback()
    logger.debug("✅ Environment setup completed")

# Initialize environment on module load
setup_environment()

class PoolManager:
    """
    Unified Stock Pool Manager (Enhanced Version)
    增强版统一股票池管理器

    集成功能:
    - 🆕 内置StockPoolDataStore独立数据存储
    - 🆕 集成StockPoolIndicatorEngine技术指标引擎
    - 🔄 保持原有三层筛选系统 (Basic → Watch → Core)
    - ⚡ 优化的数据获取和技术分析能力
    - 📊 完整的评分排序算法
    - 💾 持久化数据存储和状态管理

    主要改进:
    - 避免与DataStore的文件操作冲突
    - 支持完整的11种技术指标算法
    - 独立的数据缓存机制
    - 增强的错误处理和日志记录
    """

    # ===== 股票池配置 =====
    STOCK_POOL_CONFIG = {
        # 池大小配置
        "basic_pool_size": 500,                 #
        "watch_pool_size": 50,                  #
        "core_pool_size": 5,                   #

        # 数据获取配置
        "basic_info_batch_size": 200,            #
        "price_data_batch_size": 100,           # 优化后的批次大小，从400调整为100以获得最佳性能
        "history_days": 60,                     # 历史数据天数

        # 基础过滤条件
        "market_cap_min": 10e8,                 # 10亿 (降低门槛)
        "market_cap_max": 5000e8,               # 5000
        "price_min": 3.0,                       # 3元 (降低门槛)
        "price_max": 200.0,                     # 200元 (提高上限)
        "volume_days": 20,                      #
        "volume_min_ratio": 0.5,                #
        "turnover_min": 0.005,                  #  0.5% (降低门槛)
        "pe_acceptable_max": 100,               # PE最大值
        "volume_min_threshold": 50000,          # 成交量最小阈值

        # RSI极端值过滤
        "rsi_extreme_low": 10,                  # RSI极端低值
        "rsi_extreme_high": 90,                 # RSI极端高值

        # 分级过滤条件 (基础层级)
        "watch_turnover_min": 0.5,              #
        "watch_turnover_max": 15,               #
        "watch_rsi_min": 20,                    # RSI
        "watch_rsi_max": 80,                    # RSI
        "watch_market_cap_min": 100e8,          # 100
        "watch_market_cap_max": 2000e8,         # 2000
        "watch_pe_max": 50,                     # PE
        "watch_ma_above_min": 2,                #

        # 分级过滤条件 (核心层级)
        "core_turnover_min": 1.0,               #
        "core_turnover_max": 10,                #
        "core_rsi_min": 30,                     # RSI
        "core_rsi_max": 70,                     # RSI
        "core_market_cap_min": 200e8,           # 200
        "core_market_cap_max": 1000e8,          # 1000
        "core_pe_min": 8,                       # PE
        "core_pe_max": 30,                      # PE
        "core_pb_max": 5,                       # PB
        "core_volatility_max": 0.4,             # 40%
        "core_ma_above_min": 3,                 #
        "core_adx_min": 20,                     # ADX最小值
    }

    def __init__(self, data_dir="data", force_refresh_cache: bool = False):
        """
        Initialize enhanced stock pool manager

        Args:
            data_dir (str): Directory path for storing pool data and logs
            force_refresh_cache (bool): Whether to force refresh all cached data from network
        """
        from pathlib import Path
        import sys

        # 解析命令行参数
        self.force_refresh_cache = force_refresh_cache
        if len(sys.argv) > 1:
            # 检查命令行参数
            if '--force-refresh' in sys.argv or '-f' in sys.argv:
                self.force_refresh_cache = True
                logger.info("🔄 检测到强制刷新参数，将从网络获取最新数据")

        # Use pathlib for unified path management
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)

        # 🆕 初始化集成的独立组件
        self.data_store = StockPoolDataStore()
        self.indicator_engine = StockPoolIndicatorEngine(self.data_store)

        # 初始化统一评分引擎
        self.scoring_engine = ScoringEngine(self)
        self.scoring_engine._init_quality_evaluator(self)

        # 初始化数据质量评估器
        self.quality_evaluator: Optional[DataQualityEvaluator] = self.scoring_engine.quality_evaluator

        # 确保quality_evaluator已正确初始化
        if self.quality_evaluator is None:
            logger.error("❌ 数据质量评估器初始化失败")
            raise RuntimeError("Failed to initialize quality evaluator")

        # 数据质量统计
        self.quality_stats = {
            'total_evaluations': 0,
            'passed_evaluations': 0,
            'failed_evaluations': 0,
            'quality_issues': {}
        }

        # Three-layer data file paths for persistent storage
        self.basic_pool_file = self.data_dir / "basic_pool.json"       # Basic layer (500 stocks)
        self.watch_pool_file = self.data_dir / "watch_pool.json"       # Watch layer (50 stocks)
        self.core_pool_file = self.data_dir / "core_pool.json"         # Core layer (10 stocks)
        self.sync_log_file = self.data_dir / "sync_log.json"           # Sync operation logs

        # Sync status management for thread safety
        self.is_syncing = False
        self.last_sync_time = None

        # Use global configured logger instance
        self.logger = logger

        # 根据环境优化日志级别
        self._optimize_logging_for_performance()

        # Load comprehensive configuration parameters
        self.config = self.STOCK_POOL_CONFIG

        if self.force_refresh_cache:
            logger.info("🔄 强制刷新模式：将从网络获取最新数据")

    def _optimize_logging_for_performance(self):
        """
        根据运行环境优化日志记录性能
        - 生产环境：减少debug日志输出
        - 开发环境：保持详细日志
        """
        import os

        # 检查是否为生产环境
        is_production = (
            os.getenv('ENVIRONMENT') == 'production' or
            os.getenv('PYTHON_ENV') == 'production' or
            not os.getenv('DEBUG', '').lower() in ('true', '1', 'yes')
        )

        if is_production:
            # 生产环境：只记录重要信息
            logging.getLogger('stockpool').setLevel(logging.INFO)
            self.logger.info("🏭 生产环境模式：优化日志性能")
        else:
            # 开发环境：保持详细日志
            logging.getLogger('stockpool').setLevel(logging.DEBUG)
            self.logger.debug("🔧 开发环境模式：启用详细日志")

    def calculate_basic_layer_score(self, stock_info: Dict, technical_indicators: Dict) -> float:
        """
        计算基础层评分 - 股票池的入门筛选层

        评分逻辑:
        1. 数据质量评估 - 确保数据完整性和有效性
        2. 基础评分基准 - 50分作为起始分数
        3. 规则评分计算 - 基于PE、PB、RSI、换手率等关键指标
        4. 分数范围限制 - 确保结果在0-100分范围内

        筛选标准:
        - 数据质量必须全部通过
        - 重点关注基本面和估值指标
        - 为观察层和核心层提供基础评分

        Args:
            stock_info: 股票基本信息，包含市值、PE、PB等
            technical_indicators: 技术指标数据，包含RSI、换手率等

        Returns:
            基础层评分 (0-100)，0分表示数据质量不合格

        Raises:
            该方法内部处理所有异常，返回0分表示评分失败
        """
        stock_code = stock_info.get('stock_code', 'unknown')
        logger.debug(f"🧮 开始计算基础层评分: {stock_code}")

        try:
            # 数据质量评估 - 首要关卡
            if self.quality_evaluator is None:
                logger.error("❌ 数据质量评估器未初始化")
                return 0.0

            quality_results = self.quality_evaluator.evaluate_data_quality(
                stock_info, technical_indicators, ['valuation', 'technical']
            )

            # 如果数据质量不合格，返回0分
            if not all(quality_results.values()):
                logger.debug(f"❌ 数据质量不合格，跳过评分: {stock_code}")
                logger.debug(f"📊 质量检查结果: {quality_results}")
                return 0.0

            logger.debug(f"✅ 数据质量检查通过: {stock_code}")

            # 基础评分基准
            base_score = 50.0
            logger.debug(f"📈 基础评分基准: {base_score}")

            # 使用统一评分引擎计算规则分数
            rule_scores = self.scoring_engine.calculate_score(
                stock_info,
                technical_indicators,
                ['pe', 'pb', 'rsi', 'turnover'],
                'basic'
            )

            logger.debug(f"🎯 规则评分: {rule_scores:.2f}")

            # 计算最终分数
            final_score = max(0, min(100, base_score + rule_scores))

            logger.debug(f"🏆 基础层评分 - {stock_code}: {final_score:.1f} (基准:{base_score} + 规则:{rule_scores:.1f})")

            return final_score

        except Exception as e:
            logger.error(f"❌ 基础层评分计算失败 - {stock_code}: {e}")
            logger.debug(f"🔍 异常详情 - 股票信息: {stock_info.keys()}", exc_info=True)
            return 0.0

    def calculate_watch_layer_score(self, stock_info: Dict, technical_indicators: Dict) -> float:
        """
        计算观察层评分

        Args:
            stock_info: 股票基本信息
            technical_indicators: 技术指标数据

        Returns:
            观察层评分 (0-100)
        """
        try:
            # 基于基础层评分
            base_score = self.calculate_basic_layer_score(stock_info, technical_indicators)

            # 如果基础评分太低，直接返回
            if base_score < 30:
                return base_score

            # 观察层额外加分
            bonus_score = 0

            # 市值加分 (200亿-1000亿最优区间)
            market_cap = stock_info.get('market_cap')
            if market_cap is not None and not pd.isna(market_cap):
                if 200e8 <= market_cap <= 1000e8:
                    bonus_score += 5
                elif 100e8 <= market_cap <= 2000e8:
                    bonus_score += 3

            # MACD信号加分
            macd = technical_indicators.get('latest_values', {}).get('MACD')
            macd_signal = technical_indicators.get('latest_values', {}).get('MACD_SIGNAL')
            if macd is not None and macd_signal is not None:
                if macd > macd_signal:
                    bonus_score += 5
                elif abs(macd - macd_signal) < macd_signal * 0.01:  # MACD接近SIGNAL
                    bonus_score += 2

            final_score = max(0, min(100, base_score + bonus_score))

            stock_code = stock_info.get('stock_code', 'unknown')
            self.logger.debug(f"观察层评分 - {stock_code}: {final_score:.1f} (基础: {base_score:.1f}, 加分: {bonus_score})")

            return final_score

        except Exception as e:
            stock_code = stock_info.get('stock_code', 'unknown')
            self.logger.error(f"观察层评分计算失败 - {stock_code}: {e}")
            return 0.0

    def calculate_core_layer_score(self, stock_info: Dict, technical_indicators: Dict) -> float:
        """
        计算核心层评分 - 股票池的核心筛选层

        评分逻辑:
        1. 基于观察层评分 - 必须达到60分门槛
        2. PB比率加分 - 核心层对估值要求更严格
        3. PE比率加分 - 盈利能力评估
        4. 波动率惩罚 - 高波动股票降低评分
        5. 分数范围限制 - 确保结果在0-100分范围内

        筛选标准:
        - 观察层评分必须≥60分
        - PB比率越低评分越高 (PB<2得8分，PB<3得5分，PB<5得3分)
        - PE比率在8-25区间获得最高加分
        - 波动率>40%扣5分，>30%扣3分

        Args:
            stock_info: 股票基本信息，包含PE、PB等估值指标
            technical_indicators: 技术指标数据，包含波动率等

        Returns:
            核心层评分 (0-100)，0分表示未达到核心标准

        Raises:
            该方法内部处理所有异常，返回0分表示评分失败
        """
        stock_code = stock_info.get('stock_code', 'unknown')
        logger.debug(f"🧮 开始计算核心层评分: {stock_code}")

        try:
            # 基于观察层评分
            watch_score = self.calculate_watch_layer_score(stock_info, technical_indicators)
            logger.debug(f"📊 观察层评分: {watch_score:.1f}")

            # 如果观察层评分太低，直接返回
            if watch_score < 60:
                logger.debug(f"❌ 观察层评分不足60分，跳过核心层评分: {stock_code}")
                return watch_score

            # 核心层额外加分 - 更严格的标准
            bonus_score = 0

            # PB比率加分 (核心层要求PB < 5)
            pb_ratio = stock_info.get('pb_ratio')
            if pb_ratio is not None and not pd.isna(pb_ratio) and pb_ratio < 5:
                if pb_ratio < 2:
                    bonus_score += 8
                    logger.debug(f"💰 PB比率优秀加分: +8 (PB={pb_ratio:.2f})")
                elif pb_ratio < 3:
                    bonus_score += 5
                    logger.debug(f"💰 PB比率良好加分: +5 (PB={pb_ratio:.2f})")
                elif pb_ratio < 5:
                    bonus_score += 3
                    logger.debug(f"💰 PB比率合格加分: +3 (PB={pb_ratio:.2f})")

            # PE比率加分 (核心层要求PE在合理区间)
            pe_ratio = stock_info.get('pe_ratio')
            if pe_ratio is not None and not pd.isna(pe_ratio):
                if 8 <= pe_ratio <= 25:
                    bonus_score += 5
                    logger.debug(f"📈 PE比率最优区间加分: +5 (PE={pe_ratio:.2f})")
                elif 5 <= pe_ratio <= 40:
                    bonus_score += 3
                    logger.debug(f"📊 PE比率良好区间加分: +3 (PE={pe_ratio:.2f})")

            # 波动率惩罚 (核心层要求低波动)
            volatility = technical_indicators.get('latest_values', {}).get('volatility_20d')
            if volatility is not None and not pd.isna(volatility):
                if volatility > 0.4:  # 40%以上的波动率
                    bonus_score -= 5
                    logger.debug(f"⚠️ 高波动率惩罚: -5 (波动率={volatility:.1%})")
                elif volatility > 0.3:  # 30%以上的波动率
                    bonus_score -= 3
                    logger.debug(f"⚠️ 中等波动率惩罚: -3 (波动率={volatility:.1%})")

            final_score = max(0, min(100, watch_score + bonus_score))

            logger.debug(f"🏆 核心层评分 - {stock_code}: {final_score:.1f} (观察层:{watch_score:.1f} + 加分:{bonus_score})")

            return final_score

        except Exception as e:
            logger.error(f"❌ 核心层评分计算失败 - {stock_code}: {e}")
            logger.debug(f"🔍 异常详情 - 股票信息: {stock_info.keys()}", exc_info=True)
            return 0.0

    # ===== 原有功能方法 =====
    # 这里需要从备份文件中恢复所有原有方法
    # 由于文件过长，我会逐步恢复关键方法

    def evaluate_stock_data_quality(self, stock_info: Dict, technical_indicators: Dict,
                                  data_types: List[str] = None) -> Dict[str, bool]:
        """
        评估单只股票的数据质量

        Args:
            stock_info: 股票基本信息
            technical_indicators: 技术指标数据
            data_types: 要评估的数据类型，默认评估所有类型

        Returns:
            各数据类型的质量评估结果
        """
        try:
            self.quality_stats['total_evaluations'] += 1

            if self.quality_evaluator is None:
                self.logger.error("数据质量评估器未初始化")
                return {}

            results = self.quality_evaluator.evaluate_data_quality(
                stock_info, technical_indicators, data_types
            )

            # 统计结果
            if all(results.values()):
                self.quality_stats['passed_evaluations'] += 1
            else:
                self.quality_stats['failed_evaluations'] += 1

                # 记录质量问题
                stock_code = stock_info.get('stock_code', 'unknown')
                if self.quality_evaluator is not None:
                    quality_report = self.quality_evaluator.get_quality_report(stock_code)
                    for data_type, report in quality_report.items():
                        if report.get('overall_quality') == 'FAIL':
                            issues = report.get('issues', [])
                            if issues:
                                issue_key = f"{data_type}_{issues[0][:50]}"  # 取前50个字符作为键
                                self.quality_stats['quality_issues'][issue_key] = \
                                    self.quality_stats['quality_issues'].get(issue_key, 0) + 1

            return results

        except Exception as e:
            self.logger.error(f"数据质量评估失败: {e}")
            return {}

    def process_daily_sync_caculate_buildpool(self) -> bool:
        """运行每日同步、计算和构建股票池的完整流程"""
        try:
            self.logger.info("🚀 开始每日股票池同步...")

            # ===== 第一阶段：获取股票列表 =====
            self.logger.info("📋 第一步：获取A股股票列表...")
            stock_list_df = self.data_store.fetch_stock_list()

            if stock_list_df is None or stock_list_df.empty:
                self.logger.error("❌ 无法获取股票列表，同步失败")
                return False

            # 提取股票代码列表
            stock_codes = stock_list_df['order_book_id'].tolist()
            self.logger.info(f"✅ 获取到 {len(stock_codes)} 只股票")

            # ===== 第二阶段：批量获取基本面数据 =====
            self.logger.info("� 第二步：批量获取基本面数据...")
            target_date = self.data_store.get_target_trading_date()
            if not target_date:
                self.logger.error("❌ 无法确定目标交易日，同步失败")
                return False
            self.logger.info(f"🎯 目标分析日期: {target_date} (使用最后一个已完成交易日的数据)")

            # 计算历史数据范围
            start_date = (datetime.strptime(target_date, '%Y-%m-%d') -
                         timedelta(days=self.config['history_days'])).strftime('%Y-%m-%d')
            self.logger.info(f"📅 历史数据范围: {start_date} 至 {target_date}")

            # 批量获取估值数据（包含基本面信息）
            self.logger.info("💰 获取估值数据（市值、PE、PB等）...")
            valuation_data = self._batch_get_valuation_data(stock_codes, target_date, return_dataframe=True)
            if isinstance(valuation_data, pd.DataFrame) and not valuation_data.empty:
                valuation_dict = valuation_data.set_index('stock_code').to_dict('index')
                self.logger.info(f"✅ 获取到 {len(valuation_data)} 只股票的估值数据")
            else:
                valuation_dict = {}
                self.logger.warning("⚠️ 未获取到估值数据")

            # ===== 第三阶段：批量获取价格序列数据 =====
            self.logger.info("📈 第三步：批量获取价格序列数据...")
            price_data = self._batch_get_price_data(stock_codes, start_date, target_date)
            valid_price_stocks = [code for code, df in price_data.items() if df is not None and not df.empty]
            self.logger.info(f"✅ 获取到 {len(valid_price_stocks)} 只股票的价格数据")

            # ===== 第四阶段：计算技术指标 =====
            self.logger.info("🔧 第四步：计算技术指标...")
            precomputed_data = {}
            processed_count = 0

            for stock_code in stock_codes:
                try:
                    processed_count += 1
                    if processed_count % 100 == 0:
                        self.logger.info(f"⏳ 已处理 {processed_count}/{len(stock_codes)} 只股票...")

                    # 获取基本面数据
                    stock_info = valuation_dict.get(stock_code, {'stock_code': stock_code})

                    # 获取价格数据
                    price_df = price_data.get(stock_code)

                    if price_df is not None and not price_df.empty:
                        # 计算技术指标
                        technical_indicators = self.calculate_technical_indicators(price_df, stock_code)

                        precomputed_data[stock_code] = {
                            'stock_info': stock_info,
                            'technical_indicators': technical_indicators
                        }
                    else:
                        self.logger.debug(f"⚠️ 股票 {stock_code} 无价格数据，跳过")

                except Exception as e:
                    self.logger.warning(f"⚠️ 处理股票 {stock_code} 时出错: {e}")
                    continue

            self.logger.info(f"✅ 技术指标计算完成: {len(precomputed_data)} 只股票")

            # ===== 第五阶段：构建股票池（优化版本，直接使用DataFrame） =====
            self.logger.info("🏗️ 第五步：构建三个股票池...")
            result = self.build_all_pools_from_precomputed_data(precomputed_data, target_date)

            if result and all(not df.empty for df in result.values()):
                self.logger.info("✅ 股票池构建成功")
                return True
            else:
                self.logger.error("❌ 股票池构建失败")
                return False

        except Exception as e:
            self.logger.error(f"❌ 每日同步失败: {e}")
            return False

    def build_stock_pool(self, scored_stocks: Union[List[Dict], pd.DataFrame], target_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        构建三个股票池 - 优化版本，支持DataFrame输入以减少转换开销

        直接使用DataFrame进行池构建，避免字典到DataFrame的重复转换
        生成基础池、观察池、核心池三个层级的股票池

        Args:
            scored_stocks: 评分后的股票数据，可以是字典列表或DataFrame
            target_date: 目标分析日期(YYYY-MM-DD格式)

        Returns:
            包含三个股票池的字典:
            {
                'basic_pool': pd.DataFrame,
                'watch_pool': pd.DataFrame,
                'core_pool': pd.DataFrame
            }
        """
        try:
            # 初始化统计信息
            stats = {
                'start_time': datetime.now(),
                'total_stocks': 0,
                'basic_pool_size': 0,
                'watch_pool_size': 0,
                'core_pool_size': 0,
                'errors': []
            }

            if target_date is None:
                target_date = self._get_latest_trading_date()

            self.logger.info(f"🎯 为日期 {target_date} 构建三个股票池")

            # ===== 第一阶段：统一数据格式 =====
            if isinstance(scored_stocks, list):
                # 如果输入是字典列表，转换为DataFrame
                if scored_stocks:
                    df_scored = pd.DataFrame(scored_stocks)
                else:
                    df_scored = pd.DataFrame()
            elif isinstance(scored_stocks, pd.DataFrame):
                # 如果已经是DataFrame，直接使用，避免拷贝
                df_scored = scored_stocks
            else:
                self.logger.error("❌ 输入数据格式不支持")
                return {
                    'basic_pool': pd.DataFrame(),
                    'watch_pool': pd.DataFrame(),
                    'core_pool': pd.DataFrame()
                }

            if df_scored.empty:
                self.logger.warning("⚠️ 无可用评分数据，使用空数据")
                return {
                    'basic_pool': pd.DataFrame(),
                    'watch_pool': pd.DataFrame(),
                    'core_pool': pd.DataFrame()
                }

            stats['total_stocks'] = len(df_scored)
            self.logger.info(f"📊 处理 {len(df_scored)} 只评分股票数据")

            # ===== 第二阶段：构建基础池 =====
            self.logger.info("🏗️ 构建基础池...")
            # 按基础评分排序，选择前N名
            basic_candidates = df_scored.nlargest(self.config['basic_pool_size'], 'basic_score')
            stats['basic_pool_size'] = len(basic_candidates)

            # ===== 第三阶段：构建观察池 =====
            self.logger.info("👀 构建观察池...")
            # 从基础池中选择观察池，但使用观察层评分排序
            watch_candidates = basic_candidates[basic_candidates['watch_score'] >= 50]  # 观察池最低分数要求
            watch_candidates = watch_candidates.nlargest(self.config['watch_pool_size'], 'watch_score')
            stats['watch_pool_size'] = len(watch_candidates)

            # ===== 第四阶段：构建核心池 =====
            self.logger.info("💎 构建核心池...")
            # 从观察池中选择核心池，但使用核心层评分排序
            core_candidates = watch_candidates[watch_candidates['core_score'] >= 70]  # 核心池最低分数要求
            core_candidates = core_candidates.nlargest(self.config['core_pool_size'], 'core_score')
            stats['core_pool_size'] = len(core_candidates)

            # ===== 第五阶段：直接保存DataFrame =====
            self.logger.info("💾 保存三个股票池数据...")

            # 保存基础池
            if not basic_candidates.empty:
                # 设置内部实例（避免拷贝，直接赋值）
                self.data_store.basic_pool = basic_candidates
                if self.data_store.save_basic_pool():
                    self.logger.info("✅ 基础池数据保存成功")
                else:
                    self.logger.error("❌ 基础池数据保存失败")
                    stats['errors'].append("保存基础池数据失败")

            # 保存观察池
            if not watch_candidates.empty:
                # 设置内部实例（避免拷贝，直接赋值）
                self.data_store.watch_pool = watch_candidates
                if self.data_store.save_watch_pool():
                    self.logger.info("✅ 观察池数据保存成功")
                else:
                    self.logger.error("❌ 观察池数据保存失败")
                    stats['errors'].append("保存观察池数据失败")

            # 保存核心池
            if not core_candidates.empty:
                # 设置内部实例（避免拷贝，直接赋值）
                self.data_store.core_pool = core_candidates
                if self.data_store.save_core_pool():
                    self.logger.info("✅ 核心池数据保存成功")
                else:
                    self.logger.error("❌ 核心池数据保存失败")
                    stats['errors'].append("保存核心池数据失败")

            # ===== 第六阶段：输出统计信息 =====
            self._log_build_statistics(stats)

            # 返回三个股票池的DataFrame
            result = {
                'basic_pool': basic_candidates,
                'watch_pool': watch_candidates,
                'core_pool': core_candidates
            }

            self.logger.info("🎉 三个股票池构建完成！"            f"基础池: {len(basic_candidates)} 只, 观察池: {len(watch_candidates)} 只, 核心池: {len(core_candidates)} 只")

            return result

        except Exception as e:
            self.logger.error(f"❌ 构建股票池失败: {e}")
            return {
                'basic_pool': pd.DataFrame(),
                'watch_pool': pd.DataFrame(),
                'core_pool': pd.DataFrame()
            }

    def _batch_get_valuation_data(self, stock_codes: List[str], target_date: str,
                                 return_dataframe: bool = False) -> Union[List[Dict], pd.DataFrame]:
        """
        批量获取股票估值数据 - 优化版本

        支持返回DataFrame格式以减少转换开销

        Args:
            stock_codes: 股票代码列表
            target_date: 目标日期
            return_dataframe: 是否返回DataFrame格式，默认False保持向后兼容性

        Returns:
            估值数据列表或DataFrame
        """
        if return_dataframe:
            # 直接返回DataFrame格式
            valuation_data = []
            batch_size = self.config.get('basic_info_batch_size', 200)

            for i in range(0, len(stock_codes), batch_size):
                batch_stocks = stock_codes[i:i + batch_size]

                if (i // batch_size + 1) % 10 == 0:  # 每10个批次记录一次
                    self.logger.info(f"⏳ 已处理 {i + len(batch_stocks)}/{len(stock_codes)} 只股票的基本面数据...")

                for stock_code in batch_stocks:
                    try:
                        # 使用datastore获取单只股票的估值数据
                        valuation_df = self.data_store.get_smart_data(stock_code, 'valuation')
                        if valuation_df is not None and not valuation_df.empty:
                            # 添加股票代码列
                            valuation_df = valuation_df.copy()
                            valuation_df['stock_code'] = stock_code
                            valuation_data.append(valuation_df.iloc[0])  # 只取最新一行
                    except Exception as e:
                        self.logger.warning(f"⚠️ 获取股票 {stock_code} 估值数据失败: {e}")
                        continue

            return pd.DataFrame(valuation_data) if valuation_data else pd.DataFrame()

        else:
            # 保持原有字典格式返回
            basic_info_list = []
            batch_size = self.config.get('basic_info_batch_size', 200)

            for i in range(0, len(stock_codes), batch_size):
                batch_stocks = stock_codes[i:i + batch_size]

                if (i // batch_size + 1) % 10 == 0:  # 每10个批次记录一次
                    self.logger.info(f"⏳ 已处理 {i + len(batch_stocks)}/{len(stock_codes)} 只股票的基本面数据...")

                for stock_code in batch_stocks:
                    try:
                        # 使用datastore获取单只股票的估值数据
                        valuation_df = self.data_store.get_smart_data(stock_code, 'valuation')
                        if valuation_df is not None and not valuation_df.empty:
                            # 转换为字典格式
                            stock_info = {
                                'stock_code': stock_code,
                                'market_cap': valuation_df.get('market_cap', [None])[0] if 'market_cap' in valuation_df.columns else None,
                                'pe_ratio': valuation_df.get('pe_ratio', [None])[0] if 'pe_ratio' in valuation_df.columns else None,
                                'pb_ratio': valuation_df.get('pb_ratio', [None])[0] if 'pb_ratio' in valuation_df.columns else None,
                                'ps_ratio': valuation_df.get('ps_ratio', [None])[0] if 'ps_ratio' in valuation_df.columns else None,
                                'pcf_ratio': valuation_df.get('pcf_ratio', [None])[0] if 'pcf_ratio' in valuation_df.columns else None,
                                'turnover_ratio': valuation_df.get('turnover_ratio', [None])[0] if 'turnover_ratio' in valuation_df.columns else None,
                            }
                            basic_info_list.append(stock_info)
                    except Exception as e:
                        self.logger.warning(f"⚠️ 获取股票 {stock_code} 估值数据失败: {e}")
                        continue

            return basic_info_list

    def _batch_get_price_data(self, stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """
        批量获取股票价格数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            价格数据字典 {stock_code: price_df}
        """
        price_data = {}
        batch_size = self.config.get('price_data_batch_size', 100)

        for i in range(0, len(stock_codes), batch_size):
            batch_stocks = stock_codes[i:i + batch_size]

            if (i // batch_size + 1) % 5 == 0:  # 每5个批次记录一次
                self.logger.info(f"⏳ 已处理 {i + len(batch_stocks)}/{len(stock_codes)} 只股票的价格数据...")

            for stock_code in batch_stocks:
                try:
                    # 使用datastore获取单只股票的价格数据
                    price_df = self.data_store.get_smart_data(stock_code, 'price')
                    if price_df is not None and not price_df.empty:
                        price_data[stock_code] = price_df
                except Exception as e:
                    self.logger.warning(f"⚠️ 获取股票 {stock_code} 价格数据失败: {e}")
                    continue

        return price_data

    def _process_candidates_parallel(self, basic_info_df: pd.DataFrame, price_data: Dict[str, pd.DataFrame],
                                   target_date: str, stats: Dict) -> List[Dict]:
        """
        并行处理候选股票

        Args:
            basic_info_df: 基本面数据DataFrame
            price_data: 价格数据字典
            target_date: 目标日期
            stats: 统计信息字典

        Returns:
            候选股票列表
        """
        candidate_stocks = []
        processed_count = 0

        for _, row in basic_info_df.iterrows():
            stock_code = row['stock_code']
            processed_count += 1

            if processed_count % 500 == 0:
                self.logger.info(f"⏳ 已处理 {processed_count}/{len(basic_info_df)} 只股票...")

            if stock_code not in price_data:
                continue

            try:
                # 计算技术指标
                technical_indicators = self.calculate_technical_indicators(price_data[stock_code], stock_code)

                # 应用基础过滤条件
                stock_info = row.to_dict()
                if not self.apply_basic_filters(stock_info, technical_indicators):
                    stats['filtered_stocks'] += 1
                    continue

                # 使用scoring engine进行数据质量评估
                if self.quality_evaluator is None:
                    self.logger.error("数据质量评估器未初始化")
                    stats['filtered_stocks'] += 1
                    continue

                quality_results = self.quality_evaluator.evaluate_data_quality(
                    stock_info, technical_indicators, ['valuation', 'technical']
                )

                if not all(quality_results.values()):
                    stats['filtered_stocks'] += 1
                    continue

                # 计算综合评分
                score = self.calculate_basic_layer_score(stock_info, technical_indicators)
                stats['scored_stocks'] += 1

                candidate_stocks.append({
                    'stock_code': stock_code,
                    'score': score,
                    'market_cap': stock_info.get('market_cap'),
                    'pe_ratio': stock_info.get('pe_ratio'),
                    'pb_ratio': stock_info.get('pb_ratio'),
                    'current_price': technical_indicators.get('latest_values', {}).get('current_price'),
                    'rsi': technical_indicators.get('latest_values', {}).get('RSI_14'),
                    'turnover_rate': technical_indicators.get('latest_values', {}).get('turnover_rate'),
                    'volatility': technical_indicators.get('latest_values', {}).get('volatility_20d'),
                    'date': target_date,
                    'technical_indicators': technical_indicators,
                    'data_quality': quality_results
                })

            except Exception as e:
                self.logger.warning(f"⚠️ 处理股票 {stock_code} 时出错: {e}")
                stats['errors'].append(f"{stock_code}: {str(e)}")
                continue

        return candidate_stocks

    def _prepare_pool_data(self, stocks: List[Dict], target_date: str, layer: str) -> Dict:
        """
        准备股票池数据

        Args:
            stocks: 股票列表
            target_date: 目标日期
            layer: 层级

        Returns:
            池数据字典
        """
        return {
            'date': target_date,
            'stocks': stocks,
            'layer': layer,
            'description': f'{layer}股票池（市值、价格、流动性初步筛选）',
            'created_at': datetime.now().isoformat(),
            'metadata': {
                'total_stocks': len(stocks),
                'avg_score': sum(s.get('score', 0) for s in stocks) / len(stocks) if stocks else 0,
                'score_range': {
                    'min': min((s.get('score', 0) for s in stocks), default=0),
                    'max': max((s.get('score', 0) for s in stocks), default=0)
                },
                'market_cap_distribution': self._analyze_market_cap_distribution(stocks)
            }
        }

    def _analyze_market_cap_distribution(self, stocks: List[Dict]) -> Dict:
        """
        分析市值分布

        Args:
            stocks: 股票列表

        Returns:
            市值分布分析
        """
        market_caps = []
        for s in stocks:
            mc = s.get('market_cap')
            if mc is not None and isinstance(mc, (int, float)) and not pd.isna(mc):
                market_caps.append(float(mc))

        if not market_caps:
            return {'error': '无市值数据'}

        return {
            'count': len(market_caps),
            'avg_market_cap': sum(market_caps) / len(market_caps),
            'median_market_cap': sorted(market_caps)[len(market_caps) // 2],
            'distribution': {
                'small': len([mc for mc in market_caps if mc < 100e8]),  # < 100亿
                'medium': len([mc for mc in market_caps if 100e8 <= mc < 500e8]),  # 100-500亿
                'large': len([mc for mc in market_caps if mc >= 500e8])  # >= 500亿
            }
        }

    def _log_build_statistics(self, stats: Dict):
        """
        记录构建统计信息

        Args:
            stats: 统计信息字典
        """
        duration = datetime.now() - stats['start_time']

        self.logger.info("📊 三个股票池构建统计信息:")
        self.logger.info(f"   ⏱️ 总耗时: {duration.total_seconds():.2f}秒")
        self.logger.info(f"   📈 总股票数: {stats['total_stocks']}")
        self.logger.info(f"   🏗️ 基础池: {stats['basic_pool_size']} 只")
        self.logger.info(f"   �️ 观察池: {stats['watch_pool_size']} 只")
        self.logger.info(f"   💎 核心池: {stats['core_pool_size']} 只")

        if stats['errors']:
            self.logger.warning(f"   ⚠️ 错误数量: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # 只显示前5个错误
                self.logger.warning(f"     - {error}")

        # 计算各池的成功率
        if stats['total_stocks'] > 0:
            basic_rate = (stats['basic_pool_size'] / stats['total_stocks'] * 100)
            watch_rate = (stats['watch_pool_size'] / stats['total_stocks'] * 100)
            core_rate = (stats['core_pool_size'] / stats['total_stocks'] * 100)

            self.logger.info(f"   📊 入选率 - 基础池: {basic_rate:.2f}%, 观察池: {watch_rate:.2f}%, 核心池: {core_rate:.2f}%")

        self.logger.info("✅ 股票池构建完成")

    def prepare_precomputed_data(self, stock_codes: List[str], target_date: Optional[str] = None) -> Dict[str, Dict]:
        """
        准备预计算数据 - 供build_stock_pool使用

        这个方法演示了如何准备预计算数据，然后传递给build_stock_pool

        Args:
            stock_codes: 股票代码列表
            target_date: 目标日期

        Returns:
            预计算数据字典
        """
        if target_date is None:
            target_date = self._get_latest_trading_date()

        self.logger.info(f"🔧 准备预计算数据: {len(stock_codes)} 只股票, 日期: {target_date}")

        precomputed_data = {}

        # 计算历史数据范围
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') -
                     timedelta(days=self.config['history_days'])).strftime('%Y-%m-%d')

        try:
            # 批量获取基本面数据
            valuation_data = self._batch_get_valuation_data(stock_codes, target_date, return_dataframe=True)
            if isinstance(valuation_data, pd.DataFrame) and not valuation_data.empty:
                valuation_dict = valuation_data.set_index('stock_code').to_dict('index')
            else:
                valuation_dict = {}

            # 批量获取价格数据
            price_data = self._batch_get_price_data(stock_codes, start_date, target_date)

            # 为每只股票计算技术指标
            for stock_code in stock_codes:
                try:
                    stock_info = valuation_dict.get(stock_code, {'stock_code': stock_code})
                    price_df = price_data.get(stock_code)

                    if price_df is not None and not price_df.empty:
                        # 计算技术指标
                        technical_indicators = self.calculate_technical_indicators(price_df, stock_code)

                        precomputed_data[stock_code] = {
                            'stock_info': stock_info,
                            'technical_indicators': technical_indicators
                        }
                    else:
                        self.logger.warning(f"⚠️ 股票 {stock_code} 无价格数据，跳过")

                except Exception as e:
                    self.logger.warning(f"⚠️ 处理股票 {stock_code} 时出错: {e}")
                    continue

            self.logger.info(f"✅ 预计算数据准备完成: {len(precomputed_data)}/{len(stock_codes)} 只股票")

        except Exception as e:
            self.logger.error(f"❌ 准备预计算数据失败: {e}")

        return precomputed_data

    def build_all_pools_from_precomputed_data(self, precomputed_data: Dict[str, Dict],
                                            target_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        从预计算数据构建所有股票池 - 优化版本

        使用DataFrame进行评分计算，避免字典到DataFrame的重复转换

        Args:
            precomputed_data: 预计算数据
            target_date: 目标日期

        Returns:
            三个股票池的字典
        """
        self.logger.info("🚀 开始从预计算数据构建所有股票池")

        try:
            # 直接创建DataFrame来存储评分结果，避免中间列表
            scored_rows = []

            for stock_code, stock_data in precomputed_data.items():
                try:
                    stock_info = stock_data.get('stock_info', {})
                    technical_indicators = stock_data.get('technical_indicators', {})

                    # 确保股票代码在stock_info中
                    if 'stock_code' not in stock_info:
                        stock_info['stock_code'] = stock_code

                    # 计算三个层级的评分
                    basic_score = self.calculate_basic_layer_score(stock_info, technical_indicators)
                    watch_score = self.calculate_watch_layer_score(stock_info, technical_indicators)
                    core_score = self.calculate_core_layer_score(stock_info, technical_indicators)

                    # 直接创建DataFrame行字典
                    scored_rows.append({
                        'stock_code': stock_code,
                        'basic_score': basic_score,
                        'watch_score': watch_score,
                        'core_score': core_score,
                        'market_cap': stock_info.get('market_cap'),
                        'pe_ratio': stock_info.get('pe_ratio'),
                        'pb_ratio': stock_info.get('pb_ratio'),
                        'current_price': technical_indicators.get('latest_values', {}).get('current_price'),
                        'rsi': technical_indicators.get('latest_values', {}).get('RSI_14'),
                        'turnover_rate': technical_indicators.get('latest_values', {}).get('turnover_rate'),
                        'volatility': technical_indicators.get('latest_values', {}).get('volatility_20d'),
                        'date': target_date,
                        # 存储完整的原始数据以备后续使用
                        'technical_indicators': technical_indicators,
                        'stock_info': stock_info
                    })

                except Exception as e:
                    self.logger.warning(f"⚠️ 评分股票 {stock_code} 时出错: {e}")
                    continue

            # 一次性创建DataFrame
            if scored_rows:
                df_scored = pd.DataFrame(scored_rows)
                self.logger.info(f"✅ 评分计算完成: {len(df_scored)} 只股票")
            else:
                df_scored = pd.DataFrame()
                self.logger.warning("⚠️ 无有效评分数据")

            # 使用优化的build_stock_pool方法（直接使用DataFrame）
            result = self.build_stock_pool(df_scored, target_date)

            return result

        except Exception as e:
            self.logger.error(f"❌ 从预计算数据构建股票池失败: {e}")
            return {
                'basic_pool': pd.DataFrame(),
                'watch_pool': pd.DataFrame(),
                'core_pool': pd.DataFrame()
            }





    def _validate_stock_codes(self, stocks: List[str]) -> List[str]:
        """
        验证股票代码的有效性

        Args:
            stocks: 股票代码列表

        Returns:
            有效的股票代码列表
        """
        valid_stocks = []
        for stock in stocks:
            if isinstance(stock, str) and (stock.endswith('.XSHE') or stock.endswith('.XSHG')):
                valid_stocks.append(stock)
        return valid_stocks

    def apply_basic_filters(self, stock_info: Dict, technical_indicators: Dict) -> bool:
        """
        应用基础层过滤条件
        """
        try:
            # 价格过滤
            current_price = technical_indicators.get('latest_values', {}).get('current_price')
            if not self._check_range(current_price, 'price_min', 'price_max'):
                return False

            # 换手率过滤
            turnover_rate = technical_indicators.get('latest_values', {}).get('turnover_rate')
            if turnover_rate is not None and not pd.isna(turnover_rate):
                if turnover_rate < self._get_config_value('turnover_min', 0):
                    return False

            # RSI过滤
            rsi_14 = technical_indicators.get('latest_values', {}).get('RSI_14')
            if not self._check_range(rsi_14, 'rsi_extreme_low', 'rsi_extreme_high'):
                return False

            # PE过滤
            pe_ratio = stock_info.get('pe_ratio')
            if pe_ratio is not None and not pd.isna(pe_ratio):
                if pe_ratio < 0 or pe_ratio > self._get_config_value('pe_acceptable_max', 100):
                    return False

            # 成交量过滤
            volume_sma_5 = technical_indicators.get('latest_values', {}).get('VOLUME_SMA_5')
            if volume_sma_5 is not None and not pd.isna(volume_sma_5):
                if volume_sma_5 < self._get_config_value('volume_min_threshold', 50000):
                    return False

            return True

        except Exception as e:
            self.logger.error(f"基础层过滤失败: {e}")
            return False

    def calculate_technical_indicators(self, price_data: pd.DataFrame, stock_code: Optional[str] = None) -> Dict:
        """
        计算技术指标 - 使用StockPoolIndicatorEngine生成完整的dataframe格式指标

        Args:
            price_data: 包含OHLCV的价格数据DataFrame
            stock_code: 股票代码（用于缓存键）

        Returns:
            Dict: 包含完整技术指标的字典，包含full_series和latest_values
        """
        if price_data is None or price_data.empty:
            return {}

        try:
            self.logger.debug(f"📈 使用StockPoolIndicatorEngine计算 {stock_code or 'unknown'} 的技术指标...")

            # 使用StockPoolIndicatorEngine计算所有技术指标
            indicators_result = self.indicator_engine.calculate_all_indicators(
                price_data,
                stock_code=stock_code
            )

            if not indicators_result:
                self.logger.warning(f"⚠️ {stock_code}: 技术指标计算失败")
                return {}

            # 获取最新值用于筛选和评分
            latest_values = indicators_result.get('latest_values', {})

            # 直接在latest_values上进行修改，避免创建新字典
            if not price_data.empty:
                # 当前价格
                current_price = price_data['close'].iloc[-1]
                latest_values['current_price'] = current_price

                # 最近5日平均成交量和换手率
                if 'volume' in price_data.columns and len(price_data) >= 5:
                    recent_volume = price_data['volume'].tail(5).mean()
                    latest_values['avg_volume_5d'] = recent_volume

                    # 计算换手率（如果有总市值数据的话，这里暂时用成交量作为替代）
                    if recent_volume > 0:
                        # 这里可以根据需要调整换手率的计算逻辑
                        latest_values['turnover_rate'] = recent_volume / 1000000  # 简化的换手率计算

                # 波动率计算
                if len(price_data) >= 20:
                    returns = price_data['close'].pct_change().dropna()
                    volatility = returns.tail(20).std() * np.sqrt(252)  # 年化波动率
                    latest_values['volatility_20d'] = volatility

            # 确保current_price始终被设置
            if not price_data.empty and 'current_price' not in latest_values:
                latest_values['current_price'] = price_data['close'].iloc[-1]

            # 返回结果，复用indicators_result的结构
            result = indicators_result
            result['latest_values'] = latest_values

            if result.get('errors'):
                self.logger.warning(f"⚠️ 指标计算完成但有错误: {result['errors'][:3]}...")

            return result

        except Exception as e:
            self.logger.error(f"❌ 技术指标计算失败: {e}")
            return {}

    def _get_latest_trading_date(self, target_date: Optional[str] = None, max_attempts: int = 5) -> str:
        """
        获取有效的交易日期

        使用rqdatac的交易日历API来获取有效的交易日，而不是依赖估值数据

        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            max_attempts: 最大尝试次数

        Returns:
            有效的交易日期 (YYYY-MM-DD)
        """
        try:
            if not RQDATAC_AVAILABLE or rqdatac is None:
                fallback_date = (datetime.strptime(target_date, '%Y-%m-%d') if target_date
                               else datetime.now()).strftime('%Y-%m-%d')
                self.logger.warning(f"📅 rqdatac不可用，使用后备日期: {fallback_date}")
                return fallback_date

            # 确定基准日期
            base_date = (datetime.strptime(target_date, '%Y-%m-%d') if target_date
                        else datetime.now()).date()

            # 获取交易日历 - 使用rqdatac的get_trading_dates方法
            start_date = (base_date - timedelta(days=15)).strftime('%Y-%m-%d')
            end_date = (base_date + timedelta(days=5)).strftime('%Y-%m-%d')

            trading_dates = rqdatac.get_trading_dates(
                start_date=start_date,
                end_date=end_date
            )

            if trading_dates is None or len(trading_dates) == 0:
                fallback_date = base_date.strftime('%Y-%m-%d')
                self.logger.warning(f"📅 未获取到交易日历，使用基准日期: {fallback_date}")
                return fallback_date

            # 找到目标日期或最近的交易日
            for i in range(len(trading_dates) - 1, -1, -1):
                trade_date = trading_dates[i]

                # 确保trade_date是date对象
                if isinstance(trade_date, datetime):
                    trade_date = trade_date.date()
                elif isinstance(trade_date, str):
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                elif not isinstance(trade_date, date):
                    # 如果是其他类型，尝试转换为字符串然后解析
                    trade_date = datetime.strptime(str(trade_date), '%Y-%m-%d').date()

                if trade_date <= base_date:
                    return trade_date.strftime('%Y-%m-%d')

            fallback_date = base_date.strftime('%Y-%m-%d')
            self.logger.warning(f"⚠️ 未找到合适的交易日，使用基准日期: {fallback_date}")
            return fallback_date

        except Exception as e:
            fallback_date = (datetime.strptime(target_date, '%Y-%m-%d') if target_date
                           else datetime.now()).strftime('%Y-%m-%d')
            self.logger.error(f"❌ 获取交易日期失败，使用后备日期: {fallback_date}, 错误: {e}")
            return fallback_date

    def _get_config_value(self, key: str, default=None):
        """
        获取配置值

        Args:
            key: 配置键
            default: 默认值

        Returns:
            配置值或默认值
        """
        return self.config.get(key, default)

    def _check_range(self, value, min_key: str, max_key: str) -> bool:
        """
        检查值是否在配置范围内

        Args:
            value: 要检查的值
            min_key: 配置中的最小值键名
            max_key: 配置中的最大值键名

        Returns:
            是否在范围内
        """
        if value is None or pd.isna(value):
            return False

        min_val = self._get_config_value(min_key)
        max_val = self._get_config_value(max_key)

        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False

        return True

    def get_sync_status(self) -> Dict:
        """
        获取同步状态

        Returns:
            包含同步状态信息的字典
        """
        return {
            'is_syncing': self.is_syncing,
            'last_sync_time': self.last_sync_time,
            'data_date': date.today().isoformat(),
            'basic_pool_exists': self.basic_pool_file.exists(),
            'watch_pool_exists': self.watch_pool_file.exists(),
            'core_pool_exists': self.core_pool_file.exists(),
            'basic_pool_count': self._get_file_count(self.basic_pool_file),
            'watch_pool_count': self._get_file_count(self.watch_pool_file),
            'core_pool_count': self._get_file_count(self.core_pool_file)
        }

    def _get_file_count(self, file_path) -> int:
        """
        获取文件中的股票数量

        Args:
            file_path: 文件路径

        Returns:
            股票数量
        """
        try:
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'stocks' in data:
                        return len(data['stocks'])
                    elif isinstance(data, list):
                        return len(data)
            return 0
        except:
            return 0

    def load_all_pools(self, return_dict: bool = False) -> Union[Dict[str, pd.DataFrame], Dict[str, List[Dict]]]:
        """
        加载所有股票池数据 - 优化版本

        默认返回DataFrame格式以减少转换开销，可选择返回字典格式以保持向后兼容性

        Args:
            return_dict: 是否返回字典格式（向后兼容），默认为False返回DataFrame

        Returns:
            包含所有层级股票池的字典，格式由return_dict参数决定
        """
        try:
            result = {}

            # 加载基础层
            basic_data = self.data_store.load_pool('basic')
            if not basic_data.empty:
                result['basic_layer'] = basic_data

            # 加载观察层
            watch_data = self.data_store.load_pool('watch')
            if not watch_data.empty:
                result['watch_layer'] = watch_data

            # 加载核心层
            core_data = self.data_store.load_pool('core')
            if not core_data.empty:
                result['core_layer'] = core_data

            # 如果需要字典格式，进行转换（只在需要时进行）
            if return_dict:
                dict_result = {}
                for key, df in result.items():
                    if not df.empty:
                        # 使用to_dict('records')进行高效转换
                        dict_result[key] = df.to_dict('records')
                    else:
                        dict_result[key] = []
                return dict_result

            return result

        except Exception as e:
            self.logger.error(f"加载所有股票池失败: {e}")
            if return_dict:
                return {'basic_layer': [], 'watch_layer': [], 'core_layer': []}
            else:
                return {
                    'basic_layer': pd.DataFrame(),
                    'watch_layer': pd.DataFrame(),
                    'core_layer': pd.DataFrame()
                }

    def save_basic_pool(self, pool_data: List[Dict]) -> bool:
        """
        保存基础股票池 (使用datastore)

        Args:
            pool_data: 要保存的股票池数据

        Returns:
            bool: 是否保存成功
        """
        # 设置内部实例并保存
        self.data_store.basic_pool = pd.DataFrame(pool_data)
        return self.data_store.save_basic_pool()

    def save_watch_pool(self, pool_data: List[Dict]) -> bool:
        """
        保存观察股票池 (使用datastore)

        Args:
            pool_data: 要保存的股票池数据

        Returns:
            bool: 是否保存成功
        """
        # 设置内部实例并保存
        self.data_store.watch_pool = pd.DataFrame(pool_data)
        return self.data_store.save_watch_pool()

    def save_core_pool(self, pool_data: List[Dict]) -> bool:
        """
        保存核心股票池 (使用datastore)

        Args:
            pool_data: 要保存的股票池数据

        Returns:
            bool: 是否保存成功
        """
        # 设置内部实例并保存
        self.data_store.core_pool = pd.DataFrame(pool_data)
        return self.data_store.save_core_pool()

    def save_pool(self, pool_type: str, pool_data: List[Dict]) -> bool:
        """
        保存指定类型的股票池 (使用datastore)

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')
            pool_data: 要保存的股票池数据

        Returns:
            bool: 是否保存成功
        """
        # 设置内部实例并保存
        df_data = pd.DataFrame(pool_data)
        if pool_type == 'basic':
            self.data_store.basic_pool = df_data
        elif pool_type == 'watch':
            self.data_store.watch_pool = df_data
        elif pool_type == 'core':
            self.data_store.core_pool = df_data

        return self.data_store.save_pool_by_name(pool_type)


# ============================================================================
# 主程序入口 - 直接启动股票池同步程序
# ============================================================================

# Initialize environment on module load
setup_environment()

# ============================================================================
# STOCK POOL DATA STORE - 独立数据存储层
# ============================================================================

class StockPoolDataStore:
    """
    StockPool专用数据存储器

    核心功能:
    - 独立数据缓存: 避免与其他模块的文件操作冲突
    - 智能交易日确定: 基于交易时段的全局日期判断
    - 三层股票池管理: 基础/观察/核心池的内存和持久化管理
    - 批量数据获取: 优化的RQDatac数据获取接口

    设计优势:
    - 轻量级架构: 专注于股票池数据的存储和访问
    - 高性能缓存: 内存缓存 + 文件持久化双重保障
    - 智能错误处理: 完善的异常处理和降级策略
    - 模块化设计: 与其他组件解耦，便于维护和扩展

    数据流:
    RQDatac → 批量缓存 → 内存实例 → 文件持久化
    """

    def __init__(self, cache_dir: str = "stockpool_cache"):
        """
        初始化数据存储器

        Args:
            cache_dir: 缓存目录路径，用于存储临时数据文件

        Raises:
            OSError: 当无法创建缓存目录时抛出
        """
        logger.debug(f"🔧 初始化 StockPoolDataStore, 缓存目录: {cache_dir}")

        self.cache_dir = cache_dir
        self.data_dir = "stockpool_data"

        # 创建必要的目录结构
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            os.makedirs(self.data_dir, exist_ok=True)
            logger.debug(f"📁 创建缓存目录: {self.cache_dir}, 数据目录: {self.data_dir}")
        except OSError as e:
            logger.error(f"❌ 无法创建缓存目录: {e}")
            raise

        # 初始化数据缓存结构
        self._init_cache_structures()

        # 初始化股票池实例
        self._init_pool_instances()

        # 预先确定目标交易日（全局优化）
        self._determine_target_trading_date()

        logger.info(f"✅ StockPoolDataStore 初始化完成")
        logger.info(f"📅 预先确定的目标交易日: {self.target_trading_date}")
        logger.debug(f"🏗️ 股票池实例状态: basic={not self.basic_pool.empty}, watch={not self.watch_pool.empty}, core={not self.core_pool.empty}")

    def _init_cache_structures(self):
        """初始化缓存数据结构"""
        logger.debug("🔧 初始化缓存数据结构")

        # 股票列表缓存
        self.stock_list_cache: Optional[pd.DataFrame] = None
        self.stock_list_timestamp: Optional[float] = None

        # K线数据缓存 - {stock_code: DataFrame}
        self.kline_cache: Dict[str, pd.DataFrame] = {}

        # 批量数据缓存 - {cache_key: DataFrame}
        self.batch_cache: Dict[str, pd.DataFrame] = {}

        logger.debug("✅ 缓存结构初始化完成")

    def _init_pool_instances(self):
        """初始化股票池实例"""
        logger.debug("🔧 初始化股票池实例")

        # 三层股票池的内存实例
        self.basic_pool: pd.DataFrame = pd.DataFrame()      # 基础股票池
        self.watch_pool: pd.DataFrame = pd.DataFrame()      # 观察股票池
        self.core_pool: pd.DataFrame = pd.DataFrame()       # 核心股票池

        # 股票池元数据
        self.pool_metadata = {
            'basic_pool': {'last_updated': None, 'count': 0, 'date': None},
            'watch_pool': {'last_updated': None, 'count': 0, 'date': None},
            'core_pool': {'last_updated': None, 'count': 0, 'date': None}
        }

        # 全局交易日确定器
        self.target_trading_date: Optional[str] = None
        self.target_trading_date_determined = False

        logger.debug("✅ 股票池实例初始化完成")

    def load_data_from_file(self, filename: str) -> Optional[Dict]:
        """
        从文件加载数据

        Args:
            filename: 文件名

        Returns:
            Dict: 加载的数据或None
        """
        try:
            from pathlib import Path

            # 检查多个可能的目录
            search_dirs = [
                Path(self.data_dir),    # stockpool_data
                Path("data"),           # data目录
                Path("../data")         # 相对路径的data目录
            ]

            for search_dir in search_dirs:
                filepath = search_dir / filename
                if filepath.exists():
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    logger.info(f"📁 数据加载成功: {filename} (from {search_dir})")
                    return data

            # 如果都没找到
            logger.warning(f"⚠️ 数据文件不存在: {filename}")
            return None

        except Exception as e:
            logger.error(f"❌ 数据加载失败 {filename}: {e}")
            return None

    def save_data_to_file(self, data: Dict, filename: str) -> bool:
        """
        保存数据到文件

        Args:
            data: 要保存的数据
            filename: 文件名

        Returns:
            bool: 是否成功
        """
        try:
            filepath = os.path.join(self.data_dir, filename)

            # 原子写入操作
            temp_filepath = filepath + ".tmp"
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)

            # 原子移动
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(temp_filepath, filepath)

            logger.info(f"💾 数据保存成功: {filename}")
            return True

        except Exception as e:
            logger.error(f"❌ 数据保存失败 {filename}: {e}")
            return False

    def save_pool(self, pool_type: str) -> bool:
        """
        根据类型保存股票池数据到文件

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            bool: 保存是否成功

        Raises:
            ValueError: 当pool_type无效时抛出异常
        """
        try:
            # 根据pool_type获取对应的池数据和配置
            if pool_type == 'basic':
                pool_data = self.basic_pool
                pool_name = '基础股票池'
                filename = 'basic_pool.json'
                metadata_key = 'basic_pool'
            elif pool_type == 'watch':
                pool_data = self.watch_pool
                pool_name = '观察股票池'
                filename = 'watch_pool.json'
                metadata_key = 'watch_pool'
            elif pool_type == 'core':
                pool_data = self.core_pool
                pool_name = '核心股票池'
                filename = 'core_pool.json'
                metadata_key = 'core_pool'
            else:
                raise ValueError(f"无效的股票池类型: {pool_type}。支持的类型: 'basic', 'watch', 'core'")

            # 检查池数据是否有数据
            if pool_data.empty:
                logger.warning(f"⚠️ {pool_name}为空，跳过保存")
                return True

            # 更新元数据
            self.pool_metadata[metadata_key] = {
                'last_updated': datetime.now().isoformat(),
                'count': len(pool_data),
                'date': pool_data.iloc[0].get('date') if len(pool_data) > 0 else None
            }

            # 保存到文件 - 转换为字典列表格式
            data_list = pool_data.to_dict('records')
            data = {
                'type': f'{pool_type}_pool',
                'timestamp': datetime.now().isoformat(),
                'count': len(data_list),
                'data': data_list
            }

            success = self.save_data_to_file(data, filename)
            if success:
                logger.info(f"✅ {pool_name}已保存: {len(pool_data)} 只股票")
            return success

        except Exception as e:
            logger.error(f"❌ 保存{pool_type}股票池失败: {e}")
            return False

    def save_basic_pool(self) -> bool:
        """
        保存基础股票池数据（兼容性方法）

        Returns:
            bool: 保存是否成功
        """
        return self.save_pool('basic')

    def save_watch_pool(self) -> bool:
        """
        保存观察股票池数据（兼容性方法）

        Returns:
            bool: 保存是否成功
        """
        return self.save_pool('watch')

    def save_core_pool(self) -> bool:
        """
        保存核心股票池数据（兼容性方法）

        Returns:
            bool: 保存是否成功
        """
        return self.save_pool('core')

    # ===== 股票池实例访问方法 =====

    def get_pool(self, pool_type: str, copy: bool = True) -> pd.DataFrame:
        """
        根据类型获取股票池实例

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')
            copy: 是否返回拷贝，默认为True以保持向后兼容性

        Returns:
            pd.DataFrame: 指定类型的股票池数据

        Raises:
            ValueError: 当pool_type无效时抛出异常
        """
        if pool_type == 'basic':
            if not self.basic_pool.empty:
                return self.basic_pool.copy() if copy else self.basic_pool
            else:
                return pd.DataFrame()
        elif pool_type == 'watch':
            if not self.watch_pool.empty:
                return self.watch_pool.copy() if copy else self.watch_pool
            else:
                return pd.DataFrame()
        elif pool_type == 'core':
            if not self.core_pool.empty:
                return self.core_pool.copy() if copy else self.core_pool
            else:
                return pd.DataFrame()
        else:
            raise ValueError(f"无效的股票池类型: {pool_type}。支持的类型: 'basic', 'watch', 'core'")

    def get_basic_pool(self) -> pd.DataFrame:
        """
        获取基础股票池实例（兼容性方法）

        Returns:
            pd.DataFrame: 基础股票池数据
        """
        return self.get_pool('basic')

    def get_watch_pool(self) -> pd.DataFrame:
        """
        获取观察股票池实例（兼容性方法）

        Returns:
            pd.DataFrame: 观察股票池数据
        """
        return self.get_pool('watch')

    def get_core_pool(self) -> pd.DataFrame:
        """
        获取核心股票池实例（兼容性方法）

        Returns:
            pd.DataFrame: 核心股票池数据
        """
        return self.get_pool('core')

    def get_all_pools(self) -> Dict[str, pd.DataFrame]:
        """
        获取所有股票池实例

        Returns:
            Dict[str, pd.DataFrame]: 包含所有股票池的字典
        """
        return {
            'basic_pool': self.get_basic_pool(),
            'watch_pool': self.get_watch_pool(),
            'core_pool': self.get_core_pool()
        }

    def get_pool_info(self, pool_type: str) -> Dict:
        """
        获取指定股票池的信息

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            Dict: 股票池信息
        """
        if pool_type not in self.pool_metadata:
            return {'error': f'未知的股票池类型: {pool_type}'}

        info = self.pool_metadata[pool_type].copy()
        info['pool_type'] = pool_type

        # 添加实时数据
        if pool_type == 'basic':
            info['current_count'] = len(self.basic_pool) if not self.basic_pool.empty else 0
        elif pool_type == 'watch':
            info['current_count'] = len(self.watch_pool) if not self.watch_pool.empty else 0
        elif pool_type == 'core':
            info['current_count'] = len(self.core_pool) if not self.core_pool.empty else 0

        return info

    def get_all_pool_info(self) -> Dict[str, Dict]:
        """
        获取所有股票池的信息

        Returns:
            Dict[str, Dict]: 所有股票池的信息
        """
        return {
            'basic_pool': self.get_pool_info('basic'),
            'watch_pool': self.get_pool_info('watch'),
            'core_pool': self.get_pool_info('core')
        }

    def is_pool_available(self, pool_type: str) -> bool:
        """
        检查指定股票池是否可用

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            bool: 股票池是否可用
        """
        if pool_type == 'basic':
            return not self.basic_pool.empty
        elif pool_type == 'watch':
            return not self.watch_pool.empty
        elif pool_type == 'core':
            return not self.core_pool.empty
        return False

    def retrieve_stock_codes_from_pool(self, pool_type: str) -> List[str]:
        """
        获取指定股票池中的股票代码列表

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            List[str]: 股票代码列表
        """
        pool_data = pd.DataFrame()
        if pool_type == 'basic':
            pool_data = self.basic_pool
        elif pool_type == 'watch':
            pool_data = self.watch_pool
        elif pool_type == 'core':
            pool_data = self.core_pool

        if pool_data.empty:
            return []

        # 从DataFrame中提取股票代码列
        if 'stock_code' in pool_data.columns:
            return pool_data['stock_code'].dropna().tolist()
        else:
            return []

    def find_stock_in_pools_by_code(self, stock_code: str) -> pd.DataFrame:
        """
        在所有股票池中查找指定股票

        Args:
            stock_code: 股票代码

        Returns:
            pd.DataFrame: 包含找到的股票信息，每个股票一行，包含pool_type列标识来源
        """
        found_stocks = []

        # 在基础池中查找
        if not self.basic_pool.empty and 'stock_code' in self.basic_pool.columns:
            mask = self.basic_pool['stock_code'] == stock_code
            if mask.any():
                stock_info = self.basic_pool.loc[mask].iloc[0].to_dict()
                stock_info['pool_type'] = 'basic'
                found_stocks.append(stock_info)

        # 在观察池中查找
        if not self.watch_pool.empty and 'stock_code' in self.watch_pool.columns:
            mask = self.watch_pool['stock_code'] == stock_code
            if mask.any():
                stock_info = self.watch_pool.loc[mask].iloc[0].to_dict()
                stock_info['pool_type'] = 'watch'
                found_stocks.append(stock_info)

        # 在核心池中查找
        if not self.core_pool.empty and 'stock_code' in self.core_pool.columns:
            mask = self.core_pool['stock_code'] == stock_code
            if mask.any():
                stock_info = self.core_pool.loc[mask].iloc[0].to_dict()
                stock_info['pool_type'] = 'core'
                found_stocks.append(stock_info)

        # 如果没有找到任何股票，返回空DataFrame
        if not found_stocks:
            return pd.DataFrame()

        # 将所有找到的股票信息合并成DataFrame
        result_df = pd.DataFrame(found_stocks)

        # 重新排列列，将pool_type列放在前面
        if 'pool_type' in result_df.columns:
            cols = ['pool_type'] + [col for col in result_df.columns if col != 'pool_type']
            result_df = result_df[cols]

        return result_df

    def clear_pool_instances(self) -> None:
        """
        清空所有股票池实例（内存中的数据）
        """
        self.basic_pool = pd.DataFrame()
        self.watch_pool = pd.DataFrame()
        self.core_pool = pd.DataFrame()

        # 重置元数据
        for pool_type in self.pool_metadata:
            self.pool_metadata[pool_type] = {
                'last_updated': None,
                'count': 0,
                'date': None
            }

        logger.info("🧹 已清空所有股票池实例")

    def reload_pools_from_files(self) -> bool:
        """
        从文件重新加载所有股票池到内存实例

        Returns:
            bool: 重新加载是否成功
        """
        try:
            success_count = 0

            # 重新加载基础池
            if self.load_basic_pool():
                success_count += 1

            # 重新加载观察池
            if self.load_watch_pool():
                success_count += 1

            # 重新加载核心池
            if self.load_core_pool():
                success_count += 1

            logger.info(f"✅ 已从文件重新加载 {success_count} 个股票池到内存实例")
            return success_count > 0

        except Exception as e:
            logger.error(f"❌ 从文件重新加载股票池失败: {e}")
            return False

    def load_pool(self, pool_type: str) -> pd.DataFrame:
        """
        加载指定类型的股票池

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            pd.DataFrame: 股票池数据
        """
        try:
            from pathlib import Path

            # 根据pool_type确定文件名和日志信息
            pool_configs = {
                'basic': {
                    'filename': 'basic_pool.json',
                    'display_name': '基础股票池',
                    'emoji': '📂'
                },
                'watch': {
                    'filename': 'watch_pool.json',
                    'display_name': '观察股票池',
                    'emoji': '👀'
                },
                'core': {
                    'filename': 'core_pool.json',
                    'display_name': '核心股票池',
                    'emoji': '⭐'
                }
            }

            if pool_type not in pool_configs:
                logger.error(f"❌ 不支持的股票池类型: {pool_type}")
                return pd.DataFrame()

            config = pool_configs[pool_type]

            # 优先从保存目录加载（stockpool_data），然后尝试其他目录
            search_paths = [
                Path(self.data_dir) / config['filename'],  # 优先：stockpool_data目录
                Path("data") / config['filename'],         # 备选：data目录
                Path("../data") / config['filename']       # 备选：上级data目录
            ]

            for pool_file in search_paths:
                if pool_file.exists():
                    with open(pool_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # 解析保存的数据格式（只支持新格式）
                    if isinstance(data, dict) and 'data' in data:
                        # 这是save_pool保存的格式
                        stocks = data['data']
                        df = pd.DataFrame(stocks)
                        logger.info(f"{config['emoji']} 加载{config['display_name']}: {len(df)} 只股票 (from {pool_file.parent})")
                        return df
                    # 如果数据直接是列表格式
                    elif isinstance(data, list):
                        df = pd.DataFrame(data)
                        logger.info(f"{config['emoji']} 加载{config['display_name']}: {len(df)} 只股票 (from {pool_file.parent})")
                        return df
                    else:
                        logger.warning(f"⚠️ {config['display_name']}文件格式不正确: {pool_file}")
                        continue

            # 如果都没找到文件
            logger.warning(f"⚠️ {config['display_name']}文件不存在")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"❌ 加载{config['display_name']}失败: {e}")
            return pd.DataFrame()
        
    def save_pool_by_name(self, pool_type: str) -> bool:
        """
        通用保存股票池方法（使用内部实例）

        Args:
            pool_type: 股票池类型 ('basic', 'watch', 'core')

        Returns:
            bool: 保存是否成功
        """
        return self.save_pool(pool_type)

    def load_basic_pool(self) -> bool:
        """
        加载基础股票池数据到内部实例

        Returns:
            bool: 加载是否成功
        """
        try:
            data = self.load_pool('basic')
            if not data.empty:
                self.basic_pool = data
                self.pool_metadata['basic_pool'] = {
                    'last_updated': datetime.now().isoformat(),
                    'count': len(data),
                    'date': data.iloc[0].get('date') if len(data) > 0 else None
                }
                logger.info(f"✅ 基础股票池已加载到内存: {len(data)} 只股票")
                return True
            else:
                logger.warning("⚠️ 基础股票池数据为空")
                return False
        except Exception as e:
            logger.error(f"❌ 加载基础股票池失败: {e}")
            return False

    def load_watch_pool(self) -> bool:
        """
        加载观察股票池数据到内部实例

        Returns:
            bool: 加载是否成功
        """
        try:
            data = self.load_pool('watch')
            if not data.empty:
                self.watch_pool = data
                self.pool_metadata['watch_pool'] = {
                    'last_updated': datetime.now().isoformat(),
                    'count': len(data),
                    'date': data.iloc[0].get('date') if len(data) > 0 else None
                }
                logger.info(f"✅ 观察股票池已加载到内存: {len(data)} 只股票")
                return True
            else:
                logger.warning("⚠️ 观察股票池数据为空")
                return False
        except Exception as e:
            logger.error(f"❌ 加载观察股票池失败: {e}")
            return False

    def load_core_pool(self) -> bool:
        """
        加载核心股票池数据到内部实例

        Returns:
            bool: 加载是否成功
        """
        try:
            data = self.load_pool('core')
            if not data.empty:
                self.core_pool = data
                self.pool_metadata['core_pool'] = {
                    'last_updated': datetime.now().isoformat(),
                    'count': len(data),
                    'date': data.iloc[0].get('date') if len(data) > 0 else None
                }
                logger.info(f"✅ 核心股票池已加载到内存: {len(data)} 只股票")
                return True
            else:
                logger.warning("⚠️ 核心股票池数据为空")
                return False
        except Exception as e:
            logger.error(f"❌ 加载核心股票池失败: {e}")
            return False




    def filter_by_type(self, df: pd.DataFrame, include_st: bool = False, status_filter: str = 'Active') -> pd.Series:
        """
        A股股票过滤器 - 返回布尔Series用于过滤

        Args:
            df: 要过滤的DataFrame
            include_st: 是否包含ST股票，默认False（排除ST股票）
            status_filter: 状态过滤器，默认'Active'（只包含活跃股票）

        Returns:
            pd.Series: 布尔Series，表示每行是否满足过滤条件
        """
        try:
            if df is None or df.empty:
                logger.warning("⚠️ 输入DataFrame为空")
                return pd.Series([], dtype=bool)

            # 初始化全为True的布尔Series
            mask = pd.Series(True, index=df.index)

            # 过滤为A股股票（上海和深圳交易所）
            if 'order_book_id' in df.columns:
                a_stock_mask = (
                    df['order_book_id'].str.endswith('.XSHE') |
                    df['order_book_id'].str.endswith('.XSHG')
                )
                mask = mask & a_stock_mask

            # 根据状态过滤
            if status_filter and status_filter != 'All' and 'status' in df.columns:
                status_mask = df['status'] == status_filter
                mask = mask & status_mask

            # 根据ST股票过滤
            if not include_st and 'abbrev_symbol' in df.columns:
                st_mask = ~df['abbrev_symbol'].str.contains('ST', na=False, case=False)
                mask = mask & st_mask

            logger.debug(f"✅ A股类型过滤: {mask.sum()} 只股票满足条件")
            return mask

        except Exception as e:
            logger.error(f"❌ A股类型过滤失败: {e}")
            return pd.Series([], dtype=bool)

    def filter_by_type_to_cache(self, include_st: bool = False, status_filter: str = 'Active') -> bool:
        """
        A股股票过滤器 - 直接操作内部股票数据（兼容性函数）

        Args:
            include_st: 是否包含ST股票，默认False（排除ST股票）
            status_filter: 状态过滤器，默认'Active'（只包含活跃股票）

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            # 过滤为A股股票（上海和深圳交易所）
            a_stocks = self.stock_list_cache[
                (self.stock_list_cache['order_book_id'].str.endswith('.XSHE')) |
                (self.stock_list_cache['order_book_id'].str.endswith('.XSHG'))
            ].copy()

            if a_stocks.empty:
                logger.warning("⚠️ 未找到A股股票数据")
                self.stock_list_cache = pd.DataFrame()
                return False

            # 根据状态过滤
            if status_filter and 'status' in a_stocks.columns:
                a_stocks = a_stocks[a_stocks['status'] == status_filter]

            # 根据ST股票过滤
            if not include_st and 'abbrev_symbol' in a_stocks.columns:
                a_stocks = a_stocks[
                    ~a_stocks['abbrev_symbol'].str.contains('ST', na=False, case=False)
                ]

            # 更新内部缓存
            self.stock_list_cache = a_stocks
            logger.info(f"✅ A股过滤完成: {len(a_stocks)} 只股票 (ST过滤: {not include_st}, 状态: {status_filter})")
            return True

        except Exception as e:
            logger.error(f"❌ A股股票过滤失败: {e}")
            return False

    def filter_by_market_cap_to_cache(self, min_cap: float = None, max_cap: float = None) -> bool:
        """
        按市值过滤股票 - 直接操作内部股票数据（兼容性函数）

        Args:
            min_cap: 最小市值（万元）
            max_cap: 最大市值（万元）

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            filtered_df = self.stock_list_cache.copy()

            # 按市值下限过滤
            if min_cap is not None and 'market_cap' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['market_cap'] >= min_cap]

            # 按市值上限过滤
            if max_cap is not None and 'market_cap' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['market_cap'] <= max_cap]

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"💰 市值过滤完成: {len(filtered_df)} 只股票 (市值范围: {min_cap} - {max_cap})")
            return True

        except Exception as e:
            logger.error(f"❌ 市值过滤失败: {e}")
            return False

    def filter_by_market_cap(self, min_cap: float = None, max_cap: float = None) -> bool:
        """
        按市值过滤股票 - 直接操作内部股票数据

        Args:
            min_cap: 最小市值（万元）
            max_cap: 最大市值（万元）

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            filtered_df = self.stock_list_cache.copy()

            # 按市值下限过滤
            if min_cap is not None and 'market_cap' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['market_cap'] >= min_cap]

            # 按市值上限过滤
            if max_cap is not None and 'market_cap' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['market_cap'] <= max_cap]

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"💰 市值过滤完成: {len(filtered_df)} 只股票 (市值范围: {min_cap} - {max_cap})")
            return True

        except Exception as e:
            logger.error(f"❌ 市值过滤失败: {e}")
            return False

    def filter_by_industry_to_cache(self, industries: List[str] = None) -> bool:
        """
        按行业过滤股票 - 直接操作内部股票数据（兼容性函数）

        Args:
            industries: 行业列表

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            if not industries:
                logger.info("ℹ️ 无行业过滤条件，跳过行业过滤")
                return True

            filtered_df = self.stock_list_cache.copy()

            # 按行业过滤
            if 'industry_name' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['industry_name'].isin(industries)]
            elif 'citics_industry_name' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['citics_industry_name'].isin(industries)]
            else:
                logger.warning("⚠️ 股票数据中没有行业信息字段")
                return False

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"🏭 行业过滤完成: {len(filtered_df)} 只股票 (行业: {industries})")
            return True

        except Exception as e:
            logger.error(f"❌ 行业过滤失败: {e}")
            return False

    def filter_by_exchange_to_cache(self, exchange: str = None) -> bool:
        """
        按交易所过滤股票 - 直接操作内部股票数据（兼容性函数）

        Args:
            exchange: 交易所代码 ('XSHE' 或 'XSHG')

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            if not exchange:
                logger.info("ℹ️ 无交易所过滤条件，跳过交易所过滤")
                return True

            if exchange not in ['XSHE', 'XSHG']:
                logger.error(f"❌ 不支持的交易所: {exchange}")
                return False

            filtered_df = self.stock_list_cache[
                self.stock_list_cache['order_book_id'].str.endswith(f'.{exchange}')
            ].copy()

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"🏢 交易所过滤完成: {len(filtered_df)} 只股票 (交易所: {exchange})")
            return True

        except Exception as e:
            logger.error(f"❌ 交易所过滤失败: {e}")
            return False

    def filter_by_industry(self, industries: List[str] = None) -> bool:
        """
        按行业过滤股票 - 直接操作内部股票数据

        Args:
            industries: 行业列表

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            if not industries:
                logger.info("ℹ️ 无行业过滤条件，跳过行业过滤")
                return True

            filtered_df = self.stock_list_cache.copy()

            # 按行业过滤
            if 'industry_name' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['industry_name'].isin(industries)]
            elif 'citics_industry_name' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['citics_industry_name'].isin(industries)]
            else:
                logger.warning("⚠️ 股票数据中没有行业信息字段")
                return False

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"🏭 行业过滤完成: {len(filtered_df)} 只股票 (行业: {industries})")
            return True

        except Exception as e:
            logger.error(f"❌ 行业过滤失败: {e}")
            return False

    def filter_by_exchange(self, exchange: str = None) -> bool:
        """
        按交易所过滤股票 - 直接操作内部股票数据

        Args:
            exchange: 交易所代码 ('XSHE' 或 'XSHG')

        Returns:
            bool: 过滤是否成功
        """
        try:
            if self.stock_list_cache is None or self.stock_list_cache.empty:
                logger.warning("⚠️ 内部股票数据为空，请先调用fetch_stock_list()获取数据")
                return False

            if not exchange:
                logger.info("ℹ️ 无交易所过滤条件，跳过交易所过滤")
                return True

            if exchange not in ['XSHE', 'XSHG']:
                logger.error(f"❌ 不支持的交易所: {exchange}")
                return False

            filtered_df = self.stock_list_cache[
                self.stock_list_cache['order_book_id'].str.endswith(f'.{exchange}')
            ].copy()

            # 更新内部缓存
            self.stock_list_cache = filtered_df
            logger.info(f"🏢 交易所过滤完成: {len(filtered_df)} 只股票 (交易所: {exchange})")
            return True

        except Exception as e:
            logger.error(f"❌ 交易所过滤失败: {e}")
            return False

    def apply_filters_to_cache(self, filters: Dict[str, Any] = None) -> bool:
        """
        应用多个过滤器到内部缓存 - 链式过滤（旧版函数，保持兼容性）

        Args:
            filters: 过滤器配置字典
                {
                    'include_st': bool,          # 是否包含ST股票
                    'status_filter': str,        # 状态过滤器
                    'min_market_cap': float,     # 最小市值
                    'max_market_cap': float,     # 最大市值
                    'industries': List[str],     # 行业列表
                    'exchange': str              # 交易所
                }

        Returns:
            bool: 所有过滤器是否都应用成功
        """
        try:
            if not filters:
                logger.info("ℹ️ 无过滤条件，跳过过滤")
                return True

            success_count = 0
            total_filters = len(filters)

            # A股基础过滤
            if 'include_st' in filters or 'status_filter' in filters:
                include_st = filters.get('include_st', False)
                status_filter = filters.get('status_filter', 'Active')
                if self.filter_by_type_to_cache(include_st=include_st, status_filter=status_filter):
                    success_count += 1

            # 市值过滤
            if 'min_market_cap' in filters or 'max_market_cap' in filters:
                min_cap = filters.get('min_market_cap')
                max_cap = filters.get('max_market_cap')
                if self.filter_by_market_cap_to_cache(min_cap=min_cap, max_cap=max_cap):
                    success_count += 1

            # 行业过滤
            if 'industries' in filters:
                industries = filters['industries']
                if self.filter_by_industry_to_cache(industries=industries):
                    success_count += 1

            # 交易所过滤
            if 'exchange' in filters:
                exchange = filters['exchange']
                if self.filter_by_exchange_to_cache(exchange=exchange):
                    success_count += 1

            logger.info(f"🎯 综合过滤完成: {success_count}/{total_filters} 个过滤器成功应用")
            return success_count == total_filters

        except Exception as e:
            logger.error(f"❌ 综合过滤失败: {e}")
            return False



    def fetch_stock_list(self, filters: Dict[str, Any] = None) -> Optional[pd.DataFrame]:
        """
        获取所有A股股票列表 (带缓存和过滤)

        Args:
            filters: 过滤器配置字典
                {
                    'include_st': bool,          # 是否包含ST股票，默认False
                    'status_filter': str,        # 状态过滤器，默认'Active'
                    'min_market_cap': float,     # 最小市值（万元）
                    'max_market_cap': float,     # 最大市值（万元）
                    'industries': List[str],     # 行业列表
                    'exchange': str              # 交易所 ('XSHE' 或 'XSHG')
                }

        Returns:
            DataFrame: 所有A股股票列表或None，包含股票基本信息

        示例:
            # 获取默认过滤的股票（活跃非ST股票）
            stocks_df = datastore.fetch_stock_list()

            # 获取包含ST股票的股票
            stocks_with_st = datastore.fetch_stock_list({
                'include_st': True
            })

            # 获取大市值股票（市值>50亿）
            large_cap_stocks = datastore.fetch_stock_list({
                'min_market_cap': 500000  # 50亿 = 500000万元
            })

            # 获取特定行业的股票
            tech_stocks = datastore.fetch_stock_list({
                'industries': ['计算机', '通信', '电子']
            })

            # 获取深圳交易所的股票
            sz_stocks = datastore.fetch_stock_list({
                'exchange': 'XSHE'
            })

            # 综合过滤：非ST、大市值、科技行业、深圳交易所
            filtered_stocks = datastore.fetch_stock_list({
                'include_st': False,
                'min_market_cap': 100000,  # 10亿
                'industries': ['计算机', '通信'],
                'exchange': 'XSHE'
            })
        """
        try:
            # 检查缓存是否有效（一次性运行，无需过期检查）
            if (self.stock_list_cache is not None and
                isinstance(self.stock_list_cache, pd.DataFrame) and
                not self.stock_list_cache.empty):
                logger.debug(f"✅ 使用缓存的A股股票列表: {len(self.stock_list_cache)} 只股票")
                return self.stock_list_cache.copy()

            if not RQDATAC_AVAILABLE or rqdatac is None:
                logger.warning("⚠️ rqdatac不可用")
                return None

            # 获取所有股票数据
            all_stocks = rqdatac.all_instruments(type='Stock', market='cn')

            if all_stocks is None or all_stocks.empty:
                logger.warning("⚠️ 未获取到股票数据")
                return None

            # 先应用过滤器，然后设置缓存
            filtered_stocks = all_stocks

            # 应用过滤器
            if filters:
                filter_mask = self.apply_filters(all_stocks, filters)
                if filter_mask.empty:
                    logger.warning("⚠️ 过滤器应用失败")
                    return None
                # 使用布尔Series过滤DataFrame
                filtered_stocks = all_stocks[filter_mask]
            else:
                # 默认过滤：只保留活跃股票（排除退市股票）
                default_filters = {'status_filter': 'Active'}
                filter_mask = self.apply_filters(all_stocks, default_filters)
                if filter_mask.empty:
                    logger.warning("⚠️ 默认过滤器应用失败")
                    return None
                # 使用布尔Series过滤DataFrame
                filtered_stocks = all_stocks[filter_mask]

            # 设置过滤后的结果到缓存
            self.stock_list_cache = filtered_stocks

            if filtered_stocks.empty:
                logger.warning("⚠️ 过滤后没有符合条件的A股股票")
                return None

            # 更新时间戳
            self.stock_list_timestamp = time.time()

            filter_desc = "默认过滤" if not filters else f"自定义过滤({len(filters)}个条件)"
            logger.info(f"📋 获取A股股票列表: {len(filtered_stocks)} 只股票 ({filter_desc})")
            return filtered_stocks

        except Exception as e:
            logger.error(f"❌ 获取A股股票列表失败: {e}")
            return None

    def apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.Series:
        """
        应用过滤器并返回布尔Series

        Args:
            df: 要过滤的DataFrame
            filters: 过滤器配置字典

        Returns:
            pd.Series: 布尔Series，表示每行是否满足过滤条件
        """
        try:
            if df is None or df.empty:
                logger.warning("⚠️ 没有股票数据可供过滤")
                return pd.Series([], dtype=bool)

            # 初始化全为True的布尔Series
            mask = pd.Series(True, index=df.index)

            # 1. ST股票过滤
            if not filters.get('include_st', False):
                # 过滤掉ST股票（名称包含ST或*ST）
                st_mask = ~df['symbol'].str.contains(r'ST|\*ST', case=False, na=False)
                mask = mask & st_mask
                logger.debug(f"📊 ST过滤: {st_mask.sum()} 只股票满足条件")

            # 2. 状态过滤
            status_filter = filters.get('status_filter', 'Active')
            if status_filter != 'All':
                status_mask = df['status'] == status_filter
                mask = mask & status_mask
                logger.debug(f"📊 状态过滤({status_filter}): {status_mask.sum()} 只股票满足条件")

            # 3. 市值过滤
            min_market_cap = filters.get('min_market_cap')
            max_market_cap = filters.get('max_market_cap')

            if min_market_cap is not None or max_market_cap is not None:
                # 这里需要获取市值数据，暂时跳过市值过滤
                logger.debug("📊 市值过滤: 暂不支持（需要市值数据）")

            # 4. 行业过滤
            industries = filters.get('industries')
            if industries:
                industry_mask = df['industry_code'].isin(industries)
                mask = mask & industry_mask
                logger.debug(f"📊 行业过滤({industries}): {industry_mask.sum()} 只股票满足条件")

            # 5. 交易所过滤
            exchange = filters.get('exchange')
            if exchange:
                exchange_mask = df['exchange'] == exchange
                mask = mask & exchange_mask
                logger.debug(f"📊 交易所过滤({exchange}): {exchange_mask.sum()} 只股票满足条件")

            logger.info(f"✅ 过滤器应用成功: {mask.sum()} 只股票满足所有条件")
            return mask

        except Exception as e:
            logger.error(f"❌ 应用过滤器失败: {e}")
            return pd.Series([], dtype=bool)

    def _fetch_price_series(self, stock_code: str, start_date: str, end_date: str, force_refresh: bool = False) -> Optional[pd.DataFrame]:
        """
        获取K线数据 (优化后的缓存逻辑)

        缓存策略:
        - 内存缓存：存储完整数据用于后续计算
        - 磁盘缓存：持久化存储，减少网络访问
        - 每次从网络获取数据后同步保存到磁盘
        - 下次获取时先检查磁盘缓存，如果有足够数据则加载到内存，不再访问网络
        - 不考虑增量更新，每次都拉完整数据
        - 支持强制刷新参数

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            force_refresh: 是否强制从网络获取最新数据

        Returns:
            DataFrame: K线数据或None
        """
        import time
        start_time = time.time()

        # 生成缓存键和文件名（极简设计）
        cache_key = stock_code  # 直接使用股票代码作为缓存键
        cache_filename = f"kline_{cache_key}.json"

        # 如果不是强制刷新，先尝试从磁盘缓存加载
        if not force_refresh:
            cached_data = self.load_data_from_file(cache_filename)
            if cached_data is not None:
                try:
                    # 从缓存的数据重建DataFrame
                    records = cached_data['data']
                    df = pd.DataFrame(records)
                    if not df.empty:
                        # 将date列设置为索引
                        if 'date' in df.columns:
                            df['date'] = pd.to_datetime(df['date'])
                            df = df.set_index('date')

                        # 从缓存元数据中获取股票代码（现在直接是order_book_id）
                        cached_stock_code = cached_data.get('stock_code', stock_code)

                        # 缓存到内存用于计算（极简设计）
                        self.kline_cache[cached_stock_code] = df.copy()

                        # 注意：程序一次性运行，无需缓存大小限制

                        logger.debug(f"✅ 从磁盘缓存加载K线数据: {cached_stock_code}, {len(df)}条记录")
                        return df
                except Exception as e:
                    logger.warning(f"⚠️ 磁盘缓存数据格式错误: {cache_filename}, {e}")

        # 检查内存缓存（仅当不是强制刷新时）
        if not force_refresh:
            # 首先尝试使用输入的stock_code查找
            if stock_code in self.kline_cache:
                cached_df = self.kline_cache[stock_code]
                # 验证缓存数据的股票代码是否匹配 - 现在检查order_book_id列
                if 'order_book_id' in cached_df.columns and cached_df['order_book_id'].iloc[0] == stock_code:
                    logger.debug(f"✅ 内存缓存命中: {stock_code}")
                    return cached_df.copy()
                else:
                    # 缓存数据不匹配，清除无效缓存
                    del self.kline_cache[stock_code]
                    logger.debug(f"🧹 清除无效内存缓存: {stock_code}")
            
            # 如果输入的stock_code没有找到，尝试查找所有缓存中是否有匹配的order_book_id
            for cache_key, cached_df in self.kline_cache.items():
                if 'order_book_id' in cached_df.columns and cached_df['order_book_id'].iloc[0] == stock_code:
                    logger.debug(f"✅ 内存缓存命中 (通过order_book_id): {stock_code}")
                    return cached_df.copy()

        # 从rqdatac获取数据
        if not RQDATAC_AVAILABLE or rqdatac is None:
            logger.warning(f"⚠️ rqdatac不可用，无法获取{stock_code}的数据")
            return None

        # 添加重试机制和超时控制
        max_retries = 3
        timeout_seconds = 30  # 30秒超时

        for attempt in range(max_retries):
            try:
                logger.debug(f"📡 获取 {stock_code} 数据 (尝试 {attempt + 1}/{max_retries})...")

                # 直接在主线程调用rqdatac.get_price()
                df = rqdatac.get_price(
                    stock_code,
                    start_date=start_date,
                    end_date=end_date,
                    frequency='1d',
                    fields=['open', 'close', 'high', 'low', 'limit_up', 'limit_down', 'total_turnover', 'volume', 'num_trades']
                )

                if df is not None and not df.empty:
                    # 从MultiIndex中提取股票代码
                    if isinstance(df.index, pd.MultiIndex) and 'order_book_id' in df.index.names:
                        # 重置索引，将order_book_id和date作为列
                        df_reset = df.reset_index()
                        # 提取股票代码 - 直接使用order_book_id
                        extracted_stock_code = df_reset['order_book_id'].iloc[0]
                        # 重新设置date为索引
                        df_reset = df_reset.set_index('date')
                        df = df_reset

                        logger.debug(f"📊 从DataFrame提取股票代码: {extracted_stock_code}")
                    else:
                        # 如果没有MultiIndex，使用传入的stock_code
                        extracted_stock_code = stock_code
                        logger.debug(f"📊 使用传入的股票代码: {stock_code}")

                    # 直接使用order_book_id作为缓存键
                    cache_key = extracted_stock_code
                    cache_filename = f"kline_{cache_key}.json"

                    # 立即保存到磁盘缓存
                    cache_data = {
                        'stock_code': extracted_stock_code,
                        'start_date': start_date,
                        'end_date': end_date,
                        'data': df.reset_index().to_dict('records'),  # 重置索引后转换为记录列表，避免datetime序列化问题
                        'cached_time': datetime.now().isoformat(),
                        'data_points': len(df)
                    }

                    if self.save_data_to_file(cache_data, cache_filename):
                        logger.debug(f"💾 K线数据已保存到磁盘缓存: {cache_filename}")
                    else:
                        logger.warning(f"⚠️ K线数据保存到磁盘缓存失败: {cache_filename}")

                    # 缓存到内存用于计算（极简设计）
                    self.kline_cache[extracted_stock_code] = df.copy()

                    logger.debug(f"✅ 获取K线数据: {extracted_stock_code}, {len(df)}条记录")

                    return df
                else:
                    logger.warning(f"⚠️ 未获取到{stock_code}的K线数据")
                    return None

            except TimeoutError as e:
                logger.warning(f"⚠️ {stock_code} 请求超时: {e}")
                if attempt < max_retries - 1:
                    logger.debug(f"重试中 ({attempt + 1}/{max_retries})...")
                    continue
                else:
                    logger.error(f"❌ {stock_code} 在 {max_retries} 次尝试后仍然失败")
                    return None

            except Exception as e:
                logger.warning(f"⚠️ 获取 {stock_code} 数据失败 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    logger.debug(f"重试中 ({attempt + 1}/{max_retries})...")
                    continue
                else:
                    logger.error(f"❌ {stock_code} 在 {max_retries} 次尝试后仍然失败: {e}")
                    return None

        return None

    def _fetch_instruments_info(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        获取股票基本信息

        rqdatac.instruments() 返回的字段包括:
        - order_book_id: 订单簿ID (如: '000001.XSHE')
        - symbol: 股票名称 (如: '平安银行')
        - abbrev_symbol: 缩写符号 (如: 'PAYH')
        - trading_code: 交易代码 (如: '000001')
        - exchange: 交易所 (如: 'XSHE')
        - type: 证券类型 (如: 'CS' - 股票)
        - status: 状态 (如: 'Active')
        - listed_date: 上市日期 (如: '1991-04-03')
        - de_listed_date: 退市日期 (如: '0000-00-00')
        - board_type: 板块类型 (如: 'MainBoard')
        - industry_code: 行业代码 (如: 'J66')
        - industry_name: 行业名称 (如: '货币金融服务')
        - sector_code: 板块代码 (如: 'Financials')
        - sector_code_name: 板块名称 (如: '金融')
        - citics_industry_code: 中信行业代码 (如: '40')
        - citics_industry_name: 中信行业名称 (如: '银行')
        - province: 省份 (如: '广东省')
        - office_address: 办公地址
        - issue_price: 发行价 (如: 40.0)
        - round_lot: 每手股数 (如: 100)
        - market_tplus: T+1市场 (如: 1)
        - trading_hours: 交易时间 (如: '09:31-11:30,13:01-15:00')
        - special_type: 特殊类型 (如: 'Normal')
        - concept_names: 概念名称列表

        Returns:
            DataFrame: 股票基本信息，包含所有字段作为列
        """
        try:
            if not RQDATAC_AVAILABLE or rqdatac is None:
                logger.warning("⚠️ rqdatac不可用")
                return None

            # 获取基本信息 - 使用正确的函数名
            basic_info = rqdatac.instruments(stock_code)
            if basic_info is not None:
                # 将 Instrument 对象转换为字典
                info_dict = {}
                for attr in dir(basic_info):
                    if not attr.startswith('_') and not callable(getattr(basic_info, attr)):
                        try:
                            value = getattr(basic_info, attr)
                            info_dict[attr] = value
                        except Exception as e:
                            logger.debug(f"⚠️ 获取属性 {attr} 失败: {e}")

                # 转换为DataFrame格式，股票代码作为索引
                if info_dict:
                    df = pd.DataFrame([info_dict])
                    df.index = [stock_code]
                    logger.debug(f"📊 获取股票基本信息: {stock_code}, {len(info_dict)} 个字段")
                    return df
                else:
                    logger.warning(f"⚠️ 未获取到有效的股票信息字段: {stock_code}")
                    return None
            else:
                logger.warning(f"⚠️ 未找到股票信息: {stock_code}")
                return None

        except Exception as e:
            logger.error(f"❌ 获取股票基本信息失败 {stock_code}: {e}")
            return None

    def _fetch_valuation_series(self, stock_code: str, date: str = None) -> Optional[pd.DataFrame]:
        """
        获取股票估值信息
        支持批量获取优化

        Args:
            stock_code: 股票代码
            date: 查询日期，默认为今天

        Returns:
            DataFrame: 估值数据，包含多列估值指标
        """
        try:
            if not RQDATAC_AVAILABLE or rqdatac is None:
                logger.warning("⚠️ rqdatac不可用")
                return None

            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')

            # 获取估值因子数据
            valuation_factors = ['pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio', 'market_cap', 'turnover_ratio']

            # 尝试从批量缓存中获取
            cached_result = self._get_cached_batch_valuation(stock_code, date, valuation_factors)
            if cached_result is not None:
                return cached_result

            # 如果没有批量缓存，则逐个获取
            valuation_data = rqdatac.get_factor(stock_code, valuation_factors, start_date=date, end_date=date)

            if valuation_data is not None and not valuation_data.empty:
                logger.debug(f"💰 获取估值信息: {stock_code} ({date}), {len(valuation_data)} 条记录")

                # 检查是否所有核心字段都为NaN（数据质量问题）
                core_fields = ['pe_ratio', 'pb_ratio', 'market_cap']
                all_core_nan = True
                for field in core_fields:
                    if field in valuation_data.columns and not valuation_data[field].isna().all():
                        all_core_nan = False
                        break

                if all_core_nan:
                    logger.warning(f"⚠️ {stock_code} ({date}) 核心估值数据全部为NaN，可能是数据更新延迟")
                    return None

                return valuation_data
            else:
                logger.warning(f"⚠️ 未获取到估值数据: {stock_code} ({date})")
                return None

        except Exception as e:
            logger.error(f"❌ 获取估值信息失败 {stock_code}: {e}")
            return None

    def _get_latest_trading_date(self, stock_code: str, max_days_back: int = 30) -> Optional[str]:
        """
        获取最新的有效交易日
        使用交易日历API直接获取有效交易日，避免通过数据验证的低效方式

        Args:
            stock_code: 股票代码
            max_days_back: 最大回溯天数

        Returns:
            有效的交易日期字符串 (YYYY-MM-DD) 或 None
        """
        try:
            # 获取最近的交易日历
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=max_days_back)

            # 使用rqdatac获取交易日历
            trading_dates = rqdatac.get_trading_dates(start_date, end_date)  # type: ignore

            if trading_dates is None or len(trading_dates) == 0:
                logger.warning(f"⚠️ 未获取到交易日历数据")
                return None

            # 转换为日期列表并排序（最新的在前）
            trading_dates = sorted([pd.to_datetime(d).date() for d in trading_dates], reverse=True)

            # 返回最新的交易日
            if len(trading_dates) > 0:
                latest_date = trading_dates[0]
                date_str = latest_date.strftime('%Y-%m-%d')
                logger.debug(f"✅ 找到最新交易日: {stock_code} @ {date_str}")
                return date_str

            logger.warning(f"⚠️ 在{max_days_back}天内未找到有效的交易日")
            return None

        except Exception as e:
            logger.error(f"❌ 获取最新交易日失败 {stock_code}: {e}")
            return None

    def _determine_target_trading_date(self) -> None:
        """
        预先确定目标交易日，避免对每只股票重复检查
        使用 get_trading_hours 获取精确的交易时段进行判断

        核心逻辑:
        1. 获取最新交易日和交易时段
        2. 根据当前时间和交易时段智能判断应该使用哪个交易日
        3. 交易前/交易中/收盘后3小时内：使用前一个交易日
        4. 收盘后3小时后：使用当天数据
        5. 只做一次全局判断，后续所有股票都使用相同的结果

        性能优化:
        - 全局一次判断 vs 每只股票单独判断
        - 基于交易时段的精确判断 vs 简单时间判断
        - 智能降级策略确保系统稳定性

        Raises:
            所有异常都会被捕获并降级处理，不会影响系统运行
        """
        logger.debug("🔍 开始确定目标交易日")

        try:
            if not RQDATAC_AVAILABLE or rqdatac is None:
                logger.warning("⚠️ RQDatac不可用，使用当前日期作为后备")
                self.target_trading_date = datetime.now().strftime('%Y-%m-%d')
                self.target_trading_date_determined = True
                logger.debug(f"📅 使用后备日期: {self.target_trading_date}")
                return

            # 获取最新交易日
            latest_date = rqdatac.get_latest_trading_date()  # type: ignore
            if not latest_date:
                logger.warning("⚠️ 无法获取最新交易日，使用当前日期")
                self.target_trading_date = datetime.now().strftime('%Y-%m-%d')
                self.target_trading_date_determined = True
                return

            logger.info(f"📅 最新交易日: {latest_date}")

            # 获取当前时间
            now = datetime.now()
            latest_date_dt = pd.to_datetime(latest_date).to_pydatetime()
            logger.debug(f"🕐 当前时间: {now}, 最新交易日: {latest_date_dt.date()}")

            # 判断是否在交易日当天
            if now.date() == latest_date_dt.date():
                logger.debug("📅 当前在交易日当天，需要判断数据有效性")

                # 获取实际交易时段进行精确判断
                try:
                    # 使用平安银行作为代表股票获取交易时段
                    trading_hours_str = rqdatac.get_trading_hours('000001.XSHE', latest_date.strftime('%Y-%m-%d'))  # type: ignore
                    logger.info(f"⏰ 交易时段: {trading_hours_str}")

                    # 验证交易时段数据格式
                    if not isinstance(trading_hours_str, str):
                        logger.error(f"❌ 交易时段数据格式异常: {type(trading_hours_str)}")
                        raise ValueError(f"交易时段格式异常: {trading_hours_str}")

                    # 解析交易时段并判断数据有效性
                    should_use_previous = self._should_use_previous_trading_date(trading_hours_str, now)
                    logger.debug(f"🎯 是否使用前一个交易日: {should_use_previous}")

                    if should_use_previous:
                        # 使用前一个交易日
                        previous_date = rqdatac.get_previous_trading_date(latest_date)  # type: ignore
                        if previous_date:
                            self.target_trading_date = previous_date.strftime('%Y-%m-%d')
                            logger.info(f"🎯 确定目标交易日: {self.target_trading_date} (前一个交易日)")
                        else:
                            # 如果无法获取前一个交易日，使用最新交易日
                            self.target_trading_date = latest_date.strftime('%Y-%m-%d')
                            logger.warning(f"⚠️ 无法获取前一个交易日，使用最新交易日: {self.target_trading_date}")
                    else:
                        # 使用当天数据
                        self.target_trading_date = latest_date.strftime('%Y-%m-%d')
                        logger.info(f"🎯 确定目标交易日: {self.target_trading_date} (最新交易日)")

                except Exception as e:
                    logger.warning(f"⚠️ 获取交易时段失败，使用简单时间判断: {e}")
                    logger.debug("🔄 执行降级时间检查逻辑", exc_info=True)
                    # 降级到简单的时间判断逻辑
                    self._fallback_time_check(latest_date_dt, now)
            else:
                # 当前不在交易日当天，最新交易日数据应该有效
                logger.info("📅 当前不在交易日当天，最新交易日数据应该有效")
                self.target_trading_date = latest_date.strftime('%Y-%m-%d')
                logger.info(f"🎯 确定目标交易日: {self.target_trading_date} (最新交易日)")

            self.target_trading_date_determined = True
            logger.debug("✅ 目标交易日确定完成")

        except Exception as e:
            logger.error(f"❌ 确定目标交易日失败: {e}")
            logger.debug("🔄 使用当前日期作为最终后备", exc_info=True)
            # 降级使用当前日期
            self.target_trading_date = datetime.now().strftime('%Y-%m-%d')
            self.target_trading_date_determined = True

    def get_target_trading_date(self) -> Optional[str]:
        """
        获取预先确定的目标交易日

        Returns:
            目标交易日字符串 (YYYY-MM-DD) 或 None
        """
        if not self.target_trading_date_determined:
            self._determine_target_trading_date()
        return self.target_trading_date

    def _get_cached_batch_valuation(self, stock_code: str, date: str, factors: List[str]) -> Optional[pd.DataFrame]:
        """
        从批量缓存中获取估值数据
        如果缓存中没有，则触发批量获取

        Args:
            stock_code: 股票代码
            date: 日期
            factors: 因子列表

        Returns:
            DataFrame: 估值数据或None
        """
        # 检查是否已有批量数据缓存
        cache_key = f"batch_valuation_{date}"
        if cache_key not in self.batch_cache:
            # 如果没有缓存，检查是否有待处理的批量请求
            if not hasattr(self, '_pending_batch_stocks'):
                self._pending_batch_stocks = set()

            self._pending_batch_stocks.add(stock_code)

            # 如果积累了足够多的股票（比如5只），就执行批量获取
            if len(self._pending_batch_stocks) >= 5:
                self._execute_batch_valuation_fetch(date, factors)
                self._pending_batch_stocks.clear()

                # 重新检查缓存
                if cache_key in self.batch_cache:
                    batch_data = self.batch_cache[cache_key]
                    return self._extract_single_stock_from_batch(batch_data, stock_code, date)

        # 从缓存中提取单个股票的数据
        if cache_key in self.batch_cache:
            batch_data = self.batch_cache[cache_key]
            return self._extract_single_stock_from_batch(batch_data, stock_code, date)

        return None

    def _execute_batch_valuation_fetch(self, date: str, factors: List[str]) -> None:
        """
        执行批量估值数据获取

        批量获取策略:
        - 累积多个股票请求后统一获取
        - 减少API调用次数，提高效率
        - 智能缓存批量结果
        - 失败时不影响单个股票获取

        Args:
            date: 目标日期 (YYYY-MM-DD)
            factors: 要获取的估值因子列表

        Raises:
            该方法内部处理所有异常，不会向上传播
        """
        logger.debug(f"🔄 开始批量估值获取: 日期={date}, 因子={factors}")

        try:
            if not hasattr(self, '_pending_batch_stocks') or not self._pending_batch_stocks:
                logger.debug("📭 无待处理的批量请求，跳过")
                return

            stock_list = list(self._pending_batch_stocks)
            logger.info(f"🚀 执行批量估值获取: {len(stock_list)} 只股票 @ {date}")
            logger.debug(f"📋 股票列表: {stock_list[:5]}{'...' if len(stock_list) > 5 else ''}")

            # 执行批量获取
            batch_result = rqdatac.get_factor(stock_list, factors, start_date=date, end_date=date)

            if batch_result is not None and not batch_result.empty:
                # 缓存批量结果
                cache_key = f"batch_valuation_{date}"
                self.batch_cache[cache_key] = batch_result

                logger.info(f"✅ 批量估值获取成功: {batch_result.shape[0]} 条记录")
                logger.debug(f"📊 数据形状: {batch_result.shape}, 列: {list(batch_result.columns)}")
            else:
                logger.warning(f"⚠️ 批量估值获取失败: 返回空结果")
                logger.debug("🔍 检查: RQDatac连接状态和参数有效性")

        except Exception as e:
            logger.error(f"❌ 批量估值获取异常: {e}")
            logger.debug("🔍 异常详情", exc_info=True)
            # 不向上传播异常，保持系统稳定性

        except Exception as e:
            logger.error(f"❌ 批量估值获取失败: {e}")

    def _extract_single_stock_from_batch(self, batch_data: pd.DataFrame, stock_code: str, date: str) -> Optional[pd.DataFrame]:
        """
        从批量数据中提取单个股票的数据

        Args:
            batch_data: 批量数据
            stock_code: 股票代码
            date: 日期

        Returns:
            DataFrame: 单个股票的数据
        """
        try:
            # 批量数据的索引是 (股票代码, 日期) 的多重索引
            if isinstance(batch_data.index, pd.MultiIndex):
                # 查找对应的股票和日期
                mask = (batch_data.index.get_level_values(0) == stock_code)
                if date:
                    date_obj = pd.to_datetime(date)
                    mask = mask & (batch_data.index.get_level_values(1) == date_obj)

                if mask.any():
                    single_stock_data = batch_data[mask].copy()
                    # 重置索引，移除多重索引
                    single_stock_data = single_stock_data.reset_index()
                    # 只保留因子列
                    factor_columns = [col for col in single_stock_data.columns if col not in ['level_0', 'level_1', stock_code, date]]
                    if factor_columns:
                        single_stock_data = single_stock_data[factor_columns]
                    return single_stock_data

            return None

        except Exception as e:
            logger.error(f"❌ 从批量数据中提取单个股票失败 {stock_code}: {e}")
            return None

    def _should_use_previous_trading_date(self, trading_hours_str: str, current_time: datetime) -> bool:
        """
        判断是否应该使用前一个交易日的数据

        逻辑：
        1. 交易前：使用前一个交易日
        2. 交易中：使用前一个交易日（实时数据不稳定）
        3. 交易后3小时内：使用前一个交易日（数据可能还在更新）
        4. 交易后3小时后：使用当天数据

        Args:
            trading_hours_str: 交易时段字符串，如 "09:31-11:30,13:01-15:00"
            current_time: 当前时间

        Returns:
            是否应该使用前一个交易日
        """
        try:
            if not trading_hours_str:
                return True

            current_time_only = current_time.time()

            # 解析交易时段
            periods = trading_hours_str.split(',')
            market_open_time = None
            market_close_time = None

            for period in periods:
                if '-' not in period:
                    continue

                start_str, end_str = period.split('-')
                try:
                    start_time = datetime.strptime(start_str.strip(), '%H:%M').time()
                    end_time = datetime.strptime(end_str.strip(), '%H:%M').time()

                    if market_open_time is None or start_time < market_open_time:
                        market_open_time = start_time
                    if market_close_time is None or end_time > market_close_time:
                        market_close_time = end_time
                except ValueError:
                    continue

            if market_open_time is None or market_close_time is None:
                logger.warning(f"⚠️ 无法解析交易时段: {trading_hours_str}")
                return True

            # 计算收盘后3小时的时间点
            close_datetime = datetime.combine(current_time.date(), market_close_time)
            three_hours_after_close = close_datetime + timedelta(hours=3)
            three_hours_after_close_time = three_hours_after_close.time()

            logger.info(f"⏰ 市场开盘: {market_open_time}, 收盘: {market_close_time}")
            logger.info(f"⏰ 当前时间: {current_time_only}, 收盘后3小时: {three_hours_after_close_time}")

            if current_time_only < market_open_time:
                # 交易前：使用前一个交易日
                logger.info("⏰ 当前在交易前，使用前一个交易日")
                return True
            elif market_open_time <= current_time_only <= market_close_time:
                # 交易中：使用前一个交易日（实时数据不稳定）
                logger.info("⏰ 当前在交易中，使用前一个交易日（实时数据不稳定）")
                return True
            elif market_close_time < current_time_only <= three_hours_after_close_time:
                # 收盘后3小时内：使用前一个交易日（数据可能还在更新）
                logger.info("⏰ 当前在收盘后3小时内，使用前一个交易日（数据可能还在更新）")
                return True
            else:
                # 收盘后3小时后：使用当天数据
                logger.info("⏰ 当前在收盘后3小时后，使用当天数据")
                return False

        except Exception as e:
            logger.warning(f"⚠️ 判断是否使用前一个交易日失败: {e}")
            return True  # 出错时保守使用前一个交易日

    def _fallback_time_check(self, latest_date: datetime, now: datetime) -> None:
        """
        降级时间检查逻辑（当获取交易时段失败时使用）

        Args:
            latest_date: 最新交易日
            now: 当前时间
        """
        # 检查是否在交易时间内
        current_time = now.time()
        # 假设A股收盘时间是15:00
        market_close_time = datetime.strptime('15:00', '%H:%M').time()

        if current_time < market_close_time:
            # 当前在交易日当天且未收盘，数据必然无效
            logger.info("⏰ 当前在交易日当天且未收盘，最新交易日数据必然无效")

            # 获取前一个交易日
            previous_date = rqdatac.get_previous_trading_date(latest_date)  # type: ignore
            if previous_date:
                self.target_trading_date = previous_date.strftime('%Y-%m-%d')
                logger.info(f"🎯 确定目标交易日: {self.target_trading_date} (前一个交易日)")
            else:
                # 如果无法获取前一个交易日，使用最新交易日
                self.target_trading_date = latest_date.strftime('%Y-%m-%d')
                logger.warning(f"⚠️ 无法获取前一个交易日，使用最新交易日: {self.target_trading_date}")
        else:
            # 当前在交易日当天但已收盘，数据可能有效
            logger.info("⏰ 当前在交易日当天且已收盘，最新交易日数据可能有效")
            self.target_trading_date = latest_date.strftime('%Y-%m-%d')
            logger.info(f"🎯 确定目标交易日: {self.target_trading_date} (最新交易日)")

    def _get_previous_trading_date(self, stock_code: str, max_days_back: int = 30) -> Optional[str]:
        """
        使用RQDatac API获取最后一个已完成交易日的数据
        智能判断：优先使用最新交易日，如果数据无效则使用前一个交易日

        Args:
            stock_code: 股票代码
            max_days_back: 最大回溯天数

        Returns:
            最后一个已完成交易日的日期字符串 (YYYY-MM-DD) 或 None
        """
        try:
            # 定义核心字段
            core_fields = ['pe_ratio', 'pb_ratio', 'market_cap']

            # 使用RQDatac API获取最新交易日
            latest_date = rqdatac.get_latest_trading_date()  # type: ignore
            if not latest_date:
                logger.warning("⚠️ 无法获取最新交易日")
                return None

            logger.debug(f"📅 最新交易日: {latest_date}")

            # 检查最新交易日的数据是否有效
            valuation_df = self._fetch_data('valuation', stock_code, date=latest_date.strftime('%Y-%m-%d'))

            if valuation_df is not None and not valuation_df.empty:
                # 检查核心字段是否有效（非NaN）
                has_valid_data = False

                for field in core_fields:
                    if field in valuation_df.columns and not valuation_df[field].isna().all():
                        has_valid_data = True
                        break

                if has_valid_data:
                    logger.debug(f"✅ 最新交易日 {latest_date} 数据有效")
                    return latest_date.strftime('%Y-%m-%d')

            # 如果最新交易日数据无效，尝试前一个交易日
            logger.debug(f"⚠️ 最新交易日 {latest_date} 数据无效，尝试前一个交易日")

            # 使用RQDatac API获取前一个交易日
            previous_date = rqdatac.get_previous_trading_date(latest_date)  # type: ignore
            if not previous_date:
                logger.warning("⚠️ 无法获取前一个交易日")
                return None

            logger.debug(f"📅 前一个交易日: {previous_date}")

            # 检查前一个交易日的数据是否有效
            prev_valuation_df = self._fetch_data('valuation', stock_code, date=previous_date.strftime('%Y-%m-%d'))

            if prev_valuation_df is not None and not prev_valuation_df.empty:
                # 检查前一个交易日的数据是否有效
                has_prev_valid_data = False

                for field in core_fields:
                    if field in prev_valuation_df.columns and not prev_valuation_df[field].isna().all():
                        has_prev_valid_data = True
                        break

                if has_prev_valid_data:
                    logger.debug(f"✅ 前一个交易日 {previous_date} 数据有效")
                    return previous_date.strftime('%Y-%m-%d')

            logger.warning(f"⚠️ 前一个交易日 {previous_date} 数据也不可用")
            return None

        except Exception as e:
            logger.error(f"❌ 获取最后一个已完成交易日失败 {stock_code}: {e}")
            return None

    def get_smart_data(self, stock_code: str, data_type: str, max_retries: int = 5) -> Optional[pd.DataFrame]:
        """
        智能数据获取函数
        使用预先确定的目标交易日，避免对每只股票重复检查
        优化版：全局判断一次，后续所有股票都使用相同的结果

        Args:
            stock_code: 股票代码
            data_type: 数据类型 ('valuation', 'price')
            max_retries: 最大重试次数

        Returns:
            DataFrame: 包含完整数据的DataFrame或None
        """
        try:
            # 使用预先确定的目标交易日，避免重复检查
            target_date = self.get_target_trading_date()
            if not target_date:
                logger.warning(f"⚠️ 无法确定目标交易日")
                return None

            logger.debug(f"🎯 使用预先确定的目标交易日: {target_date}")

            # 根据数据类型获取数据
            if data_type == 'valuation':
                valuation_df = self._fetch_data('valuation', stock_code, date=target_date)
                if valuation_df is not None and not valuation_df.empty:
                    # 验证数据完整性
                    if self._validate_valuation_completeness(valuation_df):
                        logger.debug(f"✅ 获取估值数据成功: {stock_code} @ {target_date}")
                        return valuation_df
                    else:
                        logger.warning(f"⚠️ {stock_code} @ {target_date} 估值数据不完整")
                        return None
                else:
                    logger.warning(f"⚠️ {stock_code} @ {target_date} 未获取到估值数据")
                    return None

            elif data_type == 'price':
                # 获取价格数据需要更多参数 - 增加到90天以确保技术指标计算完整性
                start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=90)).strftime('%Y-%m-%d')
                price_df = self._fetch_data('price', stock_code, start_date=start_date, end_date=target_date)
                if price_df is not None and not price_df.empty:
                    # 确保数据完整性
                    if self._validate_dataframe_completeness(price_df, data_type):
                        logger.debug(f"✅ 获取价格数据成功: {stock_code}, {len(price_df)}条记录")
                        return price_df
                    else:
                        logger.warning(f"⚠️ {stock_code} 价格数据不完整")
                        return None
                else:
                    logger.warning(f"⚠️ {stock_code} 未获取到价格数据")
                    return None

            else:
                logger.error(f"❌ 不支持的数据类型: {data_type}")
                return None

        except Exception as e:
            logger.error(f"❌ 获取智能数据失败 {stock_code} ({data_type}): {e}")
            return None

    def _fetch_data(self, data_type: str, stock_code: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        统一的私有数据获取方法

        Args:
            data_type: 数据类型 ('price', 'valuation', 'instruments')
            stock_code: 股票代码
            **kwargs: 其他参数

        Returns:
            DataFrame: 请求的数据或None
        """
        if data_type == 'price':
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            force_refresh = kwargs.get('force_refresh', False)
            if start_date and end_date:
                return self._fetch_price_series(stock_code, start_date, end_date, force_refresh)
            else:
                logger.error("❌ 获取价格数据需要 start_date 和 end_date 参数")
                return None
        elif data_type == 'valuation':
            date = kwargs.get('date')
            return self._fetch_valuation_series(stock_code, date)
        elif data_type == 'instruments':
            return self._fetch_instruments_info(stock_code)
        else:
            logger.error(f"❌ 不支持的数据类型: {data_type}")
            return None

    def _validate_dataframe_completeness(self, df: pd.DataFrame, data_type: str) -> bool:
        """
        验证DataFrame数据完整性

        Args:
            df: 要验证的DataFrame
            data_type: 数据类型

        Returns:
            bool: 数据是否完整
        """
        try:
            if df is None or df.empty:
                return False

            if data_type == 'valuation':
                required_fields = ['pe_ratio', 'pb_ratio', 'market_cap']
                # 检查最新一行数据是否包含所有必需字段且不为NaN
                latest_row = df.iloc[-1] if len(df) > 0 else None
                if latest_row is None:
                    return False

                return all(
                    field in latest_row.index and
                    latest_row[field] is not None and
                    not pd.isna(latest_row[field])
                    for field in required_fields
                )

            elif data_type == 'price':
                required_fields = ['open', 'close', 'high', 'low', 'volume']
                # 检查所有行是否包含必需字段且价格大于0
                for _, row in df.iterrows():
                    if not all(
                        field in row.index and
                        row[field] is not None and
                        not pd.isna(row[field]) and
                        (field != 'volume' or row[field] >= 0) and  # volume可以为0
                        (field == 'volume' or row[field] > 0)      # 价格必须大于0
                        for field in required_fields
                    ):
                        return False
                return True

            return False

        except Exception as e:
            logger.error(f"DataFrame数据完整性验证失败: {e}")
            return False

    def _validate_valuation_completeness(self, df: pd.DataFrame) -> bool:
        """
        改进的估值数据完整性验证
        允许部分字段缺失，只要有核心字段即可

        Args:
            df: 估值数据DataFrame

        Returns:
            bool: 数据是否完整
        """
        if df is None or df.empty:
            return False

        # 核心字段 - 至少需要这些字段中的一个
        core_fields = ['pe_ratio', 'pb_ratio', 'market_cap']
        available_core_fields = [field for field in core_fields if field in df.columns]

        if not available_core_fields:
            logger.warning(f"⚠️ 估值数据缺少所有核心字段: {core_fields}")
            return False

        # 检查核心字段是否有有效值
        valid_values = 0
        for field in available_core_fields:
            if not df[field].isna().all():
                valid_values += 1

        # 至少有一个核心字段有有效值
        if valid_values == 0:
            logger.warning(f"⚠️ 估值数据核心字段全部为NaN: {available_core_fields}")
            return False

        logger.debug(f"✅ 估值数据验证通过: 核心字段 {available_core_fields}, 有效值 {valid_values}")
        return True

    def _safe_to_numpy(self, series) -> np.ndarray:
        """
        安全地将pandas Series或ArrayLike转换为numpy数组

        Args:
            series: pandas Series或ArrayLike对象

        Returns:
            numpy数组
        """
        try:
            # 处理不同类型的pandas Series和ArrayLike
            if hasattr(series, 'values'):
                return np.array(series.values, dtype=float)
            elif hasattr(series, 'to_numpy'):
                return series.to_numpy().astype(float)
            else:
                return np.array(series, dtype=float)
        except Exception as e:
            logger.warning(f"转换到numpy数组失败: {e}")
            return np.array([], dtype=float)

import talib

# ============================================================================
# STOCK POOL INDICATOR ENGINE - 技术指标引擎
# ============================================================================

class StockPoolIndicatorEngine:
    """
    StockPool专用技术指标引擎
    - 基于INDICATOR_CONFIG配置驱动
    - 提供统一的指标计算接口
    - 支持所有配置的技术指标
    """

    # 指标计算配置
    INDICATOR_CONFIG = {
        # 移动平均线
        "SMA": {
            "periods": [5, 10, 20, 30, 60],
            "description": "简单移动平均线"
        },
        # 指数移动平均线
        "EMA": {
            "periods": [5, 10, 12, 20, 26, 30, 60],
            "description": "指数移动平均线"
        },
        # RSI指标
        "RSI": {
            "periods": [6, 14, 21],
            "description": "相对强弱指数"
        },
        # MACD指标
        "MACD": {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "description": "MACD指标"
        },
        # 布林带
        "BB": {
            "period": 20,
            "std_dev": 2.0,
            "description": "布林带"
        },
        # ATR指标
        "ATR": {
            "periods": [7, 14, 21],
            "description": "平均真实波幅"
        },
        # 随机指标
        "STOCH": {
            "fastk_period": 14,
            "slowk_period": 3,
            "slowd_period": 3,
            "description": "随机指标"
        },
        # CCI指标
        "CCI": {
            "periods": [14, 20],
            "description": "顺势指标"
        },
        # ROC指标
        "ROC": {
            "periods": [10, 12, 20],
            "description": "变动率指标"
        },
        # TEMA指标
        "TEMA": {
            "periods": [20, 30],
            "description": "三重指数移动平均"
        },
        # WMA指标
        "WMA": {
            "periods": [10, 20, 30],
            "description": "加权移动平均"
        },
        # DMI指标
        "DMI": {
            "period": 14,
            "description": "动向指标"
        },
        # OBV指标
        "OBV": {
            "description": "能量潮指标"
        },
        # 成交量SMA
        "VOLUME_SMA": {
            "periods": [5, 10, 20],
            "description": "成交量简单移动平均"
        },
        # MFI指标
        "MFI": {
            "period": 14,
            "description": "资金流量指标"
        },
        # 威廉指标
        "WILLR": {
            "period": 14,
            "description": "威廉指标"
        },
        # 波动率
        "VOLATILITY": {
            "period": 20,
            "description": "价格波动率"
        }
    }

    def _safe_to_numpy(self, series) -> np.ndarray:
        """
        安全地将pandas Series或ArrayLike转换为numpy数组

        Args:
            series: pandas Series或ArrayLike对象

        Returns:
            numpy数组
        """
        try:
            # 处理不同类型的pandas Series和ArrayLike
            if hasattr(series, 'values'):
                return np.array(series.values, dtype=float)
            elif hasattr(series, 'to_numpy'):
                return series.to_numpy().astype(float)
            else:
                return np.array(series, dtype=float)
        except Exception as e:
            logger.warning(f"转换到numpy数组失败: {e}")
            return np.array([], dtype=float)

    def __init__(self, data_store: StockPoolDataStore):
        """初始化指标引擎"""
        self.data_store = data_store
        self.calculation_stats = {
            'total_calculations': 0,
            'successful_calculations': 0,
            'errors': 0
        }

        # 指标缓存 - 极简设计，与kline_cache保持一致
        self.indicator_cache: Dict[str, pd.DataFrame] = {}  # {order_book_id: indicators_df}

        logger.info("📈 StockPoolIndicatorEngine初始化完成")

    def calculate_all_indicators(self, kline_data: pd.DataFrame, stock_code: Optional[str] = None,
                                force_refresh: bool = False) -> Dict:
        """
        计算股票的所有技术指标（基于 INDICATOR_CONFIG 配置）

        缓存策略:
        - 内存缓存：存储完整的指标DataFrame用于后续计算
        - 缓存键：基于股票代码和数据时间范围生成
        - 支持强制刷新参数
        - 避免重复计算，提高性能

        Args:
            kline_data: K线数据 (包含OHLCV)
            stock_code: 股票代码（用于日志和缓存）
            force_refresh: 是否强制重新计算指标

        Returns:
            Dict: 包含所有指标的字典
        """
        if kline_data is None or kline_data.empty:
            return {}

        try:
            # 记录计算统计
            self.calculation_stats['total_calculations'] += 1

            # 生成缓存键（优化设计：使用order_book_id）
            cache_key = None
            if kline_data is not None and not kline_data.empty and 'order_book_id' in kline_data.columns:
                # 从k线数据中提取规范化的order_book_id作为缓存键
                cache_key = kline_data['order_book_id'].iloc[0]
                logger.debug(f"📊 指标缓存键: {cache_key}")
            elif stock_code:
                # 降级使用传入的stock_code（向后兼容）
                cache_key = stock_code
                logger.debug(f"📊 使用传入的股票代码作为缓存键: {stock_code}")

            # 检查缓存（仅当不是强制刷新且有有效的缓存键时）
            if not force_refresh and cache_key:
                # 首先尝试使用规范化的缓存键查找
                if cache_key in self.indicator_cache:
                    cached_df = self.indicator_cache[cache_key]

                    # 验证缓存数据的正确性
                    if 'order_book_id' in cached_df.columns and cached_df['order_book_id'].iloc[0] == cache_key:
                        logger.debug(f"✅ 指标缓存命中: {cache_key}")
                        # 从缓存的DataFrame重建结果
                        latest_values = {}
                        for col in cached_df.columns:
                            if col != 'order_book_id':  # 排除order_book_id列
                                latest_val = cached_df[col].iloc[-1] if not cached_df[col].isna().all() else None
                                if latest_val is not None and not pd.isna(latest_val):
                                    latest_values[col] = float(latest_val)

                        result = {
                            'indicators_df': cached_df,
                            'latest_values': latest_values,
                            'calculation_stats': self.calculation_stats.copy(),
                            'metadata': {
                                'stock_code': stock_code,
                                'data_points': len(kline_data),
                                'indicators_count': len(cached_df.columns) - 1,  # 排除order_book_id列
                                'calculation_time': datetime.now().isoformat(),
                                'cached': True,
                                'errors': []
                            }
                        }
                        return result
                    else:
                        # 缓存数据不匹配，清除无效缓存
                        del self.indicator_cache[cache_key]
                        logger.debug(f"🧹 清除无效指标缓存: {cache_key}")
                
                # 如果规范化键没找到，尝试查找所有缓存中是否有匹配的order_book_id
                for existing_key, cached_df in self.indicator_cache.items():
                    if ('order_book_id' in cached_df.columns and 
                        cached_df['order_book_id'].iloc[0] == cache_key):
                        logger.debug(f"✅ 指标缓存命中 (通过order_book_id匹配): {cache_key}")
                        # 从缓存的DataFrame重建结果
                        latest_values = {}
                        for col in cached_df.columns:
                            if col != 'order_book_id':  # 排除order_book_id列
                                latest_val = cached_df[col].iloc[-1] if not cached_df[col].isna().all() else None
                                if latest_val is not None and not pd.isna(latest_val):
                                    latest_values[col] = float(latest_val)

                        result = {
                            'indicators_df': cached_df,
                            'latest_values': latest_values,
                            'calculation_stats': self.calculation_stats.copy(),
                            'metadata': {
                                'stock_code': stock_code,
                                'data_points': len(kline_data),
                                'indicators_count': len(cached_df.columns) - 1,  # 排除order_book_id列
                                'calculation_time': datetime.now().isoformat(),
                                'cached': True,
                                'errors': []
                            }
                        }
                        return result

            # 缓存未命中或强制刷新，开始计算
            logger.debug(f"🔄 计算技术指标: {stock_code}")

            # 确保数据格式正确
            if not isinstance(kline_data.index, pd.DatetimeIndex):
                # 处理MultiIndex的情况，提取时间部分作为新索引
                if isinstance(kline_data.index, pd.MultiIndex):
                    kline_data = kline_data.reset_index(level=0, drop=True)
                else:
                    kline_data.index = pd.to_datetime(kline_data.index)

            # 按时间排序
            kline_data = kline_data.sort_index()

            # 提取价格数据
            close_prices = kline_data['close'].values
            high_prices = kline_data['high'].values
            low_prices = kline_data['low'].values
            open_prices = kline_data['open'].values
            volume = kline_data['volume'].values

            # 初始化结果字典
            indicators = {}
            calculation_errors = []

            # 计算所有配置的指标
            for indicator_name, config in self.INDICATOR_CONFIG.items():
                try:
                    if indicator_name == "SMA":
                        for period in config["periods"]:
                            key = f"SMA_{period}"
                            # 转换为numpy数组以兼容talib
                            close_prices_np = self._safe_to_numpy(close_prices)
                            result = talib.SMA(close_prices_np, timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "EMA":
                        for period in config["periods"]:
                            key = f"EMA_{period}"
                            result = talib.EMA(self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "RSI":
                        for period in config["periods"]:
                            key = f"RSI_{period}"
                            result = talib.RSI(self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "MACD":
                        macd, macdsignal, macdhist = talib.MACD(
                            self._safe_to_numpy(close_prices),
                            fastperiod=config["fast_period"],
                            slowperiod=config["slow_period"],
                            signalperiod=config["signal_period"]
                        )
                        # 确保长度匹配
                        for arr, name in [(macd, "MACD"), (macdsignal, "MACD_SIGNAL"), (macdhist, "MACD_HIST")]:
                            if len(arr) != len(close_prices):
                                arr = np.concatenate([np.full(len(close_prices) - len(arr), np.nan), arr])
                            indicators[name] = arr

                    elif indicator_name == "BB":
                        upper, middle, lower = talib.BBANDS(
                            self._safe_to_numpy(close_prices),
                            timeperiod=config["period"],
                            nbdevup=config["std_dev"],
                            nbdevdn=config["std_dev"]
                        )
                        # 确保长度匹配
                        for arr, name in [(upper, "BB_UPPER"), (middle, "BB_MIDDLE"), (lower, "BB_LOWER")]:
                            if len(arr) != len(close_prices):
                                arr = np.concatenate([np.full(len(close_prices) - len(arr), np.nan), arr])
                            indicators[name] = arr

                    elif indicator_name == "ATR":
                        for period in config["periods"]:
                            key = f"ATR_{period}"
                            result = talib.ATR(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "STOCH":
                        slowk, slowd = talib.STOCH(
                            self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices),
                            fastk_period=config["fastk_period"],
                            slowk_period=config["slowk_period"],
                            slowd_period=config["slowd_period"]
                        )
                        # 确保长度匹配
                        for arr, name in [(slowk, "STOCH_SLOWK"), (slowd, "STOCH_SLOWD")]:
                            if len(arr) != len(close_prices):
                                arr = np.concatenate([np.full(len(close_prices) - len(arr), np.nan), arr])
                            indicators[name] = arr

                    elif indicator_name == "CCI":
                        for period in config["periods"]:
                            key = f"CCI_{period}"
                            result = talib.CCI(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "ROC":
                        for period in config["periods"]:
                            key = f"ROC_{period}"
                            result = talib.ROC(self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "TEMA":
                        for period in config["periods"]:
                            key = f"TEMA_{period}"
                            result = talib.TEMA(self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "WMA":
                        for period in config["periods"]:
                            key = f"WMA_{period}"
                            result = talib.WMA(self._safe_to_numpy(close_prices), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "DMI":
                        plus_di = talib.PLUS_DI(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=config["period"])
                        minus_di = talib.MINUS_DI(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=config["period"])
                        adx = talib.ADX(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=config["period"])
                        # 确保长度匹配
                        for arr, name in [(plus_di, "PLUS_DI"), (minus_di, "MINUS_DI"), (adx, "ADX")]:
                            if len(arr) != len(close_prices):
                                arr = np.concatenate([np.full(len(close_prices) - len(arr), np.nan), arr])
                            indicators[name] = arr

                    elif indicator_name == "OBV":
                        result = talib.OBV(self._safe_to_numpy(close_prices), self._safe_to_numpy(volume))
                        # 确保长度匹配
                        if len(result) != len(close_prices):
                            result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                        indicators["OBV"] = result

                    elif indicator_name == "VOLUME_SMA":
                        for period in config["periods"]:
                            key = f"VOLUME_SMA_{period}"
                            result = talib.SMA(self._safe_to_numpy(volume), timeperiod=period)
                            # 确保长度匹配
                            if len(result) != len(close_prices):
                                result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                            indicators[key] = result

                    elif indicator_name == "MFI":
                        result = talib.MFI(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), self._safe_to_numpy(volume), timeperiod=config["period"])
                        # 确保长度匹配
                        if len(result) != len(close_prices):
                            result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                        indicators["MFI"] = result

                    elif indicator_name == "WILLR":
                        result = talib.WILLR(self._safe_to_numpy(high_prices), self._safe_to_numpy(low_prices), self._safe_to_numpy(close_prices), timeperiod=config["period"])
                        # 确保长度匹配
                        if len(result) != len(close_prices):
                            result = np.concatenate([np.full(len(close_prices) - len(result), np.nan), result])
                        indicators["WILLR"] = result

                    elif indicator_name == "VOLATILITY":
                        # 计算价格波动率 (使用收盘价的标准差)
                        returns = np.diff(self._safe_to_numpy(close_prices)) / self._safe_to_numpy(close_prices)[:-1]
                        volatility = pd.Series(returns).rolling(window=config["period"]).std() * np.sqrt(252)  # 年化波动率
                        # 确保长度匹配原始数据
                        volatility_full = np.full(len(close_prices), np.nan)
                        # volatility的长度是 len(close_prices) - config["period"]，因为rolling需要period个数据点
                        if volatility is not None and not volatility.empty:
                            vol_length = len(volatility.dropna())
                            if vol_length > 0:
                                start_idx = config["period"]
                                end_idx = start_idx + vol_length
                                if end_idx <= len(close_prices):
                                    volatility_full[start_idx:end_idx] = volatility.dropna().values
                        indicators["VOLATILITY"] = volatility_full

                except Exception as e:
                    calculation_errors.append(f"{indicator_name}: {str(e)}")
                    logger.warning(f"⚠️ 计算指标失败 {indicator_name}: {e}")

            # 处理计算结果
            if calculation_errors:
                logger.warning(f"⚠️ 指标计算错误: {len(calculation_errors)} 个")
                for error in calculation_errors[:5]:  # 只显示前5个错误
                    logger.warning(f"  - {error}")

            # 转换为DataFrame格式
            result_df = pd.DataFrame(indicators, index=kline_data.index)

            # 添加order_book_id列用于缓存验证（与kline_cache保持一致）
            if 'order_book_id' in kline_data.columns:
                result_df['order_book_id'] = kline_data['order_book_id']
                logger.debug(f"📊 指标DataFrame添加order_book_id列: {kline_data['order_book_id'].iloc[0]}")

            # 处理长度不匹配的问题 - 某些指标可能产生不同长度
            if len(result_df) != len(kline_data):
                logger.debug(f"🔧 对齐指标数据长度: {len(result_df)} -> {len(kline_data)}")
                # 重新索引以匹配原始数据的长度
                result_df = result_df.reindex(kline_data.index)

            # 缓存计算结果（如果有有效的缓存键）
            if cache_key:
                self.indicator_cache[cache_key] = result_df.copy()
                logger.debug(f"💾 指标数据已缓存: {cache_key}")

            # 获取最新值
            latest_values = {}
            for col in result_df.columns:
                if col != 'order_book_id':  # 排除order_book_id列
                    latest_val = result_df[col].iloc[-1] if not result_df[col].isna().all() else None
                    if latest_val is not None and not pd.isna(latest_val):
                        latest_values[col] = float(latest_val)

            # 构建返回结果
            result = {
                'indicators_df': result_df,
                'latest_values': latest_values,
                'calculation_stats': self.calculation_stats.copy(),
                'metadata': {
                    'stock_code': stock_code,
                    'data_points': len(kline_data),
                    'indicators_count': len(result_df.columns) - (1 if 'order_book_id' in result_df.columns else 0),
                    'calculation_time': datetime.now().isoformat(),
                    'cached': False,
                    'errors': calculation_errors
                }
            }

            self.calculation_stats['successful_calculations'] += 1
            logger.debug(f"✅ 指标计算完成: {stock_code}, {len(result_df.columns)}个指标")

            return result

        except Exception as e:
            self.calculation_stats['errors'] += 1
            logger.error(f"❌ 指标计算失败: {e}")
            return {}

    def get_indicator_stats(self) -> Dict:
        """获取指标计算统计信息"""
        stats = self.calculation_stats.copy()
        total = stats['total_calculations']
        if total > 0:
            stats['success_rate'] = stats['successful_calculations'] / total
            stats['error_rate'] = stats['errors'] / total
        else:
            stats['success_rate'] = 0.0
            stats['error_rate'] = 0.0

        # 添加缓存统计信息
        stats['cache_size'] = len(self.indicator_cache)
        stats['cache_keys'] = list(self.indicator_cache.keys())  # 现在是股票代码列表

        return stats

    def clear_indicator_cache(self, stock_code: Optional[str] = None) -> int:
        """
        清除指标缓存

        Args:
            stock_code: 指定股票代码，只清除该股票的缓存；如果为None则清除所有缓存

        Returns:
            int: 清除的缓存条目数量
        """
        try:
            cleared_count = 0

            if stock_code:
                # 只清除指定股票的缓存
                if stock_code in self.indicator_cache:
                    del self.indicator_cache[stock_code]
                    cleared_count += 1
                
                # 查找所有缓存中是否有匹配的order_book_id
                keys_to_remove = []
                for cache_key, cached_df in self.indicator_cache.items():
                    if ('order_book_id' in cached_df.columns and 
                        cached_df['order_book_id'].iloc[0] == stock_code):
                        keys_to_remove.append(cache_key)
                
                for key in keys_to_remove:
                    del self.indicator_cache[key]
                    cleared_count += 1
                    
                if cleared_count == 0:
                    logger.debug(f"ℹ️ 未找到股票{stock_code}的指标缓存")
            else:
                # 清除所有缓存
                cleared_count = len(self.indicator_cache)
                self.indicator_cache.clear()

            if cleared_count > 0:
                logger.info(f"🧹 清除指标缓存: {cleared_count} 条记录")
            else:
                logger.debug("ℹ️ 没有找到需要清除的缓存")

            return cleared_count

        except Exception as e:
            logger.error(f"❌ 清除指标缓存失败: {e}")
            return 0

# ============================================================================
# 数据质量评估器
# ============================================================================

class DataQualityEvaluator:
    """
    Data Quality Evaluator
    """

    # Data quality standards
    QUALITY_STANDARDS = {
        'valuation': {
            'required_fields': ['pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio', 'market_cap'],
            'min_valid_ratio': 0.7,
            'max_nan_ratio': 0.5,
            'value_ranges': {
                'pe_ratio': {'min': -100, 'max': 1000},
                'pb_ratio': {'min': 0, 'max': 50},
                'ps_ratio': {'min': 0, 'max': 100},
                'pcf_ratio': {'min': -100, 'max': 1000},
                'market_cap': {'min': 1e6, 'max': 1e13}
            }
        },
        'technical': {
            'required_indicators': ['RSI_14', 'MACD', 'BB_UPPER', 'BB_LOWER', 'SMA_5', 'SMA_10', 'SMA_20'],
            'min_valid_ratio': 0.6,
            'max_nan_ratio': 0.4,
            'value_ranges': {
                'RSI_14': {'min': 0, 'max': 100},
                'MACD': {'min': -100, 'max': 100},
                'BB_UPPER': {'min': 0, 'max': 1e6},
                'BB_LOWER': {'min': 0, 'max': 1e6},
                'SMA_5': {'min': 0, 'max': 1e6},
                'SMA_10': {'min': 0, 'max': 1e6},
                'SMA_20': {'min': 0, 'max': 1e6}
            }
        },
        'price': {
            'required_fields': ['open', 'close', 'high', 'low', 'volume'],
            'min_valid_ratio': 0.8,
            'max_nan_ratio': 0.2,
            'value_ranges': {
                'open': {'min': 0.01, 'max': 1e6},
                'close': {'min': 0.01, 'max': 1e6},
                'high': {'min': 0.01, 'max': 1e6},
                'low': {'min': 0.01, 'max': 1e6},
                'volume': {'min': 0, 'max': 1e10}
            },
            'price_consistency': True
        }
    }

    def __init__(self, manager):
        self.manager = manager
        self.quality_reports = {}

    def evaluate_data_quality(self, stock_info: Dict, technical_indicators: Dict,
                            data_types: List[str] = None) -> Dict[str, bool]:
        """
        Evaluate data quality
        """
        if data_types is None:
            data_types = ['valuation', 'technical', 'price']

        results = {}
        quality_report = {}

        for data_type in data_types:
            if data_type == 'valuation':
                is_valid, report = self._evaluate_valuation_quality(stock_info)
            elif data_type == 'technical':
                is_valid, report = self._evaluate_technical_quality(technical_indicators)
            elif data_type == 'price':
                is_valid, report = self._evaluate_price_quality(technical_indicators)
            else:
                continue

            results[data_type] = is_valid
            quality_report[data_type] = report

        # Store quality report
        stock_code = stock_info.get('stock_code', 'unknown')
        self.quality_reports[stock_code] = quality_report

        return results

    def _evaluate_valuation_quality(self, stock_info: Dict) -> Tuple[bool, Dict]:
        """
        Evaluate valuation data quality
        """
        standards = self.QUALITY_STANDARDS['valuation']
        report = {
            'total_fields': 0,
            'valid_fields': 0,
            'nan_fields': 0,
            'out_of_range_fields': 0,
            'valid_ratio': 0.0,
            'nan_ratio': 0.0,
            'issues': []
        }

        required_fields = standards['required_fields']
        value_ranges = standards['value_ranges']

        for field in required_fields:
            report['total_fields'] += 1
            value = stock_info.get(field)

            if value is None or pd.isna(value):
                report['nan_fields'] += 1
                report['issues'].append(f"{field}: NaN")
                continue

            # Check value range
            if field in value_ranges:
                min_val = value_ranges[field]['min']
                max_val = value_ranges[field]['max']
                if not (min_val <= value <= max_val):
                    report['out_of_range_fields'] += 1
                    report['issues'].append(f"{field}: value {value} out of range [{min_val}, {max_val}]")
                    continue

            report['valid_fields'] += 1

        # Calculate ratios
        if report['total_fields'] > 0:
            report['valid_ratio'] = report['valid_fields'] / report['total_fields']
            report['nan_ratio'] = report['nan_fields'] / report['total_fields']

        # Quality assessment
        is_valid = (
            report['valid_ratio'] >= standards['min_valid_ratio'] and
            report['nan_ratio'] <= standards['max_nan_ratio'] and
            report['out_of_range_fields'] == 0
        )

        report['overall_quality'] = 'PASS' if is_valid else 'FAIL'

        return is_valid, report

    def _evaluate_technical_quality(self, technical_indicators: Dict) -> Tuple[bool, Dict]:
        """
        Evaluate technical indicators quality
        """
        standards = self.QUALITY_STANDARDS['technical']
        report = {
            'total_indicators': 0,
            'valid_indicators': 0,
            'nan_indicators': 0,
            'out_of_range_indicators': 0,
            'valid_ratio': 0.0,
            'nan_ratio': 0.0,
            'issues': []
        }

        required_indicators = standards['required_indicators']
        value_ranges = standards['value_ranges']

        # Get latest values
        latest_values = technical_indicators.get('latest_values', {})

        for indicator in required_indicators:
            report['total_indicators'] += 1
            value = latest_values.get(indicator)

            if value is None or pd.isna(value):
                report['nan_indicators'] += 1
                report['issues'].append(f"{indicator}: NaN")
                continue

            # Check value range
            if indicator in value_ranges:
                min_val = value_ranges[indicator]['min']
                max_val = value_ranges[indicator]['max']
                if not (min_val <= value <= max_val):
                    report['out_of_range_indicators'] += 1
                    report['issues'].append(f"{indicator}: value {value} out of range [{min_val}, {max_val}]")
                    continue

            report['valid_indicators'] += 1

        # Calculate ratios
        if report['total_indicators'] > 0:
            report['valid_ratio'] = report['valid_indicators'] / report['total_indicators']
            report['nan_ratio'] = report['nan_indicators'] / report['total_indicators']

        # Quality assessment
        is_valid = (
            report['valid_ratio'] >= standards['min_valid_ratio'] and
            report['nan_ratio'] <= standards['max_nan_ratio'] and
            report['out_of_range_indicators'] == 0
        )

        report['overall_quality'] = 'PASS' if is_valid else 'FAIL'

        return is_valid, report

    def _evaluate_price_quality(self, technical_indicators: Dict) -> Tuple[bool, Dict]:
        """
        Evaluate price data quality
        """
        standards = self.QUALITY_STANDARDS['price']
        report = {
            'total_fields': 0,
            'valid_fields': 0,
            'nan_fields': 0,
            'out_of_range_fields': 0,
            'consistency_issues': 0,
            'valid_ratio': 0.0,
            'nan_ratio': 0.0,
            'issues': []
        }

        required_fields = standards['required_fields']
        value_ranges = standards['value_ranges']

        # Get latest values
        latest_values = technical_indicators.get('latest_values', {})

        for field in required_fields:
            report['total_fields'] += 1
            value = latest_values.get(field)

            if value is None or pd.isna(value):
                report['nan_fields'] += 1
                report['issues'].append(f"{field}: NaN")
                continue

            # Check value range
            if field in value_ranges:
                min_val = value_ranges[field]['min']
                max_val = value_ranges[field]['max']
                if not (min_val <= value <= max_val):
                    report['out_of_range_fields'] += 1
                    report['issues'].append(f"{field}: value {value} out of range [{min_val}, {max_val}]")
                    continue

            report['valid_fields'] += 1

        # Check price consistency
        if standards.get('price_consistency', False):
            open_price = latest_values.get('open')
            close_price = latest_values.get('close')
            high_price = latest_values.get('high')
            low_price = latest_values.get('low')

            if all(p is not None and not pd.isna(p) for p in [open_price, close_price, high_price, low_price]):
                # Check price logic
                if not (high_price >= close_price >= low_price):
                    report['consistency_issues'] += 1
                    report['issues'].append("Price logic error: high >= close >= low")

                if not (high_price >= open_price >= low_price):
                    report['consistency_issues'] += 1
                    report['issues'].append("Price logic error: high >= open >= low")

        # Calculate ratios
        if report['total_fields'] > 0:
            report['valid_ratio'] = report['valid_fields'] / report['total_fields']
            report['nan_ratio'] = report['nan_fields'] / report['total_fields']

        # Quality assessment
        is_valid = (
            report['valid_ratio'] >= standards['min_valid_ratio'] and
            report['nan_ratio'] <= standards['max_nan_ratio'] and
            report['out_of_range_fields'] == 0 and
            report['consistency_issues'] == 0
        )

        report['overall_quality'] = 'PASS' if is_valid else 'FAIL'

        return is_valid, report

    def get_quality_report(self, stock_code: str = None) -> Dict:
        """
        Get quality report
        """
        if stock_code:
            return self.quality_reports.get(stock_code, {})
        return self.quality_reports

    def is_data_quality_acceptable(self, stock_info: Dict, technical_indicators: Dict,
                                 data_types: List[str] = None) -> bool:
        """
        Check if data quality is acceptable
        """
        quality_results = self.evaluate_data_quality(stock_info, technical_indicators, data_types)
        return all(quality_results.values())

class ScoringEngine:
    """
    Unified Scoring Engine
    """

    def __init__(self, manager):
        self.manager = manager
        # DataQualityEvaluator will be initialized later
        self.quality_evaluator = None
        self.rules = self._define_rules()

    def _init_quality_evaluator(self, manager):
        """Initialize data quality evaluator"""
        self.quality_evaluator = DataQualityEvaluator(manager)

    def _define_rules(self):
        """Define scoring rules"""
        return {
            'pe': {
                'field': 'pe_ratio',
                'data_source': 'stock_info',
                'ideal_range': (8, 25),
                'ideal_score': 15,
                'good_range': (5, 40),
                'good_score': 8
            },
            'pb': {
                'field': 'pb_ratio',
                'data_source': 'stock_info',
                'ideal_range': (0.5, 3.0),
                'ideal_score': 12,
                'good_range': (0.3, 5.0),
                'good_score': 6
            },
            'rsi': {
                'field': 'RSI_14',
                'data_source': 'technical',
                'ideal_range': (40, 60),
                'ideal_score': 10,
                'good_range': (30, 70),
                'good_score': 5
            },
            'turnover': {
                'field': 'turnover_ratio',
                'data_source': 'stock_info',
                'ideal_range': (1.0, 8.0),
                'ideal_score': 8,
                'good_range': (0.5, 15.0),
                'good_score': 4
            }
        }

    def calculate_score(self, stock_info: Dict, technical_indicators: Dict,
    rule_keys: List[str], layer: str = 'basic') -> float:
        """
        Calculate score based on rules (with data quality assessment)
        """
        try:
            # First perform data quality assessment
            stock_code = stock_info.get('stock_code', 'unknown')

            # Determine data types to check
            data_types_to_check = []
            for rule_key in rule_keys:
                if rule_key in self.rules:
                    rule = self.rules[rule_key]
                    data_source = rule.get('data_source')
                    if data_source not in data_types_to_check:
                        data_types_to_check.append(data_source)

            # Data quality assessment
            quality_results = self.quality_evaluator.evaluate_data_quality(
                stock_info, technical_indicators, data_types_to_check
            )

            # Check if all data types pass
            if not all(quality_results.values()):
                self.manager.logger.warning(f"Data quality failed for {stock_code}, skipping scoring")
                return 0.0

            # Data quality passed, start scoring
            self.manager.logger.debug(f"Data quality passed for {stock_code}, starting scoring")

            total_score = 0
            for rule_key in rule_keys:
                if rule_key not in self.rules:
                    continue

                rule = self.rules[rule_key]
                score = self._apply_rule(rule, stock_info, technical_indicators, layer)
                total_score += score

            final_score = max(0, min(100, total_score))
            self.manager.logger.debug(f"Scoring completed for {stock_code}: {final_score:.1f}")

            return final_score

        except Exception as e:
            stock_code = stock_info.get('stock_code', 'unknown')
            self.manager.logger.error(f"Scoring calculation failed for {stock_code}: {e}")
            return 0.0

    def _apply_rule(self, rule: Dict, stock_info: Dict, technical_indicators: Dict, layer: str) -> float:
        """Apply single scoring rule"""
        try:
            # Get data value
            value = self._get_value_for_rule(rule, stock_info, technical_indicators)
            if value is None or pd.isna(value):
                return 0

            # Range scoring
            return self._calculate_range_score(rule, value, layer)

        except Exception as e:
            self.manager.logger.error(f"Rule application failed for {rule.get('field', 'unknown')}: {e}")
            return 0

    def _get_value_for_rule(self, rule: Dict, stock_info: Dict, technical_indicators: Dict):
        """Get data value for rule"""
        if rule['data_source'] == 'stock_info':
            return stock_info.get(rule['field'])
        elif rule['data_source'] == 'technical':
            return technical_indicators.get('latest_values', {}).get(rule['field'])
        return None

    def _calculate_range_score(self, rule: Dict, value: float, layer: str) -> float:
        """Calculate range score"""
        # Ideal range
        if 'ideal_range' in rule:
            min_val, max_val = rule['ideal_range']
            if min_val <= value <= max_val:
                return rule.get('ideal_score', 10)

        # Good range
        if 'good_range' in rule:
            min_val, max_val = rule['good_range']
            if min_val <= value <= max_val:
                return rule.get('good_score', 5)

        return 0

    def _calculate_common_score_factors(self, stock_info: Dict, technical_indicators: Dict) -> Dict:
        """
        计算通用评分因子
        """
        try:
            factors = {}

            # 估值因子
            pe = stock_info.get('pe_ratio')
            if pe is not None and not pd.isna(pe):
                if 8 <= pe <= 25:
                    factors['pe_score'] = 15
                elif 5 <= pe <= 40:
                    factors['pe_score'] = 8
                else:
                    factors['pe_score'] = 0

            # 技术因子
            rsi = technical_indicators.get('latest_values', {}).get('RSI_14')
            if rsi is not None and not pd.isna(rsi):
                if 40 <= rsi <= 60:
                    factors['rsi_score'] = 10
                elif 30 <= rsi <= 70:
                    factors['rsi_score'] = 5
                else:
                    factors['rsi_score'] = 0

            # 成交量因子
            turnover = stock_info.get('turnover_ratio')
            if turnover is not None and not pd.isna(turnover):
                if 1.0 <= turnover <= 8.0:
                    factors['turnover_score'] = 8
                elif 0.5 <= turnover <= 15.0:
                    factors['turnover_score'] = 4
                else:
                    factors['turnover_score'] = 0

            return factors

        except Exception as e:
            self._log_error(f"通用评分因子计算失败: {e}")
            return {}

# ============================================================================
# 主股票池管理器
# ============================================================================

def main():
    """主启动函数 - 直接启动股票池同步程序"""
    # 初始化环境管理器
    env_manager = EnvironmentManager()
    env_manager.ensure_environment_with_fallback()

    # 初始化日志
    from modules.log_manager import get_system_logger
    logger = get_system_logger()

    logger.info("=" * 60)
    logger.info("直接启动股票池同步程序")
    logger.info("=" * 60)

    # 初始化RQDatac
    from stockpool_tool import init_rqdatac
    if not init_rqdatac():
        logger.error("RQDatac初始化失败")
        return False

    try:
        # 创建PoolManager实例
        pool_manager = PoolManager()

        # 执行每日同步计算建池
        logger.info("开始执行股票池同步...")
        success = pool_manager.process_daily_sync_caculate_buildpool()

        if success:
            logger.info("✓ 股票池同步完成")
            return True
        else:
            logger.error("✗ 股票池同步失败")
            return False

    except Exception as e:
        logger.error(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        return False


def process_daily_sync_caculate_buildpool_optimized() -> bool:
    """运行每日同步、计算和构建股票池的完整流程 - 优化版本，减少数据拷贝"""
    try:
        logger.info("🚀 开始每日股票池同步（优化版本）...")

        # 创建PoolManager实例
        manager = PoolManager()

        # ===== 第一阶段：获取股票列表 =====
        logger.info("📋 第一步：获取A股股票列表...")
        stock_list_df = manager.data_store.fetch_stock_list()

        if stock_list_df is None or stock_list_df.empty:
            logger.error("❌ 无法获取股票列表，同步失败")
            return False

        # 直接使用DataFrame，避免转换为列表
        stock_codes = stock_list_df['order_book_id'].tolist()
        logger.info(f"✅ 获取到 {len(stock_codes)} 只股票")

        # ===== 第二阶段：批量获取估值数据（DataFrame格式） =====
        logger.info("💰 第二步：批量获取估值数据...")
        target_date = manager.data_store.get_target_trading_date()
        if not target_date:
            logger.error("❌ 无法确定目标交易日，同步失败")
            return False
        logger.info(f"🎯 目标分析日期: {target_date}")

        # 获取DataFrame格式的估值数据
        valuation_df = manager._batch_get_valuation_data(stock_codes, target_date, return_dataframe=True)
        if not isinstance(valuation_df, pd.DataFrame) or valuation_df.empty:
            logger.warning("⚠️ 未获取到估值数据")
            valuation_df = pd.DataFrame()

        # ===== 第三阶段：批量获取价格数据 =====
        logger.info("📈 第三步：批量获取价格序列数据...")
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') -
                     timedelta(days=manager.config['history_days'])).strftime('%Y-%m-%d')
        logger.info(f"📅 历史数据范围: {start_date} 至 {target_date}")

        price_data = manager._batch_get_price_data(stock_codes, start_date, target_date)
        valid_price_stocks = [code for code, df in price_data.items() if df is not None and not df.empty]
        logger.info(f"✅ 获取到 {len(valid_price_stocks)} 只股票的价格数据")

        # ===== 第四阶段：直接构建评分DataFrame =====
        logger.info("🔧 第四步：构建评分DataFrame...")

        # 直接创建评分DataFrame，避免中间字典转换
        scored_rows = []

        for stock_code in stock_codes:
            try:
                # 从DataFrame中获取股票信息
                if isinstance(valuation_df, pd.DataFrame) and not valuation_df.empty:
                    stock_info_row = valuation_df[valuation_df['stock_code'] == stock_code]
                    stock_info = stock_info_row.iloc[0].to_dict() if not stock_info_row.empty else {'stock_code': stock_code}
                else:
                    stock_info = {'stock_code': stock_code}

                # 获取价格数据
                price_df = price_data.get(stock_code)

                if price_df is not None and not price_df.empty:
                    # 计算技术指标
                    technical_indicators = manager.calculate_technical_indicators(price_df, stock_code)

                    # 计算评分
                    basic_score = manager.calculate_basic_layer_score(stock_info, technical_indicators)
                    watch_score = manager.calculate_watch_layer_score(stock_info, technical_indicators)
                    core_score = manager.calculate_core_layer_score(stock_info, technical_indicators)

                    # 直接添加到行列表
                    scored_rows.append({
                        'stock_code': stock_code,
                        'basic_score': basic_score,
                        'watch_score': watch_score,
                        'core_score': core_score,
                        'market_cap': stock_info.get('market_cap'),
                        'pe_ratio': stock_info.get('pe_ratio'),
                        'pb_ratio': stock_info.get('pb_ratio'),
                        'current_price': technical_indicators.get('latest_values', {}).get('current_price'),
                        'rsi': technical_indicators.get('latest_values', {}).get('RSI_14'),
                        'turnover_rate': technical_indicators.get('latest_values', {}).get('turnover_rate'),
                        'volatility': technical_indicators.get('latest_values', {}).get('volatility_20d'),
                        'date': target_date
                    })
                else:
                    logger.debug(f"⚠️ 股票 {stock_code} 无价格数据，跳过")

            except Exception as e:
                logger.warning(f"⚠️ 处理股票 {stock_code} 时出错: {e}")
                continue

        # 一次性创建评分DataFrame
        if scored_rows:
            df_scored = pd.DataFrame(scored_rows)
            logger.info(f"✅ 评分计算完成: {len(df_scored)} 只股票")
        else:
            df_scored = pd.DataFrame()
            logger.warning("⚠️ 无有效评分数据")
            return False

        # ===== 第五阶段：构建股票池 =====
        logger.info("🏗️ 第五步：构建三个股票池...")
        result = manager.build_stock_pool(df_scored, target_date)

        if result and all(not df.empty for df in result.values()):
            logger.info("✅ 股票池构建成功")
            return True
        else:
            logger.error("❌ 股票池构建失败")
            return False

    except Exception as e:
        logger.error(f"❌ 每日同步失败: {e}")
        return False


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
