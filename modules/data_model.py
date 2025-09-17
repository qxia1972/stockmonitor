"""
数据模型 (Data Model)
集成存储层和查询层的统一数据访问接口
"""

import logging
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
from datetime import datetime, timedelta
import polars as pl

from modules.storage.parquet_manager import ParquetManager
from modules.storage.schema_manager import SchemaManager
from modules.query.query_engine import QueryEngine
from modules.compute.data_processor import DataProcessor
from modules.orchestration.pipeline_manager import pipeline_manager

logger = logging.getLogger(__name__)


class DataModel:
    """数据模型 - 集成存储层和查询层"""

    def __init__(self):
        # 初始化各层组件
        self.parquet_manager = ParquetManager()
        self.schema_manager = SchemaManager()
        self.query_engine = QueryEngine()
        self.data_processor = DataProcessor()

        # 数据缓存
        self._data_cache: Dict[str, pl.DataFrame] = {}
        self._cache_expiry: Dict[str, datetime] = {}

        logger.info("🎯 数据模型初始化完成")

    def load_market_data(self,
                        symbols: Optional[List[str]] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> pl.DataFrame:
        """加载市场数据

        Args:
            symbols: 股票代码列表，如果为None则加载所有
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            市场数据DataFrame
        """
        try:
            # 首先尝试从缓存加载
            cache_key = f"market_data_{symbols}_{start_date}_{end_date}"
            if self._is_cache_valid(cache_key):
                logger.info("📋 从缓存加载市场数据")
                return self._data_cache[cache_key]

            # 从存储层加载数据
            if symbols:
                # 加载指定股票的数据
                data_frames = []
                for symbol in symbols:
                    try:
                        df = self.parquet_manager.load_parquet(f"market_data_{symbol}")
                        if df is not None:
                            data_frames.append(df)
                    except Exception as e:
                        logger.warning(f"⚠️ 加载股票 {symbol} 数据失败: {e}")
                        continue

                if data_frames:
                    market_data = pl.concat(data_frames)
                else:
                    logger.warning("⚠️ 未找到任何股票数据")
                    return pl.DataFrame()
            else:
                # 加载所有市场数据
                market_data = self.parquet_manager.load_parquet("market_data_all")

            # 应用日期过滤
            if market_data is not None and not market_data.is_empty():
                if start_date:
                    market_data = market_data.filter(pl.col("date") >= start_date)
                if end_date:
                    market_data = market_data.filter(pl.col("date") <= end_date)

                # 缓存结果
                self._cache_data(cache_key, market_data)
                logger.info(f"📊 成功加载市场数据: {len(market_data)} 行")
                return market_data
            else:
                logger.warning("⚠️ 未找到市场数据")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"❌ 加载市场数据失败: {e}")
            return pl.DataFrame()

    def save_market_data(self, data: pl.DataFrame, table_name: str = "market_data") -> bool:
        """保存市场数据

        Args:
            data: 要保存的数据
            table_name: 表名

        Returns:
            保存是否成功
        """
        try:
            # 保存到Parquet
            self.parquet_manager.save_parquet(data, table_name)

            # 更新查询引擎
            self.query_engine.load_parquet_table(table_name, f"data/{table_name}.parquet")

            logger.info(f"💾 成功保存市场数据: {table_name}")
            return True

        except Exception as e:
            logger.error(f"❌ 保存市场数据失败: {e}")
            return False

    def load_instruments(self) -> pl.DataFrame:
        """加载股票基本信息"""
        try:
            cache_key = "instruments"
            if self._is_cache_valid(cache_key):
                return self._data_cache[cache_key]

            instruments = self.parquet_manager.load_parquet("instruments")
            if instruments is None:
                logger.warning("⚠️ 未找到股票基本信息")
                return pl.DataFrame()

            self._cache_data(cache_key, instruments)
            logger.info(f"📋 成功加载股票基本信息: {len(instruments)} 只股票")
            return instruments

        except Exception as e:
            logger.error(f"❌ 加载股票基本信息失败: {e}")
            return pl.DataFrame()

    def load_factors(self,
                    factor_names: Optional[List[str]] = None,
                    symbols: Optional[List[str]] = None) -> pl.DataFrame:
        """加载因子数据

        Args:
            factor_names: 因子名称列表
            symbols: 股票代码列表

        Returns:
            因子数据DataFrame
        """
        try:
            cache_key = f"factors_{factor_names}_{symbols}"
            if self._is_cache_valid(cache_key):
                return self._data_cache[cache_key]

            # 从查询引擎获取因子数据
            if factor_names:
                factor_list = ','.join([f"'{f}'" for f in factor_names])
                query = f"SELECT * FROM factors WHERE factor_name IN ({factor_list})"
            else:
                query = "SELECT * FROM factors"

            if symbols:
                symbols_str = ','.join([f"'{s}'" for s in symbols])
                query += f" AND symbol IN ({symbols_str})"

            factors = self.query_engine.execute_query(query)

            if factors is not None and not factors.is_empty():
                self._cache_data(cache_key, factors)
                logger.info(f"📊 成功加载因子数据: {len(factors)} 行")
                return factors
            else:
                logger.warning("⚠️ 未找到因子数据")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"❌ 加载因子数据失败: {e}")
            return pl.DataFrame()

    def execute_query(self, query: str, **params) -> pl.DataFrame:
        """执行SQL查询

        Args:
            query: SQL查询语句
            **params: 查询参数

        Returns:
            查询结果DataFrame
        """
        try:
            result = self.query_engine.execute_query(query, **params)
            if result is not None:
                logger.info(f"🔍 查询执行成功: {len(result)} 行结果")
                return result
            else:
                logger.warning("⚠️ 查询无结果")
                return pl.DataFrame()

        except Exception as e:
            logger.error(f"❌ 查询执行失败: {e}")
            return pl.DataFrame()

    def get_technical_indicators(self,
                               symbols: Optional[List[str]] = None,
                               indicators: Optional[List[str]] = None) -> pl.DataFrame:
        """获取技术指标数据

        Args:
            symbols: 股票代码列表
            indicators: 指标名称列表

        Returns:
            技术指标数据DataFrame
        """
        try:
            # 使用编排层的指标计算功能
            if symbols:
                # 加载股票数据
                market_data = self.load_market_data(symbols)

                if not market_data.is_empty():
                    # 计算技术指标
                    indicators_data = pipeline_manager.indicator_calculator.calculate_indicators(
                        market_data, indicators
                    )

                    if isinstance(indicators_data, dict):
                        # 如果返回字典，合并所有股票的数据
                        all_indicators = []
                        for symbol_data in indicators_data.values():
                            if symbol_data is not None and not symbol_data.is_empty():
                                all_indicators.append(symbol_data)

                        if all_indicators:
                            return pl.concat(all_indicators)
                        else:
                            return pl.DataFrame()
                    else:
                        return indicators_data or pl.DataFrame()
                else:
                    return pl.DataFrame()
            else:
                # 从存储层加载预计算的指标数据
                return self.parquet_manager.load_parquet("technical_indicators") or pl.DataFrame()

        except Exception as e:
            logger.error(f"❌ 获取技术指标失败: {e}")
            return pl.DataFrame()

    def get_stock_scores(self,
                        symbols: Optional[List[str]] = None,
                        score_type: str = "technical") -> pl.DataFrame:
        """获取股票评分

        Args:
            symbols: 股票代码列表
            score_type: 评分类型

        Returns:
            股票评分DataFrame
        """
        try:
            if symbols:
                # 实时计算评分
                indicators_data = self.get_technical_indicators(symbols)
                if not indicators_data.is_empty():
                    scores = pipeline_manager.score_calculator.calculate_technical_score(indicators_data)
                    ranked_scores = pipeline_manager.score_calculator.rank_stocks(scores)
                    return ranked_scores
                else:
                    return pl.DataFrame()
            else:
                # 从存储层加载预计算的评分
                return self.parquet_manager.load_parquet("stock_scores") or pl.DataFrame()

        except Exception as e:
            logger.error(f"❌ 获取股票评分失败: {e}")
            return pl.DataFrame()

    def update_data_cache(self):
        """更新数据缓存"""
        try:
            # 清除过期缓存
            current_time = datetime.now()
            expired_keys = [
                key for key, expiry in self._cache_expiry.items()
                if current_time > expiry
            ]

            for key in expired_keys:
                if key in self._data_cache:
                    del self._data_cache[key]
                del self._cache_expiry[key]

            logger.info(f"🧹 缓存清理完成，清除 {len(expired_keys)} 个过期条目")

        except Exception as e:
            logger.error(f"❌ 更新数据缓存失败: {e}")

    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key in self._cache_expiry:
            return datetime.now() < self._cache_expiry[cache_key]
        return False

    def _cache_data(self, cache_key: str, data: pl.DataFrame, expiry_minutes: int = 30):
        """缓存数据

        Args:
            cache_key: 缓存键
            data: 要缓存的数据
            expiry_minutes: 过期时间（分钟）
        """
        self._data_cache[cache_key] = data
        self._cache_expiry[cache_key] = datetime.now() + timedelta(minutes=expiry_minutes)

    def get_data_quality_report(self) -> Dict[str, Any]:
        """获取数据质量报告"""
        try:
            report = {
                "total_records": 0,
                "data_sources": [],
                "last_update": None,
                "quality_score": 0.0
            }

            # 检查各个数据源
            data_sources = ["market_data", "instruments", "factors", "technical_indicators"]
            total_records = 0

            for source in data_sources:
                try:
                    df = self.parquet_manager.load_parquet(source)
                    if df is not None:
                        record_count = len(df)
                        total_records += record_count
                        report["data_sources"].append({
                            "name": source,
                            "records": record_count,
                            "columns": len(df.columns)
                        })
                except Exception as e:
                    logger.warning(f"⚠️ 检查数据源 {source} 失败: {e}")

            report["total_records"] = total_records

            # 计算质量评分
            if total_records > 0:
                report["quality_score"] = min(100.0, total_records / 10000 * 100)

            return report

        except Exception as e:
            logger.error(f"❌ 获取数据质量报告失败: {e}")
            return {}


# 创建全局实例
data_model = DataModel()
