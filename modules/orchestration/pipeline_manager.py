"""
管道管理器 (Pipeline Manager)
负责Dagster管道的配置和管理
"""

from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime, timedelta
import logging
from dagster import job, op, Definitions, OpExecutionContext, AssetMaterialization, Output, Failure, RetryPolicy, RunConfig
from dagster import get_dagster_logger, asset, multi_asset, AssetOut, AssetIn
import polars as pl

from modules.compute.data_processor import DataProcessor
from modules.compute.indicator_calculator import IndicatorCalculator
from modules.compute.stock_scorer import StockScorer
from modules.compute.parallel_processor import ParallelProcessor
from modules.storage.parquet_manager import ParquetManager
from modules.storage.schema_manager import SchemaManager
from modules.query.query_engine import QueryEngine

logger = logging.getLogger(__name__)


class PipelineManager:
    """Dagster管道管理器"""

    def __init__(self):
        self._pipelines: Dict[str, Any] = {}
        self._jobs: Dict[str, Any] = {}
        self._assets: Dict[str, Any] = {}

        # 初始化组件
        self.data_processor = DataProcessor()
        self.indicator_calculator = IndicatorCalculator()
        self.score_calculator = StockScorer()
        self.parallel_processor = ParallelProcessor()
        self.parquet_manager = ParquetManager()
        self.schema_manager = SchemaManager()
        self.query_engine = QueryEngine()

    def register_pipeline(self, name: str, pipeline_def: Any):
        """注册数据管道"""
        self._pipelines[name] = pipeline_def

    def get_pipeline(self, name: str) -> Optional[Any]:
        """获取管道定义"""
        return self._pipelines.get(name)

    def register_job(self, name: str, job_def: Any):
        """注册作业"""
        self._jobs[name] = job_def

    def register_asset(self, name: str, asset_def: Any):
        """注册资产"""
        self._assets[name] = asset_def

    def run_job(self, job_name: str, **kwargs) -> Dict[str, Any]:
        """运行指定的作业"""
        if job_name not in self._jobs:
            raise ValueError(f"Job {job_name} not found")

        try:
            job_def = self._jobs[job_name]
            logger.info(f"🚀 开始执行作业: {job_name}")

            # 使用Dagster的execute_job执行作业
            from dagster import execute_job

            # 准备运行配置
            run_config = RunConfig(
                ops={
                    "get_target_stocks": {
                        "config": {
                            "target_stocks": kwargs.get("target_stocks", [])
                        }
                    },
                    "get_trading_calendar": {
                        "config": {
                            "calendar_days": kwargs.get("calendar_days", 30)
                        }
                    },
                    "get_sync_date_range": {
                        "config": {
                            "sync_days": kwargs.get("sync_days", 30)
                        }
                    }
                }
            )

            # 执行作业
            # 暂时使用简化实现，后续可以集成完整的Dagster执行
            logger.info(f"🔄 模拟执行作业: {job_name} with params: {kwargs}")

            # 模拟执行成功
            return {
                "success": True,
                "message": f"Job {job_name} executed successfully (simulated)",
                "job_name": job_name,
                "params": kwargs
            }

        except Exception as e:
            logger.exception(f"❌ 作业 {job_name} 执行异常: {e}")
            return {
                "success": False,
                "message": f"Job {job_name} execution failed",
                "error": str(e)
            }

    def get_definitions(self) -> Definitions:
        """获取Dagster定义"""
        return Definitions(
            jobs=list(self._jobs.values()),
            assets=list(self._assets.values()),
        )


# 创建全局管道管理器实例
pipeline_manager = PipelineManager()
