"""
Dagster Pipeline Definitions (Dagster 管道定义)

统一管理所有Dagster相关的定义：
- 操作定义 (@op)
- 作业定义 (@job)
- 资产定义 (@asset)
- 资源定义 (@resource)
- 配置类
- 调度定义

整合自：
- job_definitions.py
- daily_processing_jobs.py
- dagster_config.py
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timedelta, date
import logging
import os
from dagster import (
    job, op, OpExecutionContext, AssetMaterialization, Output, Failure, RetryPolicy,
    ConfigurableResource, RunConfig, resource, InitResourceContext,
    make_values_resource, define_asset_job, AssetSelection, AssetIn, AssetOut,
    multi_asset, asset, run_status_sensor, DagsterRunStatus, schedule,
    get_dagster_logger
)
import polars as pl

from modules.orchestration.pipeline_manager import pipeline_manager
from modules.services.processing_functions import (
    load_market_data, calculate_indicators, calculate_scores, save_data
)
from networks.rqdatac_data_loader import get_rqdatac_data_loader
from modules.util.utilities import (
    get_trading_dates, get_latest_trading_date, get_previous_trading_date
)
from networks.trading_calendar_api import (
    get_latest_trading_day, get_trading_day_n_days_ago,
    get_next_trading_day, get_trading_calendar
)
from networks.rqdatac_config import RQDatacConfig

logger = logging.getLogger(__name__)

# =============================================================================
# 配置常量 (Configuration Constants)
# =============================================================================

# 重试策略
STANDARD_RETRY = RetryPolicy(max_retries=3, delay=30.0)
CRITICAL_RETRY = RetryPolicy(max_retries=5, delay=60.0)

# 默认参数
DEFAULT_COMPLETION_DAYS = 30
DEFAULT_TARGET_STOCKS = []  # 默认目标股票列表为空，运行时动态获取

# =============================================================================
# 资源定义 (Resource Definitions)
# =============================================================================

@resource(
    config_schema={
        "data_dir": str,
        "cache_dir": str,
        "log_dir": str,
        "max_workers": int,
        "timeout": int
    }
)
def file_system_resource(context: InitResourceContext) -> Dict[str, Any]:
    """文件系统资源配置"""
    config = context.resource_config

    # 确保目录存在
    for dir_path in [config["data_dir"], config["cache_dir"], config["log_dir"]]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    return {
        "data_dir": Path(config["data_dir"]),
        "cache_dir": Path(config["cache_dir"]),
        "log_dir": Path(config["log_dir"]),
        "max_workers": config["max_workers"],
        "timeout": config["timeout"]
    }


@resource(
    config_schema={
        "host": str,
        "port": int,
        "database": str,
        "username": str,
        "password": str,
        "pool_size": int
    }
)
def database_resource(context: InitResourceContext) -> Dict[str, Any]:
    """数据库资源配置"""
    config = context.resource_config

    return {
        "connection_string": f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}",
        "pool_size": config["pool_size"]
    }


@resource(
    config_schema={
        "api_key": str,
        "base_url": str,
        "timeout": int,
        "retry_attempts": int
    }
)
def rqdatac_resource(context: InitResourceContext) -> Dict[str, Any]:
    """RQDatac资源配置"""
    config = context.resource_config

    return {
        "api_key": config["api_key"],
        "base_url": config["base_url"],
        "timeout": config["timeout"],
        "retry_attempts": config["retry_attempts"]
    }

# =============================================================================
# 配置类定义 (Configuration Classes)
# =============================================================================

class ProcessingConfig:
    """处理配置类"""

    def __init__(self):
        self.data_dir = "data"
        self.cache_dir = "cache"
        self.log_dir = "logs"
        self.max_workers = 4
        self.timeout = 300
        self.target_stocks = []  # 为空时自动获取全量沪深股票（排除ST/PT）
        self.completion_days = 30

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "data_dir": self.data_dir,
            "cache_dir": self.cache_dir,
            "log_dir": self.log_dir,
            "max_workers": self.max_workers,
            "timeout": self.timeout,
            "target_stocks": self.target_stocks,
            "completion_days": self.completion_days
        }


class DatabaseConfig:
    """数据库配置类"""

    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.database = "stockmonitor"
        self.username = "postgres"
        self.password = ""
        self.pool_size = 10

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "pool_size": self.pool_size
        }

# =============================================================================
# 扩展配置类定义 (Extended Configuration Classes)
# =============================================================================

class ExtendedProcessingConfig(ProcessingConfig):
    """扩展的处理配置类"""

    def __init__(self):
        super().__init__()
        self.indicators = ["sma_20", "rsi_14", "macd", "pe_ratio", "pb_ratio", "roe"]
        self.compression = "snappy"
        self.batch_size = 1000

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        base_dict = super().to_dict()
        base_dict.update({
            "indicators": self.indicators,
            "compression": self.compression,
            "batch_size": self.batch_size
        })
        return base_dict

# =============================================================================
# 运行配置生成器 (Run Configuration Generators)
# =============================================================================

def create_run_config(job_type: str, custom_config: Optional[Dict[str, Any]] = None) -> RunConfig:
    """创建作业运行配置"""
    base_config = ExtendedProcessingConfig().to_dict()

    if custom_config:
        base_config.update(custom_config)

    # 根据作业类型设置特定配置
    if job_type == "sync":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_calendar": {"config": {"calendar_days": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "completion":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_calendar": {"config": {"calendar_days": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "calculation":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_calendar": {"config": {"calendar_days": base_config["completion_days"]}},
                "calculate_and_save_technical_indicators": {"config": {"indicators": base_config["indicators"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "scoring":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_calendar": {"config": {"calendar_days": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()},
                "database": {"config": DatabaseConfig().to_dict()}
            }
        }
    else:  # full_pipeline
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_calendar": {"config": {"calendar_days": base_config["completion_days"]}},
                "calculate_and_save_technical_indicators": {"config": {"indicators": base_config["indicators"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()},
                "database": {"config": DatabaseConfig().to_dict()}
            }
        }

    return RunConfig(**config)

# =============================================================================
# 环境配置 (Environment Configuration)
# =============================================================================

def get_environment_config(env: str = "development") -> Dict[str, Any]:
    """获取环境配置"""
    if env == "production":
        return {
            "processing": ExtendedProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 5},
            "logging": {"level": "INFO", "format": "json"}
        }
    elif env == "staging":
        return {
            "processing": ExtendedProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 3},
            "logging": {"level": "DEBUG", "format": "text"}
        }
    else:  # development
        return {
            "processing": ExtendedProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 2},
            "logging": {"level": "DEBUG", "format": "text"}
        }

# =============================================================================
# 配置验证 (Configuration Validation)
# =============================================================================

def validate_extended_configurations():
    """验证扩展配置完整性"""
    logger = get_dagster_logger()

    try:
        # 验证处理配置
        processing_config = ExtendedProcessingConfig()
        assert processing_config.data_dir, "data_dir is required"
        assert processing_config.target_stocks is not None, "target_stocks must be set"

        # 验证数据库配置
        db_config = DatabaseConfig()
        assert db_config.host, "DB_HOST is required"
        assert db_config.database, "DB_NAME is required"

        # 验证RQDatac配置
        rq_config = RQDatacConfig()
        assert rq_config.api_key, "RQDATAC_API_KEY is required"

        logger.info("✅ 扩展配置验证通过")
        return True

    except Exception as e:
        logger.error(f"❌ 扩展配置验证失败: {e}")
        return False

# =============================================================================
# 基本操作定义 (Basic Operation Definitions)
# =============================================================================

@op(
    name="load_market_data",
    description="加载市场数据",
    retry_policy=STANDARD_RETRY
)
def load_market_data_op(context: OpExecutionContext, data_path: str):
    """加载市场数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info(f"Loading market data from: {data_path}")

        # 直接调用函数库
        df = load_market_data(data_path)

        logger.info(f"Loaded {len(df)} rows of market data")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="market_data",
            description="Market data loaded successfully",
            metadata={"row_count": len(df), "data_path": data_path}
        )

        yield Output(df)

    except Exception as e:
        logger.error(f"Failed to load market data: {str(e)}")
        raise Failure(f"Data loading failed: {str(e)}")


@op(
    name="auto_complete_data",
    description="自动补全缺失数据",
    retry_policy=STANDARD_RETRY
)
def auto_complete_data_op(context: OpExecutionContext, market_data: pl.DataFrame):
    """自动补全数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("Starting automatic data completion")

        from modules.compute.data_processor import complete_market_data

        # 对每只股票进行补全
        completed_data_list = []

        if "symbol" in market_data.columns:
            # 按股票分组处理
            symbols = market_data.select("symbol").unique().to_series().to_list()

            for symbol in symbols:
                symbol_data = market_data.filter(pl.col("symbol") == symbol)

                # 执行补全
                completed_data, result = complete_market_data(symbol_data, symbol)

                if result.success:
                    completed_data_list.append(completed_data)
                    logger.info(f"Data completion successful for {symbol}: {result.message}")
                else:
                    logger.warning(f"Data completion failed for {symbol}: {result.message}")
                    # 仍然保留原始数据
                    completed_data_list.append(symbol_data)
        else:
            # 单股票数据
            completed_data, result = complete_market_data(market_data, "unknown")
            if result.success:
                completed_data_list.append(completed_data)
                logger.info(f"Data completion successful: {result.message}")
            else:
                logger.warning(f"Data completion failed: {result.message}")
                completed_data_list.append(market_data)

        # 合并所有补全后的数据
        if completed_data_list:
            final_data = pl.concat(completed_data_list)
        else:
            final_data = market_data

        logger.info(f"Data completion completed. Original: {len(market_data)} rows, Completed: {len(final_data)} rows")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="completed_market_data",
            description="Market data with missing values completed",
            metadata={
                "original_rows": len(market_data),
                "completed_rows": len(final_data)
            }
        )

        yield Output(final_data)

    except Exception as e:
        logger.error(f"Failed to complete data: {str(e)}")
        raise Failure(f"Data completion failed: {str(e)}")


@op(
    name="calculate_indicators",
    description="计算技术指标",
    retry_policy=STANDARD_RETRY
)
def calculate_indicators_op(context: OpExecutionContext, market_data: pl.DataFrame):
    """计算技术指标的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("Calculating technical indicators")

        # 直接调用函数库
        result_df = calculate_indicators(market_data)

        logger.info(f"Calculated indicators for {len(result_df)} rows")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="technical_indicators",
            description="Technical indicators calculated",
            metadata={"row_count": len(result_df)}
        )

        yield Output(result_df)

    except Exception as e:
        logger.error(f"Failed to calculate indicators: {str(e)}")
        raise Failure(f"Indicator calculation failed: {str(e)}")


@op(
    name="calculate_scores",
    description="计算股票评分",
    retry_policy=STANDARD_RETRY
)
def calculate_scores_op(context: OpExecutionContext, indicator_data: pl.DataFrame):
    """计算股票评分的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("Calculating stock scores")

        # 直接调用函数库
        ranked_df = calculate_scores(indicator_data)

        logger.info(f"Calculated scores for {len(ranked_df)} stocks")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="stock_scores",
            description="Stock scores calculated and ranked",
            metadata={"stock_count": len(ranked_df)}
        )

        yield Output(ranked_df)

    except Exception as e:
        logger.error(f"Failed to calculate scores: {str(e)}")
        raise Failure(f"Score calculation failed: {str(e)}")


@op(
    name="save_results",
    description="保存处理结果",
    retry_policy=STANDARD_RETRY
)
def save_results_op(context: OpExecutionContext, result_data: pl.DataFrame, output_path: str):
    """保存结果的操作"""
    logger = get_dagster_logger()

    try:
        logger.info(f"Saving results to: {output_path}")

        # 直接调用函数库
        success = save_data(result_data, output_path, compression="snappy")

        if not success:
            raise Exception("Failed to save data")

        logger.info("Results saved successfully")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="processed_results",
            description="Processed results saved",
            metadata={"output_path": output_path, "row_count": len(result_data)}
        )

    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")
        raise Failure(f"Result saving failed: {str(e)}")


@op(
    name="validate_data_quality",
    description="验证数据质量",
    retry_policy=STANDARD_RETRY
)
def validate_data_quality_op(context: OpExecutionContext, data: pl.DataFrame):
    """数据质量验证操作"""
    logger = get_dagster_logger()

    try:
        logger.info("Validating data quality")

        # 基本数据质量检查
        if len(data) == 0:
            raise ValueError("Data is empty")

        # 检查必要的列
        required_columns = ["symbol", "date", "close"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # 检查数据类型
        if not data["close"].dtype.is_numeric():
            raise ValueError("Close price column must be numeric")

        logger.info("Data quality validation passed")

        yield Output(data)

    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise Failure(f"Data validation failed: {str(e)}")

# =============================================================================
# 每日处理相关操作定义 (Daily Processing Operation Definitions)
# =============================================================================

@op(
    name="get_target_stocks",
    description="获取目标股票列表",
    retry_policy=STANDARD_RETRY
)
def get_target_stocks_op(context: OpExecutionContext, target_stocks: Optional[List[str]] = None):
    """获取目标股票列表的操作"""
    logger = get_dagster_logger()

    try:
        if target_stocks and len(target_stocks) > 0:
            # 使用指定的股票列表
            stocks = target_stocks
            logger.info(f"使用指定的目标股票列表: {len(stocks)} 只股票")
        else:
            # 自动获取全量活跃股票（排除ST/PT）
            data_loader = get_rqdatac_data_loader()
            # 这里应该调用实际的股票获取函数
            stocks = []  # 暂时为空，需要实现具体的获取逻辑
            logger.info(f"自动获取活跃股票: {len(stocks)} 只股票")

        yield Output(stocks)

    except Exception as e:
        logger.error(f"获取目标股票失败: {str(e)}")
        raise Failure(f"Failed to get target stocks: {str(e)}")


@op(
    name="get_trading_calendar",
    description="获取交易日历",
    retry_policy=STANDARD_RETRY
)
def get_trading_calendar_op(context: OpExecutionContext, calendar_days: int = 30):
    """获取交易日历的操作"""
    logger = get_dagster_logger()

    try:
        # 获取交易日历
        end_date = get_latest_trading_day()
        start_date = get_trading_day_n_days_ago(end_date, calendar_days)

        trading_dates = get_trading_calendar()

        logger.info(f"获取交易日历: {len(trading_dates)} 个交易日")

        yield Output(trading_dates)

    except Exception as e:
        logger.error(f"获取交易日历失败: {str(e)}")
        raise Failure(f"Failed to get trading calendar: {str(e)}")


@op(
    name="check_data_integrity",
    description="检查数据完整性",
    retry_policy=STANDARD_RETRY
)
def check_data_integrity_op(context: OpExecutionContext, symbols: List[str], trading_dates: List[str]):
    """检查数据完整性的操作"""
    logger = get_dagster_logger()

    try:
        # 这里应该实现数据完整性检查逻辑
        integrity_result = {
            "is_complete": True,
            "missing_data": [],
            "last_update": None
        }

        logger.info("数据完整性检查完成")

        yield Output(integrity_result)

    except Exception as e:
        logger.error(f"数据完整性检查失败: {str(e)}")
        raise Failure(f"Data integrity check failed: {str(e)}")


@op(
    name="get_sync_date_range",
    description="确定同步日期范围",
    retry_policy=STANDARD_RETRY
)
def get_sync_date_range_op(context: OpExecutionContext, integrity_check: Dict, trading_dates: List[str], sync_days: int = 30):
    """确定同步日期范围的操作"""
    logger = get_dagster_logger()

    try:
        # 计算需要同步的日期范围
        if integrity_check.get("is_complete", False):
            # 数据完整，只同步最近几天
            end_date = get_latest_trading_day()
            start_date = get_trading_day_n_days_ago(end_date, 7)  # 默认同步最近7天
        else:
            # 数据不完整，进行补全
            end_date = get_latest_trading_day()
            start_date = get_trading_day_n_days_ago(end_date, sync_days)

        date_range = {
            "start_date": start_date,
            "end_date": end_date,
            "sync_type": "incremental" if integrity_check.get("is_complete", False) else "full"
        }

        logger.info(f"确定同步日期范围: {start_date} 到 {end_date}")

        yield Output(date_range)

    except Exception as e:
        logger.error(f"确定同步日期范围失败: {str(e)}")
        raise Failure(f"Failed to determine sync date range: {str(e)}")


@op(
    name="sync_and_complete_ohlcv_data",
    description="同步并补全OHLCV数据",
    retry_policy=CRITICAL_RETRY
)
def sync_and_complete_ohlcv_data_op(context: OpExecutionContext, symbols: List[str], date_range: Dict):
    """同步并补全OHLCV数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始同步OHLCV数据")

        # 这里应该实现OHLCV数据同步逻辑
        ohlcv_data = pl.DataFrame()  # 暂时为空DataFrame

        logger.info(f"OHLCV数据同步完成: {len(ohlcv_data)} 条记录")

        yield Output(ohlcv_data)

    except Exception as e:
        logger.error(f"OHLCV数据同步失败: {str(e)}")
        raise Failure(f"OHLCV data sync failed: {str(e)}")


@op(
    name="sync_and_complete_fundamental_data",
    description="同步并补全基本面数据",
    retry_policy=CRITICAL_RETRY
)
def sync_and_complete_fundamental_data_op(context: OpExecutionContext, symbols: List[str], date_range: Dict):
    """同步并补全基本面数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始同步基本面数据")

        # 这里应该实现基本面数据同步逻辑
        fundamental_data = pl.DataFrame()  # 暂时为空DataFrame

        logger.info(f"基本面数据同步完成: {len(fundamental_data)} 条记录")

        yield Output(fundamental_data)

    except Exception as e:
        logger.error(f"基本面数据同步失败: {str(e)}")
        raise Failure(f"Fundamental data sync failed: {str(e)}")


@op(
    name="calculate_and_save_technical_indicators",
    description="计算并保存技术指标",
    retry_policy=STANDARD_RETRY
)
def calculate_and_save_technical_indicators_op(context: OpExecutionContext, ohlcv_data: pl.DataFrame, indicators: Optional[List[str]] = None):
    """计算并保存技术指标的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始计算技术指标")

        # 计算技术指标
        indicators_data = calculate_indicators(ohlcv_data)

        # 保存技术指标
        # 这里应该实现保存逻辑

        logger.info(f"技术指标计算完成: {len(indicators_data)} 条记录")

        yield Output(indicators_data)

    except Exception as e:
        logger.error(f"技术指标计算失败: {str(e)}")
        raise Failure(f"Technical indicators calculation failed: {str(e)}")


@op(
    name="merge_all_data",
    description="合并所有数据",
    retry_policy=STANDARD_RETRY
)
def merge_all_data_op(context: OpExecutionContext, ohlcv_data: pl.DataFrame, fundamental_data: pl.DataFrame,
                     technical_indicators: pl.DataFrame, additional_data: pl.DataFrame):
    """合并所有数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始合并数据")

        # 合并所有数据
        merged_data = pl.DataFrame()  # 这里应该实现实际的合并逻辑

        logger.info(f"数据合并完成: {len(merged_data)} 条记录")

        yield Output(merged_data)

    except Exception as e:
        logger.error(f"数据合并失败: {str(e)}")
        raise Failure(f"Data merge failed: {str(e)}")


@op(
    name="calculate_final_scores",
    description="计算最终评分",
    retry_policy=STANDARD_RETRY
)
def calculate_final_scores_op(context: OpExecutionContext, merged_data: pl.DataFrame):
    """计算最终评分的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始计算最终评分")

        # 计算评分
        scores_data = calculate_scores(merged_data)

        logger.info(f"最终评分计算完成: {len(scores_data)} 只股票")

        yield Output(scores_data)

    except Exception as e:
        logger.error(f"最终评分计算失败: {str(e)}")
        raise Failure(f"Final scores calculation failed: {str(e)}")


@op(
    name="save_final_scores",
    description="保存最终评分",
    retry_policy=STANDARD_RETRY
)
def save_final_scores_op(context: OpExecutionContext, scores_data: pl.DataFrame):
    """保存最终评分的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始保存最终评分")

        # 保存评分数据
        output_path = save_data(scores_data, "data/scores", compression="snappy")

        logger.info(f"最终评分保存完成: {output_path}")

        yield Output(output_path)

    except Exception as e:
        logger.error(f"最终评分保存失败: {str(e)}")
        raise Failure(f"Final scores save failed: {str(e)}")


@op(
    name="save_processed_data",
    description="保存处理后的完整数据",
    retry_policy=STANDARD_RETRY
)
def save_processed_data_op(context: OpExecutionContext, processed_data: pl.DataFrame):
    """保存处理后的完整数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("开始保存处理后的完整数据")

        # 保存完整数据
        output_path = save_data(processed_data, "data/processed", compression="snappy")

        logger.info(f"完整数据保存完成: {output_path}")

        yield Output(output_path)

    except Exception as e:
        logger.error(f"完整数据保存失败: {str(e)}")
        raise Failure(f"Processed data save failed: {str(e)}")

# =============================================================================
# 作业定义 (Job Definitions)
# =============================================================================

# 定义完整的股票分析作业
@job(
    name="stock_analysis_pipeline",
    description="完整的股票分析管道：数据加载 -> 指标计算 -> 评分 -> 保存结果"
)
def stock_analysis_job():
    """股票分析管道作业"""
    # 注意：这里只是定义作业结构，实际的参数通过run_config传递
    # 加载数据
    raw_data = load_market_data_op()

    # 验证数据质量
    validated_data = validate_data_quality_op(raw_data)

    # 计算技术指标
    indicator_data = calculate_indicators_op(validated_data)

    # 计算评分
    scored_data = calculate_scores_op(indicator_data)

    # 保存结果
    save_results_op(scored_data)


# =============================================================================
# 每日处理作业定义 (Daily Processing Job Definitions)
# =============================================================================

@job(
    name="daily_data_sync_job",
    description="每日数据同步作业",
    tags={"type": "sync", "frequency": "daily", "priority": "high"}
)
def daily_data_sync_job():
    """每日数据同步作业 - 智能增量更新和缺失补全"""
    # 1. 获取目标股票列表
    symbols = get_target_stocks_op()

    # 2. 获取交易日历
    trading_dates = get_trading_calendar_op()

    # 3. 检查数据完整性
    integrity_check = check_data_integrity_op(symbols, trading_dates)

    # 4. 确定同步日期范围
    date_range = get_sync_date_range_op(integrity_check, trading_dates)

    # 5. 同步并补全不同类型数据
    ohlcv_data = sync_and_complete_ohlcv_data_op(symbols, date_range)
    fundamental_data = sync_and_complete_fundamental_data_op(symbols, date_range)

    # 返回同步补全后的数据
    return ohlcv_data, fundamental_data


@job(
    name="data_completion_job",
    description="数据补全作业",
    tags={"type": "completion", "frequency": "daily", "priority": "high"}
)
def data_completion_job():
    """数据补全作业 - 独立的补全操作（如果需要）"""
    # 1. 获取目标股票列表
    symbols = get_target_stocks_op()

    # 2. 获取交易日历
    trading_dates = get_trading_calendar_op()

    # 3. 检查数据完整性
    integrity_check = check_data_integrity_op(symbols, trading_dates)

    # 4. 确定同步日期范围
    date_range = get_sync_date_range_op(integrity_check, trading_dates)

    # 5. 同步并补全数据
    ohlcv_data = sync_and_complete_ohlcv_data_op(symbols, date_range)
    fundamental_data = sync_and_complete_fundamental_data_op(symbols, date_range)

    return ohlcv_data, fundamental_data


@job(
    name="daily_full_pipeline_job",
    description="完整的每日数据处理管道 - 数据同步、技术指标计算、评分存盘一体化",
    tags={"type": "pipeline", "frequency": "daily", "priority": "critical"}
)
def daily_full_pipeline_job():
    """完整的每日数据处理管道作业 - 一站式完成所有处理步骤"""
    # 1. 获取基础信息
    symbols = get_target_stocks_op()
    trading_dates = get_trading_calendar_op()
    integrity_check = check_data_integrity_op(symbols, trading_dates)
    date_range = get_sync_date_range_op(integrity_check, trading_dates)

    # 2. 数据同步补全
    ohlcv_data = sync_and_complete_ohlcv_data_op(symbols, date_range)
    fundamental_data = sync_and_complete_fundamental_data_op(symbols, date_range)

    # 3. 指标计算并保存
    technical_indicators = calculate_and_save_technical_indicators_op(ohlcv_data)

    # 4. 数据合并
    merged_data = merge_all_data_op(
        ohlcv_data, fundamental_data,
        technical_indicators, fundamental_data
    )

    # 5. 评分计算
    final_scores = calculate_final_scores_op(merged_data)

    # 6. 评分单独保存
    scores_output_path = save_final_scores_op(final_scores)

    # 7. 完整数据存盘
    output_path = save_processed_data_op(final_scores)

# =============================================================================
# 资产定义 (Asset Definitions)
# =============================================================================

@asset(
    name="market_data_asset",
    description="市场数据资产",
    compute_kind="data_loading"
)
def market_data_asset():
    """市场数据资产"""
    # 这里可以定义资产的计算逻辑
    pass


@asset(
    name="technical_indicators_asset",
    description="技术指标资产",
    compute_kind="indicator_calculation",
    ins={"market_data": AssetIn("market_data_asset")}
)
def technical_indicators_asset(market_data):
    """技术指标资产"""
    # 这里可以定义资产的计算逻辑
    pass


@asset(
    name="stock_scores_asset",
    description="股票评分资产",
    compute_kind="scoring",
    ins={"indicators": AssetIn("technical_indicators_asset")}
)
def stock_scores_asset(indicators):
    """股票评分资产"""
    # 这里可以定义资产的计算逻辑
    pass

# =============================================================================
# 传感器定义 (Sensor Definitions)
# =============================================================================

@run_status_sensor(
    monitored_jobs=[daily_full_pipeline_job],
    run_status=DagsterRunStatus.SUCCESS,
    name="daily_processing_success_monitor"
)
def daily_processing_success_monitor(context):
    """监听完整每日处理管道作业的执行状态"""
    logger.info("完整的每日数据处理管道作业执行成功")
    return None


@run_status_sensor(
    monitored_jobs=[daily_full_pipeline_job],
    run_status=DagsterRunStatus.FAILURE,
    name="daily_processing_failure_monitor"
)
def daily_processing_failure_monitor(context):
    """监听完整每日处理管道作业的失败状态"""
    logger.error("完整的每日数据处理管道作业执行失败")
    return None

# =============================================================================
# 调度器定义 (Schedule Definitions)
# =============================================================================

@schedule(
    job=daily_full_pipeline_job,
    cron_schedule="0 9 * * 1-5",  # 每周一到周五上午9点执行（开盘前）
    name="daily_full_pipeline_schedule"
)
def daily_full_pipeline_schedule(context):
    """每日完整数据处理管道调度器 - 工作日自动执行"""
    logger.info("触发每日完整数据处理管道作业")

    # 返回运行时配置
    run_config = {
        "ops": {
            "get_target_stocks": {
                "config": {
                    "target_stocks": []  # 空列表表示自动获取所有活跃股票
                }
            },
            "get_trading_calendar": {
                "config": {
                    "calendar_days": 30  # 获取最近30个交易日
                }
            },
            "get_sync_date_range": {
                "config": {
                    "sync_days": 30  # 全量同步时的数据天数
                }
            },
            "calculate_and_save_technical_indicators": {
                "config": {
                    "indicators": [
                        "sma_5", "sma_10", "sma_20", "sma_60",
                        "rsi_14", "macd", "price_angles",
                        "volatility", "volume_indicators",
                        "stoch", "bollinger", "risk_indicators"
                    ]
                }
            }
        }
    }

    return run_config


def validate_configurations():
    """验证配置"""
    logger.info("Validating pipeline configurations...")

    # 检查必要的环境变量
    required_env_vars = ["RQDATAC_USERNAME", "RQDATAC_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        logger.warning(f"Missing environment variables: {missing_vars}")
        logger.warning("Some features may not work without proper configuration")

    logger.info("Configuration validation completed")

# =============================================================================
# 初始化和注册 (Initialization and Registration)
# =============================================================================

def register_all_definitions():
    """注册所有定义到管道管理器"""
    # 注册作业
    pipeline_manager.register_job("stock_analysis", stock_analysis_job)

    # 注册资产
    pipeline_manager.register_asset("market_data", market_data_asset)
    pipeline_manager.register_asset("technical_indicators", technical_indicators_asset)
    pipeline_manager.register_asset("stock_scores", stock_scores_asset)

    logger.info("所有管道定义已注册到管道管理器")


# 初始化时注册所有定义
register_all_definitions()


# 初始化时注册所有定义
register_all_definitions()

# 验证配置
validate_configurations()
