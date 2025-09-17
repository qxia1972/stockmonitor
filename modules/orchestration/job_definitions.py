"""
数据管道作业定义 (Data Pipeline Job Definitions)
定义具体的Dagster作业和操作
"""

from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import logging
from dagster import job, op, OpExecutionContext, AssetMaterialization, Output, Failure, RetryPolicy
from dagster import get_dagster_logger, asset, multi_asset, AssetOut, AssetIn
import polars as pl

from modules.orchestration.pipeline_manager import pipeline_manager
from modules.processing_functions import load_market_data, calculate_indicators, calculate_scores, save_data

logger = logging.getLogger(__name__)


# 定义重试策略
retry_policy = RetryPolicy(
    max_retries=3,
    delay=1.0,  # 1秒延迟
    backoff=None,  # 使用默认退避策略
    jitter=None
)


@op(
    name="load_market_data",
    description="加载市场数据",
    retry_policy=retry_policy
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
    retry_policy=retry_policy
)
def auto_complete_data_op(context: OpExecutionContext, market_data: pl.DataFrame):
    """自动补全数据的操作"""
    logger = get_dagster_logger()

    try:
        logger.info("Starting automatic data completion")

        from modules.data_completion import complete_market_data

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
    retry_policy=retry_policy
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
    retry_policy=retry_policy
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
    retry_policy=retry_policy
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
    retry_policy=retry_policy
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


# 定义资产
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


# 注册作业到管道管理器
pipeline_manager.register_job("stock_analysis", stock_analysis_job)
pipeline_manager.register_asset("market_data", market_data_asset)
pipeline_manager.register_asset("technical_indicators", technical_indicators_asset)
pipeline_manager.register_asset("stock_scores", stock_scores_asset)
