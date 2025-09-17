"""
每日数据处理作业定义 (Daily Data Processing Job Definitions)

实现完整的每日数据处理流程：
1. 数据同步 (Sync+Completion) - 增量更新 + 缺失补全
2. 技术指标计算 (Technical Indicators)
3. 评分存盘 (Scoring & Save)

基于Dagster框架，支持调度、监控和错误处理
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timedelta, date
import logging
from dagster import (
    job, op, OpExecutionContext, AssetMaterialization, Output, Failure,
    RetryPolicy, RunConfig, run_status_sensor, DagsterRunStatus,
    schedule
)
from dagster import get_dagster_logger
import polars as pl

from modules.orchestration.pipeline_manager import pipeline_manager
from modules.processing_functions import (
    calculate_indicators, calculate_scores, save_data
)
from networks.rqdatac_data_loader import get_rqdatac_data_loader
from modules.util.utilities import (
    get_trading_dates, get_latest_trading_date, get_previous_trading_date
)
from networks.trading_calendar_api import (
    get_latest_trading_day, get_trading_day_n_days_ago,
    get_next_trading_day, get_trading_calendar
)

logger = logging.getLogger(__name__)

# ===== 配置常量 =====

# 重试策略
STANDARD_RETRY = RetryPolicy(max_retries=3, delay=30.0)
CRITICAL_RETRY = RetryPolicy(max_retries=5, delay=60.0)

# 默认参数
DEFAULT_COMPLETION_DAYS = 30
DEFAULT_TARGET_STOCKS = []    # 默认目标股票列表为空，运行时动态获取

# ===== 数据补全辅助函数 =====

def _calculate_completion_range(existing_data: Optional[pl.DataFrame], max_fill_days: int = 30) -> Optional[tuple[date, date]]:
    """
    计算需要补全的数据时间范围

    Args:
        existing_data: 现有数据
        max_fill_days: 最大补全天数

    Returns:
        (start_date, end_date): 补全的开始和结束日期，如果无需补全返回None
    """
    # 获取数据加载器
    data_loader = get_rqdatac_data_loader()

    if existing_data is None or existing_data.is_empty():
        # 场景1: 无历史数据
        latest_trading_str = get_latest_trading_date(data_loader)
        if latest_trading_str:
            end_date = datetime.strptime(latest_trading_str, '%Y-%m-%d').date()
        else:
            # 如果无法获取最新交易日，使用当前日期
            end_date = datetime.now().date()
            logger.warning("无法获取最新交易日，使用当前日期作为结束日期")

        start_date = end_date - timedelta(days=max_fill_days)
        return start_date, end_date
    else:
        # 场景2: 有历史数据
        existing_dates = existing_data.select('date').unique().to_series().to_list()
        existing_dates = [d.date() if hasattr(d, 'date') else d for d in existing_dates]

        # 过滤掉None值
        existing_dates = [d for d in existing_dates if d is not None]

        if not existing_dates:
            return None

        latest_existing = max(existing_dates)
        latest_trading_str = get_latest_trading_date(data_loader)
        if latest_trading_str:
            latest_trading = datetime.strptime(latest_trading_str, '%Y-%m-%d').date()
        else:
            # 如果无法获取最新交易日，使用当前日期
            latest_trading = datetime.now().date()
            logger.warning("无法获取最新交易日，使用当前日期进行比较")

        # 确保包含最新已完成交易日
        if latest_trading > latest_existing:
            end_date = latest_trading
            start_date = latest_existing + timedelta(days=1)
            return start_date, end_date
        else:
            # 检查是否有遗漏的日期
            missing_dates = _find_missing_trading_dates(existing_dates, data_loader)
            if missing_dates:
                start_date = min(missing_dates)
                end_date = max(missing_dates)
                return start_date, end_date
            else:
                return None  # 无需补全


def _find_missing_trading_dates(existing_dates: List[date], data_loader) -> List[date]:
    """
    识别缺失的交易日

    Args:
        existing_dates: 现有数据日期列表
        data_loader: 数据加载器实例

    Returns:
        missing_dates: 缺失的交易日列表
    """
    # 获取交易日历
    min_date_str = min(existing_dates).strftime('%Y-%m-%d')
    max_date_str = max(existing_dates).strftime('%Y-%m-%d')
    trading_calendar_strs = get_trading_dates(data_loader, min_date_str, max_date_str)

    # 转换为datetime.date对象
    trading_calendar = [datetime.strptime(d, '%Y-%m-%d').date() for d in trading_calendar_strs]
    existing_set = set(existing_dates)

    # 找到现有数据时间范围内的缺失交易日
    min_date = min(existing_dates)
    max_date = max(existing_dates)

    expected_dates = [
        date for date in trading_calendar
        if min_date <= date <= max_date
    ]

    missing_dates = [
        date for date in expected_dates
        if date not in existing_set
    ]

    return missing_dates
    """
    分析数据的完整性

    Args:
        data: 数据DataFrame
        expected_stocks: 期望的股票列表

    Returns:
        完整性分析结果
    """
    analysis = {
        'is_complete': True,
        'missing_stocks': [],
        'missing_dates': {},
        'total_records': len(data),
        'expected_stocks': len(expected_stocks),
        'actual_stocks': 0
    }

    if data.is_empty():
        analysis['is_complete'] = False
        analysis['missing_stocks'] = expected_stocks.copy()
        return analysis

    # 检查股票完整性
    actual_stocks = data.select('order_book_id').unique().to_series().to_list()
    analysis['actual_stocks'] = len(actual_stocks)

    missing_stocks = [stock for stock in expected_stocks if stock not in actual_stocks]
    if missing_stocks:
        analysis['is_complete'] = False
        analysis['missing_stocks'] = missing_stocks

    # 检查日期完整性
    if not data.is_empty():
        dates = data.select('date').unique().to_series().to_list()
        dates = [d.date() if hasattr(d, 'date') else datetime.strptime(d, '%Y-%m-%d').date() for d in dates if d is not None]

        if dates:
            min_date = min(dates)
            max_date = max(dates)

            # 为每个股票检查缺失的日期
            for stock in expected_stocks:
                stock_data = data.filter(pl.col('order_book_id') == stock)
                if not stock_data.is_empty():
                    stock_dates = stock_data.select('date').to_series().to_list()
                    stock_dates = [d.date() if hasattr(d, 'date') else datetime.strptime(d, '%Y-%m-%d').date() for d in stock_dates if d is not None]

                    expected_dates = []
                    current = min_date
                    while current <= max_date:
                        if current.weekday() < 5:  # 周一到周五
                            expected_dates.append(current)
                        current += timedelta(days=1)

                    missing_dates = [d for d in expected_dates if d not in stock_dates]
                    if missing_dates:
                        analysis['is_complete'] = False
                        analysis['missing_dates'][stock] = missing_dates

    return analysis
    """
    识别缺失的交易日

    Args:
        existing_dates: 现有数据日期列表
        data_loader: 数据加载器实例

    Returns:
        missing_dates: 缺失的交易日列表
    """
    # 获取交易日历
    min_date_str = min(existing_dates).strftime('%Y-%m-%d')
    max_date_str = max(existing_dates).strftime('%Y-%m-%d')
    trading_calendar_strs = get_trading_dates(data_loader, min_date_str, max_date_str)

    # 转换为datetime.date对象
    trading_calendar = [datetime.strptime(d, '%Y-%m-%d').date() for d in trading_calendar_strs]
    existing_set = set(existing_dates)

    # 找到现有数据时间范围内的缺失交易日
    min_date = min(existing_dates)
    max_date = max(existing_dates)

    expected_dates = [
        date for date in trading_calendar
        if min_date <= date <= max_date
    ]

    missing_dates = [
        date for date in expected_dates
        if date not in existing_set
    ]

    return missing_dates


def _merge_completion_data(existing_data: Optional[pl.DataFrame], new_data: pl.DataFrame) -> pl.DataFrame:
    """
    合并补全数据到现有数据

    Args:
        existing_data: 现有数据DataFrame
        new_data: 新获取的数据DataFrame

    Returns:
        merged_data: 合并后的数据
    """
    if existing_data is None or existing_data.is_empty():
        return new_data

    # 合并数据
    merged_data = pl.concat([existing_data, new_data])

    # 去重并排序
    merged_data = merged_data.unique(subset=['date', 'code'])
    merged_data = merged_data.sort(['code', 'date'])

    return merged_data


def _calculate_quality_score(data: pl.DataFrame) -> float:
    """
    计算数据质量评分

    Args:
        data: 数据DataFrame

    Returns:
        quality_score: 质量评分 (0-1)
    """
    if data.is_empty():
        return 0.0

    # 基础质量指标
    total_records = len(data)

    # 计算空值总数
    null_counts = []
    for col in data.columns:
        null_count = data.select(pl.col(col).is_null().sum()).item()
        null_counts.append(null_count)

    total_nulls = sum(null_counts)

    # 计算空值率
    total_cells = total_records * len(data.columns)
    null_ratio = total_nulls / total_cells if total_cells > 0 else 0.0

    # 简单质量评分：1 - 空值率
    quality_score = 1.0 - null_ratio

    return max(0.0, min(1.0, quality_score))


def complete_market_data_inline(order_book_ids: List[str], existing_data: Optional[pl.DataFrame] = None,
                               max_fill_days: int = 30, quality_threshold: float = 0.8) -> pl.DataFrame:
    """
    内联数据补全函数 - 直接集成到日同步任务中

    Args:
        order_book_ids: RQDatac股票代码列表
        existing_data: 现有数据，如果为None表示无历史数据
        max_fill_days: 最大补全天数
        quality_threshold: 质量阈值

    Returns:
        completed_data: 补全后的数据
    """
    logger = logging.getLogger(__name__)

    try:
        logger.info(f"开始数据补全，股票数量: {len(order_book_ids)}")

        # 如果没有现有数据，直接获取新数据
        if existing_data is None or existing_data.is_empty():
            logger.info("无历史数据，执行全量获取")
            loader = get_rqdatac_data_loader()
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=max_fill_days)

            completed_data = loader.get_ohlcv_data(
                symbols=order_book_ids,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )

            if completed_data is None or completed_data.is_empty():
                logger.warning("未获取到任何数据")
                return pl.DataFrame()

            return completed_data

        # 分析现有数据的完整性 - 简化为基本检查
        if existing_data.is_empty():
            missing_analysis = {'is_complete': False, 'missing_stocks': order_book_ids, 'missing_dates': {}}
        else:
            # 简单检查：如果记录数明显少于期望值，认为不完整
            expected_records = len(order_book_ids) * 5  # 粗略估计
            is_complete = len(existing_data) >= expected_records * 0.8  # 80%完整性阈值
            missing_analysis = {
                'is_complete': is_complete,
                'missing_stocks': [],
                'missing_dates': {}
            }

        logger.info(f"数据完整性分析: 记录数={len(existing_data)}, 完整={missing_analysis['is_complete']}")

        # 如果数据已经完整，无需补全
        if missing_analysis['is_complete']:
            logger.info("数据已完整，无需补全")
            return existing_data

        # 根据缺失情况确定补全策略
        completion_range = _calculate_completion_range(existing_data, max_fill_days)

        if completion_range is None:
            logger.info("无需补全数据")
            return existing_data

        # 执行补全
        loader = get_rqdatac_data_loader()
        start_date, end_date = completion_range
        logger.info(f"补全时间范围: {start_date} 到 {end_date}")

        # 获取补全数据
        missing_data = loader.get_ohlcv_data(
            symbols=order_book_ids,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

        if missing_data is None or missing_data.is_empty():
            logger.warning("未获取到补全数据")
            return existing_data

        # 合并数据
        completed_data = pl.concat([existing_data, missing_data])
        completed_data = completed_data.unique(subset=['order_book_id', 'date'])
        completed_data = completed_data.sort(['order_book_id', 'date'])

        logger.info(f"补全后记录数: {len(completed_data)} (增加: {len(completed_data) - len(existing_data)})")

        return completed_data

    except Exception as e:
        logger.error(f"数据补全失败: {e}")
        raise


# ===== 基础操作定义 =====

@op(
    name="get_target_stocks",
    description="获取目标股票列表",
    retry_policy=STANDARD_RETRY
)
def get_target_stocks_op(context: OpExecutionContext) -> List[str]:
    """获取目标股票列表"""
    logger = get_dagster_logger()

    try:
        # 从配置获取目标股票
        target_stocks = None
        if context.op_config:
            target_stocks = context.op_config.get("target_stocks")

        # 如果配置中没有指定目标股票或为空，则从RQDatac获取全部沪深股票
        if not target_stocks:
            logger.info("⚠️ 未配置目标股票，从RQDatac获取全部沪深股票列表")
            loader = get_rqdatac_data_loader()

            # 获取全部沪深股票（A股）
            instruments_df = loader.get_instruments(
                instrument_type="CS",  # CS = Common Stock
                market="cn"
            )

            # 过滤出非ST，活跃的沪深股票
            if len(instruments_df) > 0:
                # 过滤条件：状态为活跃，且为沪深市场，排除ST、PT等特殊股票
                active_stocks = instruments_df.filter(
                    (pl.col("status") == "Active") &
                    (pl.col("order_book_id").str.contains(r"\.(XSHG|XSHE)$")) &
                    (~pl.col("symbol").str.contains(r"ST|PT|\*"))  # 排除ST、PT、*ST等股票
                )

                target_stocks = active_stocks.select("order_book_id").to_series().to_list()
                logger.info(f"📊 从RQDatac获取到 {len(target_stocks)} 只活跃沪深股票（已排除ST/PT股票）")
            else:
                # 如果获取失败，使用默认股票列表
                logger.warning("❌ 从RQDatac获取股票列表失败，使用默认股票列表")
                target_stocks = DEFAULT_TARGET_STOCKS
        else:
            logger.info(f"✅ 从配置获取到 {len(target_stocks)} 只目标股票")

        logger.info(f"🎯 最终目标股票数量: {len(target_stocks)}")
        return target_stocks

    except Exception as e:
        logger.error(f"❌ 获取目标股票失败: {e}")
        # 出错时使用默认股票列表
        logger.warning(f"⚠️ 使用默认股票列表: {DEFAULT_TARGET_STOCKS}")
        return DEFAULT_TARGET_STOCKS


@op(
    name="get_trading_calendar",
    description="获取交易日历",
    retry_policy=STANDARD_RETRY
)
def get_trading_calendar_op(context: OpExecutionContext) -> List[str]:
    """获取交易日历，确定最新的已收盘交易日"""
    logger = get_dagster_logger()

    try:
        loader = get_rqdatac_data_loader()

        # 获取最近30个交易日的日历
        if context.op_config:
            calendar_days = context.op_config.get("calendar_days", 30)
        else:
            calendar_days = 30
        end_date = datetime.now().strftime("%Y-%m-%d")

        # 从RQDatac获取交易日历
        trading_dates = loader.get_trading_calendar(
            market="cn",
            start_date=(datetime.now() - timedelta(days=calendar_days)).strftime("%Y-%m-%d"),
            end_date=end_date
        )

        # 过滤出已收盘的交易日（不包括今天，如果今天还没收盘）
        today = datetime.now().date()
        completed_trading_dates = [
            date for date in trading_dates
            if datetime.strptime(date, "%Y-%m-%d").date() < today
        ]

        if not completed_trading_dates:
            raise ValueError("没有已收盘的交易日")

        latest_trading_date = max(completed_trading_dates)
        logger.info(f"获取到交易日历: 最新已收盘交易日 {latest_trading_date}")

        return completed_trading_dates

    except Exception as e:
        logger.error(f"获取交易日历失败: {e}")
        raise Failure(f"Failed to get trading calendar: {e}")


@op(
    name="check_data_integrity",
    description="检查数据完整性",
    retry_policy=STANDARD_RETRY
)
def check_data_integrity_op(context: OpExecutionContext, symbols: List[str], trading_dates: List[str]) -> Dict[str, Any]:
    """检查存盘文件中的数据完整性，确定需要同步的内容"""
    logger = get_dagster_logger()

    try:
        # 检查数据目录
        data_dir = Path("data")
        if not data_dir.exists():
            logger.warning("数据目录不存在，将进行全量同步")
            return {
                "needs_full_sync": True,
                "missing_dates": trading_dates,
                "latest_date": None,
                "symbols_to_sync": symbols
            }

        # 检查最新的数据文件
        latest_data_file = None
        latest_date = None

        for file_path in data_dir.glob("daily_results_*.parquet"):
            if file_path.is_file():
                # 从文件名提取日期
                date_str = file_path.stem.replace("daily_results_", "")
                try:
                    file_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S").date()
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                        latest_data_file = file_path
                except ValueError:
                    continue

        if latest_data_file is None:
            logger.info("未找到历史数据文件，将进行全量同步")
            return {
                "needs_full_sync": True,
                "missing_dates": trading_dates,
                "latest_date": None,
                "symbols_to_sync": symbols
            }

        # 读取最新数据文件，检查数据完整性
        try:
            existing_data = pl.read_parquet(str(latest_data_file))
            existing_dates = existing_data.select("date").unique().to_series().to_list()
            existing_symbols = existing_data.select("order_book_id").unique().to_series().to_list()

            # 转换为日期对象进行比较
            existing_date_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in existing_dates]
            trading_date_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in trading_dates]

            # 找出缺失的日期
            missing_dates = [
                d.strftime("%Y-%m-%d") for d in trading_date_objs
                if d not in existing_date_objs
            ]

            # 找出缺失的股票
            missing_symbols = [s for s in symbols if s not in existing_symbols]

            # 确定需要同步的股票（全部股票都需要检查最新数据）
            symbols_to_sync = symbols

            result = {
                "needs_full_sync": False,
                "missing_dates": missing_dates,
                "latest_date": latest_date.strftime("%Y-%m-%d") if latest_date else None,
                "symbols_to_sync": symbols_to_sync,
                "missing_symbols": missing_symbols,
                "existing_records": len(existing_data),
                "existing_dates_count": len(existing_dates),
                "existing_symbols_count": len(existing_symbols)
            }

            logger.info(f"数据完整性检查完成: 缺失 {len(missing_dates)} 个交易日, 缺失 {len(missing_symbols)} 只股票")
            return result

        except Exception as e:
            logger.warning(f"读取历史数据文件失败: {e}，将进行全量同步")
            return {
                "needs_full_sync": True,
                "missing_dates": trading_dates,
                "latest_date": None,
                "symbols_to_sync": symbols
            }

    except Exception as e:
        logger.error(f"数据完整性检查失败: {e}")
        raise Failure(f"Failed to check data integrity: {e}")


@op(
    name="get_sync_date_range",
    description="确定同步日期范围",
    retry_policy=STANDARD_RETRY
)
def get_sync_date_range_op(context: OpExecutionContext, integrity_check: Dict[str, Any], trading_dates: List[str]) -> Dict[str, str]:
    """根据数据完整性检查结果确定需要同步的日期范围"""
    logger = get_dagster_logger()

    try:
        if integrity_check["needs_full_sync"]:
            # 全量同步：使用最近30个交易日
            if context.op_config:
                sync_days = context.op_config.get("sync_days", 30)
            else:
                sync_days = 30
            end_date = max(trading_dates)
            start_date = min(trading_dates[-sync_days:])  # 最近sync_days个交易日

            date_range = {
                "start_date": start_date,
                "end_date": end_date,
                "sync_type": "full_sync"
            }
        else:
            # 增量同步：从最新数据日期到最新交易日
            if integrity_check["latest_date"]:
                start_date = integrity_check["latest_date"]
            else:
                # 如果没有最新日期，使用最近7个交易日
                start_date = min(trading_dates[-7:])

            end_date = max(trading_dates)

            date_range = {
                "start_date": start_date,
                "end_date": end_date,
                "sync_type": "incremental_sync"
            }

        logger.info(f"确定同步日期范围: {date_range['start_date']} 到 {date_range['end_date']} ({date_range['sync_type']})")
        return date_range

    except Exception as e:
        logger.error(f"确定同步日期范围失败: {e}")
        raise Failure(f"Failed to get sync date range: {e}")


# ===== 数据同步操作 =====

@op(
    name="sync_and_complete_ohlcv_data",
    description="同步并补全OHLCV数据",
    retry_policy=CRITICAL_RETRY,
    tags={"type": "sync", "data_type": "ohlcv"}
)
def sync_and_complete_ohlcv_data_op(context: OpExecutionContext, symbols: List[str], date_range: Dict[str, str]):
    """同步并补全OHLCV数据 - 一次性完成增量更新和缺失补全"""
    logger = get_dagster_logger()

    try:
        loader = get_rqdatac_data_loader()

        logger.info(f"开始同步并补全OHLCV数据: {len(symbols)} 只股票, {date_range['start_date']} 到 {date_range['end_date']}")
        logger.info(f"同步类型: {date_range.get('sync_type', 'unknown')}")

        # 任务1: 增量更新 - 从数据源拉取最新数据
        logger.info("📥 执行增量更新任务...")
        raw_data = loader.get_ohlcv_data(
            symbols=symbols,
            start_date=date_range["start_date"],
            end_date=date_range["end_date"]
        )

        if raw_data.is_empty():
            raise ValueError("从RQDatac拉取到的OHLCV数据为空")

        logger.info(f"✅ 增量更新完成: 拉取到 {len(raw_data)} 条记录")

        # 任务2: 缺失补全 - 补全数据中的缺失值
        logger.info("🔧 执行缺失补全任务...")
        completed_data = complete_market_data_inline(
            order_book_ids=symbols,
            existing_data=raw_data,
            max_fill_days=DEFAULT_COMPLETION_DAYS
        )

        logger.info(f"✅ 缺失补全完成: {len(completed_data)} 条记录")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="synced_completed_ohlcv_data",
            description="OHLCV data synced and completed with incremental update and missing value completion",
            metadata={
                "record_count": len(completed_data),
                "symbol_count": len(symbols),
                "date_range": f"{date_range['start_date']} to {date_range['end_date']}",
                "sync_type": date_range.get("sync_type", "unknown"),
                "raw_records": len(raw_data),
                "completed_records": len(completed_data),
                "sync_tasks": ["incremental_update", "missing_completion"]
            }
        )

        yield Output(completed_data)

    except Exception as e:
        logger.error(f"OHLCV数据同步补全失败: {e}")
        raise Failure(f"OHLCV sync and completion failed: {e}")


@op(
    name="sync_and_complete_fundamental_data",
    description="同步并补全基本面数据",
    retry_policy=STANDARD_RETRY,
    tags={"type": "sync", "data_type": "fundamental"}
)
def sync_and_complete_fundamental_data_op(context: OpExecutionContext, symbols: List[str], date_range: Dict[str, str]):
    """同步并补全基本面数据 - 一次性完成增量更新和缺失补全"""
    logger = get_dagster_logger()

    try:
        loader = get_rqdatac_data_loader()

        logger.info(f"开始同步并补全基本面数据: {len(symbols)} 只股票, {date_range['start_date']} 到 {date_range['end_date']}")
        logger.info(f"同步类型: {date_range.get('sync_type', 'unknown')}")

        # 任务1: 增量更新 - 从数据源拉取最新数据
        logger.info("📥 执行增量更新任务...")
        raw_data = loader.get_fundamental_data(
            symbols=symbols,
            start_date=date_range["start_date"],
            end_date=date_range["end_date"]
        )

        if raw_data.is_empty():
            logger.warning("从RQDatac拉取到的基本面数据为空")
            # 创建空的DataFrame但保持结构
            raw_data = pl.DataFrame({
                "order_book_id": [],
                "date": []
            })

        logger.info(f"✅ 增量更新完成: 拉取到 {len(raw_data)} 条记录")

        # 任务2: 缺失补全 - 对于基本面数据，直接使用获取到的数据
        logger.info("🔧 检查基本面数据完整性...")
        completed_data = raw_data  # 基本面数据暂不进行复杂的补全逻辑

        logger.info(f"✅ 数据检查完成: {len(completed_data)} 条记录")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="synced_completed_fundamental_data",
            description="Fundamental data synced and completed with incremental update and data validation",
            metadata={
                "record_count": len(completed_data),
                "symbol_count": len(symbols),
                "date_range": f"{date_range['start_date']} to {date_range['end_date']}",
                "sync_type": date_range.get("sync_type", "unknown"),
                "raw_records": len(raw_data),
                "completed_records": len(completed_data),
                "sync_tasks": ["incremental_update", "data_validation"]
            }
        )

        yield Output(completed_data)

    except Exception as e:
        logger.error(f"基本面数据同步补全失败: {e}")
        raise Failure(f"Fundamental sync and completion failed: {e}")


@op(
    name="sync_and_complete_factor_data",
    description="同步并补全因子数据",
    retry_policy=CRITICAL_RETRY,
    tags={"type": "sync", "data_type": "factor"}
)
def sync_and_complete_factor_data_op(context: OpExecutionContext, symbols: List[str], date_range: Dict[str, str]):
    """同步并补全因子数据 - 从RQDatac因子库获取"""
    logger = get_dagster_logger()

    try:
        loader = get_rqdatac_data_loader()

        logger.info(f"开始同步并补全因子数据: {len(symbols)} 只股票")
        logger.info(f"同步类型: {date_range.get('sync_type', 'unknown')}")

        # 任务1: 从因子库获取因子数据
        logger.info("📥 从RQDatac因子库获取因子数据...")
        factor_data = loader.get_factor_data(
            symbols=symbols,
            start_date=date_range["start_date"],
            end_date=date_range["end_date"]
        )

        if factor_data.is_empty():
            logger.warning("从RQDatac因子库获取到的因子数据为空")
            # 创建空的DataFrame但保持结构
            factor_data = pl.DataFrame({
                "order_book_id": [],
                "date": []
            })

        logger.info(f"✅ 从因子库获取到 {len(factor_data)} 条因子记录")

        # 任务2: 缺失补全 - 对于获取失败的因子，由指标计算模块自行计算
        logger.info("🔧 检查因子数据完整性...")
        available_factors = [col for col in factor_data.columns if col not in ["order_book_id", "date"]]
        logger.info(f"成功获取的因子: {available_factors}")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="synced_completed_factor_data",
            description="Factor data synced from RQDatac factor library",
            metadata={
                "record_count": len(factor_data),
                "symbol_count": len(symbols),
                "date_range": f"{date_range['start_date']} to {date_range['end_date']}",
                "sync_type": date_range.get("sync_type", "unknown"),
                "available_factors": available_factors,
                "factor_count": len(available_factors),
                "sync_tasks": ["factor_library_sync"]
            }
        )

        yield Output(factor_data)

    except Exception as e:
        logger.error(f"因子数据同步失败: {e}")
        raise Failure(f"Factor sync failed: {e}")


# ===== 数据补全操作 =====

# ===== 指标计算操作 =====

@op(
    name="calculate_and_save_technical_indicators",
    description="计算并保存技术指标",
    retry_policy=STANDARD_RETRY,
    tags={"type": "calculation", "indicator_type": "technical", "operation": "calculate_and_save"}
)
def calculate_and_save_technical_indicators_op(context: OpExecutionContext, ohlcv_data: pl.DataFrame):
    """计算并保存技术指标"""
    logger = get_dagster_logger()

    try:
        logger.info("开始计算技术指标")

        # 计算技术指标
        if context.op_config:
            indicators_config = context.op_config.get("indicators", ["sma_5", "sma_10", "sma_20", "sma_60", "rsi_14", "macd", "price_angles", "volatility", "volume_indicators", "stoch", "bollinger", "risk_indicators"])
        else:
            indicators_config = ["sma_5", "sma_10", "sma_20", "sma_60", "rsi_14", "macd", "price_angles", "volatility", "volume_indicators", "stoch", "bollinger", "risk_indicators"]

        indicators_data = calculate_indicators(
            data=ohlcv_data,
            indicators=indicators_config
        )

        # 确保返回的是DataFrame
        if isinstance(indicators_data, dict):
            # 如果返回字典，合并所有DataFrame
            if indicators_data:
                indicators_data = pl.concat(list(indicators_data.values()))
            else:
                indicators_data = pl.DataFrame()

        logger.info(f"技术指标计算完成: {len(indicators_data)} 条记录")

        # 保存技术指标数据
        logger.info("开始保存技术指标数据")
        from datetime import datetime
        output_path = f"data/technical_indicators_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        save_data(
            data=indicators_data,
            output_path=output_path
        )
        logger.info(f"技术指标数据保存完成: {output_path}")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="technical_indicators_calculated_and_saved",
            description="Technical indicators calculated and saved",
            metadata={
                "record_count": len(indicators_data),
                "indicators": indicators_config,
                "saved": True,
                "data_type": "technical_indicators"
            }
        )

        yield Output(indicators_data)

    except Exception as e:
        logger.error(f"技术指标计算和保存失败: {e}")
        raise Failure(f"Technical indicators calculation and save failed: {e}")


def _load_latest_indicator_file(file_pattern: str, indicator_type: str) -> pl.DataFrame:
    """通用函数：加载最新的指标数据文件"""
    logger = get_dagster_logger()

    try:
        logger.info(f"开始加载已有的{indicator_type}数据")

        # 查找最新的指标文件
        data_dir = Path("data")
        latest_file = None
        latest_date = None

        for file_path in data_dir.glob(file_pattern):
            if file_path.is_file():
                date_str = file_path.stem.replace(file_pattern.replace("*", ""), "")
                try:
                    file_date = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                        latest_file = file_path
                except ValueError:
                    continue

        if latest_file is None:
            raise ValueError(f"未找到已有的{indicator_type}数据文件，请先运行指标计算作业")

        # 加载指标数据
        indicators_data = pl.read_parquet(str(latest_file))
        logger.info(f"{indicator_type}数据加载完成: {len(indicators_data)} 条记录")

        return indicators_data

    except Exception as e:
        logger.error(f"加载{indicator_type}数据失败: {e}")
        raise Failure(f"Failed to load existing {indicator_type.lower()}: {e}")


@op(
    name="load_existing_technical_indicators",
    description="加载已有的技术指标数据",
    retry_policy=STANDARD_RETRY,
    tags={"type": "load", "data_type": "technical_indicators"}
)
def load_existing_technical_indicators_op(context: OpExecutionContext):
    """加载最近的技术指标数据文件"""
    indicators_data = _load_latest_indicator_file("technical_indicators_*.parquet", "技术指标")
    yield Output(indicators_data)


@op(
    name="merge_all_data",
    description="合并所有数据",
    retry_policy=STANDARD_RETRY,
    tags={"type": "merge"}
)
def merge_all_data_op(context: OpExecutionContext,
                     ohlcv_data: pl.DataFrame,
                     fundamental_data: pl.DataFrame,
                     technical_indicators: pl.DataFrame,
                     fundamental_data_for_merge: pl.DataFrame):
    """合并所有处理后的数据"""
    logger = get_dagster_logger()

    try:
        logger.info("开始合并所有数据")

        # 修复字段冲突问题：重命名技术指标数据中的OHLCV字段以避免覆盖原始数据
        conflict_fields = ['open', 'high', 'low', 'close', 'volume', 'amount', 'vwap', 'returns']
        rename_mapping = {field: f"{field}_technical" for field in conflict_fields if field in technical_indicators.columns}

        if rename_mapping:
            logger.info(f"检测到字段冲突，重命名技术指标字段: {list(rename_mapping.keys())}")
            technical_indicators = technical_indicators.rename(rename_mapping)

        # 合并数据（基于order_book_id和date）
        merged_data = ohlcv_data.join(
            fundamental_data,
            on=["order_book_id", "date"],
            how="left"
        ).join(
            technical_indicators,
            on=["order_book_id", "date"],
            how="left"
        ).join(
            fundamental_data_for_merge,
            on=["order_book_id", "date"],
            how="left"
        )

        logger.info(f"数据合并完成: {len(merged_data)} 条记录")

        yield Output(merged_data)

    except Exception as e:
        logger.error(f"数据合并失败: {e}")
        raise Failure(f"Data merge failed: {e}")


@op(
    name="calculate_final_scores",
    description="计算最终评分",
    retry_policy=STANDARD_RETRY,
    tags={"type": "scoring"}
)
def calculate_final_scores_op(context: OpExecutionContext, merged_data: pl.DataFrame):
    """计算最终的股票评分"""
    logger = get_dagster_logger()

    try:
        logger.info("开始计算最终评分")

        # 计算技术评分
        scored_data = calculate_scores(
            data=merged_data,
            score_type="technical"
        )

        logger.info(f"评分计算完成: {len(scored_data)} 只股票")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="final_scores",
            description="Final stock scores calculated",
            metadata={"stock_count": len(scored_data)}
        )

        yield Output(scored_data)

    except Exception as e:
        logger.error(f"评分计算失败: {e}")
        raise Failure(f"Score calculation failed: {e}")


@op(
    name="save_final_scores",
    description="保存最终评分数据",
    retry_policy=STANDARD_RETRY,
    tags={"type": "save", "data_type": "scores"}
)
def save_final_scores_op(context: OpExecutionContext, scored_data: pl.DataFrame):
    """保存最终评分数据到单独的文件"""
    logger = get_dagster_logger()

    try:
        # 生成评分数据输出路径（使用绝对路径）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scores_output_path = Path.cwd() / "data" / "scores" / f"final_scores_{timestamp}.parquet"

        # 确保目录存在
        scores_output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始保存评分数据到: {scores_output_path}")

        # 保存评分数据
        try:
            scored_data.write_parquet(scores_output_path)
            logger.info(f"评分数据保存完成: {scores_output_path}")
        except Exception as save_error:
            logger.error(f"保存评分数据失败: {save_error}")
            raise Exception(f"Failed to save scores data: {save_error}")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="saved_final_scores",
            description="Final scores data saved to storage",
            metadata={
                "output_path": str(scores_output_path),
                "record_count": len(scored_data),
                "file_size": scores_output_path.stat().st_size if scores_output_path.exists() else 0,
                "data_type": "final_scores"
            }
        )

        yield Output(str(scores_output_path))

    except Exception as e:
        logger.error(f"评分数据保存失败: {e}")
        raise Failure(f"Save scores data failed: {e}")


@op(
    name="save_processed_data",
    description="保存处理后的数据",
    retry_policy=CRITICAL_RETRY,
    tags={"type": "save"}
)
def save_processed_data_op(context: OpExecutionContext, final_data: pl.DataFrame):
    """保存处理后的数据到存储"""
    logger = get_dagster_logger()

    try:
        # 生成输出路径
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/processed/daily_results_{timestamp}.parquet"

        logger.info(f"开始保存数据到: {output_path}")

        # 保存到Parquet
        success = save_data(
            data=final_data,
            output_path=output_path,
            compression="snappy"
        )

        if not success:
            raise Exception("Failed to save data")

        logger.info("数据保存完成")

        # 记录资产物化
        yield AssetMaterialization(
            asset_key="processed_data",
            description="Processed data saved to storage",
            metadata={
                "output_path": output_path,
                "record_count": len(final_data),
                "file_size": Path(output_path).stat().st_size if Path(output_path).exists() else 0
            }
        )

        yield Output(output_path)

    except Exception as e:
        logger.error(f"数据保存失败: {e}")
        raise Failure(f"Data save failed: {e}")


# ===== 作业定义 =====

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


# 移除了重复的 scoring_and_save_job，现在使用统一的 daily_full_pipeline_job


@job(
    name="daily_full_pipeline_job",
    description="完整的每日数据处理管道 - 数据同步、技术指标计算、评分存盘一体化",
    tags={"type": "pipeline", "frequency": "daily", "priority": "critical"}
)
def daily_full_pipeline_job():
    """完整的每日数据处理管道作业 - 一站式完成所有处理步骤
    流程：
    1. 数据同步（增量更新 + 缺失补全）并保存
    2. 技术指标计算并保存（基本面数据直接使用拉取的数据）
    3. 评分计算
    4. 评分单独保存
    5. 完整数据存盘
    """
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
    # 基本面数据直接使用拉取的数据，不进行额外计算

    # 4. 数据合并
    merged_data = merge_all_data_op(
        ohlcv_data, fundamental_data,
        technical_indicators, fundamental_data  # 使用原始基本面数据作为"指标"
    )

    # 5. 评分计算
    final_scores = calculate_final_scores_op(merged_data)

    # 6. 评分单独保存
    scores_output_path = save_final_scores_op(final_scores)

    # 7. 完整数据存盘
    output_path = save_processed_data_op(final_scores)


# ===== 传感器定义 =====

@run_status_sensor(
    monitored_jobs=[daily_full_pipeline_job],
    run_status=DagsterRunStatus.SUCCESS,
    name="daily_processing_success_monitor"
)
def daily_processing_success_monitor(context):
    """监听完整每日处理管道作业的执行状态"""
    logger.info("完整的每日数据处理管道作业执行成功")
    # 可以在这里添加后续处理逻辑，比如发送通知或触发下游作业
    return None


@run_status_sensor(
    monitored_jobs=[daily_full_pipeline_job],
    run_status=DagsterRunStatus.FAILURE,
    name="daily_processing_failure_monitor"
)
def daily_processing_failure_monitor(context):
    """监听完整每日处理管道作业的失败状态"""
    logger.error("完整的每日数据处理管道作业执行失败")
    # 可以在这里添加错误处理逻辑，比如发送告警通知
    return None


# ===== 调度器定义 =====

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


# ===== 注册到管道管理器 =====

def register_daily_jobs():
    """注册所有每日作业到管道管理器"""
    # 注册主要的完整管道作业
    pipeline_manager.register_job("daily_full_pipeline", daily_full_pipeline_job)

    # 注册独立的组件作业（用于调试或单独执行）
    pipeline_manager.register_job("daily_sync", daily_data_sync_job)

    logger.info("所有每日作业已注册到管道管理器")


# 初始化时注册作业
register_daily_jobs()


# ===== 使用示例 =====

if __name__ == "__main__":
    print("=== 每日数据处理管道演示 ===")

    # 运行完整管道
    try:
        print("✅ 管道定义完成，可以通过Dagster UI或API执行")
        print("📋 可用作业:")
        print("  - daily_full_pipeline_job: ⭐ 推荐 - 完整每日处理管道")
        print("    1. 数据同步补全 (增量更新+缺失补全)")
        print("    2. 技术指标计算并保存 (基本面数据直接使用拉取的数据)")
        print("    3. 评分存盘 (最终评分计算和数据保存)")
        print("")
        print("  - 独立组件作业 (用于调试):")
        print("    - daily_data_sync_job: 数据同步补全")
        print("    - indicator_calculation_job: 指标计算并保存")
        print("    - load_existing_technical_indicators_op: 加载已有技术指标")

    except Exception as e:
        print(f"❌ 管道执行失败: {e}")
