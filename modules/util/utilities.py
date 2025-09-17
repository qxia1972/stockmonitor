"""
交易日历工具函数

提供交易日历相关的工具函数，支持RQDatac数据源的交易日历操作。
"""

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from .log_manager import get_stockpool_logger

logger = get_stockpool_logger()


def get_trading_dates(rqdatac: Any, start_date: str, end_date: str) -> list[str]:
    """获取交易日历

    Args:
        rqdatac: RQDatac实例
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        List[str]: 交易日期列表 (YYYY-MM-DD格式)

    Raises:
        Exception: 获取交易日历失败时抛出
    """
    try:
        # 获取交易日历
        trading_dates = rqdatac.get_trading_dates(start_date, end_date)
        return [date.strftime("%Y-%m-%d") for date in trading_dates]

    except Exception as e:
        logger.exception("Failed to get trading dates:")
        raise e


def get_latest_trading_date(rqdatac: Any) -> str | None:
    """获取最新交易日

    Args:
        rqdatac: RQDatac实例

    Returns:
        Optional[str]: 最新交易日 (YYYY-MM-DD格式),如果获取失败返回None
    """
    try:
        latest_date = rqdatac.get_latest_trading_date()
        return latest_date.strftime("%Y-%m-%d") if latest_date else None

    except Exception:
        logger.warning("Failed to get latest trading date:")
        return None


def get_trading_hours(rqdatac: Any, stock_code: str, date: str) -> str:
    """获取交易时段

    Args:
        rqdatac: RQDatac实例
        stock_code: 股票代码
        date: 日期 (YYYY-MM-DD)

    Returns:
        str: 交易时段信息

    Raises:
        Exception: 获取交易时段失败时抛出
    """
    try:
        trading_hours_result = rqdatac.get_trading_hours(stock_code, date)
        # 确保返回字符串类型
        if isinstance(trading_hours_result, str):
            return trading_hours_result
        if isinstance(trading_hours_result, list):
            # 如果返回列表,转换为字符串
            return str(trading_hours_result)
        # 其他类型转换为字符串
        return str(trading_hours_result) if trading_hours_result is not None else ""

    except Exception as e:
        logger.exception("Failed to get trading hours:")
        raise e


def get_previous_trading_date(rqdatac: Any, date: str) -> str | None:
    """获取前一个交易日

    Args:
        rqdatac: RQDatac实例
        date: 基准日期 (YYYY-MM-DD)

    Returns:
        Optional[str]: 前一个交易日 (YYYY-MM-DD格式),如果获取失败返回None
    """
    try:
        previous_date = rqdatac.get_previous_trading_date(date)
        return previous_date.strftime("%Y-%m-%d") if previous_date else None

    except Exception:
        logger.warning("Failed to get previous trading date:")
        return None


def get_completed_trading_date(
    data_source_manager, stock_code: str | None = None, max_days_back: int = 30
) -> str:
    """
    获取已完成的交易日

    根据当前时间和交易状态智能判断应该使用哪个交易日的数据。
    返回已完成交易的日期,确保数据完整性和有效性。
    支持两种模式:
    1. 全局模式(stock_code=None):为所有股票确定统一的交易日
    2. 股票特定模式(stock_code指定):为特定股票确定交易日

    核心逻辑:
    1. 获取最新交易日
    2. 判断当前时间是否在交易日当天
    3. 如果在交易日当天,根据交易时段判断数据有效性
    4. 提供降级策略确保系统稳定性

    Args:
        data_source_manager: 数据源管理器实例
        stock_code: 股票代码,为None时返回全局交易日
        max_days_back: 最大回溯天数

    Returns:
        有效的交易日期字符串 (YYYY-MM-DD) 或 None
    """
    try:
        # 获取最新交易日
        latest_date_str = data_source_manager.get_data("get_latest_trading_date")
        if not latest_date_str:
            logger.warning("⚠️ 无法获取最新交易日, 使用当前日期")
            return datetime.now().strftime("%Y-%m-%d")

        latest_date = pd.to_datetime(latest_date_str).date()
        current_time = datetime.now()

        # 如果不在交易日当天,直接返回最新交易日
        if current_time.date() != latest_date:
            date_str = latest_date.strftime("%Y-%m-%d")
            logger.debug("📅 当前不在交易日当天，使用最新交易日: %s", date_str)
            return date_str

        # 在交易日当天,需要判断数据有效性
        logger.debug("📅 当前在交易日当天，需要判断数据有效性")

        # 尝试获取交易时段进行精确判断
        try:
            # 获取交易时段信息(函数名在数据源中约定)
            trading_hours_str = data_source_manager.get_data("get_trading_hours")
            logger.debug("⏰ 交易时段: %s", trading_hours_str)

            if trading_hours_str and isinstance(trading_hours_str, str):
                # 使用交易时段进行精确判断
                should_use_previous = should_use_previous_trading_date(
                    trading_hours_str, current_time
                )
                logger.debug("🎯 是否使用前一个交易日: %s", should_use_previous)

                if should_use_previous:
                    # 获取前一个交易日
                    previous_date_str = data_source_manager.get_data(
                        "get_previous_trading_date", latest_date_str
                    )
                    if previous_date_str:
                        logger.info(
                            "🎯 确定交易日: %s (前一个交易日)", previous_date_str
                        )
                        return previous_date_str
                    logger.warning("⚠️ 无法获取前一个交易日,使用最新交易日")

                # 使用当天数据
                logger.info("🎯 确定交易日: %s (最新交易日)", latest_date_str)
                return latest_date_str

            logger.warning("⚠️ 无法获取交易时段,使用降级时间判断")
        except Exception as e:
            logger.warning("⚠️ 获取交易时段失败,使用降级时间判断: %s", e)

        # 降级时间判断逻辑
        return fallback_time_judgment(latest_date, current_time, data_source_manager)

    except Exception:
        logger.exception("❌ 确定交易日失败")
        # 最终降级:返回当前日期
        return datetime.now().strftime("%Y-%m-%d")


def should_use_previous_trading_date(trading_hours_str: str, current_time) -> bool:
    """
    判断是否应该使用前一个交易日的数据

    逻辑:
    1. 交易前:使用前一个交易日
    2. 交易中:使用前一个交易日(实时数据不稳定)
    3. 交易后3小时内:使用前一个交易日(数据可能还在更新)
    4. 交易后3小时后:使用当天数据

    Args:
        trading_hours_str: 交易时段字符串,如 "09:31-11:30,13:01-15:00"
        current_time: 当前时间

    Returns:
        是否应该使用前一个交易日
    """
    try:
        from datetime import datetime, timedelta

        if not trading_hours_str:
            return True

        current_time_only = current_time.time()

        # 解析交易时段
        periods = trading_hours_str.split(",")
        market_open_time = None
        market_close_time = None

        for period in periods:
            if "-" not in period:
                continue

            start_str, end_str = period.split("-")
            try:
                start_time = datetime.strptime(start_str.strip(), "%H:%M").time()
                end_time = datetime.strptime(end_str.strip(), "%H:%M").time()

                if market_open_time is None or start_time < market_open_time:
                    market_open_time = start_time
                if market_close_time is None or end_time > market_close_time:
                    market_close_time = end_time
            except ValueError:
                continue

        if market_open_time is None or market_close_time is None:
            logger.warning("⚠️ 无法解析交易时段: %s", trading_hours_str)
            return True

        # 计算收盘后3小时的时间点
        close_datetime = datetime.combine(current_time.date(), market_close_time)
        three_hours_after_close = close_datetime + timedelta(hours=3)
        logger.debug(
            "⏰ 市场开盘: %s, 收盘: %s", market_open_time, market_close_time
        )
        logger.debug(
            "⏰ 当前时间: %s, 收盘后3小时: %s",
            current_time_only,
            three_hours_after_close.time(),
        )

        if current_time_only < market_open_time:
            # 交易前:使用前一个交易日
            logger.debug("⏰ 当前在交易前，使用前一个交易日")
            return True
        if market_open_time <= current_time_only <= market_close_time:
            # 交易中:使用前一个交易日(实时数据不稳定)
            logger.debug("⏰ 当前在交易中, 使用前一个交易日(实时数据不稳定)")
            return True
        if market_close_time < current_time_only <= three_hours_after_close.time():
            # 收盘后3小时内:使用前一个交易日(数据可能还在更新)
            logger.debug(
                "⏰ 当前在收盘后3小时内, 使用前一个交易日(数据可能还在更新)"
            )
            return True
        # 收盘后3小时后:使用当天数据
        logger.debug("⏰ 当前在收盘后3小时后, 使用当天数据")
        return False

    except Exception as e:
        logger.warning("⚠️ 判断是否使用前一个交易日失败: %s", e)
        return True  # 出错时保守使用前一个交易日


def fallback_time_judgment(latest_date, current_time, data_source_manager) -> str:
    """
    降级时间判断逻辑(当获取交易时段失败时使用)

    Args:
        latest_date: 最新交易日
        current_time: 当前时间
        data_source_manager: 数据源管理器

    Returns:
        交易日期字符串
    """
    from datetime import datetime

    # 检查是否在交易时间内
    current_time_only = current_time.time()
    # 假设A股收盘时间是15:00
    market_close_time = datetime.strptime("15:00", "%H:%M").time()

    if current_time_only < market_close_time:
        # 当前在交易日当天且未收盘,数据必然无效
        logger.debug("⏰ 当前在交易日当天且未收盘, 最新交易日数据必然无效")

        # 获取前一个交易日
        previous_date = data_source_manager.get_data(
            "get_previous_trading_date", latest_date.strftime("%Y-%m-%d")
        )
        if previous_date:
            logger.info("🎯 确定交易日: %s (前一个交易日 - 降级逻辑)", previous_date)
            return previous_date

        # 无法获取前一个交易日，使用最新交易日
        date_str = latest_date.strftime("%Y-%m-%d")
        logger.warning("⚠️ 无法获取前一个交易日, 使用最新交易日: %s", date_str)
        return date_str

    # 收盘后，使用最新交易日
    date_str = latest_date.strftime("%Y-%m-%d")
    logger.info("🎯 确定交易日: %s (最新交易日 - 降级逻辑)", date_str)
    return date_str


def generate_weekday_dates(start_date: str, end_date: str) -> list[str]:
    """
    生成工作日日期列表(近似交易日)

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        工作日日期列表 (YYYY-MM-DD格式)
    """
    from datetime import timedelta
    import pandas as pd

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    dates = []
    current = start
    while current <= end:
        # 排除周末 (0=Monday, 6=Sunday)
        if current.weekday() < 5:  # Monday to Friday
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def load_trading_dates_from_cache(data_dir: str, start_date: str, end_date: str) -> list[str] | None:
    """
    从本地缓存文件加载交易日历

    Args:
        data_dir: 数据目录路径
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        交易日期列表，如果加载失败返回None
    """
    import json
    import os

    cache_file = os.path.join(data_dir, "trading_dates.json")

    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                dates = json.load(f)

            # 过滤日期范围
            filtered_dates = [d for d in dates if start_date <= d <= end_date]
            return filtered_dates
        except Exception:
            logger.warning("Failed to load trading dates from cache:")

    return None


def load_latest_trading_date_from_cache(data_dir: str) -> str | None:
    """
    从本地缓存文件加载最新交易日

    Args:
        data_dir: 数据目录路径

    Returns:
        最新交易日字符串，如果加载失败返回None
    """
    import os

    cache_file = os.path.join(data_dir, "latest_trading_date.txt")

    if os.path.exists(cache_file):
        try:
            with open(cache_file, encoding="utf-8") as f:
                date_str = f.read().strip()
                return date_str
        except Exception:
            logger.warning("Failed to load latest trading date:")

    return None


def calculate_default_date_range(rqdatac, start_date: str | None, end_date: str | None) -> tuple[str, str]:
    """
    根据当前时间和交易状态计算默认的日期范围

    Args:
        rqdatac: RQDatac实例
        start_date: 用户指定的开始日期，如果为None则自动计算
        end_date: 用户指定的结束日期，如果为None则自动计算

    Returns:
        (start_date, end_date) 元组，格式为YYYY-MM-DD
    """
    from datetime import datetime, time

    # 如果都已指定，直接返回
    if start_date is not None and end_date is not None:
        return start_date, end_date

    try:
        # 获取交易日历
        trading_dates = rqdatac.get_trading_dates('2024-01-01', '2025-12-31')
        if trading_dates is not None and len(trading_dates) > 0:
            now = datetime.now()
            today = now.date()

            # 检查今天是否是交易日
            is_trading_day = today in trading_dates

            # 检查当前时间是否在交易时间内 (9:30-11:30 或 13:00-15:00)
            morning_start = time(9, 30)
            morning_end = time(11, 30)
            afternoon_start = time(13, 0)
            afternoon_end = time(15, 0)
            current_time = now.time()

            is_trading_hours = (
                (morning_start <= current_time <= morning_end) or
                (afternoon_start <= current_time <= afternoon_end)
            )

            if is_trading_day and is_trading_hours:
                # 如果是交易日且在交易时间内，使用今天作为结束日期
                latest_date = today
            else:
                # 否则使用上一个交易日
                # 找到今天或之前的最新交易日
                past_trading_dates = [d for d in trading_dates if d <= today]
                if past_trading_dates:
                    latest_date = past_trading_dates[-1]
                else:
                    latest_date = trading_dates[-1]  # 如果都没有，使用最新的

            if start_date is None:
                # 使用最近30天的交易日作为开始日期
                trading_dates_list = list(trading_dates)
                latest_idx = trading_dates_list.index(latest_date)
                start_idx = max(0, latest_idx - 29)  # 30天的数据
                start_date = trading_dates_list[start_idx].strftime('%Y-%m-%d')
            if end_date is None:
                end_date = latest_date.strftime('%Y-%m-%d')

            logger.info(f"Using calculated date range: {start_date} to {end_date} (latest trading day: {latest_date})")
        else:
            # 如果获取交易日失败，使用固定的默认日期
            start_date = start_date or '2024-01-01'
            end_date = end_date or '2024-12-31'
            logger.warning(f"Failed to get trading dates, using fixed defaults: {start_date} to {end_date}")
    except Exception as date_error:
        # 如果获取交易日失败，使用固定的默认日期
        start_date = start_date or '2024-01-01'
        end_date = end_date or '2024-12-31'
        logger.warning(f"Failed to get trading dates: {date_error}, using fixed defaults: {start_date} to {end_date}")

    # 确保返回有效的字符串
    start_date = start_date or '2024-01-01'
    end_date = end_date or '2024-12-31'

    return start_date, end_date