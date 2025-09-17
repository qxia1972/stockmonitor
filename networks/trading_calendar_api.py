"""
网络API调用模块 (Network API Calls Module)

包含所有网络API调用的逻辑，特别是交易日历相关的API调用
"""

from typing import List
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)


def get_latest_trading_day() -> datetime:
    """获取最新交易日"""
    # 这里应该调用交易日历API
    # 暂时返回今天
    today = datetime.now().date()
    # 简单处理周末
    if today.weekday() >= 5:  # 周六日
        days_to_subtract = today.weekday() - 4
        today = today - timedelta(days=days_to_subtract)
    return datetime.combine(today, datetime.min.time())


def get_trading_day_n_days_ago(base_date: datetime, n_days: int) -> datetime:
    """获取N个交易日前的时间"""
    # 简单实现，实际应该考虑节假日
    total_days = n_days * 7 // 5  # 粗略估算
    result_date = base_date - timedelta(days=total_days)
    return result_date


def get_next_trading_day(base_date: datetime) -> datetime:
    """获取下一个交易日"""
    # 简单实现，实际应该考虑节假日
    next_day = base_date + timedelta(days=1)
    # 跳过周末
    if next_day.weekday() >= 5:
        next_day = next_day + timedelta(days=2)
    return next_day


def get_trading_calendar() -> List[datetime]:
    """获取交易日历"""
    # 这里应该调用交易日历API
    # 暂时生成一个简单的日历
    start_date = datetime.now() - timedelta(days=365)
    end_date = datetime.now()
    calendar = []

    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # 周一到周五
            calendar.append(current)
        current += timedelta(days=1)

    return calendar
