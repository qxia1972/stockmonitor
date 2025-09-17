"""
数据补全模块 (Data Completion Module)

实现配置化的数据补全策略，支持多种补全算法和质量控制机制。

作者: 系统架构师
创建日期: 2025年9月17日
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class CompletionConfig:
    """补全配置类"""
    max_fill_days: int = 30  # 最大补全天数
    fill_method: str = "forward"  # 补全方法: forward, backward, linear
    min_data_points: int = 3  # 最少数据点要求
    quality_threshold: float = 0.8  # 质量阈值
    enable_validation: bool = True  # 启用验证


@dataclass
class CompletionResult:
    """补全结果类"""
    original_count: int
    completed_count: int
    quality_score: float
    missing_dates: List[str]
    filled_dates: List[str]
    success: bool
    message: str


class DataCompletionManager:
    """数据补全管理器"""

    def __init__(self, config: Optional[CompletionConfig] = None):
        self.config = config or CompletionConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def complete_market_data(
        self,
        data: pl.DataFrame,
        symbol: str,
        date_column: str = "date",
        value_columns: Optional[List[str]] = None
    ) -> Tuple[pl.DataFrame, CompletionResult]:
        """
        补全市场数据

        Args:
            data: 原始数据DataFrame
            symbol: 股票代码
            date_column: 日期列名
            value_columns: 需要补全的数值列

        Returns:
            Tuple[补全后的数据, 补全结果]
        """
        try:
            # 数据预处理
            processed_data = self._preprocess_data(data, date_column)

            # 识别缺失日期
            missing_dates = self._identify_missing_dates(
                processed_data, date_column, symbol
            )

            if not missing_dates:
                # 无缺失数据，直接返回
                result = CompletionResult(
                    original_count=len(processed_data),
                    completed_count=len(processed_data),
                    quality_score=1.0,
                    missing_dates=[],
                    filled_dates=[],
                    success=True,
                    message="数据完整，无需补全"
                )
                return processed_data, result

            # 执行补全
            completed_data = self._perform_completion(
                processed_data, missing_dates, date_column, value_columns
            )

            # 质量评估
            quality_score = self._assess_completion_quality(
                processed_data, completed_data, missing_dates
            )

            # 生成结果
            result = CompletionResult(
                original_count=len(processed_data),
                completed_count=len(completed_data),
                quality_score=quality_score,
                missing_dates=[d.strftime("%Y-%m-%d") for d in missing_dates],
                filled_dates=[d.strftime("%Y-%m-%d") for d in missing_dates],
                success=quality_score >= self.config.quality_threshold,
                message=f"补全完成，质量评分: {quality_score:.2f}"
            )

            self.logger.info(f"数据补全完成: {symbol}, 质量评分: {quality_score:.2f}")
            return completed_data, result

        except Exception as e:
            self.logger.error(f"数据补全失败: {symbol}, 错误: {str(e)}")
            result = CompletionResult(
                original_count=len(data),
                completed_count=0,
                quality_score=0.0,
                missing_dates=[],
                filled_dates=[],
                success=False,
                message=f"补全失败: {str(e)}"
            )
            return data, result

    def _preprocess_data(self, data: pl.DataFrame, date_column: str) -> pl.DataFrame:
        """数据预处理"""
        # 确保日期列格式正确
        if date_column in data.columns:
            data = data.with_columns(
                pl.col(date_column).cast(pl.Date).alias(date_column)
            )

        # 按日期排序
        data = data.sort(date_column)

        return data

    def _identify_missing_dates(
        self,
        data: pl.DataFrame,
        date_column: str,
        symbol: str
    ) -> List[datetime]:
        """识别缺失的日期"""
        if len(data) < 2:
            return []

        # 获取日期范围
        dates = data.select(date_column).to_series().to_list()
        min_date = min(dates)
        max_date = max(dates)

        # 生成完整日期序列（只考虑工作日）
        complete_dates = self._generate_trading_dates(min_date, max_date)

        # 找出缺失的日期
        existing_dates = set(dates)
        missing_dates = [
            date for date in complete_dates
            if date not in existing_dates
        ]

        # 限制补全范围
        max_fill_date = min_date + timedelta(days=self.config.max_fill_days)
        missing_dates = [
            date for date in missing_dates
            if date >= min_date and date <= max_fill_date
        ]

        self.logger.debug(f"识别到缺失日期: {symbol}, 数量: {len(missing_dates)}")
        return missing_dates

    def _generate_trading_dates(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """生成交易日序列（简化版，实际应该从交易日历获取）"""
        dates = []
        current = start_date
        while current <= end_date:
            # 简化：排除周末，实际应该用真实的交易日历
            if current.weekday() < 5:  # 周一到周五
                dates.append(current)
            current += timedelta(days=1)
        return dates

    def _perform_completion(
        self,
        data: pl.DataFrame,
        missing_dates: List[datetime],
        date_column: str,
        value_columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """执行数据补全"""
        if not missing_dates:
            return data

        # 确定需要补全的列
        if value_columns is None:
            # 自动识别数值列
            value_columns = self._identify_value_columns(data)

        # 根据补全方法执行补全
        if self.config.fill_method == "forward":
            completed_data = self._forward_fill_completion(
                data, missing_dates, date_column, value_columns
            )
        elif self.config.fill_method == "backward":
            completed_data = self._backward_fill_completion(
                data, missing_dates, date_column, value_columns
            )
        elif self.config.fill_method == "linear":
            completed_data = self._linear_interpolation_completion(
                data, missing_dates, date_column, value_columns
            )
        else:
            # 默认前向填充
            completed_data = self._forward_fill_completion(
                data, missing_dates, date_column, value_columns
            )

        return completed_data

    def _identify_value_columns(self, data: pl.DataFrame) -> List[str]:
        """识别数值列"""
        value_columns = []
        exclude_columns = {"date", "order_book_id", "symbol", "exchange"}

        for col in data.columns:
            if col not in exclude_columns:
                dtype = data.schema[col]
                if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                    value_columns.append(col)

        return value_columns

    def _forward_fill_completion(
        self,
        data: pl.DataFrame,
        missing_dates: List[datetime],
        date_column: str,
        value_columns: List[str]
    ) -> pl.DataFrame:
        """前向填充补全"""
        # 创建缺失日期的行
        missing_rows = []
        for missing_date in missing_dates:
            # 找到最近的前一个交易日数据
            prev_data = data.filter(pl.col(date_column) < missing_date)
            if len(prev_data) > 0:
                last_row = prev_data.tail(1)
                new_row = last_row.with_columns(
                    pl.lit(missing_date).alias(date_column)
                )
                missing_rows.append(new_row)

        if missing_rows:
            # 合并缺失数据
            missing_df = pl.concat(missing_rows)
            completed_data = pl.concat([data, missing_df]).sort(date_column)
        else:
            completed_data = data

        return completed_data

    def _backward_fill_completion(
        self,
        data: pl.DataFrame,
        missing_dates: List[datetime],
        date_column: str,
        value_columns: List[str]
    ) -> pl.DataFrame:
        """后向填充补全"""
        # 创建缺失日期的行
        missing_rows = []
        for missing_date in missing_dates:
            # 找到最近的后一个交易日数据
            next_data = data.filter(pl.col(date_column) > missing_date)
            if len(next_data) > 0:
                first_row = next_data.head(1)
                new_row = first_row.with_columns(
                    pl.lit(missing_date).alias(date_column)
                )
                missing_rows.append(new_row)

        if missing_rows:
            # 合并缺失数据
            missing_df = pl.concat(missing_rows)
            completed_data = pl.concat([data, missing_df]).sort(date_column)
        else:
            completed_data = data

        return completed_data

    def _linear_interpolation_completion(
        self,
        data: pl.DataFrame,
        missing_dates: List[datetime],
        date_column: str,
        value_columns: List[str]
    ) -> pl.DataFrame:
        """线性插值补全"""
        # 简化实现：使用前向填充
        # 实际线性插值需要更复杂的实现
        return self._forward_fill_completion(
            data, missing_dates, date_column, value_columns
        )

    def _assess_completion_quality(
        self,
        original_data: pl.DataFrame,
        completed_data: pl.DataFrame,
        missing_dates: List[datetime]
    ) -> float:
        """评估补全质量"""
        if len(missing_dates) == 0:
            return 1.0

        total_missing = len(missing_dates)
        total_original = len(original_data)

        # 基础质量评分
        base_score = 1.0 - (total_missing / (total_original + total_missing))

        # 考虑数据连续性
        continuity_score = self._calculate_continuity_score(completed_data)

        # 综合评分
        quality_score = (base_score * 0.7) + (continuity_score * 0.3)

        return max(0.0, min(1.0, quality_score))

    def _calculate_continuity_score(self, data: pl.DataFrame) -> float:
        """计算数据连续性评分"""
        if len(data) < 2:
            return 1.0

        # 计算日期间隔
        dates = data.select("date").to_series().to_list()
        gaps = []
        for i in range(1, len(dates)):
            gap = (dates[i] - dates[i-1]).days
            gaps.append(gap)

        # 计算连续性（理想间隔为1天）
        ideal_gaps = [1] * len(gaps)
        continuity = sum(1 for gap in gaps if gap <= 3) / len(gaps)  # 允许最多3天间隔

        return continuity


# 全局补全管理器实例
completion_manager = DataCompletionManager()


def complete_market_data(
    data: pl.DataFrame,
    symbol: str,
    config: Optional[CompletionConfig] = None
) -> Tuple[pl.DataFrame, CompletionResult]:
    """
    补全市场数据的主入口函数

    Args:
        data: 原始数据DataFrame
        symbol: 股票代码
        config: 补全配置（可选）

    Returns:
        Tuple[补全后的数据, 补全结果]
    """
    global completion_manager

    if config:
        # 使用自定义配置
        manager = DataCompletionManager(config)
        return manager.complete_market_data(data, symbol)
    else:
        # 使用默认配置
        return completion_manager.complete_market_data(data, symbol)


def get_completion_config() -> CompletionConfig:
    """获取默认补全配置"""
    return CompletionConfig()


def update_completion_config(config: CompletionConfig) -> None:
    """更新全局补全配置"""
    global completion_manager
    completion_manager.config = config


if __name__ == "__main__":
    # 测试代码
    print("=== 数据补全模块测试 ===")

    # 创建测试数据
    test_data = pl.DataFrame({
        "date": [
            datetime(2025, 9, 10),
            datetime(2025, 9, 12),  # 缺失9月11日
            datetime(2025, 9, 13)
        ],
        "close": [100.0, 102.0, 101.0],
        "volume": [1000000, 1200000, 1100000]
    })

    print("原始数据:")
    print(test_data)

    # 执行补全
    completed_data, result = complete_market_data(test_data, "000001.SZ")

    print(f"\n补全结果: {result.message}")
    print(f"质量评分: {result.quality_score:.2f}")
    print(f"缺失日期: {result.missing_dates}")
    print(f"补全日期: {result.filled_dates}")

    print("\n补全后的数据:")
    print(completed_data)