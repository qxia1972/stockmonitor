"""
数据处理器 (Data Processor)
基于Polars的高性能数据流处理

职责：
- 数据加载和保存
- 数据验证和清理
- 数据转换和预处理
- 数据流管理
"""

import polars as pl
import numpy as np
from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import logging

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


class DataProcessor:
    """Polars数据处理器 - 专注数据流处理"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self.logger = logging.getLogger(__name__)

    def load_data(self, file_path: Union[str, Path]) -> pl.DataFrame:
        """加载数据文件"""
        file_path = Path(file_path)

        if file_path.suffix == '.parquet':
            return pl.read_parquet(file_path)
        elif file_path.suffix == '.csv':
            return pl.read_csv(file_path)
        elif file_path.suffix == '.json':
            return pl.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

    def save_data(self, df: pl.DataFrame, file_path: Union[str, Path], **kwargs):
        """保存数据到文件"""
        file_path = Path(file_path)

        if file_path.suffix == '.parquet':
            df.write_parquet(file_path, **kwargs)
        elif file_path.suffix == '.csv':
            df.write_csv(file_path, **kwargs)
        elif file_path.suffix == '.json':
            df.write_json(file_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

    def calculate_missing_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算缺失的字段（vwap, returns等）

        Args:
            df: 包含OHLCV数据的DataFrame

        Returns:
            pl.DataFrame: 包含计算后字段的DataFrame
        """
        if df.is_empty():
            return df

        result_df = df.clone()

        # 计算VWAP (成交量加权平均价)
        if 'vwap' not in result_df.columns or result_df.select(pl.col('vwap').is_null().sum()).item() > 0:
            logger.info("🔄 计算VWAP (成交量加权平均价)")
            if 'amount' in result_df.columns and 'volume' in result_df.columns:
                # VWAP = amount / volume
                result_df = result_df.with_columns([
                    (pl.col('amount') / pl.col('volume')).alias('vwap')
                ])
            else:
                logger.warning("⚠️ 无法计算VWAP：缺少amount或volume字段")

        # 计算Returns (收益率)
        if 'returns' not in result_df.columns or result_df.select(pl.col('returns').is_null().sum()).item() > 0:
            logger.info("🔄 计算Returns (收益率)")
            if 'close' in result_df.columns:
                # Returns = (close - close_prev) / close_prev
                result_df = result_df.with_columns([
                    ((pl.col('close') - pl.col('close').shift(1)) / pl.col('close').shift(1)).alias('returns')
                ])
            else:
                logger.warning("⚠️ 无法计算Returns：缺少close字段")

        # 计算Volume Ratio (量比)
        if 'volume_ratio' not in result_df.columns or result_df.select(pl.col('volume_ratio').is_null().sum()).item() > 0:
            logger.info("🔄 计算Volume Ratio (量比)")
            if 'volume' in result_df.columns:
                # 量比 = 当日成交量 / 过去5日平均成交量
                result_df = result_df.with_columns([
                    (pl.col('volume') / pl.col('volume').rolling_mean(window_size=5)).alias('volume_ratio')
                ])
            else:
                logger.warning("⚠️ 无法计算Volume Ratio：缺少volume字段")

        # 计算Price Change (涨跌幅)
        if 'price_change' not in result_df.columns:
            logger.info("🔄 计算Price Change (涨跌幅)")
            if 'close' in result_df.columns:
                result_df = result_df.with_columns([
                    (pl.col('close') - pl.col('close').shift(1)).alias('price_change')
                ])

        logger.info("✅ 缺失字段计算完成")
        return result_df

    def validate_data(self, df: pl.DataFrame, required_columns: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
        """验证数据完整性

        Args:
            df: 要验证的DataFrame
            required_columns: 必需的列名列表

        Returns:
            Tuple[bool, List[str]]: (是否有效, 缺失的列)
        """
        if df.is_empty():
            return False, ["DataFrame is empty"]

        missing_columns = []
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]

        return len(missing_columns) == 0, missing_columns

    def clean_data(self, df: pl.DataFrame, remove_nulls: bool = True, remove_duplicates: bool = True) -> pl.DataFrame:
        """数据清理

        Args:
            df: 要清理的DataFrame
            remove_nulls: 是否移除空值行
            remove_duplicates: 是否移除重复行

        Returns:
            pl.DataFrame: 清理后的DataFrame
        """
        if df.is_empty():
            return df

        result_df = df.clone()

        # 移除空值
        if remove_nulls:
            # 只移除完全为空的行
            result_df = result_df.drop_nulls()

        # 移除重复行
        if remove_duplicates and 'date' in result_df.columns:
            result_df = result_df.unique(subset=['date'], keep='first')

        return result_df

    def transform_data(self, df: pl.DataFrame, transformations: Dict[str, Any]) -> pl.DataFrame:
        """数据转换

        Args:
            df: 要转换的DataFrame
            transformations: 转换配置字典

        Returns:
            pl.DataFrame: 转换后的DataFrame
        """
        if df.is_empty():
            return df

        result_df = df.clone()

        # 日期格式转换
        if 'date' in transformations:
            date_config = transformations['date']
            if date_config.get('to_datetime', False):
                result_df = result_df.with_columns([
                    pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('date')
                ])

        # 数值类型转换
        if 'numeric_columns' in transformations:
            numeric_cols = transformations['numeric_columns']
            for col in numeric_cols:
                if col in result_df.columns:
                    result_df = result_df.with_columns([
                        pl.col(col).cast(pl.Float64).alias(col)
                    ])

        return result_df

    def get_data_info(self, df: pl.DataFrame) -> Dict[str, Any]:
        """获取数据基本信息"""
        if df.is_empty():
            return {"error": "DataFrame is empty"}

        info = {
            "shape": df.shape,
            "columns": df.columns,
            "dtypes": {col: str(df[col].dtype) for col in df.columns},
            "null_counts": {col: df[col].null_count() for col in df.columns},
            "memory_usage": df.estimated_size()
        }

        return info

    def batch_process_files(self, file_paths: List[Union[str, Path]],
                          output_dir: Union[str, Path],
                          transformations: Optional[Dict[str, Any]] = None) -> List[str]:
        """批量处理数据文件

        Args:
            file_paths: 输入文件路径列表
            output_dir: 输出目录
            transformations: 转换配置

        Returns:
            List[str]: 处理后的文件路径列表
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)

        processed_files = []

        for file_path in file_paths:
            try:
                # 加载数据
                df = self.load_data(file_path)

                # 验证数据
                is_valid, missing_cols = self.validate_data(df)
                if not is_valid:
                    self.logger.warning(f"跳过文件 {file_path}: 缺少列 {missing_cols}")
                    continue

                # 清理数据
                df = self.clean_data(df)

                # 转换数据
                if transformations:
                    df = self.transform_data(df, transformations)

                # 保存处理后的数据
                input_path = Path(file_path)
                output_path = output_dir / f"processed_{input_path.name}"
                self.save_data(df, output_path)

                processed_files.append(str(output_path))
                self.logger.info(f"处理完成: {file_path} -> {output_path}")

            except Exception as e:
                self.logger.error(f"处理文件失败 {file_path}: {e}")
                continue

        return processed_files

    def convert_from_pandas(self, pdf: 'pd.DataFrame') -> pl.DataFrame:
        """从pandas DataFrame转换为Polars DataFrame"""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is not available")
        return pl.from_pandas(pdf)

    def convert_to_pandas(self, df: pl.DataFrame) -> 'pd.DataFrame':
        """从Polars DataFrame转换为pandas DataFrame"""
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is not available")
        return df.to_pandas()

    def optimize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """优化DataFrame内存使用和性能"""
        if df.is_empty():
            return df

        # 重新排列列以提高访问性能
        # 将常用的列放在前面
        priority_cols = ['order_book_id', 'date', 'close', 'volume']
        other_cols = [col for col in df.columns if col not in priority_cols]

        if all(col in df.columns for col in priority_cols):
            df = df.select(priority_cols + other_cols)

        # 使用最优的数据类型
        optimized_df = df

        # 优化数值列的数据类型
        for col in df.columns:
            if col in ['close', 'high', 'low', 'open', 'volume']:
                dtype = df[col].dtype
                if dtype == pl.Float64:
                    # 检查是否可以转换为Float32
                    try:
                        optimized_df = optimized_df.with_columns([
                            pl.col(col).cast(pl.Float32).alias(col)
                        ])
                    except:
                        pass  # 如果转换失败，保持原类型

        # 移除重复的索引（如果存在）
        if 'date' in optimized_df.columns:
            if 'order_book_id' in optimized_df.columns:
                # 检查是否有重复的日期（按股票分组）
                duplicate_count = optimized_df.group_by(['order_book_id', 'date']).agg(pl.len()).filter(pl.col('len') > 1).height
                if duplicate_count > 0:
                    logger.warning(f"发现 {duplicate_count} 个重复的日期记录，正在去重")
                    optimized_df = optimized_df.unique(subset=['order_book_id', 'date'], keep='last')
            else:
                # 如果没有股票标识列，则按日期去重
                duplicate_count = optimized_df.select(pl.col('date')).is_duplicated().sum()
                if duplicate_count > 0:
                    logger.warning(f"发现 {duplicate_count} 个重复的日期记录，正在去重")
                    optimized_df = optimized_df.unique(subset=['date'], keep='last')

        return optimized_df

    def get_memory_usage(self, df: pl.DataFrame) -> Dict[str, Any]:
        """获取DataFrame内存使用情况"""
        try:
            memory_info = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'estimated_memory_mb': df.estimated_size() / 1024 / 1024,
                'column_info': {}
            }

            for col in df.columns:
                col_info = {
                    'dtype': str(df[col].dtype),
                    'null_count': df[col].null_count()
                }
                memory_info['column_info'][col] = col_info

            return memory_info
        except Exception as e:
            logger.error(f"获取内存使用信息失败: {e}")
            return {'error': str(e)}


# 全局数据处理器实例
data_processor = DataProcessor()

# =============================================================================
# 数据补全功能 (Data Completion Features)
# =============================================================================

from typing import Tuple
from dataclasses import dataclass


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
        from datetime import timedelta
        max_fill_date = min_date + timedelta(days=self.config.max_fill_days)
        missing_dates = [
            date for date in missing_dates
            if date >= min_date and date <= max_fill_date
        ]

        self.logger.debug(f"识别到缺失日期: {symbol}, 数量: {len(missing_dates)}")
        return missing_dates

    def _generate_trading_dates(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """生成交易日序列（简化版，实际应该从交易日历获取）"""
        from datetime import timedelta
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
            else:
                # 如果没有前一个交易日，跳过这个缺失日期
                continue

        if missing_rows:
            # 合并缺失行到原数据
            missing_df = pl.concat(missing_rows)
            completed_data = pl.concat([data, missing_df])
            # 重新排序
            completed_data = completed_data.sort(date_column)
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
            else:
                # 如果没有后一个交易日，跳过这个缺失日期
                continue

        if missing_rows:
            # 合并缺失行到原数据
            missing_df = pl.concat(missing_rows)
            completed_data = pl.concat([data, missing_df])
            # 重新排序
            completed_data = completed_data.sort(date_column)
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
        # 对于线性插值，我们需要找到缺失日期前后的数据点
        completed_data = data.clone()

        for missing_date in missing_dates:
            # 找到前一个和后一个数据点
            prev_data = data.filter(pl.col(date_column) < missing_date)
            next_data = data.filter(pl.col(date_column) > missing_date)

            if len(prev_data) > 0 and len(next_data) > 0:
                prev_row = prev_data.tail(1)
                next_row = next_data.head(1)

                # 创建插值行
                new_row_data = {date_column: missing_date}

                # 对每个数值列进行线性插值
                for col in value_columns:
                    if col in prev_row.columns and col in next_row.columns:
                        prev_val = prev_row.select(col).item()
                        next_val = next_row.select(col).item()

                        if prev_val is not None and next_val is not None:
                            # 简单线性插值（这里可以改进为基于日期的插值）
                            interpolated_val = (prev_val + next_val) / 2
                            new_row_data[col] = interpolated_val
                        else:
                            # 如果任一值为None，使用前向填充
                            new_row_data[col] = prev_val if prev_val is not None else next_val

                # 添加其他非数值列（如果有的话）
                for col in data.columns:
                    if col not in new_row_data and col in prev_row.columns:
                        new_row_data[col] = prev_row.select(col).item()

                # 创建新行并添加到数据中
                new_row_df = pl.DataFrame([new_row_data])
                completed_data = pl.concat([completed_data, new_row_df])

        # 重新排序
        completed_data = completed_data.sort(date_column)

        return completed_data

    def _assess_completion_quality(
        self,
        original_data: pl.DataFrame,
        completed_data: pl.DataFrame,
        missing_dates: List[datetime]
    ) -> float:
        """评估补全质量"""
        if len(completed_data) == 0:
            return 0.0

        # 基础质量指标
        total_records = len(completed_data)

        # 计算空值率
        null_counts = []
        for col in completed_data.columns:
            if col != "date":  # 排除日期列
                null_count = completed_data.select(pl.col(col).is_null().sum()).item()
                null_counts.append(null_count)

        total_nulls = sum(null_counts)
        total_cells = total_records * (len(completed_data.columns) - 1)  # 排除日期列
        null_ratio = total_nulls / total_cells if total_cells > 0 else 0.0

        # 计算补全成功率
        expected_additional_records = len(missing_dates)
        actual_additional_records = len(completed_data) - len(original_data)
        completion_rate = actual_additional_records / expected_additional_records if expected_additional_records > 0 else 1.0

        # 综合质量评分：(1 - 空值率) * 补全成功率
        quality_score = (1.0 - null_ratio) * completion_rate

        # 限制在0-1范围内
        quality_score = max(0.0, min(1.0, quality_score))

        return quality_score


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
