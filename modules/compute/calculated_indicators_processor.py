"""
计算指标处理器 (Calculated Indicators Processor)

职责：
- 管理指标计算流程和并行处理
- 协调数据分块和计算任务分配
- 处理计算依赖关系和错误恢复
- 监控计算性能和资源使用
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
import time
from datetime import datetime

from modules.data_schema import (
    get_enabled_calculated_indicators,
    get_calculated_indicator_dependencies,
    validate_calculated_indicator_dependencies,
    is_calculated_indicators_enabled,
    is_auto_calculate_enabled,
    get_calculated_indicators_processing_options
)

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ProcessingConfig:
    """处理配置"""
    max_workers: int = 4
    chunk_size: int = 10000
    use_processes: bool = False  # True for ProcessPoolExecutor, False for ThreadPoolExecutor
    enable_progress_tracking: bool = True
    error_handling: str = "continue"  # continue, stop, retry
    max_retries: int = 3
    timeout_seconds: int = 300


@dataclass
class ProcessingResult:
    """处理结果"""
    success: bool
    processed_chunks: int
    total_rows: int
    execution_time: float
    errors: List[str]
    performance_metrics: Dict[str, Any]


class ParallelIndicatorsProcessor:
    """并行指标计算处理器"""

    def __init__(self, config: Optional[ProcessingConfig] = None):
        """初始化并行处理器"""
        self.config = config or ProcessingConfig()
        self.logger = logging.getLogger(__name__)

        # 初始化指标计算器（延迟导入以避免循环依赖）
        self._indicator_calculator = None

        self.logger.info(f"并行指标处理器初始化完成，最大工作进程: {self.config.max_workers}")

    @property
    def indicator_calculator(self):
        """延迟加载指标计算器"""
        if self._indicator_calculator is None:
            from modules.compute.indicator_calculator import IndicatorCalculator
            self._indicator_calculator = IndicatorCalculator()
        return self._indicator_calculator

    def process_data_parallel(self, df: pl.DataFrame, indicators: List[str]) -> Tuple[pl.DataFrame, ProcessingResult]:
        """
        并行处理数据指标计算

        Args:
            df: 输入数据
            indicators: 要计算的指标列表

        Returns:
            Tuple[处理后的数据, 处理结果]
        """
        start_time = time.time()

        if df.is_empty():
            return df, ProcessingResult(
                success=True,
                processed_chunks=0,
                total_rows=0,
                execution_time=0.0,
                errors=[],
                performance_metrics={}
            )

        # 数据分块
        chunks = self._split_data_into_chunks(df)

        self.logger.info(f"数据分块完成，共 {len(chunks)} 个块")

        # 准备处理任务
        tasks = []
        for i, chunk in enumerate(chunks):
            task = {
                'chunk_id': i,
                'data': chunk,
                'indicators': indicators,
                'config': self.config
            }
            tasks.append(task)

        # 执行并行处理
        processed_chunks = []
        errors = []

        try:
            with self._get_executor() as executor:
                # 提交所有任务
                future_to_task = {
                    executor.submit(self._process_chunk, task): task
                    for task in tasks
                }

                # 收集结果
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result(timeout=self.config.timeout_seconds)
                        if result['success']:
                            processed_chunks.append(result['data'])
                        else:
                            errors.extend(result['errors'])
                    except Exception as e:
                        chunk_id = task['chunk_id']
                        error_msg = f"块 {chunk_id} 处理失败: {str(e)}"
                        self.logger.error(error_msg)
                        errors.append(error_msg)

        except Exception as e:
            error_msg = f"并行处理执行失败: {str(e)}"
            self.logger.error(error_msg)
            errors.append(error_msg)

        # 合并结果
        if processed_chunks:
            try:
                result_df = pl.concat(processed_chunks)
                # 按原始顺序排序
                if 'date' in result_df.columns:
                    result_df = result_df.sort('date')
            except Exception as e:
                error_msg = f"结果合并失败: {str(e)}"
                self.logger.error(error_msg)
                errors.append(error_msg)
                result_df = df  # 返回原始数据
        else:
            result_df = df  # 返回原始数据

        # 计算性能指标
        execution_time = time.time() - start_time
        performance_metrics = {
            'total_chunks': len(chunks),
            'successful_chunks': len(processed_chunks),
            'failed_chunks': len(chunks) - len(processed_chunks),
            'avg_chunk_size': len(df) / len(chunks) if chunks else 0,
            'throughput_rows_per_sec': len(result_df) / execution_time if execution_time > 0 else 0
        }

        result = ProcessingResult(
            success=len(processed_chunks) > 0,
            processed_chunks=len(processed_chunks),
            total_rows=len(result_df),
            execution_time=execution_time,
            errors=errors,
            performance_metrics=performance_metrics
        )

        self.logger.info(f"并行处理完成，耗时: {execution_time:.2f}秒")
        return result_df, result

    def _split_data_into_chunks(self, df: pl.DataFrame) -> List[pl.DataFrame]:
        """将数据分割成块"""
        if len(df) <= self.config.chunk_size:
            return [df]

        chunks = []
        for i in range(0, len(df), self.config.chunk_size):
            chunk = df.slice(i, self.config.chunk_size)
            chunks.append(chunk)

        return chunks

    def _get_executor(self):
        """获取执行器"""
        if self.config.use_processes:
            return ProcessPoolExecutor(max_workers=self.config.max_workers)
        else:
            return ThreadPoolExecutor(max_workers=self.config.max_workers)

    def _process_chunk(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个数据块"""
        chunk_id = task['chunk_id']
        data = task['data']
        indicators = task['indicators']

        try:
            self.logger.debug(f"开始处理块 {chunk_id}, 数据行数: {len(data)}")

            # 对每个指标进行计算
            processed_data = data.clone()

            for indicator in indicators:
                try:
                    processed_data = self.indicator_calculator.calculate_indicators(
                        processed_data, [indicator]
                    )
                except Exception as e:
                    error_msg = f"块 {chunk_id} 计算指标 {indicator} 失败: {str(e)}"
                    self.logger.warning(error_msg)
                    if self.config.error_handling == "stop":
                        raise Exception(error_msg)
                    # 对于 "continue" 模式，继续处理其他指标

            return {
                'success': True,
                'data': processed_data,
                'errors': []
            }

        except Exception as e:
            error_msg = f"块 {chunk_id} 处理失败: {str(e)}"
            return {
                'success': False,
                'data': data,  # 返回原始数据
                'errors': [error_msg]
            }


class CalculatedIndicatorsProcessor:
    """计算指标处理器 - 协调器"""

    def __init__(self, processing_config: Optional[ProcessingConfig] = None):
        """初始化处理器"""
        if not is_calculated_indicators_enabled():
            raise ValueError("计算指标功能未启用，请在配置中设置 enabled: true")

        self.enabled_indicators = get_enabled_calculated_indicators()
        self.processing_options = get_calculated_indicators_processing_options()
        self.logger = logging.getLogger(__name__)

        # 初始化并行处理器
        self.parallel_processor = ParallelIndicatorsProcessor(processing_config)

        self.logger.info("计算指标处理器初始化完成")
        self.logger.info(f"启用指标数量: {len(self.enabled_indicators)}")
        self.logger.info(f"处理选项: {self.processing_options}")

    def calculate_all_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算所有启用的指标"""
        if not is_auto_calculate_enabled():
            self.logger.info("自动计算已禁用，跳过指标计算")
            return df

        indicators_to_calculate = list(self.enabled_indicators.keys())
        self.logger.info(f"开始计算 {len(indicators_to_calculate)} 个指标")

        # 使用并行处理器
        result_df, processing_result = self.parallel_processor.process_data_parallel(
            df, indicators_to_calculate
        )

        if processing_result.errors:
            self.logger.warning(f"处理过程中出现 {len(processing_result.errors)} 个错误")
            for error in processing_result.errors[:5]:  # 只显示前5个错误
                self.logger.warning(error)

        self.logger.info("指标计算完成")
        return result_df

    def calculate_indicator(self, df: pl.DataFrame, indicator_name: str) -> pl.DataFrame:
        """计算单个指标"""
        if indicator_name not in self.enabled_indicators:
            raise ValueError(f"指标 {indicator_name} 未启用或不存在")

        self.logger.info(f"计算指标: {indicator_name}")

        # 使用并行处理器处理单个指标
        result_df, processing_result = self.parallel_processor.process_data_parallel(
            df, [indicator_name]
        )

        if not processing_result.success:
            raise Exception(f"指标 {indicator_name} 计算失败: {processing_result.errors}")

        return result_df

    def calculate_indicators_batch(self, df: pl.DataFrame, indicators: List[str]) -> pl.DataFrame:
        """批量计算指定指标"""
        available_indicators = [ind for ind in indicators if ind in self.enabled_indicators]
        if not available_indicators:
            self.logger.warning("没有可用的指标进行计算")
            return df

        unavailable = [ind for ind in indicators if ind not in self.enabled_indicators]
        if unavailable:
            self.logger.warning(f"以下指标不可用: {unavailable}")

        self.logger.info(f"批量计算 {len(available_indicators)} 个指标")

        # 使用并行处理器
        result_df, processing_result = self.parallel_processor.process_data_parallel(
            df, available_indicators
        )

        return result_df

    def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计信息"""
        return {
            'enabled_indicators_count': len(self.enabled_indicators),
            'available_indicators': list(self.enabled_indicators.keys()),
            'processing_config': {
                'max_workers': self.parallel_processor.config.max_workers,
                'chunk_size': self.parallel_processor.config.chunk_size,
                'use_processes': self.parallel_processor.config.use_processes
            }
        }

    def validate_data_for_indicators(self, df: pl.DataFrame) -> Dict[str, bool]:
        """验证数据是否满足所有指标的计算要求"""
        results = {}
        for indicator_name in self.enabled_indicators.keys():
            # 使用配置验证依赖关系
            results[indicator_name] = validate_calculated_indicator_dependencies(
                indicator_name, df.columns
            )
        return results

    def get_indicator_info(self, indicator_name: str) -> Optional[Dict[str, Any]]:
        """获取指标信息"""
        return self.enabled_indicators.get(indicator_name)

    def list_available_indicators(self) -> List[str]:
        """列出所有可用的指标"""
        return list(self.enabled_indicators.keys())


def process_calculated_indicators(
    data_file: str,
    output_file: Optional[str] = None,
    processing_config: Optional[ProcessingConfig] = None
) -> str:
    """处理数据文件，计算所有指标（并行版本）"""
    logger.info(f"处理数据文件: {data_file}")

    # 读取数据
    df = pl.read_parquet(data_file)
    logger.info(f"原始数据形状: {df.shape}")

    # 初始化处理器
    processor = CalculatedIndicatorsProcessor(processing_config)

    # 验证数据
    validation_results = processor.validate_data_for_indicators(df)
    valid_indicators = [k for k, v in validation_results.items() if v]
    invalid_indicators = [k for k, v in validation_results.items() if not v]

    if invalid_indicators:
        logger.warning(f"以下指标无法计算（缺少依赖字段）: {invalid_indicators}")

    logger.info(f"可以计算的指标: {valid_indicators}")

    # 计算指标
    processed_df = processor.calculate_all_indicators(df)
    logger.info(f"处理后数据形状: {processed_df.shape}")

    # 保存结果
    if output_file is None:
        base_name = Path(data_file).stem
        output_file = f"{base_name}_with_indicators.parquet"

    output_path = Path("data") / output_file
    processed_df.write_parquet(output_path)
    logger.info(f"结果保存到: {output_path}")

    return str(output_path)


# 全局处理器实例
_calculated_indicators_processor = None

def get_calculated_indicators_processor(processing_config: Optional[ProcessingConfig] = None):
    """获取全局计算指标处理器实例"""
    global _calculated_indicators_processor
    if _calculated_indicators_processor is None:
        _calculated_indicators_processor = CalculatedIndicatorsProcessor(processing_config)
    return _calculated_indicators_processor


if __name__ == "__main__":
    # 测试代码
    print("=== 并行计算指标处理器测试 ===")

    # 创建处理配置
    config = ProcessingConfig(
        max_workers=2,
        chunk_size=5000,
        use_processes=False,  # 使用线程以避免序列化问题
        enable_progress_tracking=True
    )

    processor = CalculatedIndicatorsProcessor(config)

    stats = processor.get_processing_stats()
    print(f"启用指标数量: {stats['enabled_indicators_count']}")
    print(f"可用指标: {stats['available_indicators']}")
    print(f"处理配置: {stats['processing_config']}")

    # 如果有数据文件，测试处理
    data_dir = Path("data")
    if data_dir.exists():
        parquet_files = list(data_dir.glob("*.parquet"))
        if parquet_files:
            test_file = str(parquet_files[0])
            print(f"\n=== 测试并行处理文件: {test_file} ===")
            try:
                result_file = process_calculated_indicators(test_file, processing_config=config)
                print(f"✅ 并行处理完成，结果文件: {result_file}")
            except Exception as e:
                print(f"❌ 处理失败: {e}")
