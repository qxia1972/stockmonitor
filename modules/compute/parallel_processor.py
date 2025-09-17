"""
并行处理器 (Parallel Processor)
实现高性能的并行数据处理
"""

import polars as pl
from typing import Dict, Any, Optional, Union, List, Callable
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
import logging
import os

logger = logging.getLogger(__name__)


class ParallelProcessor:
    """并行处理器"""

    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or min(32, multiprocessing.cpu_count() * 2)
        # 延迟导入以避免循环依赖
        self._data_processor = None
        self._indicator_calculator = None
        self._score_calculator = None

    @property
    def data_processor(self):
        if self._data_processor is None:
            from .data_processor import DataProcessor
            self._data_processor = DataProcessor()
        return self._data_processor

    @property
    def indicator_calculator(self):
        if self._indicator_calculator is None:
            from .indicator_calculator import IndicatorCalculator
            self._indicator_calculator = IndicatorCalculator()
        return self._indicator_calculator

    @property
    def score_calculator(self):
        if self._score_calculator is None:
            from .score_calculator import ScoreCalculator
            self._score_calculator = ScoreCalculator()
        return self._score_calculator

    def process_batch_indicators(self,
                               price_data_dict: Dict[str, pl.DataFrame],
                               indicators: Optional[List[str]] = None,
                               use_processes: bool = True) -> Dict[str, pl.DataFrame]:
        """并行计算批量技术指标"""
        if not price_data_dict:
            return {}

        logger.info(f"开始并行计算技术指标，共 {len(price_data_dict)} 个股票")

        # 准备任务
        tasks = []
        for stock_code, price_df in price_data_dict.items():
            task = {
                'stock_code': stock_code,
                'data': price_df,
                'indicators': indicators,
                'task_type': 'indicators'
            }
            tasks.append(task)

        # 执行并行处理
        results = self._execute_parallel_tasks(tasks, use_processes)

        # 整理结果
        processed_data = {}
        for result in results:
            if result['success']:
                processed_data[result['stock_code']] = result['data']
            else:
                logger.error(f"处理失败 {result['stock_code']}: {result.get('error', 'Unknown error')}")
                # 返回原始数据
                processed_data[result['stock_code']] = price_data_dict[result['stock_code']]

        logger.info(f"并行计算完成，成功处理 {len(processed_data)} 个股票")
        return processed_data

    def process_batch_scores(self,
                           factor_data_dict: Dict[str, pl.DataFrame],
                           scoring_rules: Optional[Dict[str, Dict]] = None,
                           use_processes: bool = True) -> Dict[str, pl.DataFrame]:
        """并行计算批量评分"""
        if not factor_data_dict:
            return {}

        logger.info(f"开始并行计算评分，共 {len(factor_data_dict)} 个股票")

        # 准备任务
        tasks = []
        for stock_code, factor_df in factor_data_dict.items():
            task = {
                'stock_code': stock_code,
                'data': factor_df,
                'scoring_rules': scoring_rules,
                'task_type': 'scores'
            }
            tasks.append(task)

        # 执行并行处理
        results = self._execute_parallel_tasks(tasks, use_processes)

        # 整理结果
        processed_data = {}
        for result in results:
            if result['success']:
                processed_data[result['stock_code']] = result['data']
            else:
                logger.error(f"评分计算失败 {result['stock_code']}: {result.get('error', 'Unknown error')}")
                # 返回原始数据
                processed_data[result['stock_code']] = factor_data_dict[result['stock_code']]

        logger.info(f"并行评分计算完成，成功处理 {len(processed_data)} 个股票")
        return processed_data

    def process_combined_analysis(self,
                                data_dict: Dict[str, Dict[str, pl.DataFrame]],
                                use_processes: bool = True) -> Dict[str, pl.DataFrame]:
        """并行执行完整的股票分析流程（指标计算 + 评分）"""
        if not data_dict:
            return {}

        logger.info(f"开始并行执行完整分析，共 {len(data_dict)} 个股票")

        # 准备任务
        tasks = []
        for stock_code, data_bundle in data_dict.items():
            task = {
                'stock_code': stock_code,
                'price_data': data_bundle.get('price_data'),
                'factor_data': data_bundle.get('factor_data'),
                'task_type': 'combined'
            }
            tasks.append(task)

        # 执行并行处理
        results = self._execute_parallel_tasks(tasks, use_processes)

        # 整理结果
        processed_data = {}
        for result in results:
            if result['success']:
                processed_data[result['stock_code']] = result['data']
            else:
                logger.error(f"完整分析失败 {result['stock_code']}: {result.get('error', 'Unknown error')}")

        logger.info(f"并行完整分析完成，成功处理 {len(processed_data)} 个股票")
        return processed_data

    def _execute_parallel_tasks(self, tasks: List[Dict], use_processes: bool = True) -> List[Dict]:
        """执行并行任务（优化版：减少导入开销和提高效率）"""
        if not tasks:
            return []

        if use_processes:
            executor_class = ProcessPoolExecutor
        else:
            executor_class = ThreadPoolExecutor

        results = []

        # 分批处理任务，避免一次性提交太多任务
        batch_size = min(50, len(tasks))  # 每批最多50个任务

        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            logger.info(f"处理任务批次 {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size}，包含 {len(batch_tasks)} 个任务")

            with executor_class(max_workers=self.max_workers) as executor:
                # 提交批次任务
                future_to_task = {
                    executor.submit(self._process_single_task_optimized, task): task
                    for task in batch_tasks
                }

                # 收集批次结果
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result(timeout=300)  # 5分钟超时
                        results.append(result)
                    except Exception as e:
                        logger.error(f"任务执行失败 {task['stock_code']}: {e}")
                        # 返回失败结果
                        results.append({
                            'stock_code': task['stock_code'],
                            'success': False,
                            'error': str(e),
                            'data': None
                        })

        return results

    def _process_single_task_optimized(self, task: Dict) -> Dict:
        """处理单个任务（优化版：减少重复导入）"""
        stock_code = task['stock_code']
        task_type = task['task_type']

        try:
            # 延迟导入，只在需要时导入
            if not hasattr(self, '_data_processor'):
                from .data_processor import DataProcessor
                from .indicator_calculator import IndicatorCalculator
                from .score_calculator import ScoreCalculator

                self._data_processor = DataProcessor()
                self._indicator_calculator = IndicatorCalculator()
                self._score_calculator = ScoreCalculator()

            if task_type == 'indicators':
                # 计算技术指标
                price_data = task['data']
                indicators = task.get('indicators')

                result_data = self._indicator_calculator._calculate_single_indicators(
                    price_data, indicators
                )

            elif task_type == 'scores':
                # 计算评分
                factor_data = task['data']
                scoring_rules = task.get('scoring_rules')

                result_data = self._score_calculator._calculate_single_scores(
                    factor_data, scoring_rules
                )

            elif task_type == 'combined':
                # 执行完整分析
                price_data = task.get('price_data')
                factor_data = task.get('factor_data')

                result_data = {}

                if price_data is not None:
                    # 计算技术指标
                    price_with_indicators = self._indicator_calculator._calculate_single_indicators(
                        price_data, None
                    )
                    result_data['price_data'] = price_with_indicators

                if factor_data is not None:
                    # 计算评分
                    factor_with_scores = self._score_calculator._calculate_single_scores(
                        factor_data, None
                    )
                    result_data['factor_data'] = factor_with_scores

            else:
                raise ValueError(f"未知任务类型: {task_type}")

            return {
                'stock_code': stock_code,
                'success': True,
                'data': result_data,
                'error': None
            }

        except Exception as e:
            logger.exception(f"处理任务失败 {stock_code}: {e}")
            return {
                'stock_code': stock_code,
                'success': False,
                'data': None,
                'error': str(e)
            }

    def optimize_memory_usage(self, df: pl.DataFrame) -> pl.DataFrame:
        """优化内存使用"""
        # 使用DataProcessor的优化功能
        return self.data_processor.optimize_dataframe(df)

    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        return {
            'max_workers': self.max_workers,
            'cpu_count': multiprocessing.cpu_count(),
            'available_memory': self._get_available_memory(),
            'timestamp': datetime.now().isoformat()
        }

    def _get_available_memory(self) -> float:
        """获取可用内存（MB）"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return memory.available / 1024 / 1024  # MB
        except ImportError:
            return 0.0

    def configure_worker_settings(self, settings: Dict[str, Any]):
        """配置工作进程设置"""
        # 可以在这里设置进程特定的配置
        # 例如：内存限制、超时时间等
        pass

    def cleanup_resources(self):
        """清理资源"""
        # 清理可能存在的临时资源
        pass


# 全局并行处理器实例
parallel_processor = ParallelProcessor()