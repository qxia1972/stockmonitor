"""
查询引擎 (Query Engine)
基于DuckDB的高性能数据查询和分析
"""

import duckdb
import polars as pl
from typing import Dict, Any, Optional, Union, List, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import logging
import json
import csv
from functools import lru_cache
import hashlib

logger = logging.getLogger(__name__)


class QueryCache:
    """查询缓存管理器"""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _get_cache_key(self, query: str, params: tuple) -> str:
        """生成缓存键"""
        key_data = f"{query}_{params}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def get(self, query: str, params: tuple) -> Optional[pl.DataFrame]:
        """获取缓存的查询结果"""
        cache_key = self._get_cache_key(query, params)
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if datetime.now() - entry['timestamp'] < timedelta(minutes=5):  # 5分钟缓存
                return entry['result']
            else:
                # 缓存过期，删除
                del self._cache[cache_key]
        return None

    def set(self, query: str, params: tuple, result: pl.DataFrame):
        """设置查询结果缓存"""
        cache_key = self._get_cache_key(query, params)

        # 如果缓存满了，删除最旧的条目
        if len(self._cache) >= self.max_size:
            oldest_key = min(self._cache.keys(),
                           key=lambda k: self._cache[k]['timestamp'])
            del self._cache[oldest_key]

        self._cache[cache_key] = {
            'result': result,
            'timestamp': datetime.now()
        }


class QueryOptimizer:
    """查询优化器"""

    def __init__(self, query_engine):
        self.query_engine = query_engine

    def optimize_query(self, query: str) -> str:
        """优化SQL查询"""
        # 基本优化规则
        optimized = query

        # 1. 添加适当的索引提示（如果适用）
        if 'WHERE' in query.upper():
            optimized = self._add_index_hints(optimized)

        # 2. 优化JOIN顺序
        if 'JOIN' in query.upper():
            optimized = self._optimize_joins(optimized)

        # 3. 优化聚合查询
        if any(keyword in query.upper() for keyword in ['GROUP BY', 'COUNT', 'SUM', 'AVG']):
            optimized = self._optimize_aggregations(optimized)

        return optimized

    def _add_index_hints(self, query: str) -> str:
        """添加索引提示"""
        # 这里可以根据查询模式添加索引建议
        return query

    def _optimize_joins(self, query: str) -> str:
        """优化JOIN操作"""
        # 基本JOIN优化
        return query

    def _optimize_aggregations(self, query: str) -> str:
        """优化聚合查询"""
        # 聚合查询优化
        return query

    def analyze_query_performance(self, query: str) -> Dict[str, Any]:
        """分析查询性能"""
        conn = self.query_engine._get_connection()

        # 使用EXPLAIN分析查询计划
        explain_query = f"EXPLAIN {query}"
        try:
            result = conn.execute(explain_query).fetchall()
            return {
                'query_plan': result,
                'estimated_cost': self._estimate_cost(result),
                'optimization_suggestions': self._get_suggestions(result)
            }
        except Exception as e:
            logger.warning(f"Failed to analyze query: {e}")
            return {'error': str(e)}

    def _estimate_cost(self, query_plan) -> float:
        """估算查询成本"""
        # 简单的成本估算
        return 1.0

    def _get_suggestions(self, query_plan) -> List[str]:
        """获取优化建议"""
        suggestions = []
        # 基于查询计划提供优化建议
        return suggestions


class QueryEngine:
    """DuckDB查询引擎"""

    def __init__(self, database_path: Optional[str] = None, enable_cache: bool = True):
        self.database_path = database_path
        self._connection = None
        self._cache = QueryCache() if enable_cache else None
        self._optimizer = QueryOptimizer(self)
        self._indexes: Dict[str, List[str]] = {}

    def _get_connection(self):
        """获取数据库连接"""
        if self._connection is None:
            if self.database_path:
                self._connection = duckdb.connect(self.database_path)
            else:
                self._connection = duckdb.connect(':memory:')

            # 配置DuckDB
            self._configure_connection()

        return self._connection

    def _configure_connection(self):
        """配置数据库连接"""
        if self._connection is None:
            return

        conn = self._connection

        # 设置内存限制
        conn.execute("SET memory_limit = '2GB'")

        # 启用并行执行
        conn.execute("SET threads = 4")

        # DuckDB默认启用优化器，不需要额外设置

    def execute_query(self, query: str, use_cache: bool = True, *args, **params) -> pl.DataFrame:
        """执行SQL查询"""
        cache_key = None

        # 检查缓存
        if self._cache and use_cache:
            cache_key = (query, tuple(sorted(params.items())))
            cached_result = self._cache.get(query, cache_key[1])
            if cached_result is not None:
                logger.info("Query result from cache")
                return cached_result

        # 优化查询
        optimized_query = self._optimizer.optimize_query(query)

        conn = self._get_connection()

        try:
            start_time = datetime.now()

            if params:
                # 使用命名参数
                param_values = list(params.values())
                result = conn.execute(optimized_query, param_values)
            elif args:
                # 使用位置参数
                result = conn.execute(optimized_query, list(args))
            else:
                result = conn.execute(optimized_query)

            # 转换为Polars DataFrame
            arrow_table = result.fetch_arrow_table()
            df = pl.DataFrame(arrow_table)

            execution_time = (datetime.now() - start_time).total_seconds()

            logger.info(f"Query executed in {execution_time:.3f}s")

            # 缓存结果
            if self._cache and use_cache and cache_key:
                self._cache.set(query, cache_key[1], df)

            return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def load_parquet_table(self, table_name: str, file_path: Union[str, Path],
                          create_indexes: bool = True):
        """加载Parquet文件作为表"""
        conn = self._get_connection()
        file_path = Path(file_path)

        create_query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_parquet('{file_path}')
        """

        conn.execute(create_query)

        # 创建索引以提高查询性能
        if create_indexes:
            self._create_table_indexes(table_name)

        logger.info(f"Loaded Parquet table: {table_name}")

    def _create_table_indexes(self, table_name: str):
        """为表创建索引"""
        conn = self._get_connection()

        # 获取表结构
        schema = self.get_table_schema(table_name)

        indexes_created = []

        # 为常用列创建索引
        index_columns = []
        if 'symbol' in schema:
            index_columns.append('symbol')
        if 'date' in schema:
            index_columns.append('date')
        if 'timestamp' in schema:
            index_columns.append('timestamp')

        for column in index_columns:
            try:
                index_name = f"idx_{table_name}_{column}"
                conn.execute(f"CREATE INDEX {index_name} ON {table_name}({column})")
                indexes_created.append(index_name)
            except Exception as e:
                logger.warning(f"Failed to create index on {column}: {e}")

        self._indexes[table_name] = indexes_created

        if indexes_created:
            logger.info(f"Created indexes for {table_name}: {indexes_created}")

    def query_parquet_file(self, file_path: Union[str, Path], conditions: str = "",
                          columns: Optional[List[str]] = None) -> pl.DataFrame:
        """直接查询Parquet文件"""
        file_path = Path(file_path)

        # 构建查询
        select_clause = "*" if columns is None else ", ".join(columns)

        base_query = f"SELECT {select_clause} FROM read_parquet('{file_path}')"

        if conditions:
            full_query = f"{base_query} WHERE {conditions}"
        else:
            full_query = base_query

        return self.execute_query(full_query)

    def create_view(self, view_name: str, query: str):
        """创建视图"""
        conn = self._get_connection()
        create_view_query = f"CREATE OR REPLACE VIEW {view_name} AS {query}"
        conn.execute(create_view_query)
        logger.info(f"Created view: {view_name}")

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """获取表结构"""
        conn = self._get_connection()

        schema_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """

        result = conn.execute(schema_query).fetchall()
        return {row[0]: row[1] for row in result}

    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """分析表统计信息"""
        conn = self._get_connection()

        # 基本统计信息
        count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        count_result = conn.execute(count_query).fetchone()
        row_count = count_result[0] if count_result else 0

        # 列统计
        columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        columns = [row[0] for row in conn.execute(columns_query).fetchall()]

        # 索引信息
        indexes = self._indexes.get(table_name, [])

        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': columns,
            'column_count': len(columns),
            'indexes': indexes,
            'index_count': len(indexes),
        }

    def export_results(self, df: pl.DataFrame, output_path: Union[str, Path],
                      format: str = 'parquet', **kwargs):
        """导出查询结果"""
        output_path = Path(output_path)

        if format.lower() == 'parquet':
            df.write_parquet(output_path, **kwargs)
        elif format.lower() == 'csv':
            df.write_csv(output_path, **kwargs)
        elif format.lower() == 'json':
            df.write_json(output_path, **kwargs)
        else:
            raise ValueError(f"Unsupported export format: {format}")

        logger.info(f"Exported results to {output_path} in {format} format")

    def batch_query(self, queries: List[Tuple[str, Dict[str, Any]]]) -> List[pl.DataFrame]:
        """批量执行查询"""
        results = []

        for query, params in queries:
            try:
                result = self.execute_query(query, **params)
                results.append(result)
            except Exception as e:
                logger.error(f"Batch query failed: {e}")
                results.append(pl.DataFrame())  # 返回空DataFrame

        return results

    def get_query_performance_stats(self) -> Dict[str, Any]:
        """获取查询性能统计"""
        # 这里可以实现查询性能监控
        return {
            'total_queries': 0,
            'cache_hit_rate': 0.0,
            'avg_execution_time': 0.0,
        }

    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None


# 全局查询引擎实例
query_engine = QueryEngine()