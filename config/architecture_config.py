"""
新架构配置文件
"""

from pathlib import Path
from typing import Dict, Any

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据目录结构
DATA_CONFIG = {
    'base_path': PROJECT_ROOT / 'data',
    'basic_data': 'basic/stocks.parquet',
    'indicators': 'indicators/technical.parquet',
    'scores': 'scores/stock_scores.parquet',
    'cache': PROJECT_ROOT / 'cache',
    'logs': PROJECT_ROOT / 'logs',
}

# 编排层配置
ORCHESTRATION_CONFIG = {
    'dagster_home': PROJECT_ROOT / '.dagster',
    'pipeline_timeout': 3600,  # 1小时
    'max_retries': 3,
    'retry_delay': 60,  # 60秒
}

# 计算层配置
COMPUTE_CONFIG = {
    'polars_threads': None,  # 使用默认线程数
    'memory_limit': '4GB',
    'lazy_evaluation': True,
    'string_cache': True,
}

# 存储层配置
STORAGE_CONFIG = {
    'parquet_compression': 'snappy',
    'parquet_row_group_size': 100000,
    'partition_columns': {
        'stocks': ['date', 'market'],
        'indicators': ['date', 'stock_code'],
        'scores': ['date'],
    },
    'cleanup_days': 90,  # 保留90天的历史数据
}

# 查询层配置
QUERY_CONFIG = {
    'duckdb_memory_limit': '2GB',
    'query_timeout': 300,  # 5分钟
    'enable_caching': True,
    'cache_size': '1GB',
}

# 性能监控配置
MONITORING_CONFIG = {
    'enable_metrics': True,
    'metrics_interval': 60,  # 每60秒收集一次
    'log_level': 'INFO',
    'performance_logs': PROJECT_ROOT / 'logs' / 'performance.log',
}

# 开发环境配置
DEVELOPMENT_CONFIG = {
    'debug_mode': True,
    'test_data_size': 1000,  # 测试数据量
    'mock_external_apis': False,
}

def get_config() -> Dict[str, Any]:
    """获取完整配置"""
    return {
        'data': DATA_CONFIG,
        'orchestration': ORCHESTRATION_CONFIG,
        'compute': COMPUTE_CONFIG,
        'storage': STORAGE_CONFIG,
        'query': QUERY_CONFIG,
        'monitoring': MONITORING_CONFIG,
        'development': DEVELOPMENT_CONFIG,
    }

# 全局配置实例
config = get_config()