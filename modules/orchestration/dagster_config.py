"""
Dagster配置和运行时设置 (Dagster Configuration and Runtime Settings)

配置Dagster作业的运行参数、资源、调度和监控设置
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
import os
from dagster import (
    ConfigurableResource, RunConfig, resource, InitResourceContext,
    make_values_resource, define_asset_job, AssetSelection
)
from dagster import get_dagster_logger
import polars as pl

# 导入网络配置
from networks.rqdatac_config import RQDatacConfig

# ===== 资源定义 =====

@resource(
    config_schema={
        "data_dir": str,
        "cache_dir": str,
        "log_dir": str,
        "max_workers": int,
        "timeout": int
    }
)
def file_system_resource(context: InitResourceContext) -> Dict[str, Any]:
    """文件系统资源配置"""
    config = context.resource_config

    # 确保目录存在
    for dir_path in [config["data_dir"], config["cache_dir"], config["log_dir"]]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    return {
        "data_dir": Path(config["data_dir"]),
        "cache_dir": Path(config["cache_dir"]),
        "log_dir": Path(config["log_dir"]),
        "max_workers": config["max_workers"],
        "timeout": config["timeout"]
    }


@resource(
    config_schema={
        "host": str,
        "port": int,
        "database": str,
        "username": str,
        "password": str,
        "pool_size": int
    }
)
def database_resource(context: InitResourceContext) -> Dict[str, Any]:
    """数据库资源配置"""
    config = context.resource_config

    return {
        "connection_string": f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}",
        "pool_size": config["pool_size"]
    }


@resource(
    config_schema={
        "api_key": str,
        "base_url": str,
        "timeout": int,
        "retry_attempts": int
    }
)
def rqdatac_resource(context: InitResourceContext) -> Dict[str, Any]:
    """RQDatac资源配置"""
    config = context.resource_config

    return {
        "api_key": config["api_key"],
        "base_url": config["base_url"],
        "timeout": config["timeout"],
        "retry_attempts": config["retry_attempts"]
    }


# ===== 配置类定义 =====

class ProcessingConfig:
    """处理配置类"""

    def __init__(self):
        self.data_dir = "data"
        self.cache_dir = "cache"
        self.log_dir = "logs"
        self.max_workers = 4
        self.timeout = 300
        self.target_stocks = []  # 为空时自动获取全量沪深股票（排除ST/PT）
        self.completion_days = 30
        self.indicators = ["sma_20", "rsi_14", "macd", "pe_ratio", "pb_ratio", "roe"]
        self.compression = "snappy"
        self.batch_size = 1000

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "data_dir": self.data_dir,
            "cache_dir": self.cache_dir,
            "log_dir": self.log_dir,
            "max_workers": self.max_workers,
            "timeout": self.timeout,
            "target_stocks": self.target_stocks,
            "completion_days": self.completion_days,
            "indicators": self.indicators,
            "compression": self.compression,
            "batch_size": self.batch_size
        }


class DatabaseConfig:
    """数据库配置类"""

    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv("DB_NAME", "stockmonitor")
        self.username = os.getenv("DB_USER", "postgres")
        self.password = os.getenv("DB_PASSWORD", "")
        self.pool_size = 10

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "pool_size": self.pool_size
        }


# ===== 运行配置生成器 =====

def create_run_config(job_type: str, custom_config: Optional[Dict[str, Any]] = None) -> RunConfig:
    """创建作业运行配置"""
    base_config = ProcessingConfig().to_dict()

    if custom_config:
        base_config.update(custom_config)

    # 根据作业类型设置特定配置
    if job_type == "sync":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_dates": {"config": {"days_back": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "completion":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_dates": {"config": {"days_back": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "calculation":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_dates": {"config": {"days_back": base_config["completion_days"]}},
                "calculate_technical_indicators": {"config": {"indicators": base_config["indicators"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()}
            }
        }
    elif job_type == "scoring":
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_dates": {"config": {"days_back": base_config["completion_days"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()},
                "database": {"config": DatabaseConfig().to_dict()}
            }
        }
    else:  # full_pipeline
        config = {
            "ops": {
                "get_target_stocks": {"config": {"target_stocks": base_config["target_stocks"]}},
                "get_trading_dates": {"config": {"days_back": base_config["completion_days"]}},
                "calculate_technical_indicators": {"config": {"indicators": base_config["indicators"]}}
            },
            "resources": {
                "file_system": {"config": {
                    "data_dir": base_config["data_dir"],
                    "cache_dir": base_config["cache_dir"],
                    "log_dir": base_config["log_dir"],
                    "max_workers": base_config["max_workers"],
                    "timeout": base_config["timeout"]
                }},
                "rqdatac": {"config": RQDatacConfig().to_dict()},
                "database": {"config": DatabaseConfig().to_dict()}
            }
        }

    return RunConfig(**config)


# ===== 环境配置 =====

def get_environment_config(env: str = "development") -> Dict[str, Any]:
    """获取环境配置"""
    if env == "production":
        return {
            "processing": ProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 5},
            "logging": {"level": "INFO", "format": "json"}
        }
    elif env == "staging":
        return {
            "processing": ProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 3},
            "logging": {"level": "DEBUG", "format": "text"}
        }
    else:  # development
        return {
            "processing": ProcessingConfig(),
            "database": DatabaseConfig(),
            "rqdatac": RQDatacConfig(),
            "concurrency": {"max_concurrent_runs": 2},
            "logging": {"level": "DEBUG", "format": "text"}
        }


# ===== 作业定义集合 =====

def get_all_job_definitions():
    """获取所有作业定义"""
    from .pipeline_definitions import (
        stock_analysis_job,
        daily_data_sync_job,
        data_completion_job,
        daily_full_pipeline_job
    )

    return [
        stock_analysis_job,
        daily_data_sync_job,
        data_completion_job,
        daily_full_pipeline_job
    ]


def get_all_schedule_definitions():
    """获取所有调度定义"""
    from .pipeline_definitions import (
        daily_full_pipeline_schedule
    )

    return [
        daily_full_pipeline_schedule
    ]


def get_all_sensor_definitions():
    """获取所有传感器定义"""
    from .pipeline_definitions import (
        daily_processing_success_monitor,
        daily_processing_failure_monitor
    )

    return [
        daily_processing_success_monitor,
        daily_processing_failure_monitor
    ]


# ===== 资源定义集合 =====

def get_all_resource_definitions():
    """获取所有资源定义"""
    return {
        "file_system": file_system_resource,
        "database": database_resource,
        "rqdatac": rqdatac_resource
    }


# ===== 配置验证 =====

def validate_configurations():
    """验证配置完整性"""
    logger = get_dagster_logger()

    try:
        # 验证处理配置
        processing_config = ProcessingConfig()
        assert processing_config.data_dir, "data_dir is required"
        assert processing_config.target_stocks, "target_stocks is required"

        # 验证数据库配置
        db_config = DatabaseConfig()
        assert db_config.host, "DB_HOST is required"
        assert db_config.database, "DB_NAME is required"

        # 验证RQDatac配置
        rq_config = RQDatacConfig()
        assert rq_config.api_key, "RQDATAC_API_KEY is required"

        logger.info("✅ 所有配置验证通过")
        return True

    except Exception as e:
        logger.error(f"❌ 配置验证失败: {e}")
        return False


# ===== 使用示例 =====

if __name__ == "__main__":
    print("=== Dagster配置演示 ===")

    # 验证配置
    if validate_configurations():
        print("✅ 配置验证成功")

        # 显示配置示例
        processing_config = ProcessingConfig()
        print(f"📁 数据目录: {processing_config.data_dir}")
        print(f"🎯 目标股票数量: {len(processing_config.target_stocks)}")
        print(f"📊 指标列表: {processing_config.indicators}")

        # 创建运行配置示例
        run_config = create_run_config("sync")
        print("✅ 运行配置创建成功")

    else:
        print("❌ 配置验证失败，请检查环境变量和配置")
