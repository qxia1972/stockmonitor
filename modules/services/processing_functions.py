"""
处理函数库 (Processing Functions Library)
简化的处理函数集合，直接调用底层的计算类
支持多种数据源：文件、RQDatac API、数据库等
"""

import logging
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timedelta
import polars as pl

from modules.compute.data_processor import DataProcessor
from modules.compute.indicator_calculator import IndicatorCalculator
from modules.compute.stock_scorer import StockScorer
from modules.compute.parallel_processor import ParallelProcessor
from modules.data_model import data_model
from networks.rqdatac_data_loader import get_rqdatac_data_loader

logger = logging.getLogger(__name__)

# ===== 全局实例管理（惰性初始化） =====

_data_processor = None
_indicator_calculator = None
_score_calculator = None
_parallel_processor = None

def get_data_processor() -> DataProcessor:
    """获取数据处理器实例（惰性初始化）"""
    global _data_processor
    if _data_processor is None:
        _data_processor = DataProcessor()
    return _data_processor

def get_indicator_calculator() -> IndicatorCalculator:
    """获取指标计算器实例（惰性初始化）"""
    global _indicator_calculator
    if _indicator_calculator is None:
        _indicator_calculator = IndicatorCalculator()
    return _indicator_calculator

def get_score_calculator() -> StockScorer:
    """获取评分计算器实例（惰性初始化）"""
    global _score_calculator
    if _score_calculator is None:
        _score_calculator = StockScorer()
    return _score_calculator

def get_parallel_processor() -> ParallelProcessor:
    """获取并行处理器实例（惰性初始化）"""
    global _parallel_processor
    if _parallel_processor is None:
        _parallel_processor = ParallelProcessor()
    return _parallel_processor

# ===== 缓存机制 =====

from functools import lru_cache
import hashlib

# 计算结果缓存
_calculation_cache = {}

def _get_cache_key(func_name: str, *args, **kwargs) -> str:
    """生成缓存键"""
    key_data = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
    return hashlib.md5(key_data.encode()).hexdigest()

@lru_cache(maxsize=100)
def calculate_indicators_cached(data, indicators=None, **kwargs):
    """带缓存的指标计算函数"""
    cache_key = _get_cache_key('calculate_indicators', data.shape if hasattr(data, 'shape') else len(data), indicators)

    if cache_key in _calculation_cache:
        logger.info("使用缓存的指标计算结果")
        return _calculation_cache[cache_key]

    result = calculate_indicators(data, indicators, **kwargs)
    _calculation_cache[cache_key] = result

    return result

@lru_cache(maxsize=100)
def calculate_scores_cached(data, score_type="technical", **kwargs):
    """带缓存的评分计算函数"""
    cache_key = _get_cache_key('calculate_scores', data.shape if hasattr(data, 'shape') else len(data), score_type)

    if cache_key in _calculation_cache:
        logger.info("使用缓存的评分计算结果")
        return _calculation_cache[cache_key]

    result = calculate_scores(data, score_type, **kwargs)
    _calculation_cache[cache_key] = result

    return result

def clear_calculation_cache():
    """清空计算缓存"""
    global _calculation_cache
    _calculation_cache.clear()
    logger.info("已清空计算缓存")

def get_cache_stats() -> Dict[str, Any]:
    """获取缓存统计信息"""
    return {
        'cache_size': len(_calculation_cache),
        'cache_memory_mb': sum(
            df.estimated_size() / 1024 / 1024
            for df in _calculation_cache.values()
            if hasattr(df, 'estimated_size')
        )
    }

def load_market_data(data_source: Union[str, List[str], Dict[str, Any]]) -> pl.DataFrame:
    """加载市场数据

    支持多种数据源：
    - 字符串路径：从文件加载
    - 股票代码列表：从数据库或RQDatac加载
    - 字典配置：灵活的数据加载配置

    Args:
        data_source: 数据源配置
            - str: 文件路径
            - List[str]: 股票代码列表
            - Dict: 详细配置，包含：
                - "source": "file"|"rqdatac"|"database"
                - "symbols": 股票代码列表
                - "start_date": 开始日期
                - "end_date": 结束日期
                - "data_types": 数据类型列表 ["ohlcv", "fundamental", "technical"]
                - "path": 文件路径（当source="file"时）

    Returns:
        加载的数据DataFrame
    """
    try:
        if isinstance(data_source, str):
            # 从文件加载
            logger.info(f"📁 从文件加载数据: {data_source}")
            return get_data_processor().load_data(data_source)

        elif isinstance(data_source, list):
            # 从股票代码列表加载（兼容旧接口）
            logger.info(f"📊 从股票代码加载数据: {len(data_source)} 只股票")
            return data_model.load_market_data(data_source)

        elif isinstance(data_source, dict):
            # 字典配置方式
            source_type = data_source.get("source", "rqdatac")
            symbols = data_source.get("symbols", [])
            start_date = data_source.get("start_date")
            end_date = data_source.get("end_date")
            data_types = data_source.get("data_types", ["ohlcv"])

            logger.info(f"🔧 配置加载数据: {source_type}, {len(symbols)} 只股票, {data_types}")

            if source_type == "file":
                path = data_source.get("path")
                if not path:
                    raise ValueError("文件数据源必须提供 'path' 参数")
                return get_data_processor().load_data(path)

            elif source_type == "rqdatac":
                return _load_from_rqdatac(symbols, start_date, end_date, data_types)

            elif source_type == "database":
                return data_model.load_market_data(symbols)

            else:
                raise ValueError(f"不支持的数据源类型: {source_type}")

        else:
            raise ValueError("不支持的数据源类型")

    except Exception as e:
        logger.error(f"❌ 数据加载失败: {e}")
        return pl.DataFrame()


def save_data(data: pl.DataFrame, output_path: str, **kwargs) -> bool:
    """保存数据

    Args:
        data: 要保存的数据
        output_path: 输出路径
        **kwargs: 额外参数

    Returns:
        保存是否成功
    """
    try:
        if not output_path:
            raise ValueError("必须提供 output_path 参数")

        get_data_processor().save_data(data, output_path, **kwargs)
        return True

    except Exception as e:
        logger.error(f"❌ 数据保存失败: {e}")
        return False


def calculate_indicators(data: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
                        indicators: Optional[List[str]] = None, **kwargs) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    """计算技术指标

    Args:
        data: 输入数据
        indicators: 要计算的指标列表
        **kwargs: 额外参数

    Returns:
        计算结果
    """
    try:
        return get_indicator_calculator().calculate_indicators(data, indicators, **kwargs)

    except Exception as e:
        logger.error(f"❌ 指标计算失败: {e}")
        return pl.DataFrame() if isinstance(data, pl.DataFrame) else {}


def calculate_fundamental_indicators(data: pl.DataFrame,
                                   indicators: Optional[List[str]] = None) -> pl.DataFrame:
    """计算基本面指标

    Args:
        data: 基本面数据DataFrame
        indicators: 要计算的指标列表

    Returns:
        包含基本面指标的DataFrame
    """
    try:
        if data.is_empty():
            logger.warning("输入数据为空，返回空DataFrame")
            return pl.DataFrame()

        # 默认基本面指标
        if indicators is None:
            indicators = ["pe_ratio", "pb_ratio", "roe", "roa", "debt_to_equity"]

        result_df = data.clone()

        # 计算市盈率 (PE Ratio) = 股价 / 每股收益
        if "pe_ratio" in indicators and "close" in data.columns and "eps" in data.columns:
            result_df = result_df.with_columns(
                (pl.col("close") / pl.col("eps")).alias("pe_ratio")
            )

        # 计算市净率 (PB Ratio) = 股价 / 每股净资产
        if "pb_ratio" in indicators and "close" in data.columns and "bvps" in data.columns:
            result_df = result_df.with_columns(
                (pl.col("close") / pl.col("bvps")).alias("pb_ratio")
            )

        # ROE (净资产收益率) - 如果数据中已有，直接使用
        if "roe" in indicators and "roe" in data.columns:
            result_df = result_df.with_columns(pl.col("roe").alias("roe"))

        # ROA (总资产收益率) - 如果数据中已有，直接使用
        if "roa" in indicators and "roa" in data.columns:
            result_df = result_df.with_columns(pl.col("roa").alias("roa"))

        # 负债权益比 - 如果数据中已有，直接使用
        if "debt_to_equity" in indicators and "debt_to_equity" in data.columns:
            result_df = result_df.with_columns(pl.col("debt_to_equity").alias("debt_to_equity"))

        logger.info(f"✅ 基本面指标计算完成: {len(result_df)} 条记录")
        return result_df

    except Exception as e:
        logger.error(f"❌ 基本面指标计算失败: {e}")
        return pl.DataFrame()


def calculate_scores(data: pl.DataFrame, score_type: str = "technical", **kwargs) -> pl.DataFrame:
    """计算股票评分

    Args:
        data: 输入数据
        score_type: 评分类型
        **kwargs: 额外参数

    Returns:
        评分结果
    """
    try:
        calculator = get_score_calculator()

        if score_type == "technical":
            # 使用 StockScorer 的 score_stocks 方法
            market_env = kwargs.get('market_env', 'normal')
            return calculator.score_stocks(data, market_env)
        else:
            raise ValueError(f"不支持的评分类型: {score_type}")

    except Exception as e:
        logger.error(f"❌ 评分计算失败: {e}")
        return pl.DataFrame()


def evaluate_data_quality(data: pl.DataFrame, **kwargs) -> Dict[str, Any]:
    """评估数据质量

    Args:
        data: 要评估的数据
        **kwargs: 额外参数

    Returns:
        质量评估报告
    """
    try:
        report = {
            "total_rows": len(data),
            "total_columns": len(data.columns),
            "null_counts": {},
            "data_types": {},
            "quality_score": 0.0
        }

        # 检查每列的空值情况
        for col in data.columns:
            null_count = data[col].null_count()
            report["null_counts"][col] = null_count
            report["data_types"][col] = str(data[col].dtype)

        # 计算质量评分
        total_cells = report["total_rows"] * report["total_columns"]
        null_cells = sum(report["null_counts"].values())
        if total_cells > 0:
            report["quality_score"] = (1 - null_cells / total_cells) * 100

        return report

    except Exception as e:
        logger.error(f"❌ 数据质量评估失败: {e}")
        return {}


def process_batch_indicators(data: Dict[str, pl.DataFrame], **kwargs) -> Dict[str, pl.DataFrame]:
    """批量计算指标（并行处理）

    Args:
        data: 股票代码 -> 数据 的字典
        **kwargs: 额外参数

    Returns:
        处理结果字典
    """
    try:
        operation = kwargs.get("operation", "indicators")

        if operation == "indicators":
            return get_parallel_processor().process_batch_indicators(data, **kwargs)
        else:
            raise ValueError(f"不支持的操作类型: {operation}")

    except Exception as e:
        logger.error(f"❌ 并行处理失败: {e}")
        return {}


# ===== 便捷函数 =====

def reset_processors():
    """重置所有处理器实例（用于测试或重新初始化）"""
    global _data_processor, _indicator_calculator, _score_calculator, _parallel_processor
    _data_processor = None
    _indicator_calculator = None
    _score_calculator = None
    _parallel_processor = None
    logger.info("🔄 所有处理器实例已重置")


def get_processor_status() -> Dict[str, bool]:
    """获取处理器初始化状态"""
    return {
        "data_processor": _data_processor is not None,
        "indicator_calculator": _indicator_calculator is not None,
        "score_calculator": _score_calculator is not None,
        "parallel_processor": _parallel_processor is not None
    }


# ===== RQDatac数据加载函数 =====

def _load_from_rqdatac(symbols: List[str], start_date: Optional[str],
                      end_date: Optional[str], data_types: List[str]) -> pl.DataFrame:
    """从RQDatac加载数据

    Args:
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        data_types: 数据类型列表

    Returns:
        合并的数据DataFrame
    """
    try:
        loader = get_rqdatac_data_loader()
        combined_data = []

        # 设置默认日期范围
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"📡 从RQDatac加载数据: {len(symbols)} 只股票, {start_date} 到 {end_date}")

        # 加载不同类型的数据
        for data_type in data_types:
            if data_type == "ohlcv":
                ohlcv_data = loader.get_ohlcv_data(symbols, start_date, end_date)
                if not ohlcv_data.is_empty():
                    combined_data.append(ohlcv_data)

            elif data_type == "fundamental":
                fundamental_data = loader.get_fundamental_data(symbols, start_date=start_date, end_date=end_date)
                if not fundamental_data.is_empty():
                    combined_data.append(fundamental_data)

            elif data_type == "technical":
                technical_data = loader.get_technical_data(symbols, start_date=start_date, end_date=end_date)
                if not technical_data.is_empty():
                    combined_data.append(technical_data)

            elif data_type == "instruments":
                instruments_data = loader.get_instruments()
                if not instruments_data.is_empty():
                    combined_data.append(instruments_data)

        # 合并所有数据
        if combined_data:
            try:
                # 使用outer join合并数据，避免列重复
                result_df = combined_data[0]
                for df in combined_data[1:]:
                    # 找到共同的列用于join
                    common_cols = []
                    for col in result_df.columns:
                        if col in df.columns:
                            common_cols.append(col)

                    if common_cols:
                        # 使用共同列进行join
                        result_df = result_df.join(df, on=common_cols, how="outer")
                    else:
                        # 如果没有共同列，直接水平连接
                        result_df = pl.concat([result_df, df], how="horizontal")

                logger.info(f"✅ 成功加载 {len(result_df)} 行数据")
                return result_df
            except Exception as merge_error:
                logger.warning(f"数据合并失败: {merge_error}，返回第一个数据块")
                return combined_data[0] if combined_data else pl.DataFrame()
        else:
            logger.warning("⚠️ 没有加载到任何数据")
            return pl.DataFrame()

    except Exception as e:
        logger.error(f"❌ 从RQDatac加载数据失败: {e}")
        return pl.DataFrame()


# ===== 便捷的数据加载函数 =====

def load_ohlcv_data(symbols: List[str], start_date: Optional[str] = None,
                   end_date: Optional[str] = None, source: str = "rqdatac") -> pl.DataFrame:
    """加载OHLCV数据

    Args:
        symbols: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        source: 数据源 ("rqdatac" 或 "database")

    Returns:
        OHLCV数据
    """
    try:
        if source == "rqdatac":
            loader = get_rqdatac_data_loader()
            return loader.get_ohlcv_data(symbols, start_date or "", end_date or "")
        elif source == "database":
            return data_model.load_market_data(symbols)
        else:
            raise ValueError(f"不支持的数据源: {source}")
    except Exception as e:
        logger.error(f"❌ 加载OHLCV数据失败: {e}")
        return pl.DataFrame()


def load_fundamental_data(symbols: List[str], fields: Optional[List[str]] = None,
                         start_date: Optional[str] = None, end_date: Optional[str] = None,
                         source: str = "rqdatac") -> pl.DataFrame:
    """加载基本面数据

    Args:
        symbols: 股票代码列表
        fields: 字段列表
        start_date: 开始日期
        end_date: 结束日期
        source: 数据源

    Returns:
        基本面数据
    """
    try:
        if source == "rqdatac":
            loader = get_rqdatac_data_loader()
            return loader.get_fundamental_data(symbols, fields, start_date, end_date)
        elif source == "database":
            return data_model.load_market_data(symbols)
        else:
            raise ValueError(f"不支持的数据源: {source}")
    except Exception as e:
        logger.error(f"❌ 加载基本面数据失败: {e}")
        return pl.DataFrame()


def load_technical_data(symbols: List[str], indicators: Optional[List[str]] = None,
                       start_date: Optional[str] = None, end_date: Optional[str] = None,
                       source: str = "rqdatac") -> pl.DataFrame:
    """加载技术指标数据

    Args:
        symbols: 股票代码列表
        indicators: 指标列表
        start_date: 开始日期
        end_date: 结束日期
        source: 数据源

    Returns:
        技术指标数据
    """
    try:
        if source == "rqdatac":
            loader = get_rqdatac_data_loader()
            return loader.get_technical_data(symbols, indicators, start_date, end_date)
        elif source == "database":
            return data_model.load_market_data(symbols)
        else:
            raise ValueError(f"不支持的数据源: {source}")
    except Exception as e:
        logger.error(f"❌ 加载技术指标数据失败: {e}")
        return pl.DataFrame()


def load_instruments(instrument_type: str = "CS", market: str = "cn",
                    source: str = "rqdatac") -> pl.DataFrame:
    """加载股票列表

    Args:
        instrument_type: 证券类型
        market: 市场
        source: 数据源

    Returns:
        股票列表
    """
    try:
        if source == "rqdatac":
            loader = get_rqdatac_data_loader()
            return loader.get_instruments(instrument_type, market)
        else:
            raise ValueError(f"不支持的数据源: {source}")
    except Exception as e:
        logger.error(f"❌ 加载股票列表失败: {e}")
        return pl.DataFrame()


def load_trading_calendar(market: str = "cn", start_date: Optional[str] = None,
                         end_date: Optional[str] = None, source: str = "rqdatac") -> List[str]:
    """加载交易日历

    Args:
        market: 市场
        start_date: 开始日期
        end_date: 结束日期
        source: 数据源

    Returns:
        交易日期列表
    """
    try:
        if source == "rqdatac":
            loader = get_rqdatac_data_loader()
            return loader.get_trading_calendar(market, start_date, end_date)
        else:
            raise ValueError(f"不支持的数据源: {source}")
    except Exception as e:
        logger.error(f"❌ 加载交易日历失败: {e}")
        return []
