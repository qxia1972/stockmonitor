"""
RQDatac数据加载器 (RQDatac Data Loader)

专门用于从RQDatac获取各种数据的加载器模块：
- 股票列表获取
- 交易日历获取
- OHLCV数据获取
- 基本面数据获取
- 数据规范同步更新

作者: 系统架构师
创建日期: 2025年9月16日
"""

import logging
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json
import polars as pl

from modules.data_schema import (
    PRICE_FIELDS, FUNDAMENTAL_FIELDS, TECHNICAL_FIELDS,
    ALL_FIELDS, get_required_fields, get_optional_fields,
    FieldDefinition, DATA_FETCH_CONFIG
)

logger = logging.getLogger(__name__)


class RQDatacDataLoader:
    """RQDatac数据加载器"""

    def __init__(self, allow_mock_data: bool = False):
        """
        初始化RQDatac数据加载器

        Args:
            allow_mock_data: 是否允许在RQDatac不可用时使用模拟数据
        """
        self._allow_mock_data = allow_mock_data
        self._rqdatac = None
        self._is_initialized = False
        self._instruments_cache: Optional[pl.DataFrame] = None
        self._trading_calendar_cache: Optional[List[str]] = None
        self._cache_expiry: Dict[str, datetime] = {}

        # 元数据管理
        self._metadata: Dict[str, Any] = {
            "data_source": "RQDatac",
            "version": "2.0.0",
            "last_fetch_time": None,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_quality_metrics": {},
            "performance_stats": {}
        }

        # 初始化RQDatac连接
        self._init_rqdatac()

    def _record_request_metadata(self, operation: str, success: bool,
                               record_count: int = 0, duration: float = 0.0):
        """记录请求元数据"""
        self._metadata["total_requests"] += 1
        self._metadata["last_fetch_time"] = datetime.now()

        if success:
            self._metadata["successful_requests"] += 1
        else:
            self._metadata["failed_requests"] += 1

        # 记录性能统计
        if operation not in self._metadata["performance_stats"]:
            self._metadata["performance_stats"][operation] = {
                "count": 0,
                "total_duration": 0.0,
                "avg_duration": 0.0,
                "max_duration": 0.0,
                "min_duration": float('inf')
            }

        stats = self._metadata["performance_stats"][operation]
        stats["count"] += 1
        stats["total_duration"] += duration
        stats["avg_duration"] = stats["total_duration"] / stats["count"]
        stats["max_duration"] = max(stats["max_duration"], duration)
        stats["min_duration"] = min(stats["min_duration"], duration)

        # 记录数据质量指标
        if record_count > 0:
            if operation not in self._metadata["data_quality_metrics"]:
                self._metadata["data_quality_metrics"][operation] = {
                    "total_records": 0,
                    "avg_records_per_request": 0.0
                }

            quality = self._metadata["data_quality_metrics"][operation]
            quality["total_records"] += record_count
            quality["avg_records_per_request"] = quality["total_records"] / stats["count"]

    def get_metadata(self) -> Dict[str, Any]:
        """获取数据加载器元数据"""
        return self._metadata.copy()

    def reset_metadata(self):
        """重置元数据统计"""
        self._metadata.update({
            "last_fetch_time": None,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "data_quality_metrics": {},
            "performance_stats": {}
        })
        logger.info("🔄 RQDatac数据加载器元数据已重置")

    def _init_rqdatac(self):
        """初始化RQDatac连接"""
        try:
            import rqdatac
            rqdatac.init()
            self._rqdatac = rqdatac
            self._is_initialized = True
            logger.info("✅ RQDatac初始化成功")
        except ImportError as e:
            if not self._allow_mock_data:
                logger.error(f"❌ RQDatac未安装: {e}")
                raise ConnectionError("RQDatac不可用，无法获取数据") from e
            else:
                logger.warning("⚠️ RQDatac未安装，使用模拟模式")
                self._rqdatac = None
                self._is_initialized = False
        except Exception as e:
            if not self._allow_mock_data:
                logger.error(f"❌ RQDatac初始化失败: {e}")
                raise ConnectionError("RQDatac初始化失败，无法获取数据") from e
            else:
                logger.error(f"❌ RQDatac初始化失败: {e}")
                self._rqdatac = None
                self._is_initialized = False

    def get_instruments(self,
                       instrument_type: str = "CS",
                       market: str = "cn",
                       refresh_cache: bool = False) -> pl.DataFrame:
        """
        获取股票列表

        Args:
            instrument_type: 证券类型 (CS=股票, ETF=ETF, etc.)
            market: 市场 (cn=中国, hk=香港, us=美国)
            refresh_cache: 是否刷新缓存

        Returns:
            股票列表DataFrame
        """
        try:
            cache_key = f"instruments_{instrument_type}_{market}"

            # 检查缓存
            if not refresh_cache and self._is_cache_valid(cache_key) and self._instruments_cache is not None:
                logger.info("📋 从缓存获取股票列表")
                return self._instruments_cache

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取股票列表数据")
                else:
                    # 返回模拟数据
                    instruments_data = self._get_mock_instruments(instrument_type, market)
            else:
                # 从RQDatac获取真实数据
                instruments_data = self._fetch_instruments_from_rqdatac(instrument_type, market)

            # 转换为Polars DataFrame
            df = pl.DataFrame(instruments_data)

            # 添加元数据列
            df = df.with_columns([
                pl.lit("RQDatac").alias("_data_source"),
                pl.lit(datetime.now()).alias("_fetch_timestamp"),
                pl.lit(instrument_type).alias("_instrument_type"),
                pl.lit(market).alias("_market"),
                pl.lit(len(df)).alias("_total_instruments")
            ])

            # 缓存结果
            self._instruments_cache = df
            self._cache_expiry[cache_key] = datetime.now() + timedelta(hours=24)

            # 记录元数据
            self._record_request_metadata("get_instruments", True, len(df))

            logger.info(f"📊 获取到 {len(df)} 只证券")
            return df

        except Exception as e:
            # 记录失败的元数据
            self._record_request_metadata("get_instruments", False)
            logger.error(f"❌ 获取股票列表失败: {e}")
            return pl.DataFrame()

    def get_trading_calendar(self,
                           market: str = "cn",
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           refresh_cache: bool = False) -> List[str]:
        """
        获取交易日历

        Args:
            market: 市场
            start_date: 开始日期
            end_date: 结束日期
            refresh_cache: 是否刷新缓存

        Returns:
            交易日期列表
        """
        try:
            cache_key = f"trading_calendar_{market}"

            # 检查缓存
            if not refresh_cache and self._is_cache_valid(cache_key) and self._trading_calendar_cache is not None:
                logger.info("📋 从缓存获取交易日历")
                return self._trading_calendar_cache

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取交易日历数据")
                else:
                    # 返回模拟交易日历
                    trading_dates = self._get_mock_trading_calendar(start_date, end_date)
            else:
                # 从RQDatac获取真实交易日历
                trading_dates = self._fetch_trading_calendar_from_rqdatac(market, start_date, end_date)

            # 缓存结果
            self._trading_calendar_cache = trading_dates
            self._cache_expiry[cache_key] = datetime.now() + timedelta(hours=24)

            logger.info(f"📅 获取到 {len(trading_dates)} 个交易日")
            return trading_dates

        except Exception as e:
            logger.error(f"❌ 获取交易日历失败: {e}")
            return []

    def get_ohlcv_data(self,
                      symbols: List[str],
                      start_date: str,
                      end_date: str,
                      frequency: str = "1d",
                      adjust_type: str = "post") -> pl.DataFrame:
        """
        获取OHLCV数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率 (1d, 1m, etc.)
            adjust_type: 复权类型 (post, pre, none)

        Returns:
            OHLCV数据DataFrame
        """
        try:
            logger.info(f"📈 获取OHLCV数据: {len(symbols)} 只股票, {start_date} 到 {end_date}")

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取OHLCV数据")
                else:
                    # 返回模拟数据
                    ohlcv_data = self._get_mock_ohlcv_data(symbols, start_date, end_date)
            else:
                # 从RQDatac获取真实数据
                ohlcv_data = self._fetch_ohlcv_from_rqdatac(
                    symbols, start_date, end_date, frequency, adjust_type
                )

            df = pl.DataFrame(ohlcv_data)

            # 确保数据类型正确
            df = self._ensure_ohlcv_data_types(df)

            # 添加元数据列
            df = df.with_columns([
                pl.lit("RQDatac").alias("_data_source"),
                pl.lit(datetime.now()).alias("_fetch_timestamp"),
                pl.lit(frequency).alias("_frequency"),
                pl.lit(adjust_type).alias("_adjust_type"),
                pl.lit(len(symbols)).alias("_symbol_count"),
                pl.lit(len(df)).alias("_total_records")
            ])

            # 记录元数据
            self._record_request_metadata("get_ohlcv_data", True, len(df))

            logger.info(f"📊 获取到 {len(df)} 条OHLCV记录")
            return df

        except Exception as e:
            # 记录失败的元数据
            self._record_request_metadata("get_ohlcv_data", False)
            logger.error(f"❌ 获取OHLCV数据失败: {e}")
            return pl.DataFrame()

    def get_dual_adjustment_ohlcv_data(self,
                                       symbols: List[str],
                                       start_date: str,
                                       end_date: str,
                                       frequency: str = "1d") -> pl.DataFrame:
        """
        获取双复权OHLCV数据（同时包含后复权和前复权价格）

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率 (1d, 1m, etc.)

        Returns:
            包含两种复权价格的OHLCV数据DataFrame
        """
        try:
            logger.info(f"� 获取双复权OHLCV数据: {len(symbols)} 只股票, {start_date} 到 {end_date}")

            if not self._is_initialized:
                if not self._allow_mock_data:
                    raise ConnectionError("RQDatac不可用，无法获取OHLCV数据")
                else:
                    # 返回模拟数据（暂时使用默认复权）
                    ohlcv_data = self._get_mock_ohlcv_data(symbols, start_date, end_date)
                    df = pl.DataFrame(ohlcv_data)
            else:
                # 获取后复权数据
                post_data = self._fetch_ohlcv_from_rqdatac(
                    symbols, start_date, end_date, frequency, "post"
                )

                # 获取前复权数据
                pre_data = self._fetch_ohlcv_from_rqdatac(
                    symbols, start_date, end_date, frequency, "pre"
                )

                # 合并两种复权数据
                df = self._merge_dual_adjustment_data(post_data, pre_data)

            # 确保数据类型正确
            df = self._ensure_dual_ohlcv_data_types(df)

            # 添加元数据列
            df = df.with_columns([
                pl.lit("RQDatac").alias("_data_source"),
                pl.lit(datetime.now()).alias("_fetch_timestamp"),
                pl.lit(frequency).alias("_frequency"),
                pl.lit("dual").alias("_adjust_type"),  # 标记为双复权数据
                pl.lit(len(symbols)).alias("_symbol_count"),
                pl.lit(len(df)).alias("_total_records")
            ])

            # 记录元数据
            self._record_request_metadata("get_dual_adjustment_ohlcv_data", True, len(df))

            logger.info(f"�📊 获取到 {len(df)} 条双复权OHLCV记录")
            return df

        except Exception as e:
            # 记录失败的元数据
            self._record_request_metadata("get_dual_adjustment_ohlcv_data", False)
            logger.error(f"❌ 获取双复权OHLCV数据失败: {e}")
            return pl.DataFrame()

    def get_fundamental_data(self,
                           symbols: List[str],
                           fields: Optional[List[str]] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> pl.DataFrame:
        """
        获取基本面数据

        Args:
            symbols: 股票代码列表
            fields: 字段列表，如果为None则获取所有基本面字段
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            基本面数据DataFrame
        """
        try:
            if fields is None:
                fields = list(FUNDAMENTAL_FIELDS.keys())

            logger.info(f"📊 获取基本面数据: {len(symbols)} 只股票, {len(fields)} 个字段")

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取基本面数据")
                else:
                    # 返回模拟数据
                    fundamental_data = self._get_mock_fundamental_data(symbols, fields, start_date, end_date)
            else:
                # 从RQDatac获取真实数据
                fundamental_data = self._fetch_fundamental_from_rqdatac(
                    symbols, fields, start_date, end_date
                )

            df = pl.DataFrame(fundamental_data)

            # 确保数据类型正确
            df = self._ensure_fundamental_data_types(df)

            logger.info(f"📊 获取到 {len(df)} 条基本面记录")
            return df

        except Exception as e:
            logger.error(f"❌ 获取基本面数据失败: {e}")
            return pl.DataFrame()

    def get_factor_data(self,
                        symbols: List[str],
                        factors: Optional[List[str]] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> pl.DataFrame:
        """
        从RQDatac因子库获取因子数据

        Args:
            symbols: 股票代码列表
            factors: 因子名称列表，如果为None则获取所有可用因子
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            因子数据DataFrame
        """
        try:
            if factors is None:
                # 获取所有基本面相关的因子
                factors = [
                    'pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio', 'market_cap',
                    'circulating_market_cap', 'turnover_ratio', 'float_turnover_ratio',
                    'roe', 'roa', 'gross_profit_margin', 'net_profit_margin',
                    'current_ratio', 'debt_to_equity', 'total_assets', 'total_liabilities'
                ]

            logger.info(f"� 从因子库获取数据: {len(symbols)} 只股票, {len(factors)} 个因子")

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取因子数据")
                else:
                    # 返回模拟数据
                    factor_data = self._get_mock_factor_data(symbols, factors, start_date, end_date)
            else:
                # 从RQDatac因子库获取真实数据
                factor_data = self._fetch_factors_from_rqdatac(
                    symbols, factors, start_date, end_date
                )

            df = pl.DataFrame(factor_data)

            # 确保数据类型正确
            df = self._ensure_factor_data_types(df)

            # 添加元数据列
            df = df.with_columns([
                pl.lit("RQDatac").alias("_data_source"),
                pl.lit(datetime.now()).alias("_fetch_timestamp"),
                pl.lit("factor").alias("_data_type"),
                pl.lit(len(symbols)).alias("_symbol_count"),
                pl.lit(len(factors)).alias("_factor_count"),
                pl.lit(len(df)).alias("_total_records")
            ])

            # 记录元数据
            self._record_request_metadata("get_factor_data", True, len(df))

            logger.info(f"📊 从因子库获取到 {len(df)} 条记录")
            return df

        except Exception as e:
            # 记录失败的元数据
            self._record_request_metadata("get_factor_data", False)
            logger.error(f"❌ 从因子库获取数据失败: {e}")
            return pl.DataFrame()

    def update_data_schema_from_rqdatac(self):
        """
        根据RQDatac的实际数据结构更新数据规范

        这个方法会：
        1. 获取RQDatac的实际字段信息
        2. 更新data_field_definitions.py中的字段定义
        3. 更新config/schemas/中的schema文件
        """
        try:
            logger.info("🔄 开始更新数据规范...")

            if not self._is_initialized:
                logger.warning("⚠️ RQDatac未初始化，跳过数据规范更新")
                return

            # 获取实际的字段信息
            actual_fields = self._get_actual_fields_from_rqdatac()

            # 更新字段定义
            self._update_field_definitions(actual_fields)

            # 更新schema文件
            self._update_schema_files(actual_fields)

            logger.info("✅ 数据规范更新完成")

        except Exception as e:
            logger.error(f"❌ 更新数据规范失败: {e}")

    # ===== 私有方法 =====

    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self._cache_expiry:
            return False
        return datetime.now() < self._cache_expiry[cache_key]

    def _fetch_instruments_from_rqdatac(self, instrument_type: str, market: str) -> List[Dict]:
        """从RQDatac获取股票列表"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            # RQDatac获取证券列表 - 使用正确的API
            try:
                # 尝试获取全部A股股票
                instruments = self._rqdatac.all_instruments(type=instrument_type, market=market)  # type: ignore
            except AttributeError:
                try:
                    # 如果没有all_instruments方法，尝试使用instruments但不传参数
                    instruments = self._rqdatac.instruments(type=instrument_type, market=market)  # type: ignore
                except (AttributeError, TypeError):
                    # 如果都没有，使用备用方法
                    logger.warning("RQDatac API调用失败，使用备用股票列表")
                    return self._get_fallback_instruments(instrument_type, market)

            # 处理RQDatac返回的数据格式
            instruments_data = []
            if hasattr(instruments, 'iterrows'):  # DataFrame格式
                for idx, row in instruments.iterrows():
                    instruments_data.append({
                        "order_book_id": str(row.get('order_book_id', row.get('code', idx))),
                        "symbol": str(row.get('symbol', row.get('name', idx))),
                        "exchange": str(row.get('exchange', market.upper())),
                        "type": str(row.get('type', instrument_type)),
                        "status": str(row.get('status', 'Active')),
                        "listed_date": str(row.get('listed_date', '1990-01-01')),
                        "de_listed_date": str(row.get('de_listed_date', '')),
                        "sector_code": str(row.get('sector_code', '')),
                        "industry_code": str(row.get('industry_code', '')),
                        "board_type": str(row.get('board_type', '')),
                        "data_date": datetime.now().strftime("%Y-%m-%d"),
                        "updated_at": datetime.now().isoformat()
                    })
            elif hasattr(instruments, '__iter__'):  # 列表或类似可迭代对象
                for inst in instruments:
                    if hasattr(inst, '__dict__'):  # 对象格式
                        instruments_data.append({
                            "order_book_id": getattr(inst, 'order_book_id', getattr(inst, 'code', str(inst))),
                            "symbol": getattr(inst, 'symbol', getattr(inst, 'name', str(inst))),
                            "exchange": getattr(inst, 'exchange', market.upper()),
                            "type": getattr(inst, 'type', instrument_type),
                            "status": getattr(inst, 'status', 'Active'),
                            "listed_date": getattr(inst, 'listed_date', '1990-01-01'),
                            "de_listed_date": getattr(inst, 'de_listed_date', ''),
                            "sector_code": getattr(inst, 'sector_code', ''),
                            "industry_code": getattr(inst, 'industry_code', ''),
                            "board_type": getattr(inst, 'board_type', ''),
                            "data_date": datetime.now().strftime("%Y-%m-%d"),
                            "updated_at": datetime.now().isoformat()
                        })
                    else:  # 字典格式
                        instruments_data.append({
                            "order_book_id": str(inst.get('order_book_id', inst.get('code', inst))),
                            "symbol": str(inst.get('symbol', inst.get('name', inst))),
                            "exchange": str(inst.get('exchange', market.upper())),
                            "type": str(inst.get('type', instrument_type)),
                            "status": str(inst.get('status', 'Active')),
                            "listed_date": str(inst.get('listed_date', '1990-01-01')),
                            "de_listed_date": str(inst.get('de_listed_date', '')),
                            "sector_code": str(inst.get('sector_code', '')),
                            "industry_code": str(inst.get('industry_code', '')),
                            "board_type": str(inst.get('board_type', '')),
                            "data_date": datetime.now().strftime("%Y-%m-%d"),
                            "updated_at": datetime.now().isoformat()
                        })
            else:
                logger.warning(f"未知的数据格式: {type(instruments)}")
                return self._get_fallback_instruments(instrument_type, market)

            logger.info(f"成功获取 {len(instruments_data)} 只股票")
            return instruments_data

        except Exception as e:
            logger.error(f"获取RQDatac股票列表失败: {e}")
            return self._get_fallback_instruments(instrument_type, market)

    def _fetch_trading_calendar_from_rqdatac(self, market: str,
                                           start_date: Optional[str],
                                           end_date: Optional[str]) -> List[str]:
        """从RQDatac获取交易日历"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            # 设置默认日期范围
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")

            # RQDatac获取交易日历
            trading_dates = self._rqdatac.get_trading_dates(start_date, end_date)  # type: ignore

            # 转换为字符串格式
            return [date.strftime("%Y-%m-%d") for date in trading_dates]

        except Exception as e:
            logger.error(f"从RQDatac获取交易日历失败: {e}")
            return []

    def _fetch_ohlcv_from_rqdatac(self, symbols: List[str], start_date: str,
                                end_date: str, frequency: str, adjust_type: str) -> List[Dict]:
        """从RQDatac获取OHLCV数据"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            # RQDatac获取价格数据 - 使用正确的参数名
            price_data = self._rqdatac.get_price(  # type: ignore
                order_book_ids=symbols,  # 注意：使用order_book_ids而不是symbols
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjust_type=adjust_type
            )

            # RQDatac返回的是MultiIndex DataFrame，需要正确处理
            ohlcv_data = []

            # 重置索引以获取order_book_id和date作为列
            if hasattr(price_data.index, 'names') and price_data.index.names == ['order_book_id', 'date']:
                # MultiIndex的情况
                price_data = price_data.reset_index()

            # 转换为标准格式
            for record in price_data.to_dict('records'):
                ohlcv_data.append({
                    "order_book_id": record.get("order_book_id"),
                    "date": record.get("date").strftime("%Y-%m-%d") if hasattr(record.get("date"), 'strftime') else str(record.get("date")),
                    "open": record.get("open"),
                    "close": record.get("close"),
                    "high": record.get("high"),
                    "low": record.get("low"),
                    "volume": record.get("volume"),
                    "amount": record.get("total_turnover"),  # RQDatac使用total_turnover而不是amount
                    "vwap": record.get("vwap"),
                    "returns": record.get("returns"),
                    "volume_ratio": record.get("volume_ratio")
                })

            return ohlcv_data

        except Exception as e:
            logger.error(f"从RQDatac获取OHLCV数据失败: {e}")
            return []

    def _merge_dual_adjustment_data(self, post_data: List[Dict], pre_data: List[Dict]) -> pl.DataFrame:
        """合并后复权和前复权数据"""
        try:
            # 转换为DataFrame
            post_df = pl.DataFrame(post_data)
            pre_df = pl.DataFrame(pre_data)

            # 重命名前复权列以区分
            pre_df = pre_df.rename({
                "open": "open_pre",
                "close": "close_pre",
                "high": "high_pre",
                "low": "low_pre",
                "vwap": "vwap_pre"
            })

            # 合并数据（基于order_book_id和date）
            merged_df = post_df.join(
                pre_df.select(["order_book_id", "date", "open_pre", "close_pre", "high_pre", "low_pre", "vwap_pre"]),
                on=["order_book_id", "date"],
                how="left"
            )

            return merged_df

        except Exception as e:
            logger.error(f"合并双复权数据失败: {e}")
            # 如果合并失败，返回后复权数据
            return pl.DataFrame(post_data)

    def _ensure_dual_ohlcv_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保双复权OHLCV数据类型正确"""
        try:
            # 基础字段类型
            df = df.with_columns([
                pl.col('date').cast(pl.Date).alias('date'),
                pl.col('order_book_id').cast(pl.Utf8).alias('order_book_id'),
                pl.col('open').cast(pl.Float64).alias('open'),
                pl.col('high').cast(pl.Float64).alias('high'),
                pl.col('low').cast(pl.Float64).alias('low'),
                pl.col('close').cast(pl.Float64).alias('close'),
                pl.col('volume').cast(pl.Int64).alias('volume'),
                pl.col('amount').cast(pl.Float64).alias('amount'),
                pl.col('vwap').cast(pl.Float64).alias('vwap'),
                pl.col('returns').cast(pl.Float64).alias('returns'),
                pl.col('volume_ratio').cast(pl.Float64).alias('volume_ratio')
            ])

            # 前复权字段类型（如果存在）
            if 'open_pre' in df.columns:
                df = df.with_columns([
                    pl.col('open_pre').cast(pl.Float64).alias('open_pre'),
                    pl.col('high_pre').cast(pl.Float64).alias('high_pre'),
                    pl.col('low_pre').cast(pl.Float64).alias('low_pre'),
                    pl.col('close_pre').cast(pl.Float64).alias('close_pre'),
                    pl.col('vwap_pre').cast(pl.Float64).alias('vwap_pre')
                ])

            return df

        except Exception as e:
            logger.error(f"确保双复权数据类型失败: {e}")
            return df

    def _fetch_fundamental_from_rqdatac(self, symbols: List[str], fields: List[str],
                                      start_date: Optional[str], end_date: Optional[str]) -> List[Dict]:
        """从RQDatac获取基本面数据"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            fundamental_data = []

            # 分批获取每个字段的数据
            for field in fields:
                try:
                    field_data = self._rqdatac.get_factor(  # type: ignore
                        order_book_ids=symbols,  # 注意：使用order_book_ids而不是symbols
                        factor=field,
                        start_date=start_date,
                        end_date=end_date
                    )

                    # 合并到结果中
                    for record in field_data.to_dict('records'):
                        # 查找或创建记录
                        existing_record = None
                        for item in fundamental_data:
                            if (item.get("order_book_id") == record.get("order_book_id") and
                                item.get("date") == record.get("date")):
                                existing_record = item
                                break

                        if existing_record is None:
                            existing_record = {
                                "order_book_id": record.get("order_book_id"),
                                "date": record.get("date").strftime("%Y-%m-%d") if record.get("date") else None,
                            }
                            fundamental_data.append(existing_record)

                        # 添加字段值
                        existing_record[field] = record.get("factor_value")

                except Exception as e:
                    logger.warning(f"获取字段 {field} 数据失败: {e}")
                    continue

            return fundamental_data

        except Exception as e:
            logger.error(f"从RQDatac获取基本面数据失败: {e}")
            return []

    def _fetch_technical_from_rqdatac(self, symbols: List[str], indicators: List[str],
                                    start_date: Optional[str], end_date: Optional[str]) -> List[Dict]:
        """从RQDatac获取技术指标数据"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            technical_data = []

            # 分批获取每个指标的数据
            for indicator in indicators:
                try:
                    # RQDatac可能没有直接的技术指标API，需要使用其他方式
                    # 这里先使用get_factor来获取一些技术指标数据
                    if indicator in ["sma_20", "sma_50", "ema_20", "ema_50"]:
                        # 对于移动平均线，使用价格数据计算
                        price_data = self._rqdatac.get_price(  # type: ignore
                            order_book_ids=symbols,
                            start_date=start_date,
                            end_date=end_date,
                            frequency="1d"
                        )
                        # 这里可以添加计算移动平均线的逻辑
                        # 暂时返回空数据
                        continue
                    else:
                        # 对于其他指标，尝试使用get_factor
                        indicator_data = self._rqdatac.get_factor(  # type: ignore
                            order_book_ids=symbols,
                            factor=indicator,
                            start_date=start_date,
                            end_date=end_date
                        )

                    # 合并到结果中
                    for record in indicator_data.to_dict('records'):
                        # 查找或创建记录
                        existing_record = None
                        for item in technical_data:
                            if (item.get("order_book_id") == record.get("order_book_id") and
                                item.get("date") == record.get("date")):
                                existing_record = item
                                break

                        if existing_record is None:
                            existing_record = {
                                "order_book_id": record.get("order_book_id"),
                                "date": record.get("date").strftime("%Y-%m-%d") if record.get("date") else None,
                            }
                            technical_data.append(existing_record)

                        # 添加指标值
                        existing_record[indicator] = record.get("factor_value")

                except Exception as e:
                    logger.warning(f"获取指标 {indicator} 数据失败: {e}")
                    continue

            return technical_data

        except Exception as e:
            logger.error(f"从RQDatac获取技术指标数据失败: {e}")
            return []

    def _ensure_ohlcv_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保OHLCV数据类型正确"""
        if df.is_empty():
            return df

        # 定义类型映射
        type_mapping = {
            "open": pl.Float64,
            "close": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "vwap": pl.Float64,
            "returns": pl.Float64,
            "volume_ratio": pl.Float64
        }

        for col, dtype in type_mapping.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(dtype))

        return df

    def _ensure_fundamental_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保基本面数据类型正确"""
        if df.is_empty():
            return df

        # 所有基本面字段都是float64类型
        for col in FUNDAMENTAL_FIELDS.keys():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        return df

    def _ensure_technical_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保技术指标数据类型正确"""
        if df.is_empty():
            return df

        # 所有技术指标字段都是float64类型
        for col in TECHNICAL_FIELDS.keys():
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        return df

    def _ensure_factor_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """确保因子数据类型正确"""
        if df.is_empty():
            return df

        # 因子数据主要是数值类型
        factor_fields = [
            'pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio', 'market_cap',
            'circulating_market_cap', 'turnover_ratio', 'float_turnover_ratio',
            'roe', 'roa', 'gross_profit_margin', 'net_profit_margin',
            'current_ratio', 'debt_to_equity', 'total_assets', 'total_liabilities'
        ]

        for col in factor_fields:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        return df

    def _get_mock_factor_data(self, symbols: List[str], factors: List[str],
                             start_date: Optional[str], end_date: Optional[str]) -> List[Dict]:
        """获取模拟因子数据"""
        import numpy as np
        from datetime import datetime, timedelta

        factor_data = []

        # 生成日期范围
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.now()
            start = end - timedelta(days=30)

        dates = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # 周一到周五
                dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # 为每个股票和日期生成因子数据
        for symbol in symbols:
            for date in dates:
                record = {
                    "order_book_id": symbol,
                    "date": date
                }

                # 生成模拟因子值
                np.random.seed(hash(symbol + date) % 2**32)
                for factor in factors:
                    if 'ratio' in factor or 'margin' in factor:
                        record[factor] = round(np.random.uniform(0.1, 5.0), 2)
                    elif 'market_cap' in factor:
                        record[factor] = round(np.random.uniform(1e9, 1e12), 2)
                    elif 'turnover' in factor:
                        record[factor] = round(np.random.uniform(0.1, 10.0), 2)
                    else:
                        record[factor] = round(np.random.uniform(0.1, 100.0), 2)

                factor_data.append(record)

        return factor_data

    def _fetch_factors_from_rqdatac(self, symbols: List[str], factors: List[str],
                                   start_date: Optional[str], end_date: Optional[str]) -> List[Dict]:
        """从RQDatac因子库获取因子数据"""
        try:
            if self._rqdatac is None:
                raise ValueError("RQDatac未初始化")

            factor_data = []

            # 分批获取每个因子的数据
            for factor in factors:
                try:
                    logger.info(f"从因子库获取因子: {factor}")

                    factor_result = self._rqdatac.get_factor(  # type: ignore
                        order_book_ids=symbols,
                        factor=factor,
                        start_date=start_date,
                        end_date=end_date
                    )

                    # 检查结果是否为空
                    if factor_result is None or factor_result.empty:
                        logger.warning(f"因子 {factor} 返回空结果")
                        continue

                    # 重置索引以获取order_book_id和date作为列
                    factor_result = factor_result.reset_index()

                    # 合并到结果中
                    for record in factor_result.to_dict('records'):
                        # 查找或创建记录
                        existing_record = None
                        for item in factor_data:
                            if (item.get("order_book_id") == record.get("order_book_id") and
                                item.get("date") == record.get("date")):
                                existing_record = item
                                break

                        if existing_record is None:
                            existing_record = {
                                "order_book_id": record.get("order_book_id"),
                                "date": record.get("date"),  # 已经是字符串格式
                            }
                            factor_data.append(existing_record)

                        # 添加因子值 - 使用因子名作为键
                        existing_record[factor] = record.get(factor)

                except Exception as e:
                    logger.warning(f"获取因子 {factor} 数据失败: {e}")
                    # 对于获取失败的因子，标记为None，后续由指标计算模块处理
                    continue

            return factor_data

        except Exception as e:
            logger.error(f"从RQDatac因子库获取数据失败: {e}")
            return []

    def get_technical_data(self,
                          symbols: List[str],
                          indicators: Optional[List[str]] = None,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pl.DataFrame:
        """
        获取技术指标数据

        Args:
            symbols: 股票代码列表
            indicators: 技术指标列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            技术指标数据DataFrame
        """
        try:
            if indicators is None:
                indicators = list(TECHNICAL_FIELDS.keys())

            logger.info(f"📈 获取技术指标数据: {len(symbols)} 只股票, {len(indicators)} 个指标")

            if not self._is_initialized:
                if not self._allow_mock_data:
                    # RQDatac不可用，直接失败
                    raise ConnectionError("RQDatac不可用，无法获取技术指标数据")
                else:
                    # 返回模拟数据
                    technical_data = self._get_mock_technical_data(symbols, indicators, start_date, end_date)
            else:
                # 从RQDatac获取真实数据
                technical_data = self._fetch_technical_from_rqdatac(
                    symbols, indicators, start_date, end_date
                )

            df = pl.DataFrame(technical_data)

            # 确保数据类型正确
            df = self._ensure_technical_data_types(df)

            logger.info(f"📊 获取到 {len(df)} 条技术指标记录")
            return df

        except Exception as e:
            logger.error(f"❌ 获取技术指标数据失败: {e}")
            return pl.DataFrame()

    # ===== 模拟数据方法（用于测试） =====

    def _get_mock_instruments(self, instrument_type: str, market: str) -> List[Dict]:
        """获取模拟股票列表数据"""
        mock_instruments = [
            {
                "order_book_id": "000001.XSHE",
                "symbol": "平安银行",
                "exchange": "XSHE",
                "type": "CS",
                "status": "Active",
                "listed_date": "1991-04-03",
                "sector_code": "J66",
                "industry_code": "J661",
                "board_type": "主板",
                "data_date": datetime.now().strftime("%Y-%m-%d"),
                "updated_at": datetime.now().isoformat()
            },
            {
                "order_book_id": "000002.XSHE",
                "symbol": "万科A",
                "exchange": "XSHE",
                "type": "CS",
                "status": "Active",
                "listed_date": "1991-01-29",
                "sector_code": "K70",
                "industry_code": "K701",
                "board_type": "主板",
                "data_date": datetime.now().strftime("%Y-%m-%d"),
                "updated_at": datetime.now().isoformat()
            }
        ]
        return mock_instruments

    def _get_mock_trading_calendar(self, start_date: Optional[str], end_date: Optional[str]) -> List[str]:
        """获取模拟交易日历"""
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # 生成工作日列表（排除周末）
        trading_dates = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while current <= end:
            if current.weekday() < 5:  # 周一到周五
                trading_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        return trading_dates

    def _get_mock_ohlcv_data(self, symbols: List[str], start_date: str, end_date: str) -> List[Dict]:
        """获取模拟OHLCV数据"""
        ohlcv_data = []
        trading_dates = self._get_mock_trading_calendar(start_date, end_date)

        for symbol in symbols:
            for date in trading_dates:
                ohlcv_data.append({
                    "order_book_id": symbol,
                    "date": date,
                    "open": 100.0 + hash(symbol + date) % 50,
                    "close": 102.0 + hash(symbol + date) % 50,
                    "high": 105.0 + hash(symbol + date) % 50,
                    "low": 98.0 + hash(symbol + date) % 50,
                    "volume": 1000000 + hash(symbol + date) % 5000000,
                    "amount": 100000000.0 + hash(symbol + date) % 500000000,
                    "vwap": 101.5 + hash(symbol + date) % 10,
                    "returns": 0.02 + (hash(symbol + date) % 100 - 50) / 10000,
                    "volume_ratio": 1.0 + hash(symbol + date) % 5
                })

        return ohlcv_data

    def _get_mock_fundamental_data(self, symbols: List[str], fields: List[str],
                                 start_date: Optional[str], end_date: Optional[str]) -> List[Dict[str, Any]]:
        """获取模拟基本面数据"""
        fundamental_data: List[Dict[str, Any]] = []
        trading_dates = self._get_mock_trading_calendar(start_date, end_date)

        for symbol in symbols:
            for date in trading_dates[:5]:  # 只生成最近5个交易日的数据
                record: Dict[str, Any] = {
                    "order_book_id": symbol,
                    "date": date
                }

                # 生成各个字段的值
                for field in fields:
                    if field in ["pe_ratio", "pb_ratio", "ps_ratio"]:
                        record[field] = 10.0 + hash(symbol + field + date) % 40
                    elif field in ["market_cap", "circulating_market_cap"]:
                        record[field] = 1000000000.0 + hash(symbol + field + date) % 9000000000
                    elif field == "turnover_ratio":
                        record[field] = 0.5 + hash(symbol + field + date) % 10
                    elif field == "roe":
                        record[field] = 0.05 + (hash(symbol + field + date) % 200 - 100) / 10000
                    else:
                        record[field] = 1.0 + hash(symbol + field + date) % 100

                fundamental_data.append(record)

        return fundamental_data

    def _get_mock_technical_data(self, symbols: List[str], indicators: List[str],
                               start_date: Optional[str], end_date: Optional[str]) -> List[Dict[str, Any]]:
        """获取模拟技术指标数据"""
        technical_data: List[Dict[str, Any]] = []
        trading_dates = self._get_mock_trading_calendar(start_date, end_date)

        for symbol in symbols:
            for date in trading_dates:
                record: Dict[str, Any] = {
                    "order_book_id": symbol,
                    "date": date
                }

                # 生成各个指标的值
                for indicator in indicators:
                    if "sma" in indicator or "ema" in indicator:
                        record[indicator] = 100.0 + hash(symbol + indicator + date) % 50
                    elif "rsi" in indicator:
                        record[indicator] = 30.0 + hash(symbol + indicator + date) % 40
                    elif "macd" in indicator:
                        record[indicator] = (hash(symbol + indicator + date) % 200 - 100) / 100
                    elif "bollinger" in indicator:
                        record[indicator] = 100.0 + hash(symbol + indicator + date) % 30
                    elif "stoch" in indicator:
                        record[indicator] = hash(symbol + indicator + date) % 100
                    elif "atr" in indicator:
                        record[indicator] = 1.0 + hash(symbol + indicator + date) % 10
                    else:
                        record[indicator] = hash(symbol + indicator + date) % 100

                technical_data.append(record)

        return technical_data

    def _get_actual_fields_from_rqdatac(self) -> Dict[str, Any]:
        """从RQDatac获取实际的字段信息"""
        # 这个方法需要根据RQDatac的实际API来实现
        # 暂时返回空字典
        return {}

    def _update_field_definitions(self, actual_fields: Dict[str, Any]):
        """更新字段定义"""
        # 这个方法需要实现字段定义的动态更新
        pass

    def _update_schema_files(self, actual_fields: Dict[str, Any]):
        """更新schema文件"""
        # 这个方法需要实现schema文件的动态更新
        pass

    def _get_fallback_instruments(self, instrument_type: str, market: str) -> List[Dict[str, Any]]:
        """获取备用股票列表数据（当RQDatac API不可用时使用）"""
        logger.info("使用备用股票列表数据")

        # 扩展的A股股票代码列表（包含更多常见股票）
        fallback_instruments = [
            # 深圳主板
            {"order_book_id": "000001.XSHE", "symbol": "平安银行", "exchange": "XSHE", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "000002.XSHE", "symbol": "万科A", "exchange": "XSHE", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "000004.XSHE", "symbol": "国华网安", "exchange": "XSHE", "sector_code": "I65", "industry_code": "I650", "board_type": "主板"},
            {"order_book_id": "000005.XSHE", "symbol": "世纪星源", "exchange": "XSHE", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "000006.XSHE", "symbol": "深振业A", "exchange": "XSHE", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "000008.XSHE", "symbol": "神州高铁", "exchange": "XSHE", "sector_code": "G54", "industry_code": "G549", "board_type": "主板"},
            {"order_book_id": "000009.XSHE", "symbol": "中国宝安", "exchange": "XSHE", "sector_code": "I65", "industry_code": "I650", "board_type": "主板"},
            {"order_book_id": "000010.XSHE", "symbol": "美丽生态", "exchange": "XSHE", "sector_code": "N77", "industry_code": "N772", "board_type": "主板"},
            {"order_book_id": "000011.XSHE", "symbol": "深物业A", "exchange": "XSHE", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "000012.XSHE", "symbol": "南玻A", "exchange": "XSHE", "sector_code": "C32", "industry_code": "C321", "board_type": "主板"},

            # 上海主板
            {"order_book_id": "600000.XSHG", "symbol": "浦发银行", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600004.XSHG", "symbol": "白云机场", "exchange": "XSHG", "sector_code": "G53", "industry_code": "G531", "board_type": "主板"},
            {"order_book_id": "600006.XSHG", "symbol": "东风汽车", "exchange": "XSHG", "sector_code": "C36", "industry_code": "C361", "board_type": "主板"},
            {"order_book_id": "600007.XSHG", "symbol": "中国国贸", "exchange": "XSHG", "sector_code": "L72", "industry_code": "L721", "board_type": "主板"},
            {"order_book_id": "600008.XSHG", "symbol": "首创环保", "exchange": "XSHG", "sector_code": "N77", "industry_code": "N772", "board_type": "主板"},
            {"order_book_id": "600009.XSHG", "symbol": "上海机场", "exchange": "XSHG", "sector_code": "G53", "industry_code": "G531", "board_type": "主板"},
            {"order_book_id": "600010.XSHG", "symbol": "包钢股份", "exchange": "XSHG", "sector_code": "C31", "industry_code": "C311", "board_type": "主板"},
            {"order_book_id": "600011.XSHG", "symbol": "华能国际", "exchange": "XSHG", "sector_code": "D44", "industry_code": "D441", "board_type": "主板"},
            {"order_book_id": "600012.XSHG", "symbol": "皖通高速", "exchange": "XSHG", "sector_code": "G54", "industry_code": "G549", "board_type": "主板"},
            {"order_book_id": "600015.XSHG", "symbol": "华夏银行", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600016.XSHG", "symbol": "民生银行", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600018.XSHG", "symbol": "上港集团", "exchange": "XSHG", "sector_code": "G53", "industry_code": "G531", "board_type": "主板"},
            {"order_book_id": "600019.XSHG", "symbol": "宝钢股份", "exchange": "XSHG", "sector_code": "C31", "industry_code": "C311", "board_type": "主板"},
            {"order_book_id": "600020.XSHG", "symbol": "中原高速", "exchange": "XSHG", "sector_code": "G54", "industry_code": "G549", "board_type": "主板"},
            {"order_book_id": "600021.XSHG", "symbol": "上海电力", "exchange": "XSHG", "sector_code": "D44", "industry_code": "D441", "board_type": "主板"},
            {"order_book_id": "600022.XSHG", "symbol": "山东钢铁", "exchange": "XSHG", "sector_code": "C31", "industry_code": "C311", "board_type": "主板"},
            {"order_book_id": "600023.XSHG", "symbol": "浙能电力", "exchange": "XSHG", "sector_code": "D44", "industry_code": "D441", "board_type": "主板"},
            {"order_book_id": "600025.XSHG", "symbol": "华能水电", "exchange": "XSHG", "sector_code": "D44", "industry_code": "D441", "board_type": "主板"},
            {"order_book_id": "600026.XSHG", "symbol": "中远海能", "exchange": "XSHG", "sector_code": "G51", "industry_code": "G511", "board_type": "主板"},
            {"order_book_id": "600027.XSHG", "symbol": "华电国际", "exchange": "XSHG", "sector_code": "D44", "industry_code": "D441", "board_type": "主板"},
            {"order_book_id": "600028.XSHG", "symbol": "中国石化", "exchange": "XSHG", "sector_code": "C25", "industry_code": "C251", "board_type": "主板"},
            {"order_book_id": "600029.XSHG", "symbol": "南方航空", "exchange": "XSHG", "sector_code": "G53", "industry_code": "G531", "board_type": "主板"},
            {"order_book_id": "600030.XSHG", "symbol": "中信证券", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600031.XSHG", "symbol": "三一重工", "exchange": "XSHG", "sector_code": "C35", "industry_code": "C351", "board_type": "主板"},
            {"order_book_id": "600036.XSHG", "symbol": "招商银行", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600037.XSHG", "symbol": "歌华有线", "exchange": "XSHG", "sector_code": "I64", "industry_code": "I641", "board_type": "主板"},
            {"order_book_id": "600038.XSHG", "symbol": "中直股份", "exchange": "XSHG", "sector_code": "C37", "industry_code": "C371", "board_type": "主板"},
            {"order_book_id": "600039.XSHG", "symbol": "四川路桥", "exchange": "XSHG", "sector_code": "E48", "industry_code": "E481", "board_type": "主板"},
            {"order_book_id": "600048.XSHG", "symbol": "保利地产", "exchange": "XSHG", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "600050.XSHG", "symbol": "中国联通", "exchange": "XSHG", "sector_code": "I63", "industry_code": "I631", "board_type": "主板"},
            {"order_book_id": "600051.XSHG", "symbol": "宁波联合", "exchange": "XSHG", "sector_code": "C28", "industry_code": "C281", "board_type": "主板"},
            {"order_book_id": "600052.XSHG", "symbol": "浙江广厦", "exchange": "XSHG", "sector_code": "K70", "industry_code": "K701", "board_type": "主板"},
            {"order_book_id": "600053.XSHG", "symbol": "九鼎投资", "exchange": "XSHG", "sector_code": "J66", "industry_code": "J661", "board_type": "主板"},
            {"order_book_id": "600054.XSHG", "symbol": "黄山旅游", "exchange": "XSHG", "sector_code": "N78", "industry_code": "N781", "board_type": "主板"},
            {"order_book_id": "600055.XSHG", "symbol": "万东医疗", "exchange": "XSHG", "sector_code": "C37", "industry_code": "C371", "board_type": "主板"},
            {"order_book_id": "600056.XSHG", "symbol": "中国医药", "exchange": "XSHG", "sector_code": "F51", "industry_code": "F511", "board_type": "主板"},
            {"order_book_id": "600057.XSHG", "symbol": "厦门象屿", "exchange": "XSHG", "sector_code": "G51", "industry_code": "G511", "board_type": "主板"},
            {"order_book_id": "600058.XSHG", "symbol": "五矿发展", "exchange": "XSHG", "sector_code": "C31", "industry_code": "C311", "board_type": "主板"},
            {"order_book_id": "600059.XSHG", "symbol": "古越龙山", "exchange": "XSHG", "sector_code": "C15", "industry_code": "C151", "board_type": "主板"},
            {"order_book_id": "600060.XSHG", "symbol": "海信视像", "exchange": "XSHG", "sector_code": "C39", "industry_code": "C391", "board_type": "主板"},
        ]

        # 为所有股票添加公共字段
        current_time = datetime.now()
        for instrument in fallback_instruments:
            instrument["type"] = instrument_type
            instrument["status"] = "Active"
            instrument["listed_date"] = "1990-01-01"  # 默认上市日期
            instrument["de_listed_date"] = ""  # 空字符串表示未退市
            instrument["data_date"] = current_time.strftime("%Y-%m-%d")
            instrument["updated_at"] = current_time.isoformat()

        logger.info(f"备用股票列表包含 {len(fallback_instruments)} 只股票")
        return fallback_instruments


# 全局实例
_rqdatac_data_loader = None


def get_rqdatac_data_loader(allow_mock_data: bool = False) -> RQDatacDataLoader:
    """
    获取RQDatac数据加载器实例

    Args:
        allow_mock_data: 是否允许在RQDatac不可用时使用模拟数据
    """
    global _rqdatac_data_loader
    if _rqdatac_data_loader is None:
        _rqdatac_data_loader = RQDatacDataLoader(allow_mock_data=allow_mock_data)
    return _rqdatac_data_loader


# 便捷函数
def load_instruments(instrument_type: str = "CS", market: str = "cn") -> pl.DataFrame:
    """加载股票列表"""
    loader = get_rqdatac_data_loader()
    return loader.get_instruments(instrument_type, market)


def load_trading_calendar(market: str = "cn", start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> List[str]:
    """加载交易日历"""
    loader = get_rqdatac_data_loader()
    return loader.get_trading_calendar(market, start_date, end_date)


def load_ohlcv_data(symbols: List[str], start_date: str, end_date: str,
                   frequency: str = "1d", adjust_type: str = "post") -> pl.DataFrame:
    """加载OHLCV数据"""
    loader = get_rqdatac_data_loader()
    return loader.get_ohlcv_data(symbols, start_date, end_date, frequency, adjust_type)


def load_dual_adjustment_ohlcv_data(symbols: List[str], start_date: str, end_date: str,
                                       frequency: str = "1d") -> pl.DataFrame:
    """加载双复权OHLCV数据（同时包含后复权和前复权价格）"""
    loader = get_rqdatac_data_loader()
    return loader.get_dual_adjustment_ohlcv_data(symbols, start_date, end_date, frequency)


def load_fundamental_data(symbols: List[str], fields: Optional[List[str]] = None,
                         start_date: Optional[str] = None, end_date: Optional[str] = None) -> pl.DataFrame:
    """加载基本面数据"""
    loader = get_rqdatac_data_loader()
    return loader.get_fundamental_data(symbols, fields, start_date, end_date)


def load_technical_data(symbols: List[str], indicators: Optional[List[str]] = None,
                       start_date: Optional[str] = None, end_date: Optional[str] = None) -> pl.DataFrame:
    """加载技术指标数据"""
    loader = get_rqdatac_data_loader()
    return loader.get_technical_data(symbols, indicators, start_date, end_date)


def load_factor_data(symbols: List[str], factors: Optional[List[str]] = None,
                    start_date: Optional[str] = None, end_date: Optional[str] = None) -> pl.DataFrame:
    """加载因子数据"""
    loader = get_rqdatac_data_loader()
    return loader.get_factor_data(symbols, factors, start_date, end_date)


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.INFO)

    print("=== RQDatac数据加载器测试 ===")

    # 测试股票列表
    print("\n1. 测试股票列表加载")
    instruments = load_instruments()
    print(f"获取到 {len(instruments)} 只股票")

    # 测试交易日历
    print("\n2. 测试交易日历加载")
    trading_dates = load_trading_calendar()
    print(f"获取到 {len(trading_dates)} 个交易日")

    # 测试OHLCV数据
    print("\n3. 测试OHLCV数据加载")
    symbols = ["000001.XSHE", "000002.XSHE"]
    ohlcv_data = load_ohlcv_data(symbols, "2025-09-01", "2025-09-15")
    print(f"获取到 {len(ohlcv_data)} 条OHLCV记录")

    # 测试基本面数据
    print("\n4. 测试基本面数据加载")
    fundamental_data = load_fundamental_data(symbols, ["pe_ratio", "pb_ratio"])
    print(f"获取到 {len(fundamental_data)} 条基本面记录")

    print("\n=== 测试完成 ===")


# ===== 便捷函数 =====
