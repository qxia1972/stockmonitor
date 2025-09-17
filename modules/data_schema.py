"""
数据字段定义 (Data Field Definitions)

定义从RQDatac等数据源拉取的字段信息
包括价格数据、技术指标、基本面数据等字段的规范定义

作者: 系统架构师
创建日期: 2025年9月16日
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class FieldDefinition:
    """字段定义类"""
    name: str
    type: str
    nullable: bool = True
    description: str = ""
    source: str = "rqdatac"  # 数据源
    category: str = "price"  # 字段类别


# 价格数据字段定义
PRICE_FIELDS = {
    "open": FieldDefinition(
        name="open",
        type="float64",
        nullable=False,
        description="开盘价",
        source="rqdatac",
        category="price"
    ),
    "close": FieldDefinition(
        name="close",
        type="float64",
        nullable=False,
        description="收盘价",
        source="rqdatac",
        category="price"
    ),
    "high": FieldDefinition(
        name="high",
        type="float64",
        nullable=False,
        description="最高价",
        source="rqdatac",
        category="price"
    ),
    "low": FieldDefinition(
        name="low",
        type="float64",
        nullable=False,
        description="最低价",
        source="rqdatac",
        category="price"
    ),
    "volume": FieldDefinition(
        name="volume",
        type="int64",
        nullable=False,
        description="成交量",
        source="rqdatac",
        category="volume"
    ),
    "amount": FieldDefinition(
        name="amount",
        type="float64",
        nullable=True,
        description="成交额",
        source="rqdatac",
        category="volume"
    )
}

# 基本面数据字段定义
FUNDAMENTAL_FIELDS = {
    "pe_ratio": FieldDefinition(
        name="pe_ratio",
        type="float64",
        nullable=True,
        description="市盈率",
        source="rqdatac",
        category="valuation"
    ),
    "pb_ratio": FieldDefinition(
        name="pb_ratio",
        type="float64",
        nullable=True,
        description="市净率",
        source="rqdatac",
        category="valuation"
    ),
    "ps_ratio": FieldDefinition(
        name="ps_ratio",
        type="float64",
        nullable=True,
        description="市销率",
        source="rqdatac",
        category="valuation"
    ),
    "pcf_ratio": FieldDefinition(
        name="pcf_ratio",
        type="float64",
        nullable=True,
        description="市现率",
        source="rqdatac",
        category="valuation"
    ),
    "market_cap": FieldDefinition(
        name="market_cap",
        type="float64",
        nullable=True,
        description="总市值",
        source="rqdatac",
        category="market"
    ),
    "circulating_market_cap": FieldDefinition(
        name="circulating_market_cap",
        type="float64",
        nullable=True,
        description="流通市值",
        source="rqdatac",
        category="market"
    ),
    "total_shares": FieldDefinition(
        name="total_shares",
        type="float64",
        nullable=True,
        description="总股本",
        source="rqdatac",
        category="market"
    ),
    "float_shares": FieldDefinition(
        name="float_shares",
        type="float64",
        nullable=True,
        description="流通股本",
        source="rqdatac",
        category="market"
    ),
    "turnover_ratio": FieldDefinition(
        name="turnover_ratio",
        type="float64",
        nullable=True,
        description="换手率",
        source="rqdatac",
        category="liquidity"
    ),
    "float_turnover_ratio": FieldDefinition(
        name="float_turnover_ratio",
        type="float64",
        nullable=True,
        description="流通换手率",
        source="rqdatac",
        category="liquidity"
    ),
    "roe": FieldDefinition(
        name="roe",
        type="float64",
        nullable=True,
        description="净资产收益率",
        source="rqdatac",
        category="profitability"
    ),
    "roa": FieldDefinition(
        name="roa",
        type="float64",
        nullable=True,
        description="总资产收益率",
        source="rqdatac",
        category="profitability"
    ),
    "gross_profit_margin": FieldDefinition(
        name="gross_profit_margin",
        type="float64",
        nullable=True,
        description="毛利率",
        source="rqdatac",
        category="profitability"
    ),
    "net_profit_margin": FieldDefinition(
        name="net_profit_margin",
        type="float64",
        nullable=True,
        description="净利率",
        source="rqdatac",
        category="profitability"
    ),
    "debt_to_equity": FieldDefinition(
        name="debt_to_equity",
        type="float64",
        nullable=True,
        description="负债权益比",
        source="rqdatac",
        category="solvency"
    ),
    "current_ratio": FieldDefinition(
        name="current_ratio",
        type="float64",
        nullable=True,
        description="流动比率",
        source="rqdatac",
        category="solvency"
    )
}

# 仪器/证券基本信息字段定义
INSTRUMENT_FIELDS = {
    "order_book_id": FieldDefinition(
        name="order_book_id",
        type="string",
        nullable=False,
        description="证券代码",
        source="rqdatac",
        category="instrument"
    ),
    "symbol": FieldDefinition(
        name="symbol",
        type="string",
        nullable=False,
        description="证券简称",
        source="rqdatac",
        category="instrument"
    ),
    "exchange": FieldDefinition(
        name="exchange",
        type="string",
        nullable=False,
        description="交易所",
        source="rqdatac",
        category="instrument"
    ),
    "type": FieldDefinition(
        name="type",
        type="string",
        nullable=False,
        description="证券类型",
        source="rqdatac",
        category="instrument"
    ),
    "status": FieldDefinition(
        name="status",
        type="string",
        nullable=True,
        description="交易状态",
        source="rqdatac",
        category="instrument"
    ),
    "listed_date": FieldDefinition(
        name="listed_date",
        type="string",
        nullable=True,
        description="上市日期",
        source="rqdatac",
        category="instrument"
    ),
    "de_listed_date": FieldDefinition(
        name="de_listed_date",
        type="string",
        nullable=True,
        description="退市日期",
        source="rqdatac",
        category="instrument"
    ),
    "sector_code": FieldDefinition(
        name="sector_code",
        type="string",
        nullable=True,
        description="板块代码",
        source="rqdatac",
        category="instrument"
    ),
    "industry_code": FieldDefinition(
        name="industry_code",
        type="string",
        nullable=True,
        description="行业代码",
        source="rqdatac",
        category="instrument"
    ),
    "board_type": FieldDefinition(
        name="board_type",
        type="string",
        nullable=True,
        description="板块类型",
        source="rqdatac",
        category="instrument"
    )
}

# 因子数据字段定义
FACTOR_FIELDS = {
    "date": FieldDefinition(
        name="date",
        type="string",
        nullable=False,
        description="交易日期",
        source="calculated",
        category="temporal"
    ),
    "factor_name": FieldDefinition(
        name="factor_name",
        type="string",
        nullable=False,
        description="因子名称",
        source="calculated",
        category="factor"
    ),
    "factor_value": FieldDefinition(
        name="factor_value",
        type="float64",
        nullable=True,
        description="因子值",
        source="calculated",
        category="factor"
    ),
    "factor_category": FieldDefinition(
        name="factor_category",
        type="string",
        nullable=True,
        description="因子类别",
        source="calculated",
        category="factor"
    )
}

# 技术指标字段定义
TECHNICAL_FIELDS = {
    "sma_5": FieldDefinition(
        name="sma_5",
        type="float64",
        nullable=True,
        description="5日简单移动平均",
        category="technical"
    ),
    "sma_10": FieldDefinition(
        name="sma_10",
        type="float64",
        nullable=True,
        description="10日简单移动平均",
        category="technical"
    ),
    "sma_20": FieldDefinition(
        name="sma_20",
        type="float64",
        nullable=True,
        description="20日简单移动平均",
        category="technical"
    ),
    "sma_30": FieldDefinition(
        name="sma_30",
        type="float64",
        nullable=True,
        description="30日简单移动平均",
        category="technical"
    ),
    "ema_5": FieldDefinition(
        name="ema_5",
        type="float64",
        nullable=True,
        description="5日指数移动平均",
        category="technical"
    ),
    "ema_10": FieldDefinition(
        name="ema_10",
        type="float64",
        nullable=True,
        description="10日指数移动平均",
        category="technical"
    ),
    "ema_20": FieldDefinition(
        name="ema_20",
        type="float64",
        nullable=True,
        description="20日指数移动平均",
        category="technical"
    ),
    "ema_30": FieldDefinition(
        name="ema_30",
        type="float64",
        nullable=True,
        description="30日指数移动平均",
        category="technical"
    ),
    "rsi_6": FieldDefinition(
        name="rsi_6",
        type="float64",
        nullable=True,
        description="6日RSI指标",
        category="technical"
    ),
    "rsi_12": FieldDefinition(
        name="rsi_12",
        type="float64",
        nullable=True,
        description="12日RSI指标",
        category="technical"
    ),
    "rsi_14": FieldDefinition(
        name="rsi_14",
        type="float64",
        nullable=True,
        description="14日RSI指标",
        category="technical"
    ),
    "macd": FieldDefinition(
        name="macd",
        type="float64",
        nullable=True,
        description="MACD指标",
        category="technical"
    ),
    "macd_signal": FieldDefinition(
        name="macd_signal",
        type="float64",
        nullable=True,
        description="MACD信号线",
        category="technical"
    ),
    "macd_hist": FieldDefinition(
        name="macd_hist",
        type="float64",
        nullable=True,
        description="MACD柱状图",
        category="technical"
    ),
    "bollinger_upper": FieldDefinition(
        name="bollinger_upper",
        type="float64",
        nullable=True,
        description="布林带上轨",
        category="technical"
    ),
    "bollinger_middle": FieldDefinition(
        name="bollinger_middle",
        type="float64",
        nullable=True,
        description="布林带中轨",
        category="technical"
    ),
    "bollinger_lower": FieldDefinition(
        name="bollinger_lower",
        type="float64",
        nullable=True,
        description="布林带下轨",
        category="technical"
    ),
    "stoch_k": FieldDefinition(
        name="stoch_k",
        type="float64",
        nullable=True,
        description="随机指标K值",
        category="technical"
    ),
    "stoch_d": FieldDefinition(
        name="stoch_d",
        type="float64",
        nullable=True,
        description="随机指标D值",
        category="technical"
    ),
    "atr_14": FieldDefinition(
        name="atr_14",
        type="float64",
        nullable=True,
        description="14日平均真实波幅",
        category="technical"
    ),
    "vwap": FieldDefinition(
        name="vwap",
        type="float64",
        nullable=True,
        description="成交均价（计算指标）",
        category="technical"
    ),
    "returns": FieldDefinition(
        name="returns",
        type="float64",
        nullable=True,
        description="收益率（计算指标）",
        category="technical"
    ),
    "volume_ratio": FieldDefinition(
        name="volume_ratio",
        type="float64",
        nullable=True,
        description="量比（计算指标）",
        category="technical"
    ),
    "price_change": FieldDefinition(
        name="price_change",
        type="float64",
        nullable=True,
        description="涨跌额（计算指标）",
        category="price"
    ),
    "price_change_pct": FieldDefinition(
        name="price_change_pct",
        type="float64",
        nullable=True,
        description="涨跌幅（计算指标）",
        category="price"
    ),
    "high_low_range": FieldDefinition(
        name="high_low_range",
        type="float64",
        nullable=True,
        description="振幅（计算指标）",
        category="price"
    ),
    "avg_price": FieldDefinition(
        name="avg_price",
        type="float64",
        nullable=True,
        description="平均价（计算指标）",
        category="price"
    )
}

# 所有字段的统一集合
ALL_FIELDS = {
    **PRICE_FIELDS,
    **FUNDAMENTAL_FIELDS,
    **INSTRUMENT_FIELDS,
    **FACTOR_FIELDS,
    **TECHNICAL_FIELDS
}

# 按类别分组的字段
FIELDS_BY_CATEGORY = {
    "price": {k: v for k, v in ALL_FIELDS.items() if v.category == "price"},
    "volume": {k: v for k, v in ALL_FIELDS.items() if v.category == "volume"},
    "return": {k: v for k, v in ALL_FIELDS.items() if v.category == "return"},
    "valuation": {k: v for k, v in ALL_FIELDS.items() if v.category == "valuation"},
    "market": {k: v for k, v in ALL_FIELDS.items() if v.category == "market"},
    "liquidity": {k: v for k, v in ALL_FIELDS.items() if v.category == "liquidity"},
    "profitability": {k: v for k, v in ALL_FIELDS.items() if v.category == "profitability"},
    "solvency": {k: v for k, v in ALL_FIELDS.items() if v.category == "solvency"},
    "technical": {k: v for k, v in ALL_FIELDS.items() if v.category == "technical"},
    "instrument": {k: v for k, v in ALL_FIELDS.items() if v.category == "instrument"},
    "factor": {k: v for k, v in ALL_FIELDS.items() if v.category == "factor"},
    "temporal": {k: v for k, v in ALL_FIELDS.items() if v.category == "temporal"}
}

# 数据拉取配置
DATA_FETCH_CONFIG = {
    "price_data": {
        "fields": ["open", "close", "high", "low", "volume", "amount"],
        "frequency": "1d",
        "adjust_type": "pre",  # 修改为前复权，与实际使用一致
        "required": True
    },
    "fundamental_data": {
        "fields": ["pe_ratio", "pb_ratio", "market_cap", "turnover_ratio", "roe"],
        "frequency": "1d",
        "required": False
    },
    "technical_data": {
        "fields": ["sma_5", "sma_10", "sma_20", "rsi_14", "macd"],
        "frequency": "1d",
        "required": False
    }
}

# 数据分区配置
DATA_PARTITION_CONFIG = {
    "time_partitions": {
        "enabled": True,
        "partition_size": "1year",  # 按年分区
        "overlap_days": 30  # 重叠天数，避免边界问题
    },
    "stock_partitions": {
        "enabled": True,
        "partition_size": 500,  # 每批500只股票
        "shuffle_stocks": True  # 随机打乱股票顺序
    },
    "retry_config": {
        "max_retries": 3,
        "retry_delay": 60,  # 秒
        "backoff_factor": 2
    }
}

# 计算指标配置
CALCULATED_INDICATORS_CONFIG = {
    "enabled": True,  # 是否启用计算指标功能
    "auto_calculate": True,  # 是否自动计算所有启用的指标
    "frequency": "1d",  # 计算频率
    "indicators": {
        "vwap": {
            "formula": "amount / volume",
            "description": "成交均价",
            "dependencies": ["amount", "volume"],
            "enabled": True,
            "category": "technical"
        },
        "returns": {
            "formula": "(close - prev_close) / prev_close",
            "description": "收益率",
            "dependencies": ["close"],
            "parameters": {"shift": 1},
            "enabled": True,
            "category": "technical"
        },
        "volume_ratio": {
            "formula": "volume / sma_volume_20",
            "description": "量比（当前成交量/20日平均成交量）",
            "dependencies": ["volume"],
            "parameters": {"window": 20},
            "enabled": True,
            "category": "technical"
        },
        "price_change": {
            "formula": "close - open",
            "description": "涨跌额",
            "dependencies": ["close", "open"],
            "enabled": True,
            "category": "price"
        },
        "price_change_pct": {
            "formula": "(close - open) / open",
            "description": "涨跌幅",
            "dependencies": ["close", "open"],
            "enabled": True,
            "category": "price"
        },
        "high_low_range": {
            "formula": "(high - low) / low",
            "description": "振幅",
            "dependencies": ["high", "low"],
            "enabled": True,
            "category": "price"
        },
        "avg_price": {
            "formula": "(open + close + high + low) / 4",
            "description": "平均价",
            "dependencies": ["open", "close", "high", "low"],
            "enabled": True,
            "category": "price"
        }
    },
    "processing_options": {
        "group_by_symbol": True,  # 是否按股票代码分组计算
        "handle_missing_data": "skip",  # 缺失数据处理方式: skip, fill, error
        "parallel_processing": False,  # 是否启用并行处理
        "batch_size": 1000  # 批处理大小
    }
}

def get_required_fields() -> List[str]:
    """获取必需的字段列表"""
    required = []
    for config in DATA_FETCH_CONFIG.values():
        if config.get("required", False):
            required.extend(config["fields"])
    return list(set(required))


def get_optional_fields() -> List[str]:
    """获取可选的字段列表"""
    optional = []
    for config in DATA_FETCH_CONFIG.values():
        if not config.get("required", False):
            if "fields" in config:
                optional.extend(config["fields"])
    return list(set(optional))


def get_calculated_indicators() -> Dict[str, Dict[str, Any]]:
    """获取所有计算指标配置"""
    return CALCULATED_INDICATORS_CONFIG.get("indicators", {})


def get_enabled_calculated_indicators() -> Dict[str, Dict[str, Any]]:
    """获取所有启用的计算指标配置"""
    all_indicators = get_calculated_indicators()
    return {name: config for name, config in all_indicators.items()
            if config.get("enabled", True)}


def is_calculated_indicators_enabled() -> bool:
    """检查计算指标功能是否启用"""
    return CALCULATED_INDICATORS_CONFIG.get("enabled", False)


def is_auto_calculate_enabled() -> bool:
    """检查是否启用自动计算"""
    return CALCULATED_INDICATORS_CONFIG.get("auto_calculate", False)


def get_calculated_indicator_dependencies(indicator_name: str) -> List[str]:
    """获取计算指标的依赖字段"""
    indicators = get_calculated_indicators()
    if indicator_name in indicators:
        return indicators[indicator_name].get("dependencies", [])
    return []


def validate_calculated_indicator_dependencies(indicator_name: str, available_fields: List[str]) -> bool:
    """验证计算指标的依赖字段是否都可用"""
    dependencies = get_calculated_indicator_dependencies(indicator_name)
    return all(dep in available_fields for dep in dependencies)


def get_calculated_indicators_by_dependency(dependency_field: str) -> List[str]:
    """获取依赖特定字段的所有计算指标"""
    indicators = get_calculated_indicators()
    result = []
    for name, config in indicators.items():
        if dependency_field in config.get("dependencies", []):
            result.append(name)
    return result


def get_calculated_indicators_processing_options() -> Dict[str, Any]:
    """获取计算指标处理选项"""
    return CALCULATED_INDICATORS_CONFIG.get("processing_options", {})


def get_fields_by_category(category: str) -> Dict[str, FieldDefinition]:
    """按类别获取字段定义"""
    return FIELDS_BY_CATEGORY.get(category, {})


def validate_field_data(data: Dict[str, Any], field_name: str) -> bool:
    """验证字段数据的有效性"""
    if field_name not in ALL_FIELDS:
        return False

    field_def = ALL_FIELDS[field_name]
    value = data.get(field_name)

    # 检查必需字段
    if not field_def.nullable and value is None:
        return False

    # 检查数据类型
    if value is not None:
        expected_type = field_def.type
        if expected_type == "float64" and not isinstance(value, (int, float)):
            return False
        elif expected_type == "int64" and not isinstance(value, int):
            return False
        elif expected_type == "string" and not isinstance(value, str):
            return False

    return True


def get_field_type(field_name: str) -> Optional[str]:
    """从元数据定义中获取字段的输出类型"""
    field_def = ALL_FIELDS.get(field_name)
    return field_def.type if field_def else None


def get_calculated_indicator_output_type(indicator_name: str) -> Optional[str]:
    """获取计算指标的输出类型（基于元数据定义）"""
    return get_field_type(indicator_name)


def get_field_source(field_name: str) -> str:
    """动态获取字段的数据源"""
    # 检查是否是计算指标
    calculated_indicators = get_calculated_indicators()
    if field_name in calculated_indicators:
        return "calculated"

    # 检查是否在数据拉取配置中
    for config_name, config in DATA_FETCH_CONFIG.items():
        if "fields" in config and field_name in config["fields"]:
            if config_name == "price_data":
                return "rqdatac"
            elif config_name == "fundamental_data":
                return "rqdatac"
            elif config_name == "technical_data":
                return "rqdatac"

    # 默认数据源
    return "unknown"


def get_fields_by_source(source: str) -> List[str]:
    """获取指定数据源的所有字段"""
    return [name for name, field_def in ALL_FIELDS.items()
            if get_field_source(name) == source]


def get_data_source_summary() -> Dict[str, List[str]]:
    """获取数据源汇总"""
    sources = {}
    for field_name in ALL_FIELDS.keys():
        source = get_field_source(field_name)
        if source not in sources:
            sources[source] = []
        sources[source].append(field_name)

    # 排序字段名
    for source in sources:
        sources[source].sort()

    return sources


if __name__ == "__main__":
    # 测试代码
    print("=== 数据字段定义测试 ===")

    print(f"总字段数量: {len(ALL_FIELDS)}")
    print(f"必需字段: {get_required_fields()}")
    print(f"可选字段数量: {len(get_optional_fields())}")

    print("\n=== 按类别统计 ===")
    for category, fields in FIELDS_BY_CATEGORY.items():
        print(f"{category}: {len(fields)} 个字段")

    print("\n=== 计算指标配置测试 ===")
    print(f"计算指标功能启用: {is_calculated_indicators_enabled()}")
    print(f"自动计算启用: {is_auto_calculate_enabled()}")

    calculated_indicators = get_calculated_indicators()
    enabled_indicators = get_enabled_calculated_indicators()

    print(f"总计算指标数量: {len(calculated_indicators)}")
    print(f"启用计算指标数量: {len(enabled_indicators)}")

    processing_options = get_calculated_indicators_processing_options()
    print(f"处理选项: {processing_options}")

    print("\n=== 计算指标详情 ===")
    for name, config in enabled_indicators.items():
        dependencies = config.get("dependencies", [])
        formula = config.get("formula", "")
        category = config.get("category", "")
        print(f"📊 {name} ({category}): {config.get('description', '')}")
        print(f"   公式: {formula}")
        print(f"   依赖: {dependencies}")
        print(f"   参数: {config.get('parameters', {})}")
        print()

    print("\n=== 数据源分析 ===")
    source_summary = get_data_source_summary()
    for source, fields in source_summary.items():
        print(f"{source}: {len(fields)} 个字段")
        if len(fields) <= 10:  # 只显示少量字段的完整列表
            print(f"  字段: {fields}")
        else:
            print(f"  示例字段: {fields[:5]}...")

    print("\n=== 字段数据源验证 ===")
    test_fields = ["close", "pe_ratio", "sma_5", "vwap", "returns"]
    for field in test_fields:
        source = get_field_source(field)
        print(f"{field}: {source}")

    print("\n=== 示例字段定义 ===")
    close_field = ALL_FIELDS["close"]
    print(f"close字段: {close_field.description}, 类型: {close_field.type}, 必需: {not close_field.nullable}")

    pe_field = ALL_FIELDS["pe_ratio"]
    print(f"pe_ratio字段: {pe_field.description}, 类型: {pe_field.type}, 可选: {pe_field.nullable}")


# =============================================================================
# 表级Schema定义 (Table-level Schema Definitions)
# =============================================================================

@dataclass
class TableSchema:
    """表级schema定义类"""
    name: str
    columns: Dict[str, FieldDefinition]
    primary_key: List[str]
    indexes: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None
    description: str = ""

    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []
        if self.partition_by is None:
            self.partition_by = []


def create_table_schema_from_fields(
    table_name: str,
    field_names: List[str],
    primary_key: List[str],
    indexes: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    description: str = ""
) -> TableSchema:
    """从字段名称列表创建表schema"""
    columns = {}
    for field_name in field_names:
        if field_name in ALL_FIELDS:
            columns[field_name] = ALL_FIELDS[field_name]
        else:
            raise ValueError(f"字段 '{field_name}' 不在 ALL_FIELDS 中定义")

    return TableSchema(
        name=table_name,
        columns=columns,
        primary_key=primary_key,
        indexes=indexes or [],
        partition_by=partition_by or [],
        description=description
    )


# 预定义的表schema
INSTRUMENTS_TABLE_SCHEMA = create_table_schema_from_fields(
    table_name="instruments",
    field_names=[
        "order_book_id", "symbol", "exchange", "type", "status",
        "listed_date", "de_listed_date", "sector_code", "industry_code", "board_type"
    ],
    primary_key=["order_book_id"],
    indexes=["symbol", "exchange"],
    partition_by=["exchange"],
    description="证券基本信息表"
)

FACTORS_TABLE_SCHEMA = create_table_schema_from_fields(
    table_name="factors",
    field_names=[
        "order_book_id", "symbol", "date", "factor_name", "factor_value", "factor_category"
    ],
    primary_key=["order_book_id", "date", "factor_name"],
    indexes=["date", "factor_name"],
    partition_by=["date"],
    description="因子数据表"
)

VALUATION_TABLE_SCHEMA = create_table_schema_from_fields(
    table_name="valuation",
    field_names=[
        "order_book_id", "date", "pe_ratio", "pb_ratio", "ps_ratio", "pcf_ratio",
        "market_cap", "circulating_market_cap", "total_shares", "float_shares",
        "turnover_ratio", "float_turnover_ratio", "roe", "roa",
        "gross_profit_margin", "net_profit_margin", "debt_to_equity", "current_ratio"
    ],
    primary_key=["order_book_id", "date"],
    indexes=["date"],
    partition_by=["date"],
    description="估值数据表"
)

# 所有预定义表schema的集合
ALL_TABLE_SCHEMAS = {
    "instruments": INSTRUMENTS_TABLE_SCHEMA,
    "factors": FACTORS_TABLE_SCHEMA,
    "valuation": VALUATION_TABLE_SCHEMA
}


# =============================================================================
# 兼容性函数 (Compatibility Functions)
# =============================================================================

def get_table_schema(table_name: str) -> TableSchema:
    """获取表schema（兼容旧接口）"""
    if table_name in ALL_TABLE_SCHEMAS:
        return ALL_TABLE_SCHEMAS[table_name]
    else:
        raise ValueError(f"表 '{table_name}' 的schema未定义")


def get_table_columns(table_name: str) -> Dict[str, Dict[str, Any]]:
    """获取表的列定义（兼容schema_manager.py格式）"""
    schema = get_table_schema(table_name)
    columns = {}
    for field_name, field_def in schema.columns.items():
        columns[field_name] = {
            'type': field_def.type,
            'nullable': field_def.nullable,
            'description': field_def.description
        }
    return columns


def get_table_primary_key(table_name: str) -> List[str]:
    """获取表的主键"""
    return get_table_schema(table_name).primary_key


def get_table_indexes(table_name: str) -> List[str]:
    """获取表的索引"""
    return get_table_schema(table_name).indexes or []


def get_table_partition_by(table_name: str) -> List[str]:
    """获取表的分区字段"""
    return get_table_schema(table_name).partition_by or []