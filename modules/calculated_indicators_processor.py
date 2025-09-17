"""
计算指标处理器 (Calculated Indicators Processor)

根据配置自动计算各种技术指标和衍生指标
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
from pathlib import Path

from modules.compute.indicator_calculator import indicator_calculator
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


class CalculatedIndicatorsProcessor:
    """计算指标处理器"""

    def __init__(self):
        """初始化处理器"""
        if not is_calculated_indicators_enabled():
            raise ValueError("计算指标功能未启用，请在配置中设置 enabled: true")

        self.enabled_indicators = get_enabled_calculated_indicators()
        self.processing_options = get_calculated_indicators_processing_options()
        self.logger = logging.getLogger(__name__)

        self.logger.info(f"计算指标处理器初始化完成")
        self.logger.info(f"启用指标数量: {len(self.enabled_indicators)}")
        self.logger.info(f"处理选项: {self.processing_options}")

    def calculate_all_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算所有启用的指标"""
        if not is_auto_calculate_enabled():
            self.logger.info("自动计算已禁用，跳过指标计算")
            return df

        self.logger.info(f"开始计算 {len(self.enabled_indicators)} 个指标")

        result_df = df.clone()

        for indicator_name, config in self.enabled_indicators.items():
            try:
                self.logger.info(f"计算指标: {indicator_name}")
                result_df = self.calculate_indicator(result_df, indicator_name, config)
            except Exception as e:
                self.logger.error(f"计算指标 {indicator_name} 失败: {e}")
                continue

        self.logger.info("指标计算完成")
        return result_df

    def calculate_indicator(self, df: pl.DataFrame, indicator_name: str, config: Dict[str, Any]) -> pl.DataFrame:
        """计算单个指标"""
        formula = config.get("formula", "")
        dependencies = config.get("dependencies", [])
        parameters = config.get("parameters", {})

        # 验证依赖字段是否存在
        if not validate_calculated_indicator_dependencies(indicator_name, df.columns):
            missing_deps = [dep for dep in dependencies if dep not in df.columns]
            raise ValueError(f"指标 {indicator_name} 缺少依赖字段: {missing_deps}")

        # 根据公式计算指标
        if formula == "amount / volume":
            # VWAP: 成交均价
            result_df = indicator_calculator.calculate_vwap(df)

        elif formula == "(close - prev_close) / prev_close":
            # Returns: 收益率
            shift = parameters.get("shift", 1)
            result_df = indicator_calculator.calculate_returns(df, shift)

        elif formula == "volume / sma_volume_20":
            # Volume Ratio: 量比
            window = parameters.get("window", 20)
            result_df = indicator_calculator.calculate_volume_ratio(df, window)

        elif formula == "amount":
            # Total Turnover: 总成交额（直接使用amount）
            result_df = indicator_calculator.calculate_total_turnover(df)

        elif formula == "close - open":
            # Price Change: 涨跌额
            result_df = indicator_calculator.calculate_price_change(df)

        elif formula == "(close - open) / open":
            # Price Change Percentage: 涨跌幅
            result_df = indicator_calculator.calculate_price_change_pct(df)

        elif formula == "(high - low) / low":
            # High-Low Range: 振幅
            result_df = indicator_calculator.calculate_amplitude(df)

        elif formula == "(open + close + high + low) / 4":
            # Average Price: 平均价
            result_df = indicator_calculator.calculate_avg_price(df)

        else:
            raise ValueError(f"不支持的计算公式: {formula}")

        return result_df

    def get_indicator_info(self, indicator_name: str) -> Optional[Dict[str, Any]]:
        """获取指标信息"""
        return self.enabled_indicators.get(indicator_name)

    def list_available_indicators(self) -> List[str]:
        """列出所有可用的指标"""
        return list(self.enabled_indicators.keys())

    def validate_data_for_indicators(self, df: pl.DataFrame) -> Dict[str, bool]:
        """验证数据是否满足所有指标的计算要求"""
        results = {}
        for indicator_name in self.enabled_indicators.keys():
            results[indicator_name] = validate_calculated_indicator_dependencies(
                indicator_name, df.columns
            )
        return results


def process_calculated_indicators(data_file: str, output_file: Optional[str] = None) -> str:
    """处理数据文件，计算所有指标"""
    logger.info(f"处理数据文件: {data_file}")

    # 读取数据
    df = pl.read_parquet(data_file)
    logger.info(f"原始数据形状: {df.shape}")

    # 初始化处理器
    processor = CalculatedIndicatorsProcessor()

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


if __name__ == "__main__":
    # 测试代码
    print("=== 计算指标处理器测试 ===")

    processor = CalculatedIndicatorsProcessor()

    print(f"启用指标数量: {len(processor.enabled_indicators)}")
    print(f"可用指标: {processor.list_available_indicators()}")

    print("\n=== 指标详情 ===")
    for name in processor.list_available_indicators():
        info = processor.get_indicator_info(name)
        if info:
            print(f"{name}: {info.get('description', '')}")
            print(f"  依赖: {info.get('dependencies', [])}")
            print(f"  公式: {info.get('formula', '')}")
            print()

    # 如果有数据文件，测试处理
    data_dir = Path("data")
    if data_dir.exists():
        parquet_files = list(data_dir.glob("*.parquet"))
        if parquet_files:
            test_file = str(parquet_files[0])
            print(f"\n=== 测试处理文件: {test_file} ===")
            try:
                result_file = process_calculated_indicators(test_file)
                print(f"✅ 处理完成，结果文件: {result_file}")
            except Exception as e:
                print(f"❌ 处理失败: {e}")
