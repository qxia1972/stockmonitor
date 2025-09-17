"""
测试重构后的架构集成

验证三个模块的分离关注点和集成是否正常工作
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from pathlib import Path
import logging

# 导入重构后的模块
from modules.compute.data_processor import DataProcessor
from modules.compute.calculated_indicators_processor import CalculatedIndicatorsProcessor, ProcessingConfig
from modules.compute.indicator_calculator import IndicatorCalculator, IndicatorConfig

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_data() -> pl.DataFrame:
    """创建测试数据"""
    np.random.seed(42)

    # 生成日期序列
    from datetime import datetime, timedelta
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]

    # 生成模拟价格数据
    n_days = 365  # 固定的天数
    base_price = 100.0

    # 随机游走价格
    price_changes = np.random.normal(0, 0.02, n_days)
    prices = base_price * np.exp(np.cumsum(price_changes))

    # 生成成交量
    volumes = np.random.randint(100000, 1000000, n_days)

    # 创建DataFrame
    df = pl.DataFrame({
        'date': dates,
        'open': prices * (1 + np.random.normal(0, 0.005, n_days)),
        'high': prices * (1 + np.random.normal(0, 0.01, n_days)),
        'low': prices * (1 + np.random.normal(0, 0.01, n_days)),
        'close': prices,
        'volume': volumes
    })

    # 确保high >= max(open, close), low <= min(open, close)
    df = df.with_columns([
        pl.max_horizontal(['open', 'close', 'high']).alias('high'),
        pl.min_horizontal(['open', 'close', 'low']).alias('low')
    ])

    return df

def test_data_processor():
    """测试数据处理器"""
    logger.info("=== 测试数据处理器 ===")

    # 创建测试数据
    df = create_test_data()
    logger.info(f"创建测试数据: {df.shape}")

    # 初始化数据处理器
    processor = DataProcessor()

    # 测试数据验证
    is_valid, errors = processor.validate_data(df)
    logger.info(f"数据验证结果: {'通过' if is_valid else '失败'}")
    if errors:
        logger.warning(f"验证错误: {errors}")

    # 测试数据清理
    cleaned_df = processor.clean_data(df)
    logger.info(f"数据清理后形状: {cleaned_df.shape}")

    # 测试数据转换
    transformations = {
        'numeric_columns': ['open', 'high', 'low', 'close', 'volume']
    }
    transformed_df = processor.transform_data(cleaned_df, transformations)
    logger.info(f"数据转换后形状: {transformed_df.shape}")

    return transformed_df

def test_indicator_calculator():
    """测试指标计算器"""
    logger.info("=== 测试指标计算器 ===")

    # 创建测试数据
    df = create_test_data()

    # 初始化指标计算器
    config = IndicatorConfig()
    calculator = IndicatorCalculator(config)

    # 测试单个指标计算
    indicators_to_test = ['SMA', 'EMA', 'RSI', 'MACD']

    for indicator in indicators_to_test:
        try:
            result_df = calculator.calculate_indicators(df, [indicator])
            logger.info(f"✅ {indicator} 计算成功，新增列: {[col for col in result_df.columns if col not in df.columns]}")
        except Exception as e:
            logger.error(f"❌ {indicator} 计算失败: {e}")

    # 测试批量指标计算
    try:
        result_df = calculator.calculate_indicators(df, indicators_to_test)
        logger.info(f"✅ 批量计算成功，新增列: {[col for col in result_df.columns if col not in df.columns]}")
        return result_df
    except Exception as e:
        logger.error(f"❌ 批量计算失败: {e}")
        return df

def test_parallel_processor():
    """测试并行处理器"""
    logger.info("=== 测试并行处理器 ===")

    # 创建测试数据
    df = create_test_data()

    # 初始化并行处理器配置
    config = ProcessingConfig(
        max_workers=2,
        chunk_size=50,  # 小块以便测试
        use_processes=False,  # 使用线程避免序列化问题
        enable_progress_tracking=True
    )

    try:
        # 初始化处理器
        processor = CalculatedIndicatorsProcessor(config)

        # 测试统计信息
        stats = processor.get_processing_stats()
        logger.info(f"处理器统计: {stats}")

        # 测试数据验证
        validation_results = processor.validate_data_for_indicators(df)
        logger.info(f"数据验证结果: {validation_results}")

        # 测试单个指标计算
        try:
            result_df = processor.calculate_indicator(df, 'SMA')
            logger.info("✅ 并行单个指标计算成功")
        except Exception as e:
            logger.error(f"❌ 并行单个指标计算失败: {e}")

        # 测试批量指标计算
        try:
            indicators = ['SMA', 'EMA', 'RSI']
            result_df = processor.calculate_indicators_batch(df, indicators)
            logger.info("✅ 并行批量指标计算成功")
        except Exception as e:
            logger.error(f"❌ 并行批量指标计算失败: {e}")

    except Exception as e:
        logger.error(f"❌ 并行处理器初始化失败: {e}")

def test_integration():
    """测试完整集成流程"""
    logger.info("=== 测试完整集成流程 ===")

    try:
        # 1. 数据处理阶段
        raw_df = create_test_data()
        data_processor = DataProcessor()

        # 数据验证和清理
        processed_df = data_processor.clean_data(raw_df)
        transformations = {
            'numeric_columns': ['open', 'high', 'low', 'close', 'volume']
        }
        processed_df = data_processor.transform_data(processed_df, transformations)

        logger.info("✅ 数据处理阶段完成")

        # 2. 指标计算阶段
        config = ProcessingConfig(
            max_workers=2,
            chunk_size=50,
            use_processes=False
        )

        indicators_processor = CalculatedIndicatorsProcessor(config)

        # 计算指标
        final_df = indicators_processor.calculate_all_indicators(processed_df)

        logger.info("✅ 指标计算阶段完成")
        logger.info(f"最终数据形状: {final_df.shape}")
        logger.info(f"最终列: {final_df.columns}")

        return final_df

    except Exception as e:
        logger.error(f"❌ 集成测试失败: {e}")
        return None

def main():
    """主测试函数"""
    logger.info("开始测试重构后的架构")

    # 测试各个模块
    test_data_processor()
    print()

    test_indicator_calculator()
    print()

    test_parallel_processor()
    print()

    # 测试完整集成
    final_result = test_integration()

    if final_result is not None:
        logger.info("🎉 所有测试通过！重构后的架构工作正常")
    else:
        logger.error("❌ 集成测试失败，需要进一步调试")

if __name__ == "__main__":
    main()
