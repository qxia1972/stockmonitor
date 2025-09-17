#!/usr/bin/env python3
"""
检查数据在进入_calculate_price_angles之前的状态
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from modules.compute.data_processor import DataProcessor
import logging

# 设置详细日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def check_data_before_price_angles():
    """检查数据在进入价格角度计算之前的状态"""
    print("🔍 检查数据在进入价格角度计算之前的状态...")

    # 加载数据
    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    if not ohlcv_files:
        print("❌ 未找到OHLCV数据文件")
        return

    latest_ohlcv_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    print(f"使用数据文件: {latest_ohlcv_file.name}")

    df = pl.read_parquet(latest_ohlcv_file)

    # 获取300072.XSHE的数据
    stock_300072 = df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"300072.XSHE原始记录数: {len(stock_300072)}")

    if len(stock_300072) == 0:
        print("❌ 未找到300072.XSHE的记录")
        return

    # 显示原始数据的列和类型
    print("\n📊 原始数据信息:")
    print(f"列名: {stock_300072.columns}")
    print(f"记录数: {len(stock_300072)}")

    for col in stock_300072.columns:
        dtype = stock_300072[col].dtype
        print(f"  {col}: {dtype}")

    # 检查价格列的数据
    price_cols = ['open', 'high', 'low', 'close']
    print("\n💰 价格列数据检查:")
    for col in price_cols:
        if col in stock_300072.columns:
            values = stock_300072[col]
            print(f"  {col}:")
            print(f"    类型: {values.dtype}")
            print(f"    最小值: {values.min()}")
            print(f"    最大值: {values.max()}")
            print(f"    平均值: {values.mean()}")
            print(f"    null值: {values.null_count()}")
            print(f"    NaN值: {values.is_nan().sum()}")
            print(f"    inf值: {values.is_infinite().sum()}")

    # 现在模拟数据处理流程
    print("\n🏭 模拟数据处理流程...")

    processor = DataProcessor()
    test_df = stock_300072.clone()

    # 1. 检查是否包含close列
    has_close = 'close' in test_df.columns
    print(f"1. 包含close列: {has_close}")

    if not has_close:
        print("❌ 数据不包含close列，_calculate_price_angles将直接返回")
        return

    # 2. 手动执行过滤逻辑（复制_calculate_price_angles的逻辑）
    print("\n2. 执行过滤逻辑...")

    filter_conditions = []
    for col in price_cols:
        if col in test_df.columns:
            print(f"  处理列 {col}...")
            col_data = test_df[col]

            # 检查数据类型
            print(f"    数据类型: {col_data.dtype}")

            # 创建条件
            cond1 = col_data <= 10000
            cond2 = col_data >= 0.1

            print(f"    <= 10000 通过: {cond1.sum()}/{len(test_df)}")
            print(f"    >= 0.1 通过: {cond2.sum()}/{len(test_df)}")

            filter_conditions.extend([cond1, cond2])

    print(f"  总条件数: {len(filter_conditions)}")

    if filter_conditions:
        # 组合条件
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition

        print(f"  组合过滤后通过数: {combined_filter.sum()}/{len(test_df)}")

        # 找出异常股票
        abnormal_stocks = test_df.filter(~combined_filter).select('order_book_id').unique()
        abnormal_stock_ids = abnormal_stocks.to_series().to_list()

        print(f"  异常股票数量: {len(abnormal_stock_ids)}")
        if abnormal_stock_ids:
            print(f"  异常股票: {abnormal_stock_ids}")

            # 显示异常记录
            abnormal_records = test_df.filter(~combined_filter)
            print("  异常记录详情:")
            for row in abnormal_records.iter_rows(named=True):
                print(f"    {row['date']}: O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}")

    # 3. 现在直接调用_calculate_price_angles方法
    print("\n3. 直接调用_calculate_price_angles方法...")

    try:
        result_df = processor._calculate_price_angles(test_df)
        print("✅ _calculate_price_angles执行完成")
        print(f"结果记录数: {len(result_df)}")

        remaining_300072 = result_df.filter(pl.col('order_book_id') == '300072.XSHE')
        print(f"300072.XSHE剩余记录数: {len(remaining_300072)}")

    except Exception as e:
        print(f"❌ _calculate_price_angles执行失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_data_before_price_angles()
