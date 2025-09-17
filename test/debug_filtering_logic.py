#!/usr/bin/env python3
"""
调试数据处理器中的过滤逻辑，找出为什么300072.XSHE被错误过滤
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from modules.compute.data_processor import DataProcessor

def debug_filtering_logic():
    """调试过滤逻辑"""
    print("🔍 调试数据处理器过滤逻辑...")

    # 加载数据
    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    if not ohlcv_files:
        print("❌ 未找到OHLCV数据文件")
        return

    latest_ohlcv_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    print(f"使用数据文件: {latest_ohlcv_file.name}")

    df = pl.read_parquet(latest_ohlcv_file)
    print(f"原始数据总记录数: {len(df)}")

    # 获取300072.XSHE的数据
    stock_300072 = df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"300072.XSHE原始记录数: {len(stock_300072)}")

    if len(stock_300072) == 0:
        print("❌ 未找到300072.XSHE的记录")
        return

    # 检查数据类型
    print("\n📊 数据类型检查:")
    for col in ['open', 'high', 'low', 'close']:
        if col in stock_300072.columns:
            dtype = stock_300072[col].dtype
            print(f"  {col}: {dtype}")

    # 检查是否有NaN或null值
    print("\n🔍 Null值检查:")
    for col in ['open', 'high', 'low', 'close']:
        if col in stock_300072.columns:
            null_count = stock_300072[col].null_count()
            print(f"  {col} null值: {null_count}")

    # 手动模拟过滤逻辑
    print("\n🔧 手动模拟过滤逻辑:")

    price_cols = ['open', 'high', 'low', 'close']
    filter_conditions = []

    for col in price_cols:
        if col in stock_300072.columns:
            print(f"  检查列 {col}:")
            values = stock_300072[col]

            # 检查数据类型
            print(f"    数据类型: {values.dtype}")

            # 检查范围
            min_val = values.min()
            max_val = values.max()
            print(f"    最小值: {min_val} (类型: {type(min_val)})")
            print(f"    最大值: {max_val} (类型: {type(max_val)})")

            # 创建过滤条件
            cond1 = values <= 10000
            cond2 = values >= 0.1

            print(f"    <= 10000 的记录数: {cond1.sum()}")
            print(f"    >= 0.1 的记录数: {cond2.sum()}")

            filter_conditions.append(cond1)
            filter_conditions.append(cond2)

    if filter_conditions:
        print(f"\n  总过滤条件数: {len(filter_conditions)}")

        # 组合过滤条件
        combined_filter = filter_conditions[0]
        for i, condition in enumerate(filter_conditions[1:], 1):
            print(f"  组合条件 {i}: {condition.sum()} 条记录满足")
            combined_filter = combined_filter & condition

        print(f"  最终组合过滤结果: {combined_filter.sum()} 条记录满足")

        # 应用过滤
        filtered_stock = stock_300072.filter(combined_filter)
        removed_stock = stock_300072.filter(~combined_filter)

        print(f"\n📊 过滤结果:")
        print(f"  过滤前: {len(stock_300072)} 条记录")
        print(f"  过滤后: {len(filtered_stock)} 条记录")
        print(f"  被移除: {len(removed_stock)} 条记录")

        if len(removed_stock) > 0:
            print("  被移除的记录:")
            print(removed_stock.select(['date', 'open', 'high', 'low', 'close']))
        else:
            print("  ✅ 没有记录被移除")

    # 现在测试实际的数据处理器
    print("\n🏭 测试实际数据处理器:")
    processor = DataProcessor()

    # 创建一个只包含300072.XSHE的DataFrame来测试
    test_df = stock_300072.clone()

    # 调用价格角度计算方法（其中包含过滤逻辑）
    try:
        result_df = processor._calculate_price_angles(test_df)
        print(f"  处理后记录数: {len(result_df)}")

        remaining_300072 = result_df.filter(pl.col('order_book_id') == '300072.XSHE')
        print(f"  300072.XSHE剩余记录数: {len(remaining_300072)}")

        if len(remaining_300072) == 0:
            print("  ❌ 300072.XSHE被数据处理器过滤掉了")
        else:
            print("  ✅ 300072.XSHE通过了数据处理器过滤")

    except Exception as e:
        print(f"  ❌ 数据处理器调用失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_filtering_logic()
