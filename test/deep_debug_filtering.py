#!/usr/bin/env python3
"""
深入调试数据处理器过滤逻辑，找出为什么300072.XSHE被误判为异常
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
import numpy as np

def deep_debug_filtering():
    """深入调试过滤逻辑"""
    print("🔍 深入调试过滤逻辑...")

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
    print(f"300072.XSHE记录数: {len(stock_300072)}")

    if len(stock_300072) == 0:
        print("❌ 未找到300072.XSHE的记录")
        return

    # 逐列检查价格数据
    price_cols = ['open', 'high', 'low', 'close']

    print("\n📊 逐列详细检查:")

    all_conditions = []

    for col in price_cols:
        if col in stock_300072.columns:
            print(f"\n检查列 {col}:")
            values = stock_300072[col]

            # 检查数据类型和值
            print(f"  数据类型: {values.dtype}")
            print(f"  形状: {values.shape}")

            # 获取所有值
            all_values = values.to_list()
            print(f"  所有值 (前10个): {all_values[:10]}")

            # 检查每个值
            print("  逐个检查值:")
            cond1_results = []
            cond2_results = []

            for i, val in enumerate(all_values):
                cond1 = val <= 10000
                cond2 = val >= 0.1

                cond1_results.append(cond1)
                cond2_results.append(cond2)

                if not cond1 or not cond2:
                    print(f"    记录 {i}: 值={val}, <=10000={cond1}, >=0.1={cond2} ❌")

            # 统计
            cond1_pass = sum(cond1_results)
            cond2_pass = sum(cond2_results)

            print(f"  <=10000 通过: {cond1_pass}/{len(all_values)}")
            print(f"  >=0.1 通过: {cond2_pass}/{len(all_values)}")

            # 创建Polars条件
            cond1_pl = values <= 10000
            cond2_pl = values >= 0.1

            all_conditions.extend([cond1_pl, cond2_pl])

    print(f"\n总条件数: {len(all_conditions)}")

    # 组合所有条件
    print("\n🔧 组合条件检查:")

    if all_conditions:
        combined = all_conditions[0]
        print(f"初始条件通过数: {combined.sum()}")

        for i, cond in enumerate(all_conditions[1:], 1):
            prev_combined = combined.sum()
            combined = combined & cond
            new_combined = combined.sum()
            print(f"组合条件 {i} 后通过数: {new_combined} (之前: {prev_combined})")

        print(f"\n最终结果:")
        print(f"  总记录数: {len(stock_300072)}")
        print(f"  通过过滤数: {combined.sum()}")
        print(f"  被过滤数: {len(stock_300072) - combined.sum()}")

        # 找出被过滤的记录
        filtered_out = stock_300072.filter(~combined)
        if len(filtered_out) > 0:
            print("  被过滤的记录:")
            print(filtered_out.select(['date', 'open', 'high', 'low', 'close']))

            # 检查这些记录的具体值
            print("  详细检查被过滤的记录:")
            for row in filtered_out.iter_rows(named=True):
                print(f"    {row['date']}: O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}")

                # 检查每个价格是否异常
                for col in price_cols:
                    val = row[col]
                    if val > 10000 or val < 0.1:
                        print(f"      ❌ {col}={val} 异常!")

    # 检查是否有NaN或inf值
    print("\n🔍 检查特殊值:")
    for col in price_cols:
        if col in stock_300072.columns:
            values = stock_300072[col]

            # 检查NaN
            nan_count = values.is_nan().sum()
            print(f"  {col} NaN值: {nan_count}")

            # 检查inf
            inf_count = values.is_infinite().sum()
            print(f"  {col} inf值: {inf_count}")

            # 检查null
            null_count = values.is_null().sum()
            print(f"  {col} null值: {null_count}")

if __name__ == "__main__":
    deep_debug_filtering()