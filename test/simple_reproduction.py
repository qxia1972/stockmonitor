#!/usr/bin/env python3
"""
最简单的重现high列损坏问题
"""

import polars as pl
import sys
import os

def simple_reproduction():
    """最简单的重现"""
    print("🔍 最简单的重现high列损坏问题...")

    # 创建测试数据
    data = {
        'order_book_id': ['300072.XSHE'] * 5,
        'date': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'open': [3.19, 3.09, 3.71, 3.54, 3.55],
        'high': [3.24, 3.19, 3.71, 3.73, 3.63],
        'low': [3.09, 3.09, 3.16, 3.53, 3.5],
        'close': [3.19, 3.09, 3.71, 3.54, 3.55],
        'volume': [100, 200, 300, 400, 500]
    }

    df = pl.DataFrame(data)
    print("📊 初始数据:")
    print(f"   high列: {df.select('high').to_series().to_list()}")

    # 模拟_precompute_common_values的逻辑
    print("\n🏭 模拟预计算...")

    # 方法1: 分别创建列
    print("方法1: 分别创建列")
    df1 = df.clone()
    df1 = df1.with_columns([(pl.col('high') - pl.col('low')).alias('hl')])
    print(f"   创建hl后high列: {df1.select('high').to_series().to_list()}")
    print(f"   hl列: {df1.select('hl').to_series().to_list()}")

    # 方法2: 同时创建多个列
    print("\n方法2: 同时创建多个列")
    df2 = df.clone()
    df2 = df2.with_columns([
        (pl.col('high') - pl.col('low')).alias('hl'),
        (pl.col('high') - pl.col('close').shift(1)).abs().alias('hc'),
        (pl.col('low') - pl.col('close').shift(1)).abs().alias('lc')
    ])
    print(f"   创建hl,hc,lc后high列: {df2.select('high').to_series().to_list()}")
    print(f"   hl列: {df2.select('hl').to_series().to_list()}")

    # 方法3: 使用实际的数据文件
    print("\n方法3: 使用实际数据文件")
    actual_df = pl.read_parquet('data/ohlcv_synced_20250917_013209.parquet')
    actual_stock = actual_df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"   实际数据high列前5条: {actual_stock.select('high').head(5).to_series().to_list()}")

    actual_stock = actual_stock.with_columns([
        (pl.col('high') - pl.col('low')).alias('hl'),
        (pl.col('high') - pl.col('close').shift(1)).abs().alias('hc'),
        (pl.col('low') - pl.col('close').shift(1)).abs().alias('lc')
    ])
    print(f"   实际数据创建hl,hc,lc后high列前5条: {actual_stock.select('high').head(5).to_series().to_list()}")
    print(f"   实际数据hl列前5条: {actual_stock.select('hl').head(5).to_series().to_list()}")

if __name__ == "__main__":
    simple_reproduction()