#!/usr/bin/env python3
"""
详细调试_precompute_common_values方法中的high列问题
"""

import polars as pl
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def debug_precompute_common_values():
    """详细调试预计算方法"""
    print("🔍 详细调试_precompute_common_values方法...")

    # 1. 加载数据
    data_file = "data/ohlcv_synced_20250917_013209.parquet"
    df = pl.read_parquet(data_file)
    stock_df = df.filter(pl.col('order_book_id') == '300072.XSHE')

    print("📊 原始数据:")
    print(f"   high列前5条: {stock_df.select('high').head(5).to_series().to_list()}")
    print(f"   所有列: {stock_df.columns}")

    # 2. 手动执行_precompute_common_values的逻辑
    print("\n🏭 手动执行预计算逻辑...")

    # 检查是否需要price_change
    if 'price_change' not in stock_df.columns:
        print("   创建price_change列...")
        stock_df = stock_df.with_columns([
            (pl.col('close') - pl.col('close').shift(1)).alias('price_change')
        ])
        print(f"   price_change创建后high列: {stock_df.select('high').head(5).to_series().to_list()}")

    # 创建hl, hc, lc列
    print("   创建hl, hc, lc列...")
    print("   执行前high列值:", stock_df.select('high').head(3).to_series().to_list())

    # 分别执行每个with_columns
    print("   执行hl列创建...")
    stock_df = stock_df.with_columns([
        (pl.col('high') - pl.col('low')).alias('hl')
    ])
    print(f"   hl创建后high列: {stock_df.select('high').head(3).to_series().to_list()}")
    print(f"   hl列值: {stock_df.select('hl').head(3).to_series().to_list()}")

    print("   执行hc列创建...")
    stock_df = stock_df.with_columns([
        (pl.col('high') - pl.col('close').shift(1)).abs().alias('hc')
    ])
    print(f"   hc创建后high列: {stock_df.select('high').head(3).to_series().to_list()}")
    print(f"   hc列值: {stock_df.select('hc').head(3).to_series().to_list()}")

    print("   执行lc列创建...")
    stock_df = stock_df.with_columns([
        (pl.col('low') - pl.col('close').shift(1)).abs().alias('lc')
    ])
    print(f"   lc创建后high列: {stock_df.select('high').head(3).to_series().to_list()}")
    print(f"   lc列值: {stock_df.select('lc').head(3).to_series().to_list()}")

    print("\n📋 最终结果:")
    print(f"   high列: {stock_df.select('high').head(5).to_series().to_list()}")
    print(f"   所有h开头的列: {[col for col in stock_df.columns if col.startswith('h')]}")

if __name__ == "__main__":
    debug_precompute_common_values()