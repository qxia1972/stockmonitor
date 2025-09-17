#!/usr/bin/env python3
"""
逐步调试_calculate_price_angles方法中的过滤逻辑
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from modules.compute.data_processor import DataProcessor
import logging

# 设置日志级别以捕获所有信息
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def debug_price_angles_filtering():
    """逐步调试价格角度过滤逻辑"""
    print("🔍 逐步调试价格角度过滤逻辑...")

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

    # 初始化数据处理器
    processor = DataProcessor()

    # 手动执行_calculate_price_angles方法中的过滤逻辑
    print("\n🏭 手动执行过滤逻辑...")

    # 复制_calculate_price_angles中的逻辑
    test_df = stock_300072.clone()

    print("测试数据准备完成")
    print(f"测试数据记录数: {len(test_df)}")
    print(f"列名: {test_df.columns}")

    # 数据质量检查：过滤掉价格异常的股票
    print("\n📊 执行数据质量检查...")
    price_cols = ['open', 'high', 'low', 'close']
    filter_conditions = []

    print("创建过滤条件...")
    for col in price_cols:
        if col in test_df.columns:
            print(f"  处理列 {col}...")
            col_data = test_df[col]
            print(f"    数据类型: {col_data.dtype}")
            print(f"    最小值: {col_data.min()}")
            print(f"    最大值: {col_data.max()}")

            # 创建条件
            cond1 = col_data <= 10000
            cond2 = col_data >= 0.1

            print(f"    <= 10000 条件通过数: {cond1.sum()}")
            print(f"    >= 0.1 条件通过数: {cond2.sum()}")

            filter_conditions.append(cond1)
            filter_conditions.append(cond2)
        else:
            print(f"  ❌ 列 {col} 不存在")

    print(f"总过滤条件数: {len(filter_conditions)}")

    if filter_conditions:
        print("\n🔧 组合过滤条件...")

        # 组合所有过滤条件
        combined_filter = filter_conditions[0]
        print(f"初始条件通过数: {combined_filter.sum()}")

        for i, condition in enumerate(filter_conditions[1:], 1):
            print(f"组合条件 {i}...")
            print(f"  当前条件通过数: {condition.sum()}")
            combined_filter = combined_filter & condition
            print(f"  组合后通过数: {combined_filter.sum()}")

        print("\n最终过滤结果:")
        print(f"  总记录数: {len(test_df)}")
        print(f"  通过过滤数: {combined_filter.sum()}")
        print(f"  被过滤数: {len(test_df) - combined_filter.sum()}")

        # 找出有异常价格的股票
        abnormal_stocks = test_df.filter(~combined_filter).select('order_book_id').unique()
        abnormal_stock_ids = abnormal_stocks.to_series().to_list()

        print(f"  异常股票数量: {len(abnormal_stock_ids)}")
        if abnormal_stock_ids:
            print(f"  异常股票列表: {abnormal_stock_ids}")

            # 显示异常记录的详细信息
            abnormal_records = test_df.filter(~combined_filter)
            print("  异常记录详情:")
            for row in abnormal_records.iter_rows(named=True):
                print(f"    {row['date']}: O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}")

                # 检查每个价格是否异常
                for col in price_cols:
                    val = row[col]
                    if val > 10000 or val < 0.1:
                        print(f"      ❌ {col}={val} 异常!")

        # 应用过滤
        filtered_df = test_df.filter(combined_filter)
        print(f"\n过滤后记录数: {len(filtered_df)}")

if __name__ == "__main__":
    debug_price_angles_filtering()
