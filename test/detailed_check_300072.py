#!/usr/bin/env python3
"""
详细检查300072.XSHE的每条价格记录，找出可能的异常值
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path

def detailed_check_300072():
    """详细检查300072.XSHE的每条记录"""
    print("🔍 详细检查股票300072.XSHE的价格数据...")

    # 加载数据
    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    if not ohlcv_files:
        print("❌ 未找到OHLCV数据文件")
        return

    latest_ohlcv_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    print(f"使用数据文件: {latest_ohlcv_file.name}")

    df = pl.read_parquet(latest_ohlcv_file)

    # 获取300072.XSHE的所有记录
    stock_300072 = df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"\n300072.XSHE总记录数: {len(stock_300072)}")

    if len(stock_300072) == 0:
        print("❌ 未找到300072.XSHE的记录")
        return

    # 检查每条记录的价格
    price_cols = ['open', 'high', 'low', 'close']

    print("\n📋 检查每条记录的价格:")
    abnormal_records = []

    for i, row in enumerate(stock_300072.iter_rows(named=True)):
        date = row['date']
        is_abnormal = False
        reasons = []

        for col in price_cols:
            price = row[col]
            if price > 10000:
                is_abnormal = True
                reasons.append(f"{col}={price:.2f}>10000")
            elif price < 0.1:
                is_abnormal = True
                reasons.append(f"{col}={price:.2f}<0.1")

        if is_abnormal:
            abnormal_records.append({
                'index': i,
                'date': date,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'reasons': reasons
            })
            print(f"❌ 异常记录 #{i}: {date} - {', '.join(reasons)}")
        else:
            print(f"✅ 正常记录 #{i}: {date} - O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")

    print(f"\n📊 总结:")
    print(f"  总记录数: {len(stock_300072)}")
    print(f"  异常记录数: {len(abnormal_records)}")
    print(f"  正常记录数: {len(stock_300072) - len(abnormal_records)}")

    if abnormal_records:
        print("\n❌ 异常记录详情:")
        for record in abnormal_records:
            print(f"  {record['date']}: {', '.join(record['reasons'])}")
    else:
        print("\n✅ 所有记录都正常")

    # 手动应用过滤逻辑
    print("\n🔍 手动应用过滤逻辑:")
    filter_conditions = []

    for col in price_cols:
        if col in stock_300072.columns:
            filter_conditions.append(stock_300072[col] <= 10000)
            filter_conditions.append(stock_300072[col] >= 0.1)

    if filter_conditions:
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition

        filtered_stock = stock_300072.filter(combined_filter)
        removed_stock = stock_300072.filter(~combined_filter)

        print(f"  过滤前记录数: {len(stock_300072)}")
        print(f"  过滤后记录数: {len(filtered_stock)}")
        print(f"  被移除记录数: {len(removed_stock)}")

        if len(removed_stock) > 0:
            print("  被移除的记录:")
            print(removed_stock.select(['date', 'open', 'high', 'low', 'close']))

if __name__ == "__main__":
    detailed_check_300072()