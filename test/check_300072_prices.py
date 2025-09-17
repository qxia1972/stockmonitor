#!/usr/bin/env python3
"""
检查股票300072.XSHE的价格数据，验证是否被异常价格过滤
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from networks.rqdatac_data_loader import RQDatacDataLoader
import polars as pl
from datetime import datetime, timedelta

def check_stock_300072():
    """检查股票300072.XSHE的价格数据"""
    print("🔍 检查股票300072.XSHE的价格数据...")

    # 初始化数据加载器
    data_loader = RQDatacDataLoader(allow_mock_data=False)

    # 获取最近30天的交易日历
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    print(f"📅 检查日期范围: {start_date} 到 {end_date}")

    # 获取股票数据
    symbols = ['300072.XSHE']
    ohlcv_data = data_loader.get_ohlcv_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        frequency='1d',
        adjust_type='pre'  # 使用前复权数据
    )

    if ohlcv_data.is_empty():
        print("❌ 未能获取到股票数据")
        return

    print(f"📊 获取到 {len(ohlcv_data)} 条记录")

    # 检查价格数据
    price_cols = ['open', 'high', 'low', 'close']

    print("\n📈 价格数据统计:")
    for col in price_cols:
        if col in ohlcv_data.columns:
            prices = ohlcv_data[col]
            min_price = prices.min()
            max_price = prices.max()
            mean_price = prices.mean()

            print(f"  {col.upper()}: 最小={min_price:.2f}, 最大={max_price:.2f}, 平均={mean_price:.2f}")

            # 检查异常价格
            abnormal_high = prices.filter(prices > 10000)
            abnormal_low = prices.filter(prices < 0.1)

            if len(abnormal_high) > 0:
                print(f"    ⚠️ 发现 {len(abnormal_high)} 个价格超过10000的记录")
                print(f"    异常高价: {abnormal_high.to_list()[:5]}...")

            if len(abnormal_low) > 0:
                print(f"    ⚠️ 发现 {len(abnormal_low)} 个价格低于0.1的记录")
                print(f"    异常低价: {abnormal_low.to_list()[:5]}...")

    # 显示最新的几条记录
    print("\n📋 最新的5条记录:")
    latest_records = ohlcv_data.tail(5)
    print(latest_records.select(['date', 'open', 'high', 'low', 'close', 'volume']))

    # 检查是否会被过滤
    filter_conditions = []
    for col in price_cols:
        if col in ohlcv_data.columns:
            filter_conditions.append(ohlcv_data[col] <= 10000)
            filter_conditions.append(ohlcv_data[col] >= 0.1)

    if filter_conditions:
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition

        abnormal_records = ohlcv_data.filter(~combined_filter)
        normal_records = ohlcv_data.filter(combined_filter)

        print("\n🔍 过滤结果:")
        print(f"  正常记录: {len(normal_records)} 条")
        print(f"  异常记录: {len(abnormal_records)} 条")

        if len(abnormal_records) > 0:
            print("  ❌ 该股票会被过滤掉")
            print("  异常记录详情:")
            print(abnormal_records.select(['date', 'open', 'high', 'low', 'close']))
        else:
            print("  ✅ 该股票不会被过滤")

if __name__ == "__main__":
    check_stock_300072()