#!/usr/bin/env python3
"""
诊断收益率计算问题的脚本
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def diagnose_yield_problem():
    """诊断为什么收益率全为负数的问题"""
    print("🔍 开始诊断收益率问题...")

    # 加载数据
    scores_df = pl.read_parquet('data/scores/final_scores_20250917_030926.parquet')
    price_df = pl.read_parquet('data/ohlcv_synced_latest_filled.parquet')

    print("=== 数据概览 ===")
    print(f"评分数据行数: {len(scores_df)}")
    print(f"价格数据行数: {len(price_df)}")

    # 检查数据类型
    print("\n=== 数据类型检查 ===")
    print(f"评分数据date列类型: {scores_df.schema['date']}")
    print(f"价格数据date列类型: {price_df.schema['date']}")

    # 1. 检查整体市场趋势
    print("\n=== 整体市场趋势分析 ===")
    market_trend = price_df.group_by('date').agg([
        pl.col('close').mean().alias('avg_price'),
        pl.col('close').std().alias('price_std'),
        pl.col('close').count().alias('stock_count')
    ]).sort('date')

    print("市场整体趋势 (前15个交易日):")
    market_data = market_trend.head(15)
    prev_price = None
    for row in market_data.iter_rows(named=True):
        date_str = str(row['date'])
        avg_price = row['avg_price']
        stock_count = row['stock_count']

        if prev_price is not None:
            change_pct = (avg_price - prev_price) / prev_price * 100
            print(f"{date_str}: 平均价格={avg_price:.2f}, 变化={change_pct:+.2f}%, 股票数={stock_count}")
        else:
            print(f"{date_str}: 平均价格={avg_price:.2f}, 股票数={stock_count}")
        prev_price = avg_price

    # 2. 检查筛选条件的影响
    print("\n=== 筛选条件分析 ===")

    # 检查10日线斜率分布
    slope_data = scores_df.select('ma10_angle').filter(pl.col('ma10_angle').is_not_null())
    slope_array = slope_data.to_numpy().flatten()
    print("10日线斜率统计:")
    print(f"  均值: {np.mean(slope_array):.4f}%")
    print(f"  中位数: {np.median(slope_array):.4f}%")
    print(f"  标准差: {np.std(slope_array):.4f}%")
    print(f"  最小值: {np.min(slope_array):.4f}%")
    print(f"  最大值: {np.max(slope_array):.4f}%")

    # 检查筛选前后收益率差异
    print("\n筛选条件对比:")

    # 全部股票的平均收益率
    all_stocks = scores_df.filter(pl.col('ma10_angle').is_not_null())
    if len(all_stocks) > 0:
        all_returns = all_stocks.select('pct_change').filter(pl.col('pct_change').is_not_null()).to_numpy().flatten()
        if len(all_returns) > 0:
            print(f"  全部股票平均收益率: {np.mean(all_returns):.4f}%")

    # 陡峭上升股票的平均收益率
    steep_stocks = scores_df.filter(
        (pl.col('ma10_angle').is_not_null()) &
        (pl.col('ma10_angle') > 1.0)
    )
    if len(steep_stocks) > 0:
        steep_returns = steep_stocks.select('pct_change').filter(pl.col('pct_change').is_not_null()).to_numpy().flatten()
        if len(steep_returns) > 0:
            print(f"  陡峭上升股票平均收益率: {np.mean(steep_returns):.4f}%")

    # 3. 检查具体股票的价格变化
    print("\n=== 个股权分析 ===")

    # 选择几个代表性股票
    sample_stocks = ['688017.XSHG', '000001.XSHE', '600000.XSHG']

    for stock_id in sample_stocks:
        print(f"\n股票 {stock_id} 分析:")

        stock_scores = scores_df.filter(pl.col('order_book_id') == stock_id).sort('date')
        stock_prices = price_df.filter(pl.col('order_book_id') == stock_id).sort('date')

        if len(stock_scores) == 0 or len(stock_prices) == 0:
            print("  数据不足")
            continue

        # 显示价格变化
        print("  价格时间序列 (前10天):")
        price_series = stock_prices.head(10)
        for row in price_series.iter_rows(named=True):
            date_str = str(row['date'])
            close_price = row['close']
            print(f"    {date_str}: {close_price:.2f}")

        # 检查10日线斜率
        slope_series = stock_scores.select(['date', 'ma10_angle']).head(10)
        print("  10日线斜率 (前10天):")
        for row in slope_series.iter_rows(named=True):
            date_str = str(row['date'])
            slope = row['ma10_angle']
            print(f"    {date_str}: {slope:.4f}%")

    # 4. 检查计算逻辑
    print("\n=== 计算逻辑验证 ===")

    # 模拟一个简单的收益率计算
    test_date = datetime(2025, 7, 8).date()
    test_stock = '688017.XSHG'

    # 获取测试数据
    test_scores = scores_df.filter(
        (pl.col('order_book_id') == test_stock) &
        (pl.col('date') == test_date)
    )

    test_prices = price_df.filter(
        (pl.col('order_book_id') == test_stock)
    ).sort('date')

    if len(test_scores) > 0 and len(test_prices) > 0:
        current_price = test_scores.select('close').row(0)[0]
        print(f"  当前价格 (测试): {current_price:.2f}")

        # 计算未来3日和5日价格
        price_rows = test_prices.rows()
        current_idx = None

        for i, row in enumerate(price_rows):
            if str(row[1]) == str(test_date):  # date column
                current_idx = i
                break

        if current_idx is not None:
            print(f"  当前价格位置: 第{current_idx + 1}个交易日")

            # 3日后
            if current_idx + 3 < len(price_rows):
                price_3d = price_rows[current_idx + 3][2]  # close column
                return_3d = (price_3d - current_price) / current_price * 100
                print(f"  3日后价格: {price_3d:.2f}, 收益率: {return_3d:.2f}%")

            # 5日后
            if current_idx + 5 < len(price_rows):
                price_5d = price_rows[current_idx + 5][2]  # close column
                return_5d = (price_5d - current_price) / current_price * 100
                print(f"  5日后价格: {price_5d:.2f}, 收益率: {return_5d:.2f}%")

    # 5. 给出诊断结论
    print("\n=== 诊断结论 ===")

    # 检查市场整体趋势
    if len(market_data) >= 2:
        start_price = market_data.head(1).select('avg_price').item()
        end_price = market_data.tail(1).select('avg_price').item()
        total_market_return = (end_price - start_price) / start_price * 100
        print(f"整体市场累计收益率: {total_market_return:.2f}%")

        if total_market_return < -5:
            print("⚠️  整体市场处于下跌趋势，这是收益率为负的主要原因")
        elif total_market_return > 5:
            print("✅ 整体市场处于上涨趋势，筛选条件可能过于严格")
        else:
            print("🤔 整体市场波动较小，需要进一步分析筛选条件")

    # 检查筛选条件
    if len(all_stocks) > 0:
        steep_ratio = len(steep_stocks) / len(all_stocks) * 100
        print(f"陡峭上升股票比例: {steep_ratio:.1f}%")

        if steep_ratio < 10:
            print("⚠️  筛选条件过于严格，只保留了很少的股票")
        elif steep_ratio > 50:
            print("🤔 筛选条件可能不够严格，需要调整阈值")

    print("\n💡 建议解决方案:")
    print("1. 检查整体市场环境 - 当前是否处于熊市")
    print("2. 调整10日线斜率阈值 - 可能需要降低要求")
    print("3. 检查数据质量 - 确认价格数据是否正确")
    print("4. 考虑不同的时间窗口 - 避免选择市场整体下跌的时期")

if __name__ == "__main__":
    diagnose_yield_problem()