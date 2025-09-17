#!/usr/bin/env python3
"""
异常价格股票分析脚本
统计异常价格股票的个数和原因
"""

import polars as pl
from pathlib import Path
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_abnormal_prices(data_path: str):
    """分析异常价格股票"""
    logger.info(f"加载数据文件: {data_path}")

    # 加载数据
    df = pl.read_parquet(data_path)
    logger.info(f"数据加载完成，共 {len(df)} 行，{len(df.columns)} 列")

    # 异常价格判断条件
    price_cols = ['open', 'high', 'low', 'close']

    # 统计结果
    abnormal_stocks = []
    abnormal_type_counts = {
        'price_too_high': 0,
        'price_too_low': 0,
        'price_zero_or_negative': 0
    }

    total_stocks = len(df['order_book_id'].unique())
    logger.info(f"开始分析 {total_stocks} 只股票的异常价格...")

    # 逐只股票分析
    for stock_id in df['order_book_id'].unique():
        stock_data = df.filter(pl.col('order_book_id') == stock_id)
        stock_abnormal_count = 0
        stock_abnormal_details = {}

        # 检查每种价格列
        for col in price_cols:
            if col in stock_data.columns:
                prices = stock_data[col].drop_nulls()

                if len(prices) > 0:
                    # 检查价格过高 (>10000)
                    high_count = (prices > 10000).sum()
                    if high_count > 0:
                        if 'price_too_high' not in stock_abnormal_details:
                            stock_abnormal_details['price_too_high'] = {}
                        stock_abnormal_details['price_too_high'][col] = int(high_count)
                        abnormal_type_counts['price_too_high'] += int(high_count)
                        stock_abnormal_count += int(high_count)

                    # 检查价格过低 (<0.1)
                    low_count = (prices < 0.1).sum()
                    if low_count > 0:
                        if 'price_too_low' not in stock_abnormal_details:
                            stock_abnormal_details['price_too_low'] = {}
                        stock_abnormal_details['price_too_low'][col] = int(low_count)
                        abnormal_type_counts['price_too_low'] += int(low_count)
                        stock_abnormal_count += int(low_count)

                    # 检查价格为零或负数
                    zero_neg_count = (prices <= 0).sum()
                    if zero_neg_count > 0:
                        if 'price_zero_or_negative' not in stock_abnormal_details:
                            stock_abnormal_details['price_zero_or_negative'] = {}
                        stock_abnormal_details['price_zero_or_negative'][col] = int(zero_neg_count)
                        abnormal_type_counts['price_zero_or_negative'] += int(zero_neg_count)
                        stock_abnormal_count += int(zero_neg_count)

        # 如果有异常记录，加入结果
        if stock_abnormal_count > 0:
            abnormal_stocks.append({
                'stock_id': stock_id,
                'total_records': len(stock_data),
                'abnormal_records': stock_abnormal_count,
                'abnormal_percentage': round(stock_abnormal_count / len(stock_data) * 100, 2),
                'abnormal_details': stock_abnormal_details
            })

    # 生成报告
    print_report(abnormal_stocks, abnormal_type_counts, total_stocks)

    return abnormal_stocks, abnormal_type_counts


def print_report(abnormal_stocks, abnormal_type_counts, total_stocks):
    """打印分析报告"""
    print("\n" + "="*80)
    print("异常价格股票分析报告")
    print("="*80)

    # 总体统计
    abnormal_stock_count = len(abnormal_stocks)
    total_abnormal_records = sum(stock['abnormal_records'] for stock in abnormal_stocks)

    print("
📊 总体统计:"    print(f"  总股票数: {total_stocks}")
    print(f"  异常股票数: {abnormal_stock_count}")
    print(f"  异常记录数: {total_abnormal_records}")
    print(".2f"
    # 异常类型统计
    print("
🔍 异常类型统计:"    print(f"  价格过高(>10000): {abnormal_type_counts['price_too_high']} 条记录")
    print(f"  价格过低(<0.1): {abnormal_type_counts['price_too_low']} 条记录")
    print(f"  价格为零或负数(≤0): {abnormal_type_counts['price_zero_or_negative']} 条记录")

    # 按异常记录数排序
    sorted_stocks = sorted(abnormal_stocks, key=lambda x: x['abnormal_records'], reverse=True)

    # 前20只最异常的股票
    print("
🏆 前20只最异常的股票:"    print("排名   股票代码         异常记录数   总记录数   异常占比   主要异常类型")
    print("-" * 80)

    for i, stock in enumerate(sorted_stocks[:20], 1):
        stock_id = stock['stock_id']
        abnormal_records = stock['abnormal_records']
        total_records = stock['total_records']
        abnormal_pct = stock['abnormal_percentage']

        # 找出主要异常类型
        main_abnormal_type = ""
        max_count = 0
        for condition_name, condition_details in stock['abnormal_details'].items():
            for col, count in condition_details.items():
                if count > max_count:
                    max_count = count
                    type_desc = {
                        'price_too_high': '价格过高',
                        'price_too_low': '价格过低',
                        'price_zero_or_negative': '价格异常'
                    }.get(condition_name, condition_name)
                    main_abnormal_type = f"{type_desc}({col})"

        print("2d"
    print("\n" + "="*80)


def main():
    """主函数"""
    # 数据文件路径
    data_path = "data/ohlcv_synced_20250916_204000.parquet"

    # 执行分析
    abnormal_stocks, abnormal_type_counts = analyze_abnormal_prices(data_path)


if __name__ == "__main__":
    main()