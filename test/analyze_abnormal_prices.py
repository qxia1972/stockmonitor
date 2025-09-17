#!/usr/bin/env python3
"""
异常价格股票分析脚本
统计异常价格股票的个数和原因
"""

import polars as pl
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AbnormalPriceAnalyzer:
    """异常价格股票分析器"""

    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.df = None
        self.abnormal_stocks = {}
        self.analysis_results = {}

    def load_data(self):
        """加载数据"""
        logger.info(f"加载数据文件: {self.data_path}")
        self.df = pl.read_parquet(self.data_path)
        logger.info(f"数据加载完成，共 {len(self.df)} 行，{len(self.df.columns)} 列")

    def analyze_abnormal_prices(self):
        """分析异常价格股票"""
        if self.df is None:
            raise ValueError("请先调用 load_data() 方法加载数据")

        logger.info("开始分析异常价格股票...")

        # 异常价格判断条件
        price_cols = ['open', 'high', 'low', 'close']
        abnormal_conditions = {
            'price_too_high': lambda x: x > 10000,
            'price_too_low': lambda x: x < 0.1,
            'price_zero_or_negative': lambda x: x <= 0
        }

        # 统计每只股票的异常情况
        stock_abnormal_stats = {}

        for stock_id in self.df['order_book_id'].unique():
            stock_data = self.df.filter(pl.col('order_book_id') == stock_id)
            stock_stats = {
                'total_records': len(stock_data),
                'abnormal_records': 0,
                'abnormal_details': {},
                'price_ranges': {}
            }

            # 检查每种价格列
            for col in price_cols:
                if col in stock_data.columns:
                    prices = stock_data[col].drop_nulls()

                    # 记录价格范围
                    if len(prices) > 0:
                        try:
                            min_val = prices.min()
                            max_val = prices.max()
                            mean_val = prices.mean()

                            stock_stats['price_ranges'][col] = {
                                'min': float(min_val) if min_val is not None and str(min_val) != 'NaN' else None,
                                'max': float(max_val) if max_val is not None and str(max_val) != 'NaN' else None,
                                'mean': float(mean_val) if mean_val is not None and str(mean_val) != 'NaN' else None
                            }
                        except (ValueError, TypeError):
                            stock_stats['price_ranges'][col] = {
                                'min': None,
                                'max': None,
                                'mean': None
                            }
                    else:
                        stock_stats['price_ranges'][col] = {
                            'min': None,
                            'max': None,
                            'mean': None
                        }

                    # 检查各种异常条件
                    for condition_name, condition_func in abnormal_conditions.items():
                        abnormal_mask = prices.map_elements(condition_func, return_dtype=pl.Boolean)
                        abnormal_count = abnormal_mask.sum()

                        if abnormal_count > 0:
                            if condition_name not in stock_stats['abnormal_details']:
                                stock_stats['abnormal_details'][condition_name] = {}

                            stock_stats['abnormal_details'][condition_name][col] = int(abnormal_count)
                            stock_stats['abnormal_records'] += int(abnormal_count)

            # 如果有异常记录，加入统计
            if stock_stats['abnormal_records'] > 0:
                stock_abnormal_stats[stock_id] = stock_stats

        self.abnormal_stocks = stock_abnormal_stats
        logger.info(f"异常价格股票分析完成，共发现 {len(self.abnormal_stocks)} 只异常股票")

    def generate_report(self):
        """生成分析报告"""
        logger.info("生成分析报告...")

        # 总体统计
        total_abnormal_stocks = len(self.abnormal_stocks)
        total_abnormal_records = sum(stats['abnormal_records'] for stats in self.abnormal_stocks.values())

        # 按异常类型统计
        abnormal_type_stats = {}
        price_col_stats = {}

        for stock_id, stats in self.abnormal_stocks.items():
            for condition_name, condition_details in stats['abnormal_details'].items():
                if condition_name not in abnormal_type_stats:
                    abnormal_type_stats[condition_name] = 0

                for col, count in condition_details.items():
                    abnormal_type_stats[condition_name] += count

                    if col not in price_col_stats:
                        price_col_stats[col] = 0
                    price_col_stats[col] += count

        # 生成报告
        report = {
            'summary': {
                'total_stocks_in_data': len(self.df['order_book_id'].unique()),
                'abnormal_stocks_count': total_abnormal_stocks,
                'abnormal_records_count': total_abnormal_records,
                'abnormal_percentage': round(total_abnormal_stocks / len(self.df['order_book_id'].unique()) * 100, 2)
            },
            'abnormal_types': abnormal_type_stats,
            'price_columns_affected': price_col_stats,
            'top_abnormal_stocks': []
        }

        # 按异常记录数排序的股票列表
        sorted_stocks = sorted(self.abnormal_stocks.items(),
                             key=lambda x: x[1]['abnormal_records'],
                             reverse=True)

        for stock_id, stats in sorted_stocks[:20]:  # 前20只最异常的股票
            stock_info = {
                'stock_id': stock_id,
                'abnormal_records': stats['abnormal_records'],
                'total_records': stats['total_records'],
                'abnormal_percentage': round(stats['abnormal_records'] / stats['total_records'] * 100, 2),
                'price_ranges': stats['price_ranges'],
                'abnormal_details': stats['abnormal_details']
            }
            report['top_abnormal_stocks'].append(stock_info)

        self.analysis_results = report
        return report

    def print_report(self):
        """打印分析报告"""
        if not self.analysis_results:
            logger.warning("请先运行 generate_report() 方法")
            return

        report = self.analysis_results

        print("\n" + "="*80)
        print("异常价格股票分析报告")
        print("="*80)

        # 总体统计
        summary = report['summary']
        print("
📊 总体统计:"        print(f"  总股票数: {summary['total_stocks_in_data']}")
        print(f"  异常股票数: {summary['abnormal_stocks_count']}")
        print(f"  异常记录数: {summary['abnormal_records_count']}")
        print(".2f"
        # 异常类型统计
        print("
🔍 异常类型统计:"        for abnormal_type, count in report['abnormal_types'].items():
            type_name = {
                'price_too_high': '价格过高(>10000)',
                'price_too_low': '价格过低(<0.1)',
                'price_zero_or_negative': '价格为零或负数(≤0)'
            }.get(abnormal_type, abnormal_type)
            print(f"  {type_name}: {count} 条记录")

        # 价格列统计
        print("
💰 受影响的价格列:"        for col, count in report['price_columns_affected'].items():
            print(f"  {col}: {count} 条记录")

        # 前20只最异常的股票
        print("
🏆 前20只最异常的股票:"        print("排名   股票代码         异常记录数   总记录数   异常占比   主要异常类型")
        print("-" * 80)

        for i, stock in enumerate(report['top_abnormal_stocks'][:20], 1):
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
                        main_abnormal_type = f"{condition_name}({col})"

            print("2d"
        print("\n" + "="*80)

    def save_detailed_report(self, output_path: str = None):
        """保存详细报告到文件"""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"abnormal_price_analysis_{timestamp}.txt"

        logger.info(f"保存详细报告到: {output_path}")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("异常价格股票详细分析报告\n")
            f.write("="*80 + "\n\n")

            # 写入总体统计
            summary = self.analysis_results['summary']
            f.write("总体统计:\n")
            f.write(f"  总股票数: {summary['total_stocks_in_data']}\n")
            f.write(f"  异常股票数: {summary['abnormal_stocks_count']}\n")
            f.write(f"  异常记录数: {summary['abnormal_records_count']}\n")
            f.write(".2f"
            # 写入每只异常股票的详细信息
            f.write("\n每只异常股票的详细信息:\n")
            f.write("-" * 80 + "\n")

            for stock_id, stats in self.abnormal_stocks.items():
                f.write(f"\n股票代码: {stock_id}\n")
                f.write(f"  总记录数: {stats['total_records']}\n")
                f.write(f"  异常记录数: {stats['abnormal_records']}\n")
                f.write(".2f"
                # 价格范围
                f.write("  价格范围:\n")
                for col, price_range in stats['price_ranges'].items():
                    if price_range['min'] is not None:
                        f.write(".2f"
                # 异常详情
                f.write("  异常详情:\n")
                for condition_name, condition_details in stats['abnormal_details'].items():
                    condition_desc = {
                        'price_too_high': '价格过高(>10000)',
                        'price_too_low': '价格过低(<0.1)',
                        'price_zero_or_negative': '价格为零或负数(≤0)'
                    }.get(condition_name, condition_name)

                    for col, count in condition_details.items():
                        f.write(f"    {condition_desc} - {col}: {count} 条记录\n")


def main():
    """主函数"""
    # 数据文件路径
    data_path = "data/ohlcv_synced_20250916_204000.parquet"

    # 创建分析器
    analyzer = AbnormalPriceAnalyzer(data_path)

    # 执行分析
    analyzer.load_data()
    analyzer.analyze_abnormal_prices()
    analyzer.generate_report()
    analyzer.print_report()

    # 保存详细报告
    analyzer.save_detailed_report()


if __name__ == "__main__":
    main()