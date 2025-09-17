#!/usr/bin/env python3
"""
模拟完整的评分流程，追踪300072.XSHE在哪个阶段被过滤
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from networks.rqdatac_data_loader import RQDatacDataLoader
from modules.compute.data_processor import data_processor
from modules.compute.indicator_calculator import IndicatorCalculator
from modules.compute.stock_scorer import stock_scorer
import polars as pl
from pathlib import Path
from datetime import datetime

def trace_300072_filtering():
    """追踪300072.XSHE的过滤过程"""
    print("🔍 追踪股票300072.XSHE的过滤过程...")

    # 1. 加载数据
    print("\n1. 加载数据...")
    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    if not ohlcv_files:
        print("❌ 未找到OHLCV数据文件")
        return

    latest_ohlcv_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    print(f"   使用数据文件: {latest_ohlcv_file.name}")

    df = pl.read_parquet(latest_ohlcv_file)
    print(f"   总记录数: {len(df)}")

    # 2. 检查300072.XSHE是否存在
    stock_300072 = df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"\n2. 300072.XSHE原始数据:")
    print(f"   记录数: {len(stock_300072)}")
    if len(stock_300072) > 0:
        print("   价格范围:")
        for col in ['open', 'high', 'low', 'close']:
            min_val = stock_300072[col].min()
            max_val = stock_300072[col].max()
            print(f"     {col}: {min_val:.2f} - {max_val:.2f}")

    # 3. 初始化处理器
    print("\n3. 初始化数据处理器...")
    indicator_calc = IndicatorCalculator()

    # 4. 处理数据 - 逐步跟踪
    print("\n4. 数据处理过程...")

    # 4.1 基础数据处理
    print("   4.1 基础数据处理...")
    processed_df = data_processor.optimize_dataframe(df)
    print(f"      处理后记录数: {len(processed_df)}")

    stock_300072_processed = processed_df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"      300072.XSHE记录数: {len(stock_300072_processed)}")

    # 4.2 计算技术指标
    print("   4.2 计算技术指标...")
    try:
        indicators_df = indicator_calc.calculate_indicators(processed_df)
        print(f"      指标计算完成，记录数: {len(indicators_df)}")

        stock_300072_indicators = indicators_df.filter(pl.col('order_book_id') == '300072.XSHE')
        print(f"      300072.XSHE记录数: {len(stock_300072_indicators)}")

        if len(stock_300072_indicators) == 0:
            print("      ❌ 300072.XSHE在指标计算阶段被过滤")
            return

    except Exception as e:
        print(f"      ❌ 指标计算失败: {e}")
        return

    # 4.3 计算评分
    print("   4.3 计算评分...")
    try:
        from modules.business_model import business_model
        scored_df = business_model.calculate_scores(['300072.XSHE'], score_type="technical")
        print(f"      评分计算完成，记录数: {len(scored_df)}")

        stock_300072_scored = scored_df.filter(pl.col('order_book_id') == '300072.XSHE')
        print(f"      300072.XSHE记录数: {len(stock_300072_scored)}")

        if len(stock_300072_scored) == 0:
            print("      ❌ 300072.XSHE在评分计算阶段被过滤")
            return
        else:
            print("      ✅ 300072.XSHE成功通过所有处理阶段")

            # 显示评分结果
            latest_score = stock_300072_scored.select([
                'order_book_id', 'date', 'close', 'total_score'
            ]).sort('date', descending=True).head(1)
            print("      最新评分:")
            print(latest_score)

    except Exception as e:
        print(f"      ❌ 评分计算失败: {e}")
        return

if __name__ == "__main__":
    trace_300072_filtering()
