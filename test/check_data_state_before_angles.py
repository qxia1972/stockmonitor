#!/usr/bin/env python3
"""
检查数据在进入_calculate_price_angles之前的确切状态
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from pathlib import Path
from modules.compute.data_processor import DataProcessor
import logging

# 设置详细日志
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

def check_data_state_before_price_angles():
    """检查数据在进入价格角度计算之前的确切状态"""
    print("🔍 检查数据在进入价格角度计算之前的确切状态...")

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

    processor = DataProcessor()
    result_df = stock_300072.clone()

    # 模拟前面的处理步骤
    print("\n🏭 模拟前面的处理步骤...")

    # 排序
    if 'date' in result_df.columns:
        result_df = result_df.sort('date')

    # 预计算
    indicators = ['sma', 'ema', 'rsi', 'macd', 'bollinger', 'stoch', 'atr', 'price_angles', 'volatility', 'volume_indicators', 'risk_indicators']
    result_df = processor._precompute_common_values(result_df, indicators)

    # 计算各种指标
    result_df = processor._calculate_sma(result_df)
    result_df = processor._calculate_ema(result_df)
    result_df = processor._calculate_rsi(result_df)
    result_df = processor._calculate_macd(result_df)
    result_df = processor._calculate_bollinger(result_df)
    result_df = processor._calculate_stoch(result_df)
    result_df = processor._calculate_atr(result_df)

    print("\n📊 数据在进入价格角度计算之前的状态:")
    print(f"记录数: {len(result_df)}")
    print(f"列数: {len(result_df.columns)}")
    print(f"列名: {result_df.columns}")

    # 检查300072.XSHE
    remaining = result_df.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"300072.XSHE记录数: {len(remaining)}")

    # 检查价格列的数据类型和值
    price_cols = ['open', 'high', 'low', 'close']
    print("\n💰 价格列检查:")
    for col in price_cols:
        if col in result_df.columns:
            values = result_df[col]
            print(f"  {col}:")
            print(f"    类型: {values.dtype}")
            print(f"    最小值: {values.min()}")
            print(f"    最大值: {values.max()}")
            print(f"    平均值: {values.mean()}")
            print(f"    null值: {values.null_count()}")
            print(f"    NaN值: {values.is_nan().sum()}")
            print(f"    inf值: {values.is_infinite().sum()}")

            # 检查是否有异常值
            abnormal_high = values.filter(values > 10000)
            abnormal_low = values.filter(values < 0.1)
            print(f"    >10000的记录数: {len(abnormal_high)}")
            print(f"    <0.1的记录数: {len(abnormal_low)}")

    # 现在手动执行过滤逻辑，看看是否能重现问题
    print("\n🔧 手动执行过滤逻辑:")

    filter_conditions = []
    for col in price_cols:
        if col in result_df.columns:
            print(f"  处理列 {col}...")
            col_data = result_df[col]

            cond1 = col_data <= 10000
            cond2 = col_data >= 0.1

            print(f"    <= 10000 通过: {cond1.sum()}/{len(result_df)}")
            print(f"    >= 0.1 通过: {cond2.sum()}/{len(result_df)}")

            filter_conditions.extend([cond1, cond2])

    print(f"  总条件数: {len(filter_conditions)}")

    if filter_conditions:
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition

        print(f"  组合过滤后通过数: {combined_filter.sum()}/{len(result_df)}")

        # 找出异常股票
        abnormal_stocks = result_df.filter(~combined_filter).select('order_book_id').unique()
        abnormal_stock_ids = abnormal_stocks.to_series().to_list()

        print(f"  异常股票数量: {len(abnormal_stock_ids)}")
        if abnormal_stock_ids:
            print(f"  异常股票: {abnormal_stock_ids}")

            # 显示异常记录
            abnormal_records = result_df.filter(~combined_filter)
            print("  异常记录详情:")
            for row in abnormal_records.iter_rows(named=True):
                print(f"    {row['date']}: O={row['open']}, H={row['high']}, L={row['low']}, C={row['close']}")

                # 检查每个价格是否异常
                for col in price_cols:
                    val = row[col]
                    if val > 10000 or val < 0.1:
                        print(f"      ❌ {col}={val} 异常!")

    # 最后调用实际的_calculate_price_angles方法
    print("\n🏭 调用实际的_calculate_price_angles方法...")
    final_result = processor._calculate_price_angles(result_df)
    print(f"最终结果记录数: {len(final_result)}")

    final_remaining = final_result.filter(pl.col('order_book_id') == '300072.XSHE')
    print(f"300072.XSHE最终记录数: {len(final_remaining)}")

if __name__ == "__main__":
    check_data_state_before_price_angles()
