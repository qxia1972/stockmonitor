#!/usr/bin/env python3
"""
十日区间涨幅验证评分合理性分析脚本
"""

import polars as pl
import pandas as pd

def main():
    print('=== 十日涨幅与评分相关性深度分析 ===')

    # 读取最新的OHLCV数据
    import os
    from pathlib import Path

    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    if not ohlcv_files:
        raise FileNotFoundError("No OHLCV data files found")

    latest_ohlcv_file = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
    print(f"Loading OHLCV data: {latest_ohlcv_file.name}")
    ohlcv_df = pl.read_parquet(latest_ohlcv_file)

    # 计算十日涨幅
    ohlcv_df = ohlcv_df.with_columns(
        pl.col('date').str.strptime(pl.Date, '%Y-%m-%d').alias('date_parsed')
    ).sort(['order_book_id', 'date_parsed'])

    returns_df = ohlcv_df.with_columns([
        pl.col('close').shift(10).over('order_book_id').alias('close_10d_ago'),
        pl.col('close').alias('close_current')
    ]).with_columns([
        ((pl.col('close_current') - pl.col('close_10d_ago')) / pl.col('close_10d_ago') * 100).alias('return_10d')
    ]).filter(pl.col('return_10d').is_not_null())

    # 获取最新的评分数据
    scores_dir = Path('data/scores')
    scores_files = list(scores_dir.glob('final_scores_*.parquet'))
    if not scores_files:
        raise FileNotFoundError("No scoring data files found")

    latest_scores_file = max(scores_files, key=lambda x: x.stat().st_mtime)
    print(f"Loading scoring data: {latest_scores_file.name}")
    scores_df = pl.read_parquet(latest_scores_file)

    # 合并数据
    latest_returns = returns_df.group_by('order_book_id').agg([
        pl.col('return_10d').last().alias('latest_return_10d'),
        pl.col('date').last().alias('latest_date')
    ])

    merged_df = scores_df.join(latest_returns, on='order_book_id', how='inner')

    print(f'合并后数据量: {len(merged_df)}')

    # 计算相关性
    correlation = merged_df.select([
        pl.corr('composite_score', 'latest_return_10d').alias('score_return_corr'),
        pl.corr('trend_score', 'latest_return_10d').alias('trend_return_corr'),
        pl.corr('capital_score', 'latest_return_10d').alias('capital_return_corr'),
        pl.corr('technical_score', 'latest_return_10d').alias('technical_return_corr'),
        pl.corr('risk_score', 'latest_return_10d').alias('risk_return_corr')
    ])

    print('\n=== 各维度评分与十日涨幅相关性 ===')
    corr_df = correlation.to_pandas()
    corr_values = corr_df.iloc[0]

    print('.4f')
    print('.4f')
    print('.4f')
    print('.4f')
    print('.4f')

    # 按评分分位数分析涨幅
    print('\n=== 按评分分位数分析十日涨幅 ===')
    score_percentiles = merged_df.select([
        pl.quantile('composite_score', 0.1).alias('p10'),
        pl.quantile('composite_score', 0.25).alias('p25'),
        pl.quantile('composite_score', 0.5).alias('p50'),
        pl.quantile('composite_score', 0.75).alias('p75'),
        pl.quantile('composite_score', 0.9).alias('p90')
    ])

    p_df = score_percentiles.to_pandas()
    p_values = p_df.iloc[0]
    p10, p25, p50, p75, p90 = p_values['p10'], p_values['p25'], p_values['p50'], p_values['p75'], p_values['p90']

    print('.1f')
    print('.1f')
    print('.1f')
    print('.1f')
    print('.1f')

    # 分析各分位数区间的平均涨幅
    for label, threshold in [('前10%', p10), ('前25%', p25), ('前50%', p50), ('前75%', p75), ('前90%', p90)]:
        group_data = merged_df.filter(pl.col('composite_score') >= threshold)
        avg_return_df = group_data.select('latest_return_10d').mean().to_pandas()
        avg_return = avg_return_df.iloc[0, 0]
        count = group_data.height
        print(f'{label} 评分股票 ({count}只): 平均10日涨幅 {avg_return:.1f}%')
    # 验证高分股票的表现
    print('\n=== 高分股票验证 ===')
    top_50_stocks = merged_df.sort('composite_score', descending=True).head(50)
    top_50_avg_return = top_50_stocks.select('latest_return_10d').mean().to_pandas().iloc[0, 0]
    top_50_positive = top_50_stocks.filter(pl.col('latest_return_10d') > 0).height

    print(f'前50名股票平均10日涨幅: {top_50_avg_return:.2f}%')
    print(f'前50名股票中正收益占比: {top_50_positive/50:.1f}%')

    # 验证低分股票的表现
    print('\n=== 低分股票验证 ===')
    bottom_50_stocks = merged_df.sort('composite_score').head(50)
    bottom_50_avg_return = bottom_50_stocks.select('latest_return_10d').mean().to_pandas().iloc[0, 0]
    bottom_50_negative = bottom_50_stocks.filter(pl.col('latest_return_10d') < 0).height

    print(f'后50名股票平均10日涨幅: {bottom_50_avg_return:.2f}%')
    print(f'后50名股票中负收益占比: {bottom_50_negative/50:.1f}%')

    print('\n=== 验证结论 ===')
    if corr_values['score_return_corr'] > 0.2:
        print('✅ 评分系统具有较好的预测能力')
    else:
        print('⚠️ 评分系统预测能力有待改进')

    # 比较高分和低分股票的表现
    try:
        if top_50_avg_return > bottom_50_avg_return:
            print('✅ 高分股票确实表现更好')
        else:
            print('⚠️ 高分股票表现未优于低分股票')
    except:
        print('⚠️ 无法比较高分和低分股票表现')

if __name__ == '__main__':
    main()