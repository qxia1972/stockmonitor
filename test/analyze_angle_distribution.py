import polars as pl
import numpy as np
from modules.compute.data_processor import DataProcessor
import matplotlib.pyplot as plt
from pathlib import Path

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def analyze_angle_distribution():
    """分析均线斜率（角度）分布"""
    print('📊 开始分析均线斜率分布...')

    # 创建数据处理器实例
    processor = DataProcessor()

    # 加载数据
    ohlcv_df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')
    print(f'原始数据: {len(ohlcv_df):,} 行, {ohlcv_df.select("order_book_id").n_unique():,} 只股票')

    # 计算技术指标
    print('计算技术指标...')
    indicators = ['sma_5', 'sma_10', 'sma_20', 'sma_30', 'sma_60', 'price_angles']
    result_df = processor.process_technical_indicators(ohlcv_df, indicators=indicators)

    print(f'处理后数据: {len(result_df):,} 行, {result_df.select("order_book_id").n_unique():,} 只股票')

    # 分析角度分布
    angle_cols = ['ma5_angle', 'ma10_angle', 'ma20_angle', 'ma30_angle', 'ma60_angle']
    angle_names = ['5日均线', '10日均线', '20日均线', '30日均线', '60日均线']

    print('\n' + '='*80)
    print('📈 均线斜率分布统计')
    print('='*80)

    # 创建图表
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    axes = axes.ravel()

    for i, (col, name) in enumerate(zip(angle_cols, angle_names)):
        if col in result_df.columns:
            angles = result_df.select(col).drop_nulls().to_series()

            if len(angles) > 0:
                print(f'\n🔹 {name} ({col})')
                print('-' * 40)

                # 基本统计
                print(f'  数据点数量: {len(angles):,}')
                print(f'  最小值: {angles.min():.2f}°')
                print(f'  最大值: {angles.max():.2f}°')
                print(f'  平均值: {angles.mean():.2f}°')
                print(f'  中位数: {angles.median():.2f}°')
                print(f'  标准差: {angles.std():.2f}°')

                # 分位数统计
                percentiles = [10, 25, 50, 75, 90]
                for p in percentiles:
                    value = angles.quantile(p/100)
                    print(f'  {p}分位数: {value:.2f}°')

                # 角度范围分布
                ranges = [
                    ('极度下跌', -90, -60),
                    ('大幅下跌', -60, -30),
                    ('温和下跌', -30, -10),
                    ('横盘震荡', -10, 10),
                    ('温和上涨', 10, 30),
                    ('大幅上涨', 30, 60),
                    ('极度上涨', 60, 90)
                ]

                print(f'\n  趋势分布:')
                total_count = len(angles)
                for range_name, min_val, max_val in ranges:
                    count = len(angles.filter((angles >= min_val) & (angles < max_val)))
                    percentage = count / total_count * 100
                    print(f'    {range_name:8} ({min_val:3}°~{max_val:3}°): {count:6,} ({percentage:5.1f}%)')

                # 绘制直方图
                ax = axes[i]
                ax.hist(angles.to_numpy(), bins=50, alpha=0.7, edgecolor='black')
                ax.set_title(f'{name}斜率分布', fontsize=12, fontweight='bold')
                ax.set_xlabel('角度 (度)')
                ax.set_ylabel('频次')
                ax.grid(True, alpha=0.3)

                # 添加均值线
                mean_val = angles.mean()
                ax.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'均值: {mean_val:.1f}°')
                ax.legend()

    # 隐藏最后一个子图（如果有）
    if len(angle_cols) < len(axes):
        axes[-1].set_visible(False)

    plt.tight_layout()
    plt.savefig('data/angle_distribution_analysis.png', dpi=300, bbox_inches='tight')
    print(f'\n💾 图表已保存至: data/angle_distribution_analysis.png')

    # 整体趋势分析
    print('\n' + '='*80)
    print('🎯 整体趋势分析')
    print('='*80)

    # 计算每只股票的平均角度
    stock_avg_angles = {}
    for col, name in zip(angle_cols, angle_names):
        if col in result_df.columns:
            stock_avg = result_df.group_by('order_book_id').agg(
                pl.col(col).mean().alias(f'avg_{col}')
            ).drop_nulls()

            stock_avg_angles[name] = stock_avg.select(f'avg_{col}').to_series()

    # 分析整体市场趋势
    print('📊 整体市场趋势分布:')
    for name, angles in stock_avg_angles.items():
        print(f'\n{name}:')
        print(f'  平均趋势: {angles.mean():.2f}°')
        print(f'  趋势标准差: {angles.std():.2f}°')

        # 趋势分类
        up_trend = len(angles.filter(angles > 10))
        down_trend = len(angles.filter(angles < -10))
        sideway = len(angles) - up_trend - down_trend

        print(f'  上涨趋势股票: {up_trend} ({up_trend/len(angles)*100:.1f}%)')
        print(f'  下跌趋势股票: {down_trend} ({down_trend/len(angles)*100:.1f}%)')
        print(f'  横盘震荡股票: {sideway} ({sideway/len(angles)*100:.1f}%)')

    print('\n✅ 均线斜率分布分析完成!')
    return result_df

if __name__ == '__main__':
    analyze_angle_distribution()
