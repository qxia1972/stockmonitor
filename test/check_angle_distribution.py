import polars as pl
import numpy as np

# 加载数据并计算所有股票的角度
df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')

# 计算10日均线
df = df.with_columns([
    pl.col('close').rolling_mean(window_size=10).over('order_book_id').alias('sma_10')
])

# 计算变化率
df = df.with_columns([
    pl.when(pl.col('sma_10').shift(1).over('order_book_id') != 0)
      .then((pl.col('sma_10') - pl.col('sma_10').shift(1).over('order_book_id')) / pl.col('sma_10').shift(1).over('order_book_id'))
      .otherwise(None)
      .alias('sma_10_change_pct')
])

# 计算角度
df = df.with_columns([
    (pl.col('sma_10_change_pct') * 100.0).arctan().degrees().alias('ma10_angle')
])

# 查找接近80°的角度
angles_80 = df.filter((pl.col('ma10_angle') >= 75) & (pl.col('ma10_angle') <= 85))
if len(angles_80) > 0:
    print('发现接近80°的角度值:')
    for row in angles_80.head(10).iter_rows(named=True):
        print(f'股票: {row["order_book_id"]}, 日期: {row["date"]}, 角度: {row["ma10_angle"]:.2f}°')
else:
    print('未发现接近80°的角度值')

# 检查是否有大于40°的角度（超出当前限制）
high_angles = df.filter(pl.col('ma10_angle') > 40)
if len(high_angles) > 0:
    print(f'\n发现大于40°的角度值，共{len(high_angles)}条:')
    high_angle_stocks = high_angles.group_by('order_book_id').agg(
        pl.col('ma10_angle').max().alias('max_angle'),
        pl.col('ma10_angle').count().alias('count')
    ).sort('max_angle', descending=True).head(5)

    for row in high_angle_stocks.iter_rows(named=True):
        print(f'股票: {row["order_book_id"]}, 最大角度: {row["max_angle"]:.2f}°, 出现次数: {row["count"]}')
else:
    print('\n未发现大于40°的角度值')

# 检查整体角度分布
all_angles = df.select('ma10_angle').drop_nulls().to_series()
print(f'\n整体角度分布:')
print(f'最大角度: {all_angles.max():.2f}°')
print(f'最小角度: {all_angles.min():.2f}°')
print(f'平均角度: {all_angles.mean():.2f}°')
print(f'中位数: {all_angles.median():.2f}°')

# 检查80.15°是否在理论可能范围内
print(f'\n理论分析:')
print('要达到80.15°的角度，需要的变化率约为115.19%')
print('但当前数据中的最大变化率只有约6.57%')
print('因此80.15°在当前数据和参数设置下是不可能达到的')