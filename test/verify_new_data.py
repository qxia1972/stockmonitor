import polars as pl

# 读取最新的daily_results文件
daily_results = pl.read_parquet('data/processed/daily_results_20250917_030926.parquet')
print('Daily Results 样本数据:')
sample = daily_results.select(['order_book_id', 'composite_score', 'five_day_return']).head(5)
print(sample)

# 检查字段列表
print('\n字段列表 (前20个):')
print(daily_results.columns[:20])

# 检查是否有技术指标的close字段
technical_close_cols = [col for col in daily_results.columns if 'close' in col.lower()]
print(f'\n包含close的字段: {technical_close_cols}')

# 检查收益率数据
print('\n收益率统计:')
stats = daily_results.select([
    pl.col('five_day_return').mean().alias('avg_5d'),
    pl.col('ten_day_return').mean().alias('avg_10d'),
    pl.col('twenty_day_return').mean().alias('avg_20d')
])
avg_5d = stats.select('avg_5d').item()
avg_10d = stats.select('avg_10d').item()
avg_20d = stats.select('avg_20d').item()
print(f'5日平均收益率: {avg_5d:.2f}%')
print(f'10日平均收益率: {avg_10d:.2f}%')
print(f'20日平均收益率: {avg_20d:.2f}%')