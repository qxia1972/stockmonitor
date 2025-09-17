import polars as pl

# 读取现有的前复权数据
df = pl.read_parquet('data/ohlcv_synced_filled_20250917.parquet')

print('=== 现有前复权数据分析 ===')
print(f'总记录数: {len(df):,}')
print(f'股票数量: {df.select("order_book_id").n_unique()}')
print(f'日期范围: {df.select("date").min().item()} 到 {df.select("date").max().item()}')
print(f'复权类型: {df.select("_adjust_type").unique().to_series().to_list()}')

# 检查数据质量
print('\n=== 数据质量检查 ===')
required_fields = ['open', 'close', 'high', 'low', 'volume', 'amount']
for field in required_fields:
    if field in df.columns:
        null_count = df.select(pl.col(field).is_null().sum()).item()
        completeness = ((len(df) - null_count) / len(df)) * 100
        print(f'{field}: {completeness:.1f}% 完整 ({null_count:,} 个空值)')
    else:
        print(f'{field}: ❌ 字段缺失')

# 显示样本数据
print('\n=== 数据样本 ===')
sample_stocks = df.select('order_book_id').unique().limit(3).to_series().to_list()
for stock in sample_stocks:
    stock_data = df.filter(pl.col('order_book_id') == stock).sort('date').limit(2)
    print(f'\n{stock} 最新数据:')
    for row in stock_data.to_dicts():
        print(f'  {row["date"]}: 开{row["open"]:.2f} 收{row["close"]:.2f} 高{row["high"]:.2f} 低{row["low"]:.2f} 量{row["volume"]:,}')