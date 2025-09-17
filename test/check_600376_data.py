import polars as pl
import os

data_dir = 'data'
files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]
latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
df = pl.read_parquet(os.path.join(data_dir, latest_file))
stock_data = df.filter(pl.col('order_book_id') == '600376.XSHG')

print('600376.XSHG 历史数据统计:')
print(f'数据行数: {len(stock_data)}')
print(f'最高价: {stock_data.select(pl.col("high").max()).item():.2f}')
print(f'最低价: {stock_data.select(pl.col("low").min()).item():.2f}')
print(f'最新收盘价: {stock_data.select(pl.col("close")).tail(1).item():.2f}')
print(f'日期范围: {stock_data.select(pl.col("date").min()).item()} 到 {stock_data.select(pl.col("date").max()).item()}')

# 检查最近10天的价格走势
recent_data = stock_data.sort('date').tail(10)
print('\n最近10天价格走势:')
for row in recent_data.rows():
    print(f'{row[1]}: 开{row[2]:.2f} 高{row[3]:.2f} 低{row[4]:.2f} 收{row[5]:.2f} 量{row[6]}')