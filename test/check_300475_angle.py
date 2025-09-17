import polars as pl
import numpy as np

# 加载数据
df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')

# 过滤出300475股票
stock_data = df.filter(pl.col('order_book_id') == '300475.XSHE').sort('date')
print('股票300475数据行数:', len(stock_data))

if len(stock_data) > 0:
    # 显示最近几行数据
    print('\n最近5个交易日数据:')
    recent_data = stock_data.tail(5)
    for row in recent_data.iter_rows(named=True):
        print(f'日期: {row["date"]}, 收盘价: {row["close"]:.2f}')

    # 计算10日均线
    stock_data = stock_data.with_columns([
        pl.col('close').rolling_mean(window_size=10).alias('sma_10')
    ])

    # 计算10日均线变化率
    stock_data = stock_data.with_columns([
        pl.when(pl.col('sma_10').shift(1) != 0)
          .then((pl.col('sma_10') - pl.col('sma_10').shift(1)) / pl.col('sma_10').shift(1))
          .otherwise(None)
          .alias('sma_10_change_pct')
    ])

    # 计算角度
    stock_data = stock_data.with_columns([
        (pl.col('sma_10_change_pct') * 100.0).arctan().degrees().alias('ma10_angle')
    ])

    # 显示最近几行的计算结果
    print('\n最近5个交易日10日均线计算:')
    recent_calc = stock_data.tail(5)
    for row in recent_calc.iter_rows(named=True):
        change_pct = row.get('sma_10_change_pct')
        angle = row.get('ma10_angle')
        sma10 = row.get('sma_10')
        print(f'日期: {row["date"]}, 10日均线: {sma10:.2f}, 变化率: {change_pct:.6f}, 角度: {angle:.2f}°')

    # 特别检查用户提到的80.15°的情况
    print('\n检查是否存在80.15°的情况:')
    angles = stock_data.select('ma10_angle').to_series()
    close_to_80 = angles.filter((angles >= 80) & (angles <= 81))
    if len(close_to_80) > 0:
        print(f'发现接近80°的角度值: {close_to_80.to_list()}')
    else:
        print('未发现接近80°的角度值')

    print(f'\n角度统计:')
    print(f'最大角度: {angles.max():.2f}°')
    print(f'最小角度: {angles.min():.2f}°')
    print(f'平均角度: {angles.mean():.2f}°')

    # 检查是否存在80.15°的确切值
    angle_80_15 = angles.filter(angles == 80.15)
    if len(angle_80_15) > 0:
        print(f'找到确切的80.15°值，共出现{len(angle_80_15)}次')
    else:
        print('未找到确切的80.15°值')
else:
    print('未找到股票300475的数据')