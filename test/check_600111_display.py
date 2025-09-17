# 检查评分任务中600111的数据显示
import os
import polars as pl

print('🔍 检查评分任务中600111的数据显示...')

# 读取最新的数据文件
data_dir = 'data'
files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]
if files:
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
    file_path = os.path.join(data_dir, latest_file)

    df = pl.read_parquet(file_path)

    # 过滤600111的数据
    stock_600111 = df.filter(pl.col('order_book_id') == '600111.XSHG')

    if len(stock_600111) > 0:
        # 获取评分最高的记录（应该是最新的）
        latest_record = stock_600111.sort('date', descending=True).head(1)

        print('📊 评分任务中显示的600111数据:')
        record_data = latest_record.to_pandas().iloc[0]

        print(f'  股票代码: {record_data["order_book_id"]}')
        print(f'  日期: {record_data["date"]}')
        print(f'  开盘价: {record_data["open"]:.2f}')
        print(f'  收盘价: {record_data["close"]:.2f}')
        print(f'  最高价: {record_data["high"]:.2f}')
        print(f'  最低价: {record_data["low"]:.2f}')
        print(f'  成交量: {record_data["volume"]}')
        print(f'  成交额: {record_data["amount"]:.2f}')

        # 检查是否有其他相关的字段
        print('\n🔍 检查其他相关字段:')
        for col in latest_record.columns:
            if col not in ['order_book_id', 'date', 'open', 'close', 'high', 'low', 'volume', 'amount']:
                value = record_data[col]
                if isinstance(value, (int, float)) and not str(value).lower() in ['nan', 'inf', '-inf']:
                    print(f'  {col}: {value}')

        # 检查数据类型
        print('\n🔍 数据类型检查:')
        for col in ['open', 'close', 'high', 'low']:
            dtype = latest_record.schema[col]
            print(f'  {col}: {dtype}')

    else:
        print('❌ 未找到600111的数据')
else:
    print('❌ 未找到数据文件')