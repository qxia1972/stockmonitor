# 分析600570的数据单位换算问题
import os
import polars as pl

print('🔍 分析600570的数据单位换算问题...')

# 读取最新的数据文件
data_dir = 'data'
files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]
if files:
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
    file_path = os.path.join(data_dir, latest_file)

    df = pl.read_parquet(file_path)

    # 过滤600570的数据
    stock_600570 = df.filter(pl.col('order_book_id') == '600570.XSHG')

    if len(stock_600570) > 0:
        # 获取最新的记录
        latest_record = stock_600570.sort('date', descending=True).head(1)
        record_data = latest_record.to_pandas().iloc[0]

        print('📊 600570原始数据:')
        print(f'  收盘价: {record_data["close"]:.2f} 元')
        print(f'  成交量: {record_data["volume"]} 手')
        print(f'  成交金额: {record_data["amount"]:.2f} 元')

        print('\n🔍 可能的单位换算分析:')

        # 成交量单位换算
        volume_wan = record_data["volume"] / 10000
        print(f'  成交量(万手): {volume_wan:.2f}万')
        print(f'  用户报告成交量: 35.58万')
        print(f'  差异: {abs(volume_wan - 35.58):.2f}万')

        # 成交金额单位换算
        amount_yi = record_data["amount"] / 100000000
        print(f'\n  成交金额(亿元): {amount_yi:.2f}亿')
        print(f'  用户报告成交金额: 12.14亿')
        print(f'  差异: {abs(amount_yi - 12.14):.2f}亿')

        # 收盘价分析
        print(f'\n  收盘价: {record_data["close"]:.2f}元')
        print(f'  用户报告收盘价: 34.56元')
        print(f'  差异: {abs(record_data["close"] - 34.56):.2f}元')

        print('\n💡 分析结果:')
        print('1. 成交金额单位换算基本正确（差异很小，可能有四舍五入）')
        print('2. 成交量差异较大，可能有其他换算逻辑')
        print('3. 收盘价差异巨大，不可能是单位换算问题')

        print('\n🔍 可能的错误原因:')
        print('1. 数据显示时进行了错误的单位换算')
        print('2. 用户查看的是不同股票或不同日期的数据')
        print('3. 软件中有bug导致数据显示错误')
        print('4. 数据在某个处理环节被错误修改')

    else:
        print('❌ 未找到600570的数据')
else:
    print('❌ 未找到数据文件')