import polars as pl
from pathlib import Path

def check_300290_data():
    """检查300290股票的数据质量"""

    # 检查最新的OHLCV数据中300290的情况
    ohlcv_files = list(Path('data').glob('ohlcv_*.parquet'))
    if not ohlcv_files:
        print('❌ 未找到OHLCV数据文件')
        return

    latest_ohlcv = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
    print(f'📊 检查最新的OHLCV文件: {latest_ohlcv.name}')

    ohlcv_df = pl.read_parquet(latest_ohlcv)

    # 获取300290的数据
    stock_300290 = ohlcv_df.filter(pl.col('order_book_id') == '300290.XSHE')
    print(f'300290.XSHE 数据记录数: {len(stock_300290)}')

    if len(stock_300290) > 0:
        print('\n📈 300290.XSHE 最新数据:')
        latest_record = stock_300290.sort('date', descending=True).head(1)
        row = latest_record.row(0, named=True)

        date = row.get('date', 'N/A')
        open_price = row.get('open', 'N/A')
        close_price = row.get('close', 'N/A')
        high_price = row.get('high', 'N/A')
        low_price = row.get('low', 'N/A')
        volume = row.get('volume', 'N/A')
        amount = row.get('amount', 'N/A')

        print(f'   日期: {date}')
        print(f'   开盘价: {open_price}')
        print(f'   收盘价: {close_price}')
        print(f'   最高价: {high_price}')
        print(f'   最低价: {low_price}')
        print(f'   成交量: {volume}')
        print(f'   成交额: {amount}')

        # 检查是否有缺失的字段
        print(f'\n🔍 数据完整性检查:')
        required_fields = ['open', 'close', 'high', 'low', 'volume', 'amount']
        for field in required_fields:
            value = row.get(field)
            if value is not None and str(value) != 'nan':
                print(f'   ✅ {field}: {value}')
            else:
                print(f'   ❌ {field}: 缺失或为null')

        # 检查vwap, returns等计算字段
        calc_fields = ['vwap', 'returns', 'volume_ratio', 'price_change']
        print(f'\n🔧 计算字段检查:')
        for field in calc_fields:
            value = row.get(field)
            if value is not None and str(value) != 'nan':
                print(f'   ✅ {field}: {value}')
            else:
                print(f'   ❌ {field}: null或NaN (需要计算)')

        # 检查数据的时间范围
        min_date = stock_300290.select('date').min().item()
        max_date = stock_300290.select('date').max().item()
        print(f'\n📅 数据时间范围:')
        print(f'   开始日期: {min_date}')
        print(f'   结束日期: {max_date}')

        print(f'\n💡 建议:')
        print('   300290数据存在但未进入评分，可能是因为:')
        print('   1. 数据处理流程中被过滤')
        print('   2. 计算字段缺失导致评分失败')
        print('   3. 需要重新运行完整的评分任务')

    else:
        print('❌ 未找到300290.XSHE的数据')

if __name__ == '__main__':
    check_300290_data()