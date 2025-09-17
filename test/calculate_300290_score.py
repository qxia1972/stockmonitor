import polars as pl
from pathlib import Path

def calculate_300290_score():
    """从原始数据计算300290的评分"""

    print('🔍 从原始OHLCV数据计算300290的评分...')

    # 读取最新的OHLCV数据
    ohlcv_files = list(Path('data').glob('ohlcv_*.parquet'))
    if not ohlcv_files:
        print('❌ 未找到OHLCV数据文件')
        return

    latest_ohlcv = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
    print(f'📊 读取OHLCV文件: {latest_ohlcv.name}')

    df = pl.read_parquet(latest_ohlcv)

    # 获取300290的数据
    stock_300290 = df.filter(pl.col('order_book_id') == '300290.XSHE')
    print(f'300290.XSHE 数据记录数: {len(stock_300290)}')

    if len(stock_300290) > 0:
        # 获取最新一条记录
        latest_record = stock_300290.sort('date', descending=True).head(1)
        row = latest_record.row(0, named=True)

        print('\n📈 300290.XSHE 最新数据:')
        print(f'   日期: {row["date"]}')
        print(f'   开盘价: {row.get("open", "N/A")}')
        print(f'   收盘价: {row.get("close", "N/A")}')
        print(f'   最高价: {row.get("high", "N/A")}')
        print(f'   最低价: {row.get("low", "N/A")}')
        print(f'   成交量: {row.get("volume", "N/A")}')
        print(f'   成交额: {row.get("amount", "N/A")}')

        # 计算基本指标
        close = row.get('close', 0)
        open_price = row.get('open', 0)
        high = row.get('high', 0)
        low = row.get('low', 0)
        volume = row.get('volume', 0)
        amount = row.get('amount', 0)

        # 计算涨跌幅
        if open_price > 0:
            price_change = (close - open_price) / open_price * 100
        else:
            price_change = 0

        # 计算VWAP (成交量加权平均价)
        if volume > 0:
            vwap = amount / volume
        else:
            vwap = close

        # 计算量比 (这里用简单的成交量作为示例)
        volume_ratio = volume / 1000000  # 简化为相对值

        print('\n🔧 计算的指标:')
        print(f'   涨跌幅: {price_change:.2f}%')
        print(f'   VWAP: {vwap:.2f}')
        print(f'   量比: {volume_ratio:.2f}')

        # 简化的评分计算
        print('\n📊 简化的评分计算:')

        # 趋势评分 (基于涨跌幅)
        if price_change > 5:
            trend_score = 24
        elif price_change > 2:
            trend_score = 18
        elif price_change > 0:
            trend_score = 12
        elif price_change > -2:
            trend_score = 6
        else:
            trend_score = 0

        # 资金评分 (基于成交量)
        if volume > 10000000:  # 1000万股
            capital_score = 30
        elif volume > 5000000:
            capital_score = 24
        elif volume > 2000000:
            capital_score = 18
        elif volume > 1000000:
            capital_score = 12
        else:
            capital_score = 6

        # 技术评分 (简化为固定值)
        technical_score = 20

        # 风险评分 (简化为固定值)
        risk_score = 15

        # 综合评分
        composite_score = (trend_score * 0.3 + capital_score * 0.3 +
                          technical_score * 0.25 + risk_score * 0.15)

        print(f'   趋势评分: {trend_score} (基于涨跌幅 {price_change:.2f}%)')
        print(f'   资金评分: {capital_score} (基于成交量 {volume:.0f})')
        print(f'   技术评分: {technical_score}')
        print(f'   风险评分: {risk_score}')
        print(f'   综合评分: {composite_score:.2f}')

        # 评分等级
        if composite_score >= 22:
            score_level = '高风险股'
        elif composite_score >= 18:
            score_level = '中等风险股'
        else:
            score_level = '低风险股'

        print(f'   评分等级: {score_level}')

        print('\n💡 注意: 这是一个简化的评分计算')
        print('   实际评分系统会考虑更多技术指标和历史数据')

    else:
        print('❌ 未找到300290.XSHE的数据')

if __name__ == '__main__':
    calculate_300290_score()