import polars as pl
from modules.compute.data_processor import DataProcessor

def check_latest_angles():
    """检查最新角度的实际值"""
    print('🔍 检查最新角度的实际值...')

    # 创建数据处理器实例
    processor = DataProcessor()

    # 加载数据
    ohlcv_df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')

    # 计算技术指标
    indicators = ['sma_5', 'sma_10', 'sma_20', 'sma_30', 'sma_60', 'price_angles']
    result_df = processor.process_technical_indicators(ohlcv_df, indicators=indicators)

    # 检查60日均线的最新角度分布
    if 'ma60_angle' in result_df.columns:
        print('\n📊 60日均线角度分布检查:')

        # 获取所有非空的角度值
        angles = result_df.select('ma60_angle').drop_nulls().to_series()
        print(f'总数据点数: {len(result_df):,}')
        print(f'非空角度数: {len(angles):,}')

        if len(angles) > 0:
            print(f'角度范围: {angles.min():.2f}° ~ {angles.max():.2f}°')
            print(f'角度均值: {angles.mean():.2f}°')

            # 检查超过阈值的角度
            strong_up = angles.filter(angles > 30)
            mild_up = angles.filter((angles >= 10) & (angles <= 30))
            sideway = angles.filter((angles >= -10) & (angles <= 10))
            mild_down = angles.filter((angles >= -30) & (angles < -10))
            strong_down = angles.filter(angles < -30)

            print(f'强势上涨 (>30°): {len(strong_up)} 个数据点')
            print(f'温和上涨 (10°~30°): {len(mild_up)} 个数据点')
            print(f'横盘震荡 (-10°~10°): {len(sideway)} 个数据点')
            print(f'温和下跌 (-30°~-10°): {len(mild_down)} 个数据点')
            print(f'强势下跌 (<-30°): {len(strong_down)} 个数据点')

        # 检查按股票分组的最新角度
        print('\n🏢 按股票分组检查最新角度:')

        stock_latest = result_df.group_by('order_book_id').agg(
            pl.col('ma60_angle').last().alias('latest_ma60_angle')
        ).drop_nulls()

        latest_angles = stock_latest.select('latest_ma60_angle').to_series()
        print(f'股票数量: {len(latest_angles):,}')
        print(f'最新角度范围: {latest_angles.min():.2f}° ~ {latest_angles.max():.2f}°')
        print(f'最新角度均值: {latest_angles.mean():.2f}°')

        # 检查最新角度的分布
        strong_up_stocks = len(latest_angles.filter(latest_angles > 30))
        mild_up_stocks = len(latest_angles.filter((latest_angles >= 10) & (latest_angles <= 30)))
        sideway_stocks = len(latest_angles.filter((latest_angles >= -10) & (latest_angles <= 10)))
        mild_down_stocks = len(latest_angles.filter((latest_angles >= -30) & (latest_angles < -10)))
        strong_down_stocks = len(latest_angles.filter(latest_angles < -30))

        print(f'强势上涨股票: {strong_up_stocks}')
        print(f'温和上涨股票: {mild_up_stocks}')
        print(f'横盘震荡股票: {sideway_stocks}')
        print(f'温和下跌股票: {mild_down_stocks}')
        print(f'强势下跌股票: {strong_down_stocks}')

        # 显示一些具体的例子
        print('\n📋 具体股票例子:')
        sample_stocks = stock_latest.head(10)
        for row in sample_stocks.iter_rows():
            stock_id = row[0]
            latest_angle = row[1]
            print(f'  {stock_id}: {latest_angle:.2f}°')

if __name__ == '__main__':
    check_latest_angles()
