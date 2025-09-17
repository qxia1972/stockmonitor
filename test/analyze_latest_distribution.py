import polars as pl
from modules.compute.data_processor import DataProcessor

def analyze_latest_angle_distribution():
    """分析最新角度的实际分布"""
    print('🔍 分析最新角度的实际分布...')

    # 创建数据处理器实例
    processor = DataProcessor()

    # 加载数据
    ohlcv_df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')

    # 计算技术指标
    indicators = ['sma_5', 'sma_10', 'sma_20', 'sma_30', 'sma_60', 'price_angles']
    result_df = processor.process_technical_indicators(ohlcv_df, indicators=indicators)

    # 分析不同周期的最新角度分布
    periods = [5, 10, 20, 30, 60]
    angle_cols = [f'ma{period}_angle' for period in periods]

    for col, period in zip(angle_cols, periods):
        if col in result_df.columns:
            print(f'\n📊 {period}日均线最新角度分布:')

            # 获取每只股票的最新角度
            stock_latest = result_df.group_by('order_book_id').agg(
                pl.col(col).last().alias(f'latest_{col}')
            ).drop_nulls()

            latest_angles = stock_latest.select(f'latest_{col}').to_series()

            print(f'  股票数量: {len(latest_angles):,}')
            print(f'  角度范围: {latest_angles.min():.2f}° ~ {latest_angles.max():.2f}°')
            print(f'  角度均值: {latest_angles.mean():.2f}°')
            print(f'  角度中位数: {latest_angles.median():.2f}°')

            # 统计不同区间的股票数量
            strong_up = len(latest_angles.filter(latest_angles > 30))
            mild_up = len(latest_angles.filter((latest_angles >= 10) & (latest_angles <= 30)))
            sideway = len(latest_angles.filter((latest_angles >= -10) & (latest_angles <= 10)))
            mild_down = len(latest_angles.filter((latest_angles >= -30) & (latest_angles < -10)))
            strong_down = len(latest_angles.filter(latest_angles < -30))

            total = len(latest_angles)
            print(f'  强势上涨 (>30°): {strong_up} ({strong_up/total*100:.1f}%)')
            print(f'  温和上涨 (10°~30°): {mild_up} ({mild_up/total*100:.1f}%)')
            print(f'  横盘震荡 (-10°~10°): {sideway} ({sideway/total*100:.1f}%)')
            print(f'  温和下跌 (-30°~-10°): {mild_down} ({mild_down/total*100:.1f}%)')
            print(f'  强势下跌 (<-30°): {strong_down} ({strong_down/total*100:.1f}%)')

            # 显示一些极端值的例子
            if strong_up > 0:
                strong_up_examples = latest_angles.filter(latest_angles > 30).head(3)
                print(f'  强势上涨例子: {strong_up_examples.to_list()[:3]}')

            if mild_up > 0:
                mild_up_examples = latest_angles.filter((latest_angles >= 10) & (latest_angles <= 30)).head(3)
                print(f'  温和上涨例子: {mild_up_examples.to_list()[:3]}')

if __name__ == '__main__':
    analyze_latest_angle_distribution()
