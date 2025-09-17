import polars as pl
import numpy as np
from modules.compute.data_processor import DataProcessor

def analyze_angle_distribution():
    """详细分析角度分布问题"""
    print('🔍 详细分析角度分布问题...')

    # 创建数据处理器实例
    processor = DataProcessor()

    # 加载数据
    ohlcv_df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')
    print(f'原始数据: {len(ohlcv_df):,} 行, {ohlcv_df.select("order_book_id").n_unique():,} 只股票')

    # 计算技术指标
    indicators = ['sma_5', 'sma_10', 'sma_20', 'sma_30', 'sma_60', 'price_angles']
    result_df = processor.process_technical_indicators(ohlcv_df, indicators=indicators)

    print(f'处理后数据: {len(result_df):,} 行, {result_df.select("order_book_id").n_unique():,} 只股票')

    # 检查一些具体股票的角度分布
    sample_stocks = ['000001.XSHE', '000002.XSHE', '600000.XSHG', '600036.XSHG']

    print('\n📊 检查具体股票的角度分布:')
    for stock in sample_stocks:
        stock_data = result_df.filter(pl.col('order_book_id') == stock).sort('date')

        if len(stock_data) > 60:
            print(f'\n{stock}:')
            for period in [5, 10, 20, 30, 60]:
                angle_col = f'ma{period}_angle'
                change_pct_col = f'sma_{period}_change_pct'

                if angle_col in stock_data.columns and change_pct_col in stock_data.columns:
                    # 获取最后30天的角度数据
                    recent_data = stock_data.tail(30)
                    angles = recent_data.select(angle_col).to_series().drop_nulls()
                    change_pcts = recent_data.select(change_pct_col).to_series().drop_nulls()

                    if len(angles) > 0:
                        print(f'  {period}日均线:')
                        print(f'    角度范围: {angles.min():.2f}° ~ {angles.max():.2f}°')
                        print(f'    角度均值: {angles.mean():.2f}°')
                        print(f'    变化率范围: {change_pcts.min():.4f} ~ {change_pcts.max():.4f}')
                        print(f'    变化率均值: {change_pcts.mean():.4f}')

                        # 检查tanh函数的输入输出
                        sample_change_pct = change_pcts.mean()
                        if sample_change_pct is not None and isinstance(sample_change_pct, (int, float)):
                            tanh_input = sample_change_pct * 5
                            tanh_output = np.tanh(tanh_input)
                            final_angle = tanh_output * 45

                            print(f'    tanh计算示例: tanh({sample_change_pct:.4f} * 5) = tanh({tanh_input:.4f}) = {tanh_output:.4f} → {final_angle:.2f}°')
                        else:
                            print(f'    变化率数据类型异常: {type(sample_change_pct)}')

    # 检查整体的变化率分布
    print('\n📈 检查整体变化率分布:')
    for period in [5, 10, 20, 30, 60]:
        change_pct_col = f'sma_{period}_change_pct'
        angle_col = f'ma{period}_angle'

        if change_pct_col in result_df.columns and angle_col in result_df.columns:
            change_pcts = result_df.select(change_pct_col).drop_nulls().to_series()
            angles = result_df.select(angle_col).drop_nulls().to_series()

            print(f'\n{period}日均线整体分布:')
            print(f'  变化率 - 均值: {change_pcts.mean():.6f}, 标准差: {change_pcts.std():.6f}')
            print(f'  变化率 - 最小值: {change_pcts.min():.6f}, 最大值: {change_pcts.max():.6f}')
            print(f'  角度 - 均值: {angles.mean():.2f}°, 标准差: {angles.std():.2f}°')
            print(f'  角度 - 最小值: {angles.min():.2f}°, 最大值: {angles.max():.2f}°')

            # 计算分位数
            change_pct_95 = change_pcts.quantile(0.95)
            change_pct_5 = change_pcts.quantile(0.05)
            print(f'  变化率95%分位数: {change_pct_95:.6f}, 5%分位数: {change_pct_5:.6f}')

if __name__ == '__main__':
    analyze_angle_distribution()
