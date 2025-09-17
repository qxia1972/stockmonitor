import polars as pl
from modules.compute.data_processor import DataProcessor

def debug_stock_grouping():
    """调试按股票分组的问题"""
    print('🔍 调试按股票分组的问题...')

    # 创建数据处理器实例
    processor = DataProcessor()

    # 加载数据
    ohlcv_df = pl.read_parquet('data/ohlcv_synced_20250916_204000.parquet')

    # 计算技术指标
    indicators = ['sma_5', 'sma_10', 'sma_20', 'sma_30', 'sma_60', 'price_angles']
    result_df = processor.process_technical_indicators(ohlcv_df, indicators=indicators)

    # 选择一只具体的股票进行详细分析
    test_stock = '301009.XSHE'  # 从前面看到这只股票角度是9.23°

    print(f'\n📊 详细分析股票 {test_stock}:')

    stock_data = result_df.filter(pl.col('order_book_id') == test_stock).sort('date')
    print(f'数据行数: {len(stock_data)}')

    if len(stock_data) > 0:
        # 显示最后几行数据
        print('\n最后5行数据:')
        last_5 = stock_data.tail(5)
        for i, row in enumerate(last_5.iter_rows()):
            date = row[last_5.columns.index('date')]
            ma60_angle = row[last_5.columns.index('ma60_angle')] if 'ma60_angle' in last_5.columns else None
            sma60 = row[last_5.columns.index('sma_60')] if 'sma_60' in last_5.columns else None
            print(f'  {date}: sma60={sma60:.2f}, angle={ma60_angle:.2f}°')

        # 检查group_by的结果
        print('\n按股票分组的结果:')
        grouped = stock_data.group_by('order_book_id').agg(
            pl.col('ma60_angle').last().alias('latest_ma60_angle')
        )
        latest_angle = grouped.select('latest_ma60_angle').item()
        print(f'  分组后最新角度: {latest_angle:.2f}°')

        # 手动验证最新角度
        manual_latest = stock_data.select('ma60_angle').to_series().drop_nulls().tail(1).item()
        print(f'  手动获取最新角度: {manual_latest:.2f}°')

        # 检查数据完整性
        null_angles = stock_data.select(pl.col('ma60_angle').is_null().sum()).item()
        print(f'  空角度值数量: {null_angles}')

        # 检查变化率计算
        if 'sma_60_change_pct' in stock_data.columns:
            change_pcts = stock_data.select('sma_60_change_pct').to_series().drop_nulls()
            print(f'  变化率范围: {change_pcts.min():.6f} ~ {change_pcts.max():.6f}')
            print(f'  最新变化率: {change_pcts.tail(1).item():.6f}')

if __name__ == '__main__':
    debug_stock_grouping()
