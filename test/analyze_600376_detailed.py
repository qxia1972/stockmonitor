"""
专门分析600376.XSHG的趋势强度计算详情
"""

import sys
import os
sys.path.append('.')

from networks.rqdatac_data_loader import RQDatacDataLoader
from modules.compute.data_processor import data_processor
from modules.compute.indicator_calculator import IndicatorCalculator
from modules.compute.stock_scorer import stock_scorer
import polars as pl
from datetime import datetime

def analyze_600376_detailed():
    print('=== 600376.XSHG 详细趋势强度分析 ===')

    # 初始化组件
    data_loader = RQDatacDataLoader(allow_mock_data=False)
    indicator_calc = IndicatorCalculator()

    # 加载数据
    data_dir = os.path.join('data')
    ohlcv_files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]

    if not ohlcv_files:
        print('❌ 未找到OHLCV数据文件')
        return

    latest_file = max(ohlcv_files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
    data_path = os.path.join(data_dir, latest_file)

    print(f'加载数据文件: {latest_file}')
    combined_df = pl.read_parquet(data_path)

    # 筛选600376.XSHG的数据
    stock_data = combined_df.filter(pl.col('order_book_id') == '600376.XSHG').sort('date')
    print(f'600376.XSHG 数据行数: {len(stock_data)}')

    if len(stock_data) == 0:
        print('❌ 未找到600376.XSHG的数据')
        return

    # 显示最新的数据
    latest_data = stock_data.tail(1).to_pandas().iloc[0]
    print('\n=== 600376.XSHG 最新数据 ===')
    print(f'日期: {latest_data["date"]}')
    print(f'收盘价: {latest_data["close"]:.2f}')
    print(f'开盘价: {latest_data["open"]:.2f}')
    print(f'最高价: {latest_data["high"]:.2f}')
    print(f'最低价: {latest_data["low"]:.2f}')
    print(f'成交量: {latest_data["volume"]}')

    # 计算技术指标
    print('\n=== 计算技术指标 ===')
    indicator_df = indicator_calc.calculate_indicators(stock_data)

    # 检查返回类型
    if isinstance(indicator_df, dict):
        # 如果返回字典，获取第一个股票的数据
        stock_code = list(indicator_df.keys())[0]
        indicator_df = indicator_df[stock_code]
        print(f'从字典中获取股票 {stock_code} 的指标数据')

    latest_indicator = indicator_df.tail(1).to_pandas().iloc[0]

    print('技术指标结果:')
    indicator_fields = ['sma_5', 'sma_10', 'sma_20', 'sma_60', 'ma10_angle', 'year_high', 'year_low', 'volatility_20d', 'volume_ratio', 'pct_change']
    for field in indicator_fields:
        if field in latest_indicator:
            value = latest_indicator[field]
            if isinstance(value, float):
                print(f'  {field}: {value:.4f}')
            else:
                print(f'  {field}: {value}')

    # 计算趋势强度得分
    print('\n=== 趋势强度得分计算 ===')
    scored_df = stock_scorer.score_stocks(indicator_df, market_env='normal')
    latest_score = scored_df.tail(1).to_pandas().iloc[0]

    print('趋势强度各组件得分:')
    score_fields = ['score_arrangement', 'score_slope', 'score_position', 'score_stability', 'trend_score']
    for field in score_fields:
        if field in latest_score:
            print(f'  {field}: {latest_score[field]:.1f}')

    # 详细分析多空排列
    print('\n=== 多空排列详细分析 ===')
    sma5 = latest_indicator.get('sma_5', 0)
    sma10 = latest_indicator.get('sma_10', 0)
    sma20 = latest_indicator.get('sma_20', 0)

    print(f'SMA5: {sma5:.2f}')
    print(f'SMA10: {sma10:.2f}')
    print(f'SMA20: {sma20:.2f}')

    if sma5 > sma10 and sma10 > sma20:
        arrangement_score = 30.0
        print('多空排列: 完美多头排列 (30分)')
    elif sma5 > sma10:
        arrangement_score = 21.0
        print('多空排列: 部分多头排列 (21分)')
    elif sma5 > sma20:
        arrangement_score = 12.0
        print('多空排列: 弱势多头排列 (12分)')
    else:
        arrangement_score = 3.0
        print('多空排列: 空头排列 (3分)')

    # 详细分析趋势斜率
    print('\n=== 趋势斜率详细分析 ===')
    ma10_angle = latest_indicator.get('ma10_angle', 0)
    print(f'MA10角度: {ma10_angle:.2f}度')

    if ma10_angle >= 45:
        slope_score = 30.0
        print('趋势斜率: 良好上涨趋势 (30分)')
    elif ma10_angle >= 15:
        slope_score = 25.0
        print('趋势斜率: 适中上涨趋势 (25分)')
    elif ma10_angle >= 5:
        slope_score = 20.0
        print('趋势斜率: 温和上涨趋势 (20分)')
    elif -5 <= ma10_angle <= 5:
        slope_score = 15.0
        print('趋势斜率: 平稳趋势 (15分)')
    elif ma10_angle >= -15:
        slope_score = 10.0
        print('趋势斜率: 温和下跌趋势 (10分)')
    else:
        slope_score = 5.0
        print('趋势斜率: 明显下跌趋势 (5分)')

    # 详细分析位置强度
    print('\n=== 位置强度详细分析 ===')
    close_price = latest_data['close']
    year_high = latest_indicator.get('year_high', 0)
    year_low = latest_indicator.get('year_low', 0)

    print(f'收盘价: {close_price:.2f}')
    print(f'年最高价: {year_high:.2f}')
    print(f'年最低价: {year_low:.2f}')
    print(f'距离最高价比例: {close_price/year_high:.3f}')

    if close_price > year_high:
        position_score = 25.0
        print('位置强度: 突破历史新高 (25分)')
    elif close_price > year_high * 0.95:
        position_score = 20.0
        print('位置强度: 接近历史高位 (20分)')
    elif close_price > year_high * 0.9:
        position_score = 15.0
        print('位置强度: 较高位置 (15分)')
    elif close_price < year_low:
        position_score = 0.0
        print('位置强度: 跌破历史新低 (0分)')
    else:
        position_score = 8.0
        print('位置强度: 中性位置 (8分)')

    # 详细分析稳定性
    print('\n=== 稳定性详细分析 ===')
    volatility = latest_indicator.get('volatility_20d', 0)
    print(f'20日波动率: {volatility:.4f}')

    if volatility < 0.15:
        stability_score = 15.0
        print('稳定性: 非常稳定 (15分)')
    elif 0.15 <= volatility <= 0.25:
        stability_score = 10.0
        print('稳定性: 适中稳定 (10分)')
    elif 0.25 <= volatility <= 0.35:
        stability_score = 5.0
        print('稳定性: 波动较大 (5分)')
    else:
        stability_score = 0.0
        print('稳定性: 极度波动 (0分)')

    # 计算总分
    total_calculated = arrangement_score + slope_score + position_score + stability_score
    print(f'\n=== 总分计算 ===')
    print(f'多空排列得分: {arrangement_score}')
    print(f'趋势斜率得分: {slope_score}')
    print(f'位置强度得分: {position_score}')
    print(f'稳定性得分: {stability_score}')
    print(f'计算总分: {total_calculated}')
    print(f'系统给分: {latest_score.get("trend_score", 0):.1f}')

    if abs(total_calculated - latest_score.get("trend_score", 0)) > 0.1:
        print('⚠️  计算结果与系统结果不一致！')

if __name__ == '__main__':
    analyze_600376_detailed()
