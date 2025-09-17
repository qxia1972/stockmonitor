#!/usr/bin/env python3
"""
600376.XSHG 完整评分计算过程分析
检查为什么趋势强度满分但综合评分偏低
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
import polars as pl
from modules.compute.indicator_calculator import IndicatorCalculator
from modules.compute.stock_scorer import StockScorer

def analyze_600376_scoring():
    """分析600376.XSHG的评分计算过程"""

    print("=== 600376.XSHG 完整评分计算过程分析 ===")

    # 加载数据
    data_dir = Path('data')
    ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
    latest_ohlcv_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    combined_df = pl.read_parquet(latest_ohlcv_file)

    # 筛选600376.XSHG的数据
    stock_data = combined_df.filter(pl.col('order_book_id') == '600376.XSHG').sort('date')

    if len(stock_data) > 0:
        # 计算技术指标
        indicator_calc = IndicatorCalculator()
        indicators = ['sma', 'ema', 'rsi', 'macd', 'bollinger', 'stoch', 'atr', 'price_angles', 'volatility', 'volume_indicators', 'risk_indicators']
        full_data = indicator_calc.calculate_indicators(stock_data, indicators)

        # 确保full_data是DataFrame
        if isinstance(full_data, dict):
            full_data = pl.concat(list(full_data.values()))

        # 计算评分
        scorer = StockScorer()
        scored_df = scorer.score_stocks(full_data, market_env='normal')
        latest_score = scored_df.tail(1)

        print("\n各维度原始评分:")
        trend_score = latest_score.select('trend_score').item()
        capital_score = latest_score.select('capital_score').item()
        technical_score = latest_score.select('technical_score').item()
        risk_score = latest_score.select('risk_score').item()

        print(f"趋势强度: {trend_score:.1f}/100")
        print(f"资金动能: {capital_score:.1f}/100")
        print(f"技术指标: {technical_score:.1f}/100")
        print(f"风险控制: {risk_score:.1f}/100")

        # 获取实际权重
        weights = scorer._get_dynamic_weights('normal')
        print(f"\n实际权重配置: {weights}")

        # 计算加权得分
        weighted_trend = trend_score * weights['trend_strength']
        weighted_capital = capital_score * weights['capital_power']
        weighted_technical = technical_score * weights['technical']
        weighted_risk = risk_score * weights['risk_control']

        print("\n加权计算过程:")
        print(f"趋势强度贡献: {trend_score:.1f} * {weights['trend_strength']:.3f} = {weighted_trend:.2f}")
        print(f"资金动能贡献: {capital_score:.1f} * {weights['capital_power']:.3f} = {weighted_capital:.2f}")
        print(f"技术指标贡献: {technical_score:.1f} * {weights['technical']:.3f} = {weighted_technical:.2f}")
        print(f"风险控制贡献: {risk_score:.1f} * {weights['risk_control']:.3f} = {weighted_risk:.2f}")

        raw_composite = weighted_trend + weighted_capital + weighted_technical + weighted_risk
        print(f"\n原始综合评分: {raw_composite:.2f}")

        # 检查是否有调整因子
        final_composite = latest_score.select('composite_score').item()
        print(f"最终综合评分: {final_composite:.2f}")

        if abs(raw_composite - final_composite) > 0.01:
            adjustment_factor = final_composite / raw_composite
            print(f"调整因子: {adjustment_factor:.3f}")
            print("⚠️  发现调整因子，可能有行业或市值调整")
        else:
            print("✅ 无调整因子")

        # 分析问题
        print("\n=== 问题分析 ===")
        if trend_score == 100.0 and final_composite < 90.0:
            print("❌ 问题确认：趋势强度满分但综合评分偏低")
            print("🔍 主要原因：")

            if capital_score < 80:
                print(f"   - 资金动能评分过低 ({capital_score:.1f}/100)，拖累整体评分")
            if technical_score < 80:
                print(f"   - 技术指标评分一般 ({technical_score:.1f}/100)")
            if risk_score < 80:
                print(f"   - 风险控制评分一般 ({risk_score:.1f}/100)")

            print(f"\n💡 建议：考虑调整权重配置，增加趋势强度的权重占比")
            print(f"   当前权重: 趋势强度 {weights['trend_strength']*100:.1f}%, 资金动能 {weights['capital_power']*100:.1f}%")
            print(f"   建议权重: 趋势强度 35%, 资金动能 30%, 技术指标 20%, 风险控制 15%")

            # 提供具体的权重调整建议
            print("\n🔧 权重调整方案:")
            print("   方案1 - 激进调整: 趋势强度40%, 资金动能25%, 技术指标20%, 风险控制15%")
            print("   方案2 - 温和调整: 趋势强度35%, 资金动能30%, 技术指标20%, 风险控制15%")
            print("   方案3 - 平衡调整: 趋势强度30%, 资金动能35%, 技术指标20%, 风险控制15%")

        elif trend_score == 100.0 and final_composite >= 90.0:
            print("✅ 评分合理：趋势强度满分且综合评分优秀")

        else:
            print("ℹ️  评分正常：趋势强度不是满分，综合评分符合预期")

if __name__ == "__main__":
    analyze_600376_scoring()
