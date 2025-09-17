#!/usr/bin/env python3
"""
专门分析300252.XSHE股票低分原因的脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import numpy as np
from modules.compute.stock_scorer import StockScorer
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_300252_detailed():
    """详细分析300252.XSHE的评分情况"""
    print("=== 300252.XSHE 详细评分分析 ===\n")

    # 1. 获取股票数据
    print("1. 获取股票数据...")
    try:
        # 读取数据文件
        data_dir = "data"
        tech_file = os.path.join(data_dir, "technical_indicators_20250916_231214.parquet")

        if not os.path.exists(tech_file):
            print(f"❌ 找不到技术指标文件: {tech_file}")
            return

        # 读取技术指标数据
        factors_df = pl.read_parquet(tech_file)
        print(f"✅ 成功读取技术指标数据，共 {factors_df.shape[0]} 行，{factors_df.shape[1]} 列")
        print(f"✅ 数据包含 {factors_df.select('order_book_id').unique().height} 只股票")

        # 查找300252.XSHE的数据
        stock_data = factors_df.filter(pl.col("order_book_id") == "300252.XSHE")

        if stock_data.is_empty():
            print("❌ 未找到300252.XSHE的数据")
            return

        print(f"✅ 找到300252.XSHE的数据")
        print(f"数据列数: {stock_data.shape[1]}")

        # 显示基本信息（使用最新的数据）
        print("\n2. 基本信息:")
        latest_data = stock_data.sort('date', descending=True).head(1)
        print(f"股票代码: {latest_data.select('order_book_id').item()}")
        print(f"数据日期: {latest_data.select('date').item()}")
        print(f"当前价格: {latest_data.select('close').item():.2f}")
        print(f"开盘价: {latest_data.select('open').item():.2f}")
        print(f"最高价: {latest_data.select('high').item():.2f}")
        print(f"最低价: {latest_data.select('low').item():.2f}")
        print(f"成交量: {latest_data.select('volume').item():,.0f}")
        print(f"成交额: {latest_data.select('amount').item():,.0f}")

        # 计算10日收益率（如果有足够的数据）
        if len(stock_data) >= 10:
            latest_close = latest_data.select('close').item()
            ten_days_ago_data = stock_data.sort('date', descending=True).limit(10).sort('date').head(1)
            ten_days_ago_close = ten_days_ago_data.select('close').item()
            ten_day_return = (latest_close - ten_days_ago_close) / ten_days_ago_close * 100
            print(f"10日收益率: {ten_day_return:.2f}%")
        else:
            print(f"10日收益率: 数据不足（只有{len(stock_data)}条记录）")

        # 3. 分析技术指标（使用最新数据）
        print("\n3. 技术指标分析:")
        tech_columns = ['close', 'volume', 'amount', 'vwap', 'high', 'low', 'open']

        for col in tech_columns:
            if col in latest_data.columns:
                value = latest_data.select(col).item()
                if isinstance(value, (int, float)) and not np.isnan(value):
                    if col in ['volume', 'amount']:
                        print(f"  {col}: {value:,.0f}")
                    else:
                        print(f"  {col}: {value:.2f}")
                else:
                    print(f"  {col}: {value}")

        # 显示数据统计信息
        print(f"\n  数据统计 (最近43个交易日):")
        print(f"  平均收盘价: {stock_data.select('close').mean().item():.2f}")
        print(f"  最高收盘价: {stock_data.select('close').max().item():.2f}")
        print(f"  最低收盘价: {stock_data.select('close').min().item():.2f}")
        print(f"  平均成交量: {stock_data.select('volume').mean().item():,.0f}")
        print(f"  总成交量: {stock_data.select('volume').sum().item():,.0f}")

        # 4. 使用评分器计算各维度得分
        print("\n4. 各维度得分计算:")
        scorer = StockScorer()

        # 计算趋势强度得分
        try:
            trend_result = scorer._calculate_trend_strength(stock_data)
            if 'trend_score' in trend_result.columns:
                trend_score_val = trend_result.select('trend_score').to_series().mean()
                trend_score = float(trend_score_val) if trend_score_val is not None else None
                print(f"  趋势强度得分: {trend_score:.2f}")

                # 显示趋势强度计算的中间结果
                intermediate_cols = ['score_arrangement', 'score_slope', 'score_position', 'score_stability']
                for col in intermediate_cols:
                    if col in trend_result.columns:
                        value = trend_result.select(col).to_series().mean()
                        if value is not None:
                            print(f"    {col}: {float(value):.2f}")
            else:
                print("  ❌ 趋势强度计算结果中没有trend_score字段")
                trend_score = None

        except Exception as e:
            print(f"  ❌ 计算趋势强度失败: {e}")
            trend_score = None

        # 计算资金动能得分
        try:
            capital_result = scorer._calculate_capital_power(stock_data)
            if 'capital_score' in capital_result.columns:
                capital_score_val = capital_result.select('capital_score').to_series().mean()
                capital_score = float(capital_score_val) if capital_score_val is not None else None
                print(f"  资金动能得分: {capital_score:.2f}")
            else:
                print("  ❌ 资金动能计算结果中没有capital_score字段")
                capital_score = None
        except Exception as e:
            print(f"  ❌ 计算资金动能失败: {e}")
            capital_score = None

        # 计算技术指标得分
        try:
            technical_result = scorer._calculate_technical_indicators(stock_data)
            if 'technical_score' in technical_result.columns:
                technical_score_val = technical_result.select('technical_score').to_series().mean()
                technical_score = float(technical_score_val) if technical_score_val is not None else None
                print(f"  技术指标得分: {technical_score:.2f}")
            else:
                print("  ❌ 技术指标计算结果中没有technical_score字段")
                technical_score = None
        except Exception as e:
            print(f"  ❌ 计算技术指标失败: {e}")
            technical_score = None

        # 计算风险控制得分
        try:
            risk_result = scorer._calculate_risk_control(stock_data)
            if 'risk_score' in risk_result.columns:
                risk_score_val = risk_result.select('risk_score').to_series().mean()
                risk_score = float(risk_score_val) if risk_score_val is not None else None
                print(f"  风险控制得分: {risk_score:.2f}")
            else:
                print("  ❌ 风险控制计算结果中没有risk_score字段")
                risk_score = None
        except Exception as e:
            print(f"  ❌ 计算风险控制失败: {e}")
            risk_score = None

        # 5. 计算综合得分
        print("\n5. 综合评分计算:")
        if all(score is not None for score in [trend_score, capital_score, technical_score, risk_score]):
            weights = {
                'trend_strength': 0.45,
                'capital_power': 0.24,
                'technical': 0.18,
                'risk_control': 0.15
            }

            composite_score = (
                (trend_score if trend_score is not None else 0) * weights['trend_strength'] +
                (capital_score if capital_score is not None else 0) * weights['capital_power'] +
                (technical_score if technical_score is not None else 0) * weights['technical'] +
                (risk_score if risk_score is not None else 0) * weights['risk_control']
            )

            print(f"  趋势强度 ({weights['trend_strength']:.2f}): {trend_score:.2f}")
            print(f"  资金动能 ({weights['capital_power']:.2f}): {capital_score:.2f}")
            print(f"  技术指标 ({weights['technical']:.2f}): {technical_score:.2f}")
            print(f"  风险控制 ({weights['risk_control']:.2f}): {risk_score:.2f}")
            print(f"  综合得分: {composite_score:.2f}")

            # 判断评分等级
            if composite_score >= 85:
                grade = "强势股"
            elif composite_score >= 70:
                grade = "潜力股"
            else:
                grade = "弱势股"
            print(f"  评分等级: {grade}")
        else:
            print("  ❌ 无法计算综合得分，部分维度得分计算失败")
            composite_score = None

        # 6. 分析问题原因
        print("\n6. 问题原因分析:")

        issues = []

        if trend_score is not None and trend_score < 70:
            issues.append(f"趋势强度得分过低 ({trend_score:.2f})")
        if capital_score is not None and capital_score < 50:
            issues.append(f"资金动能得分过低 ({capital_score:.2f})")
        if technical_score is not None and technical_score < 50:
            issues.append(f"技术指标得分过低 ({technical_score:.2f})")
        if risk_score is not None and risk_score < 50:
            issues.append(f"风险控制得分过低 ({risk_score:.2f})")

        if issues:
            print("  发现的问题:")
            for issue in issues:
                print(f"    - {issue}")
        else:
            print("  未发现明显问题")

        # 7. 给出建议
        print("\n7. 改进建议:")
        suggestions = []

        if trend_score is not None and trend_score < 70:
            suggestions.append("股价趋势偏弱，短期内缺乏上涨动能")
        if capital_score is not None and capital_score < 50:
            suggestions.append("资金关注度较低，成交量偏低")
        if technical_score is not None and technical_score < 50:
            suggestions.append("技术面偏弱，可能存在调整压力")
        if risk_score is not None and risk_score < 50:
            suggestions.append("估值偏高或波动较大，风险较高")

        if suggestions:
            for suggestion in suggestions:
                print(f"    - {suggestion}")
        else:
            print("    暂无具体建议")

    except Exception as e:
        print(f"❌ 分析过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_300252_detailed()
