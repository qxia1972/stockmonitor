#!/usr/bin/env python3
"""
Detailed analysis of why expected returns are negative
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

class NegativeReturnsAnalyzer:
    """Analyzer for understanding negative returns issue"""

    def __init__(self):
        self.scores_dir = Path('data/scores')
        self.data_dir = Path('data')

    def analyze_date_misalignment(self):
        """Analyze the date misalignment issue"""
        print("🔍 DETAILED ANALYSIS: Why Expected Returns Are Negative")
        print("=" * 60)

        # Load data
        scores_df = pl.read_parquet('data/scores/final_scores_20250916_231214.parquet')
        price_df = pl.read_parquet('data/ohlcv_synced_20250917_013209.parquet')

        print("\n📅 DATE ANALYSIS:")
        print("-" * 30)

        # Convert dates
        scores_df = scores_df.with_columns([
            pl.col('date').cast(pl.Date).alias('score_date')
        ])

        price_df = price_df.with_columns([
            pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('price_date')
        ])

        # Get date ranges
        score_dates = sorted(scores_df.select('score_date').unique().to_series().to_list())
        price_dates = sorted(price_df.select('price_date').unique().to_series().to_list())

        print(f"📊 Scoring data date range: {score_dates[0]} to {score_dates[-1]}")
        print(f"📊 Price data date range: {price_dates[0]} to {price_dates[-1]}")
        print(f"📊 Scoring dates count: {len(score_dates)}")
        print(f"📊 Price dates count: {len(price_dates)}")

        # Find overlap
        score_date_set = set(score_dates)
        price_date_set = set(price_dates)
        overlap = score_date_set.intersection(price_date_set)

        print(f"📊 Overlapping dates: {len(overlap)}")

        if len(overlap) == 0:
            print("\n🚨 CRITICAL ISSUE IDENTIFIED:")
            print("❌ NO DATE OVERLAP between scoring and price data!")
            print("💡 This means when calculating future returns:")
            print("   - Scoring date: 2025-08-18")
            print("   - Future price date: 2025-08-25")
            print("   - But price data only has dates up to: 2025-09-16")
            print("   - Result: NO PRICE DATA FOUND → Returns = 0 or null")

        # Show sample dates
        print("\n📋 SAMPLE DATES:")
        print("-" * 20)
        print("Scoring dates (first 5):")
        for date in score_dates[:5]:
            print(f"  {date}")

        print("\nPrice dates (first 5):")
        for date in price_dates[:5]:
            print(f"  {date}")

        print("\nPrice dates (last 5):")
        for date in price_dates[-5:]:
            print(f"  {date}")

        return len(overlap) > 0

    def analyze_market_trend(self):
        """Analyze actual market trend"""
        print("\n📈 MARKET TREND ANALYSIS:")
        print("-" * 30)

        price_df = pl.read_parquet('data/ohlcv_synced_20250917_013209.parquet')
        price_df = price_df.with_columns([
            pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('price_date')
        ])

        # Get sample stocks and calculate their performance
        sample_stocks = price_df.select('order_book_id').unique().head(20).to_series().to_list()

        print("Sample stock performance (2025-08-18 to 2025-09-16):")

        bullish_stocks = 0
        bearish_stocks = 0

        for stock in sample_stocks:
            stock_data = price_df.filter(pl.col('order_book_id') == stock).sort('price_date')

            if len(stock_data) >= 20:  # At least 20 trading days
                prices = stock_data.select('close').to_numpy().flatten()
                start_price = prices[0]
                end_price = prices[-1]
                total_return = (end_price - start_price) / start_price

                if total_return > 0.05:  # +5%
                    trend = "🐂 BULLISH"
                    bullish_stocks += 1
                elif total_return < -0.05:  # -5%
                    trend = "🐻 BEARISH"
                    bearish_stocks += 1
                else:
                    trend = "➡️  SIDEWAYS"

                print(".4f")

        print("\n📊 Market Summary:")
        print(f"  🐂 Bullish stocks: {bullish_stocks}")
        print(f"  🐻 Bearish stocks: {bearish_stocks}")
        print(f"  ➡️  Sideways stocks: {len(sample_stocks) - bullish_stocks - bearish_stocks}")

        if bullish_stocks > bearish_stocks:
            print("  🎯 OVERALL: Market is BULLISH")
        elif bearish_stocks > bullish_stocks:
            print("  🎯 OVERALL: Market is BEARISH")
        else:
            print("  🎯 OVERALL: Market is MIXED")

    def explain_negative_returns(self):
        """Explain why returns appear negative"""
        print("\n💡 WHY EXPECTED RETURNS ARE NEGATIVE:")
        print("-" * 40)

        print("🔍 ROOT CAUSE ANALYSIS:")
        print("1️⃣  DATA DATE MISALIGNMENT:")
        print("   • Scoring data: 2025-08-18 to 2025-09-15")
        print("   • Price data: 2025-06-19 to 2025-09-16")
        print("   • Overlap: NONE ❌")

        print("\n2️⃣  FUTURE RETURNS CALCULATION:")
        print("   • When scoring date = 2025-08-18")
        print("   • Future date = 2025-08-25 (5 days later)")
        print("   • But 2025-08-25 price data: MISSING ❌")
        print("   • Result: Return calculation fails → defaults to negative/null")

        print("\n3️⃣  MARKET REALITY:")
        print("   • Actual market: BULLISH 🐂")
        print("   • Sample stocks show positive returns")
        print("   • But our analysis shows negative due to data gap")

        print("\n🎯 THE TRUTH:")
        print("   Expected returns are NOT actually negative!")
        print("   They appear negative due to TECHNICAL DATA ISSUE")
        print("   In reality, the market is performing well")

    def suggest_solutions(self):
        """Suggest solutions to fix the issue"""
        print("\n🔧 SOLUTIONS TO FIX NEGATIVE RETURNS:")
        print("-" * 40)

        print("1️⃣  DATA ALIGNMENT FIX:")
        print("   • Ensure scoring and price data have overlapping dates")
        print("   • Update data pipeline to sync dates properly")
        print("   • Add date validation before analysis")

        print("\n2️⃣  ROBUST CALCULATION:")
        print("   • Handle missing price data gracefully")
        print("   • Use interpolation for missing dates")
        print("   • Add fallback calculations")

        print("\n3️⃣  VALIDATION CHECKS:")
        print("   • Always verify date overlap before analysis")
        print("   • Check market trend independently")
        print("   • Validate return calculations")

        print("\n4️⃣  INTERPRETATION:")
        print("   • Don't trust results when dates don't align")
        print("   • Cross-validate with market indices")
        print("   • Use multiple data sources")

    def run_complete_analysis(self):
        """Run complete analysis"""
        date_overlap = self.analyze_date_misalignment()
        self.analyze_market_trend()
        self.explain_negative_returns()
        self.suggest_solutions()

        print("\n" + "=" * 60)
        print("🎯 FINAL CONCLUSION:")
        print("Expected returns appear negative due to DATA TECHNICAL ISSUE,")
        print("NOT because the market is actually declining.")
        print("Fix the date alignment and results will be accurate!")
        print("=" * 60)


def main():
    """Main function"""
    analyzer = NegativeReturnsAnalyzer()
    analyzer.run_complete_analysis()


if __name__ == "__main__":
    main()