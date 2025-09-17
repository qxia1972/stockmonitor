#!/usr/bin/env python3
"""
Analyze market overall performance to understand negative returns
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt

class MarketPerformanceAnalyzer:
    """Analyzer for overall market performance"""

    def __init__(self):
        self.scores_dir = Path('data/scores')
        self.data_dir = Path('data')
        self.results = {}

    def load_data(self):
        """Load scoring and price data"""
        print("📂 Loading data...")

        # Load latest scoring file
        score_files = list(self.scores_dir.glob('final_scores_*.parquet'))
        if not score_files:
            raise FileNotFoundError("No scoring data files found")

        latest_score_file = max(score_files, key=lambda x: x.stat().st_mtime)
        print(f"   📄 Loading scoring data: {latest_score_file.name}")

        scores_df = pl.read_parquet(latest_score_file)

        # Load price data
        ohlcv_files = list(self.data_dir.glob('ohlcv_synced_*.parquet'))
        if not ohlcv_files:
            raise FileNotFoundError("No OHLCV data files found")

        latest_price_file = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
        print(f"   📄 Loading price data: {latest_price_file.name}")

        price_df = pl.read_parquet(latest_price_file)

        print(f"   ✅ Data loaded: {len(scores_df)} scoring records, {len(price_df)} price records")
        return scores_df, price_df

    def analyze_market_performance(self, scores_df, price_df):
        """Analyze overall market performance"""
        print("📊 Analyzing overall market performance...")

        # Convert date columns to proper format
        scores_df = scores_df.with_columns([
            pl.col('date').cast(pl.Date).alias('score_date')
        ])

        price_df = price_df.with_columns([
            pl.col('date').str.strptime(pl.Date, "%Y-%m-%d").alias('price_date')
        ])

        # Get unique dates
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        price_dates = price_df.select('price_date').unique().sort('price_date')

        print(f"   📅 Score dates: {len(score_dates)} dates from {score_dates.min()} to {score_dates.max()}")
        print(f"   📅 Price dates: {len(price_dates)} dates from {price_dates.min()} to {price_dates.max()}")

        # Calculate daily returns for a sample of stocks
        print("\n   📈 Calculating daily returns for sample stocks...")

        # Get sample of stocks
        sample_stocks = scores_df.select('order_book_id').unique().head(10).to_series().to_list()

        market_returns = []

        for stock in sample_stocks[:3]:  # Just check first 3 stocks
            stock_prices = price_df.filter(pl.col('order_book_id') == stock).sort('price_date')

            if len(stock_prices) > 1:
                # Calculate daily returns
                prices = stock_prices.select('close').to_numpy().flatten()
                dates = stock_prices.select('price_date').to_series().to_list()

                daily_returns = []
                for i in range(1, len(prices)):
                    ret = (prices[i] - prices[i-1]) / prices[i-1]
                    daily_returns.append({
                        'date': dates[i],
                        'return': ret,
                        'stock': stock
                    })

                market_returns.extend(daily_returns)
                print(f"      {stock}: {len(daily_returns)} daily returns, avg: {np.mean([r['return'] for r in daily_returns]):.4f}")

        # Analyze market trend
        print("\n   📊 Market Trend Analysis:")

        # Group by date and calculate average returns
        if market_returns:
            returns_df = pl.DataFrame(market_returns)

            # Calculate average return by date
            avg_returns_by_date = returns_df.group_by('date').agg([
                pl.col('return').mean().alias('avg_return'),
                pl.col('return').count().alias('stock_count')
            ]).sort('date')

            print("   Daily average returns:")
            for row in avg_returns_by_date.iter_rows():
                print(".4f")

            overall_avg_return = np.mean([r['return'] for r in market_returns])
            print(".4f")

            # Check if it's a bear market
            if overall_avg_return < -0.001:  # -0.1%
                print("   🐻 BEAR MARKET DETECTED: Overall market is in decline")
            elif overall_avg_return > 0.001:  # +0.1%
                print("   🐂 BULL MARKET DETECTED: Overall market is rising")
            else:
                print("   ➡️  SIDEWAYS MARKET: Market is relatively stable")

        return market_returns

    def analyze_future_returns_calculation(self, scores_df, price_df):
        """Analyze the future returns calculation process"""
        print("\n   🔍 Analyzing future returns calculation...")

        # Check data alignment
        scores_dates = scores_df.select('date').unique().sort('date')
        price_dates = price_df.select('date').unique().sort('date')

        print(f"   📅 Scores data dates: {len(scores_dates)} unique dates")
        print(f"   📅 Price data dates: {len(price_dates)} unique dates")

        # Check date overlap
        scores_date_set = set(scores_dates.to_series().to_list())
        price_date_set = set(price_dates.to_series().to_list())

        overlap_dates = scores_date_set.intersection(price_date_set)
        print(f"   📅 Overlapping dates: {len(overlap_dates)} dates")

        if len(overlap_dates) == 0:
            print("   ❌ NO DATE OVERLAP: This explains negative returns!")
            print("   💡 Solution: Need to align dates between scoring and price data")
            return False

        # Check sample calculation
        sample_date = list(overlap_dates)[0]
        print(f"   🔍 Checking sample date: {sample_date}")

        sample_scores = scores_df.filter(pl.col('date') == sample_date)
        sample_prices = price_df.filter(pl.col('date') == sample_date)

        print(f"   📊 Sample date stocks: scores={len(sample_scores)}, prices={len(sample_prices)}")

        return True

    def run_analysis(self):
        """Run complete market performance analysis"""
        print("🎯 Starting market performance analysis...")
        print("=" * 60)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Analyze market performance
            market_returns = self.analyze_market_performance(scores_df, price_df)

            # 3. Analyze future returns calculation
            date_alignment_ok = self.analyze_future_returns_calculation(scores_df, price_df)

            print("\n" + "=" * 60)
            print("🎉 Analysis completed!")

            return market_returns, date_alignment_ok

        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None, None


def main():
    """Main function"""
    analyzer = MarketPerformanceAnalyzer()
    market_returns, date_alignment_ok = analyzer.run_analysis()

    if market_returns is not None:
        print("\n✅ Analysis completed successfully!")
        print("\n📋 SUMMARY OF FINDINGS:")
        print("=" * 40)

        if market_returns:
            overall_avg_return = np.mean([r['return'] for r in market_returns])
            print(".4f")

            if overall_avg_return < -0.001:
                print("🐻 CONCLUSION: The market is in a BEAR PHASE")
                print("💡 This explains why all expected returns are negative")
                print("📈 Strategy: Consider defensive/contrarian approaches")
            elif overall_avg_return > 0.001:
                print("🐂 CONCLUSION: The market is in a BULL PHASE")
                print("❓ Expected returns should be positive - data issue suspected")
            else:
                print("➡️ CONCLUSION: The market is SIDEWAYS")
                print("💡 Mixed signals - need more detailed analysis")

        if not date_alignment_ok:
            print("\n🚨 CRITICAL ISSUE: Date misalignment between datasets")
            print("💡 This is likely the primary cause of negative returns")

    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()