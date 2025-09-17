#!/usr/bin/env python3
"""
Analyze the correlation between score acceleration (rate of change in scores) and future 5-day returns
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Set font for better display
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False

class ScoreAccelerationAnalyzer:
    """Score acceleration and return correlation analyzer"""

    def __init__(self):
        self.scores_dir = Path('data/scores')
        self.data_dir = Path('data')
        self.results = {}

    def load_historical_scores(self, limit_days=30):
        """Load historical scoring data"""
        print(f"📂 Loading scoring data for the last {limit_days} days...")

        # Get the latest scoring file (containing historical data)
        score_files = list(self.scores_dir.glob('final_scores_*.parquet'))
        if not score_files:
            raise FileNotFoundError("No scoring data files found")

        # Take the latest file
        latest_file = max(score_files, key=lambda x: x.stat().st_mtime)
        print(f"   📄 Loading file: {latest_file.name}")

        try:
            scores_df = pl.read_parquet(latest_file)

            # Check required columns
            required_cols = ['order_book_id', 'date', 'composite_score']
            missing_cols = [col for col in required_cols if col not in scores_df.columns]
            if missing_cols:
                raise ValueError(f"Scoring data missing required columns: {missing_cols}")

            # Convert date format
            scores_df = scores_df.with_columns([
                pl.col('date').cast(pl.Date).alias('score_date')
            ])

            # Sort by stock and date
            scores_df = scores_df.sort(['order_book_id', 'score_date'])

            print(f"   ✅ Loading completed: {len(scores_df)} records")
            print(f"   📅 Date range: {scores_df.select('score_date').min()} to {scores_df.select('score_date').max()}")
            print(f"   📊 Unique stocks: {scores_df.select('order_book_id').n_unique()}")
            print(f"   📅 Unique dates: {scores_df.select('score_date').n_unique()}")

            return scores_df

        except Exception as e:
            raise ValueError(f"Failed to load scoring data: {e}")

    def load_price_data(self):
        """Load price data for return calculation"""
        print("📊 Loading price data...")

        # Find the latest OHLCV data file
        ohlcv_files = list(self.data_dir.glob('ohlcv_synced_*.parquet'))
        if not ohlcv_files:
            raise FileNotFoundError("No OHLCV data files found")

        # Take the latest file
        latest_file = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
        price_df = pl.read_parquet(latest_file)

        print(f"   ✅ Loading price data: {latest_file.name} - {len(price_df)} records")
        return price_df

    def calculate_score_acceleration(self, scores_df):
        """Calculate score acceleration (second derivative)"""
        print("🚀 Calculating score acceleration...")

        # Check if data has enough dates for time series analysis
        unique_dates = scores_df.select('score_date').n_unique()
        print(f"   📅 Data contains {unique_dates} unique dates")

        if unique_dates < 3:
            raise ValueError("Need at least 3 dates of data to calculate acceleration")

        # Calculate score change rate (first derivative)
        scores_df = scores_df.with_columns([
            pl.col('composite_score').diff().over('order_book_id').alias('score_velocity')
        ])

        # Calculate score acceleration (second derivative)
        scores_df = scores_df.with_columns([
            pl.col('score_velocity').diff().over('order_book_id').alias('score_acceleration')
        ])

        # Filter out NaN values
        acceleration_data = scores_df.filter(
            pl.col('score_acceleration').is_not_null()
        )

        print(f"   ✅ Calculation completed: {len(acceleration_data)} valid records")
        return acceleration_data

    def calculate_future_returns(self, acceleration_data, price_data, days_ahead=5):
        """Calculate N-day future returns (using historical data)"""
        print(f"📈 Calculating {days_ahead}-day future returns...")

        # Get all available scoring dates
        score_dates = acceleration_data.select('score_date').unique()
        score_dates_list = sorted(score_dates.to_series().to_list())

        # Get price data date range
        price_dates = price_data.select('date').unique()
        price_dates_list = sorted(price_dates.to_series().to_list())

        print(f"   📅 Scoring data date range: {score_dates_list[0]} to {score_dates_list[-1]}")
        print(f"   📅 Price data date range: {price_dates_list[0]} to {price_dates_list[-1]}")

        future_returns = []

        # For each scoring date, calculate subsequent N-day returns
        for i, score_date in enumerate(score_dates_list):
            # Find the position of scoring date in price data
            try:
                date_idx = price_dates_list.index(str(score_date))
            except ValueError:
                print(f"   ⚠️  Skipping date {score_date}: not found in price data")
                continue

            # Calculate future date index
            future_idx = date_idx + days_ahead
            if future_idx >= len(price_dates_list):
                print(f"   ⚠️  Skipping date {score_date}: insufficient future data")
                continue

            future_date = price_dates_list[future_idx]

            # Get current day's scoring data
            day_scores = acceleration_data.filter(pl.col('score_date') == score_date)

            # Get future date's price data
            future_prices = price_data.filter(pl.col('date') == future_date)

            if future_prices.is_empty():
                print(f"   ⚠️  Skipping date {score_date}: future price data does not exist")
                continue

            # Calculate returns (directly subtract future price from current price)
            returns_data = day_scores.join(
                future_prices.select(['order_book_id', 'close']).rename({'close': 'future_close'}),
                on='order_book_id',
                how='inner'
            ).with_columns([
                ((pl.col('future_close') - pl.col('close')) / pl.col('close')).alias(f'{days_ahead}d_return')
            ])

            if len(returns_data) > 0:
                future_returns.append(returns_data)
                print(f"   ✅ Processing date {score_date} -> {future_date}: {len(returns_data)} stocks")

        if not future_returns:
            raise ValueError("No valid return data found")

        all_returns = pl.concat(future_returns)
        valid_returns = all_returns.filter(pl.col(f'{days_ahead}d_return').is_not_null())

        print(f"   ✅ Calculation completed: {len(valid_returns)} valid records")
        return valid_returns

    def calculate_correlation(self, data, acceleration_col='score_acceleration', return_col='5d_return'):
        """Calculate correlation"""
        print("📊 Calculating correlation analysis...")

        # Filter valid data
        valid_data = data.filter(
            pl.col(acceleration_col).is_not_null() &
            pl.col(return_col).is_not_null() &
            pl.col(acceleration_col).is_finite() &
            pl.col(return_col).is_finite()
        )

        if valid_data.is_empty():
            raise ValueError("No valid correlation analysis data")

        # Convert to numpy arrays
        acceleration_values = valid_data.select(acceleration_col).to_numpy().flatten()
        return_values = valid_data.select(return_col).to_numpy().flatten()

        # Calculate Pearson correlation coefficient
        pearson_corr, pearson_p = stats.pearsonr(acceleration_values, return_values)

        # Calculate Spearman rank correlation coefficient
        spearman_corr, spearman_p = stats.spearmanr(acceleration_values, return_values)

        # Calculate statistics
        stats_info = {
            'sample_size': len(valid_data),
            'acceleration_mean': float(np.mean(acceleration_values)),
            'acceleration_std': float(np.std(acceleration_values)),
            'return_mean': float(np.mean(return_values)),
            'return_std': float(np.std(return_values)),
            'pearson_correlation': pearson_corr,
            'pearson_p_value': pearson_p,
            'spearman_correlation': spearman_corr,
            'spearman_p_value': spearman_p
        }

        print(f"   📈 Sample size: {stats_info['sample_size']}")
        print(f"   📊 Pearson correlation: {stats_info['pearson_correlation']:.4f}")
        print(f"   📊 Pearson p-value: {stats_info['pearson_p_value']:.4f}")
        print(f"   📊 Spearman correlation: {stats_info['spearman_correlation']:.4f}")
        print(f"   📊 Spearman p-value: {stats_info['spearman_p_value']:.4f}")
        return stats_info, valid_data

    def create_visualizations(self, data, stats_info):
        """Create visualization charts"""
        print("📊 Creating visualization charts...")

        # Set chart style
        plt.style.use('default')
        sns.set_palette("husl")

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Score Acceleration vs 5-Day Return Correlation Analysis', fontsize=16, fontweight='bold')

        # 1. Scatter plot
        acceleration = data.select('score_acceleration').to_numpy().flatten()
        returns = data.select('5d_return').to_numpy().flatten()

        axes[0, 0].scatter(acceleration, returns, alpha=0.6, s=30)
        axes[0, 0].set_xlabel('Score Acceleration')
        axes[0, 0].set_ylabel('5-Day Return')
        axes[0, 0].set_title('Score Acceleration vs 5-Day Return Scatter Plot')
        axes[0, 0].grid(True, alpha=0.3)

        # Add trend line
        if len(acceleration) > 1:
            z = np.polyfit(acceleration, returns, 1)
            p = np.poly1d(z)
            x_trend = np.linspace(min(acceleration), max(acceleration), 100)
            axes[0, 0].plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2)

        # 2. Correlation heatmap
        corr_matrix = np.corrcoef(acceleration, returns)
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0,
                   xticklabels=['Score Acceleration', '5-Day Return'],
                   yticklabels=['Score Acceleration', '5-Day Return'],
                   ax=axes[0, 1])
        axes[0, 1].set_title('Correlation Matrix')

        # 3. Distribution plot
        axes[1, 0].hist(acceleration, bins=50, alpha=0.7, label='Score Acceleration', density=True)
        axes[1, 0].set_xlabel('Score Acceleration')
        axes[1, 0].set_ylabel('Density')
        axes[1, 0].set_title('Score Acceleration Distribution')
        axes[1, 0].legend()

        # 4. Quartile analysis
        accel_quartiles = np.percentile(acceleration, [25, 50, 75])
        return_by_quartile = []

        for i, (lower, upper) in enumerate([(0, 25), (25, 50), (50, 75), (75, 100)]):
            mask = (acceleration >= np.percentile(acceleration, lower)) & \
                   (acceleration <= np.percentile(acceleration, upper))
            if np.any(mask):
                avg_return = np.mean(returns[mask])
                return_by_quartile.append(avg_return)
            else:
                return_by_quartile.append(0)

        quartile_labels = ['Q1 (Bottom 25%)', 'Q2 (25%-50%)', 'Q3 (50%-75%)', 'Q4 (Top 25%)']
        bars = axes[1, 1].bar(quartile_labels, return_by_quartile)
        axes[1, 1].set_xlabel('Score Acceleration Quartiles')
        axes[1, 1].set_ylabel('Average 5-Day Return')
        axes[1, 1].set_title('Average Returns by Acceleration Quartile')

        # Add value labels
        for bar, value in zip(bars, return_by_quartile):
            axes[1, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                           '.2%', ha='center', va='bottom')

        plt.tight_layout()
        plt.savefig('data/score_acceleration_analysis.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/score_acceleration_analysis.png")

        return fig

    def run_analysis(self):
        """Run complete analysis"""
        print("🎯 Starting score acceleration and return correlation analysis...")
        print("=" * 60)

        try:
            # 1. Load historical scoring data
            historical_scores = self.load_historical_scores(limit_days=30)

            # 2. Load price data
            price_data = self.load_price_data()

            # 3. Calculate score acceleration
            acceleration_data = self.calculate_score_acceleration(historical_scores)

            # 4. Calculate 5-day future returns
            returns_data = self.calculate_future_returns(acceleration_data, price_data, days_ahead=5)

            # 5. Calculate correlation
            stats_info, analysis_data = self.calculate_correlation(returns_data)

            # 6. Create visualizations
            fig = self.create_visualizations(analysis_data, stats_info)

            # 7. Print detailed results
            self.print_detailed_results(stats_info, analysis_data)

            print("\n" + "=" * 60)
            print("🎉 Analysis completed!")

            return stats_info, analysis_data

        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    def print_detailed_results(self, stats_info, data):
        """Print detailed analysis results"""
        print("\n📊 Detailed Analysis Results:")
        print("-" * 40)
        print(f"Sample size: {stats_info['sample_size']:,} observations")
        print("\nData Statistics:")
        print(f"   Score acceleration mean: {stats_info['acceleration_mean']:.4f}")
        print(f"   Score acceleration std: {stats_info['acceleration_std']:.4f}")
        print(f"   5-day return mean: {stats_info['return_mean']:.4f}")
        print(f"   5-day return std: {stats_info['return_std']:.4f}")
        print("\nCorrelation Analysis:")
        print(f"   Pearson correlation: {stats_info['pearson_correlation']:.4f}")
        print(f"   Pearson p-value: {stats_info['pearson_p_value']:.4f}")
        print(f"   Spearman correlation: {stats_info['spearman_correlation']:.4f}")
        print(f"   Spearman p-value: {stats_info['spearman_p_value']:.4f}")
        # Significance judgment
        if stats_info['pearson_p_value'] < 0.05:
            significance = "Significant" if abs(stats_info['pearson_correlation']) > 0.3 else "Weak but significant"
        else:
            significance = "Not significant"

        print(f"Correlation significance: {significance}")

        # Practical interpretation
        corr = stats_info['pearson_correlation']
        if abs(corr) > 0.5:
            strength = "Strong"
        elif abs(corr) > 0.3:
            strength = "Moderate"
        else:
            strength = "Weak"

        direction = "positive" if corr > 0 else "negative"
        print(f"Correlation strength: {strength} {direction} correlation")

        if abs(corr) > 0.2:
            print("💡 Investment suggestion: Score acceleration can be used as a reference for predicting 5-day returns")


def main():
    """Main function"""
    analyzer = ScoreAccelerationAnalyzer()
    stats_info, analysis_data = analyzer.run_analysis()

    if stats_info and analysis_data is not None:
        print("\n✅ Analysis completed successfully!")
        print(f"📁 Result chart saved to: data/score_acceleration_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()