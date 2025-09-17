#!/usr/bin/env python3
"""
Analyze volume ratio * RSI composite indicator distribution and trading strategies
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Set font for better display
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False
    print("⚠️  Seaborn not available, using matplotlib only")

class VolumeRatioRSIAnalyzer:
    """Analyzer for volume ratio * RSI composite indicator distribution and strategies"""

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
        scores_df = scores_df.with_columns([
            pl.col('date').cast(pl.Date).alias('score_date')
        ])

        # Load price data
        ohlcv_files = list(self.data_dir.glob('ohlcv_synced_*.parquet'))
        if not ohlcv_files:
            raise FileNotFoundError("No OHLCV data files found")

        latest_price_file = max(ohlcv_files, key=lambda x: x.stat().st_mtime)
        print(f"   📄 Loading price data: {latest_price_file.name}")

        price_df = pl.read_parquet(latest_price_file)

        print(f"   ✅ Data loaded: {len(scores_df)} scoring records, {len(price_df)} price records")
        return scores_df, price_df

    def calculate_volume_ratio_rsi_composite(self, scores_df):
        """Calculate volume ratio * RSI composite indicator for all stocks"""
        print("📊 Calculating volume ratio * RSI composite positions...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        all_positions = []

        for score_date in score_dates_list:
            # Get data for current date
            current_scores = scores_df.filter(pl.col('score_date') == score_date)

            # Select volume ratio and RSI indicators
            position_data = current_scores.select([
                'order_book_id', 'volume_ratio', 'rsi_6', 'rsi_10', 'rsi_14', 'close', 'date'
            ])

            # Calculate composite indicators
            position_data = position_data.with_columns([
                (pl.col('volume_ratio') * pl.col('rsi_6')).alias('volume_rsi_6_composite'),
                (pl.col('volume_ratio') * pl.col('rsi_10')).alias('volume_rsi_10_composite'),
                (pl.col('volume_ratio') * pl.col('rsi_14')).alias('volume_rsi_14_composite')
            ])

            # Filter valid data
            valid_position_data = position_data.filter(
                pl.col('volume_ratio').is_not_null()
            ).filter(
                pl.col('volume_ratio').is_finite()
            ).filter(
                pl.col('volume_ratio') > 0
            ).filter(
                pl.col('rsi_6').is_not_null()
            ).filter(
                pl.col('rsi_10').is_not_null()
            ).filter(
                pl.col('rsi_14').is_not_null()
            ).filter(
                pl.col('volume_rsi_6_composite').is_finite()
            ).filter(
                pl.col('volume_rsi_10_composite').is_finite()
            ).filter(
                pl.col('volume_rsi_14_composite').is_finite()
            )

            if len(valid_position_data) > 0:
                all_positions.append(valid_position_data)
                print(f"   ✅ Processed {score_date}: {len(valid_position_data)} stocks")

        if not all_positions:
            raise ValueError("No valid volume ratio * RSI composite data found")

        combined_positions = pl.concat(all_positions)
        print(f"   ✅ Volume Ratio * RSI composite calculation completed: {len(combined_positions)} valid records")

        return combined_positions

    def analyze_composite_distribution(self, position_data, rsi_period):
        """Analyze the distribution of volume ratio * RSI composite values"""
        print(f"📊 Analyzing volume ratio * RSI_{rsi_period} composite distribution...")

        composite_col = f'volume_rsi_{rsi_period}_composite'
        composite_values = position_data.select(composite_col).to_numpy().flatten()

        # Basic statistics
        stats_dict = {
            'mean': np.mean(composite_values),
            'median': np.median(composite_values),
            'std': np.std(composite_values),
            'min': np.min(composite_values),
            'max': np.max(composite_values),
            'skewness': stats.skew(composite_values),
            'kurtosis': stats.kurtosis(composite_values)
        }

        # Percentiles
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            stats_dict[f'p{p}'] = np.percentile(composite_values, p)

        print(f"   📈 Volume Ratio * RSI_{rsi_period} Composite Statistics:")
        print(f"      Mean: {stats_dict['mean']:.4f}")
        print(f"      Median: {stats_dict['median']:.4f}")
        print(f"      Std: {stats_dict['std']:.4f}")
        print(f"      Range: [{stats_dict['min']:.4f}, {stats_dict['max']:.4f}]")
        print(f"      Skewness: {stats_dict['skewness']:.4f}")
        print(f"      Kurtosis: {stats_dict['kurtosis']:.4f}")

        return stats_dict

    def analyze_composite_by_quantile(self, position_data, future_returns_data, rsi_period, n_quantiles=10):
        """Analyze returns by volume ratio * RSI composite quantiles"""
        print(f"📊 Analyzing returns by {n_quantiles} volume ratio * RSI_{rsi_period} composite quantiles...")

        composite_col = f'volume_rsi_{rsi_period}_composite'

        # Merge position data with future returns
        merged_data = position_data.join(
            future_returns_data.select(['order_book_id', 'score_date', '5d_return', '5d_return_discrete_10pct']),
            left_on=['order_book_id', 'date'],
            right_on=['order_book_id', 'score_date'],
            how='inner'
        )

        if len(merged_data) == 0:
            print("   ⚠️  No merged data found")
            return None

        composite_values = merged_data.select(composite_col).to_numpy().flatten()
        returns = merged_data.select('5d_return').to_numpy().flatten()

        # Create quantiles
        quantile_labels = [f'Q{i+1}' for i in range(n_quantiles)]
        merged_data = merged_data.with_columns([
            pl.Series(np.digitize(composite_values, np.percentile(composite_values, np.linspace(0, 100, n_quantiles+1)[1:-1]))).alias(f'volume_rsi_{rsi_period}_composite_quantile')
        ])

        # Analyze each quantile
        quantile_stats = []
        for i in range(1, n_quantiles + 1):
            quantile_data = merged_data.filter(pl.col(f'volume_rsi_{rsi_period}_composite_quantile') == i)
            if len(quantile_data) > 0:
                q_returns = quantile_data.select('5d_return').to_numpy().flatten()
                q_composites = quantile_data.select(composite_col).to_numpy().flatten()

                stats_dict = {
                    'quantile': i,
                    'label': f'Q{i}',
                    'count': len(q_returns),
                    'mean_return': np.mean(q_returns),
                    'median_return': np.median(q_returns),
                    'std_return': np.std(q_returns),
                    'min_composite': np.min(q_composites),
                    'max_composite': np.max(q_composites),
                    'mean_composite': np.mean(q_composites)
                }
                quantile_stats.append(stats_dict)

        # Print results
        print(f"   📈 Volume Ratio * RSI_{rsi_period} Composite Quantile Analysis Results:")
        print("   " + "="*70)
        for stat in quantile_stats:
            print(f"   {stat['label']:>3}: Count={stat['count']:>5}, "
                  f"Mean Return={stat['mean_return']:>+7.4f}, "
                  f"Composite=[{stat['min_composite']:>+6.2f}, {stat['max_composite']:>+6.2f}]")

        return quantile_stats

    def create_composite_visualization(self, position_data, quantile_stats_dict):
        """Create comprehensive visualization of volume ratio * RSI composites"""
        print("📊 Creating volume ratio * RSI composite distribution visualization...")

        fig, axes = plt.subplots(3, 2, figsize=(16, 18))
        fig.suptitle('Volume Ratio * RSI Composite Indicators Analysis', fontsize=16, fontweight='bold')

        rsi_periods = [6, 10, 14]

        for idx, rsi_period in enumerate(rsi_periods):
            composite_col = f'volume_rsi_{rsi_period}_composite'
            composite_values = position_data.select(composite_col).to_numpy().flatten()
            quantile_stats = quantile_stats_dict[rsi_period]

            # Histogram with KDE
            if HAS_SEABORN:
                sns.histplot(composite_values, bins=50, kde=True, ax=axes[idx, 0])
            else:
                axes[idx, 0].hist(composite_values, bins=50, alpha=0.7, density=True)
            axes[idx, 0].axvline(np.mean(composite_values), color='red', linestyle='--', label=f'Mean: {np.mean(composite_values):.2f}')
            axes[idx, 0].axvline(np.median(composite_values), color='green', linestyle='--', label=f'Median: {np.median(composite_values):.2f}')
            axes[idx, 0].set_xlabel(f'Volume Ratio * RSI_{rsi_period}')
            axes[idx, 0].set_ylabel('Density')
            axes[idx, 0].set_title(f'Volume Ratio * RSI_{rsi_period} Distribution')
            axes[idx, 0].legend()
            axes[idx, 0].grid(True, alpha=0.3)

            # Quantile returns analysis
            if quantile_stats:
                quantiles = [stat['quantile'] for stat in quantile_stats]
                mean_returns = [stat['mean_return'] for stat in quantile_stats]
                quantile_labels = [stat['label'] for stat in quantile_stats]

                bars = axes[idx, 1].bar(quantiles, mean_returns, color='skyblue', alpha=0.7)
                axes[idx, 1].set_xlabel('Composite Quantile')
                axes[idx, 1].set_ylabel('Mean 5-Day Return')
                axes[idx, 1].set_title(f'Returns by Volume Ratio * RSI_{rsi_period} Quantile')
                axes[idx, 1].set_xticks(quantiles)
                axes[idx, 1].set_xticklabels(quantile_labels)
                axes[idx, 1].grid(True, alpha=0.3)

                # Add value labels
                for bar, ret in zip(bars, mean_returns):
                    axes[idx, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                                     f'{ret:.4f}', ha='center', va='bottom', fontsize=8)

        plt.tight_layout()
        plt.savefig('data/volume_ratio_rsi_composite_analysis.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/volume_ratio_rsi_composite_analysis.png")

        return fig

    def suggest_composite_strategies(self, quantile_stats_dict):
        """Suggest trading strategies based on volume ratio * RSI composite analysis"""
        print("\n🎯 Volume Ratio * RSI Composite Trading Strategy Suggestions:")
        print("="*70)

        rsi_periods = [6, 10, 14]

        for rsi_period in rsi_periods:
            quantile_stats = quantile_stats_dict[rsi_period]

            if not quantile_stats:
                continue

            print(f"\n   📊 RSI_{rsi_period} Composite Analysis:")

            # Find best and worst performing quantiles
            sorted_quantiles = sorted(quantile_stats, key=lambda x: x['mean_return'], reverse=True)

            best_quantile = sorted_quantiles[0]
            worst_quantile = sorted_quantiles[-1]

            print(f"      📈 Best: {best_quantile['label']} (Return: {best_quantile['mean_return']:.4f})")
            print(f"      📉 Worst: {worst_quantile['label']} (Return: {worst_quantile['mean_return']:.4f})")

            # Strategy recommendations
            print(f"      💡 RSI_{rsi_period} Strategy:")

            # High composite strategy (high volume + high RSI)
            high_composite_quantiles = [q for q in quantile_stats if q['min_composite'] >= 100]
            if high_composite_quantiles:
                best_high_comp = max(high_composite_quantiles, key=lambda x: x['mean_return'])
                print(f"         🚀 HIGH VOLUME + HIGH RSI: Focus on composite > 100")
                print(f"            Best range: [{best_high_comp['min_composite']:.1f}, {best_high_comp['max_composite']:.1f}]")
                print(f"            Expected return: {best_high_comp['mean_return']:.2%}")

            # Low composite strategy (low volume + low RSI)
            low_composite_quantiles = [q for q in quantile_stats if q['max_composite'] <= 50]
            if low_composite_quantiles:
                best_low_comp = max(low_composite_quantiles, key=lambda x: x['mean_return'])
                print(f"         📉 LOW VOLUME + LOW RSI: Consider composite < 50")
                print(f"            Best range: [{best_low_comp['min_composite']:.1f}, {best_low_comp['max_composite']:.1f}]")
                print(f"            Expected return: {best_low_comp['mean_return']:.2%}")

            # Mean reversion strategy
            extreme_low_quantiles = [q for q in quantile_stats if q['quantile'] <= 3]
            if extreme_low_quantiles:
                print("         🔄 MEAN REVERSION: Very low composite values may indicate oversold")

        # Overall composite strategy recommendations
        print("\n   💡 Overall Composite Strategy Recommendations:")

        # Compare across RSI periods
        best_performers = {}
        for rsi_period in rsi_periods:
            if quantile_stats_dict[rsi_period]:
                sorted_quantiles = sorted(quantile_stats_dict[rsi_period], key=lambda x: x['mean_return'], reverse=True)
                best_performers[rsi_period] = sorted_quantiles[0]

        if best_performers:
            best_period = max(best_performers.items(), key=lambda x: x[1]['mean_return'])
            print(f"   🏆 Best Overall: RSI_{best_period[0]} with return {best_period[1]['mean_return']:.4f}")
            print(f"      Composite range: [{best_period[1]['min_composite']:.1f}, {best_period[1]['max_composite']:.1f}]")

        print("\n   ⚠️ Risk Management:")
        print("   • Position sizing: Reduce size for extreme composite values")
        print("   • Stop loss: Set at 2-3 standard deviations from entry")
        print("   • Holding period: 1-3 days for momentum trades")
        print("   • Diversification: Spread across different sectors")
        print("   • Confirmation: Use with price action and other indicators")

    def run_analysis(self):
        """Run complete volume ratio * RSI composite analysis"""
        print("🎯 Starting volume ratio * RSI composite indicator analysis...")
        print("=" * 70)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Calculate composite positions
            position_data = self.calculate_volume_ratio_rsi_composite(scores_df)

            # 3. Calculate future returns (reuse from correlation analysis)
            from analyze_indicator_correlations import IndicatorCorrelationAnalyzer
            analyzer = IndicatorCorrelationAnalyzer()
            returns_data = analyzer.calculate_future_returns(scores_df, price_df, days_ahead=5)
            returns_data, discrete_col = analyzer.discretize_returns(returns_data, '5d_return', bin_size=0.1)

            # 4. Analyze distribution for each RSI period
            stats_dict = {}
            quantile_stats_dict = {}

            for rsi_period in [6, 10, 14]:
                print(f"\n   🔍 Analyzing RSI_{rsi_period} composite...")
                stats_dict[rsi_period] = self.analyze_composite_distribution(position_data, rsi_period)
                quantile_stats_dict[rsi_period] = self.analyze_composite_by_quantile(position_data, returns_data, rsi_period)

            # 5. Create visualization
            fig = self.create_composite_visualization(position_data, quantile_stats_dict)

            # 6. Suggest strategies
            self.suggest_composite_strategies(quantile_stats_dict)

            print("\n" + "=" * 70)
            print("🎉 Analysis completed!")

            return position_data, stats_dict, quantile_stats_dict

        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None


def main():
    """Main function"""
    analyzer = VolumeRatioRSIAnalyzer()
    position_data, stats_dict, quantile_stats_dict = analyzer.run_analysis()

    if position_data is not None:
        print("\n✅ Analysis completed successfully!")
        print("📁 Charts saved to: data/volume_ratio_rsi_composite_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()