#!/usr/bin/env python3
"""
Analyze 10-day volume ratio indicator distribution and trading strategies
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

class VolumeRatio10DAnalyzer:
    """Analyzer for 10-day volume ratio indicator distribution and strategies"""

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

    def calculate_volume_ratio_10d(self, scores_df):
        """Calculate 10-day volume ratio for all stocks"""
        print("📊 Calculating 10-day volume ratio positions...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        all_positions = []

        for score_date in score_dates_list:
            # Get data for current date
            current_scores = scores_df.filter(pl.col('score_date') == score_date)

            # Calculate 10-day volume moving average if not exists
            if 'volume_ma10' not in current_scores.columns:
                print("   📊 Calculating volume_ma10...")
                # Group by order_book_id and calculate rolling volume_ma10
                current_scores = current_scores.sort(['order_book_id', 'score_date'])

                # Calculate volume_ma10 using rolling mean
                current_scores = current_scores.with_columns([
                    pl.col('volume').rolling_mean(window_size=10).alias('volume_ma10')
                ])

            # Calculate 10-day volume ratio: current volume / 10-day volume MA
            position_data = current_scores.with_columns([
                (pl.col('volume') / pl.col('volume_ma10')).alias('volume_ratio_10d')
            ])

            # Select relevant columns
            position_data = position_data.select([
                'order_book_id', 'volume_ratio_10d', 'volume', 'volume_ma10', 'close', 'date'
            ])

            # Filter valid data
            valid_position_data = position_data.filter(
                pl.col('volume_ratio_10d').is_not_null()
            ).filter(
                pl.col('volume_ratio_10d').is_finite()
            ).filter(
                pl.col('volume_ratio_10d') > 0  # Volume ratio should be positive
            ).filter(
                pl.col('volume_ma10').is_not_null()  # Ensure volume_ma10 is valid
            )

            if len(valid_position_data) > 0:
                all_positions.append(valid_position_data)
                print(f"   ✅ Processed {score_date}: {len(valid_position_data)} stocks")

        if not all_positions:
            raise ValueError("No valid 10-day volume ratio data found")

        combined_positions = pl.concat(all_positions)
        print(f"   ✅ 10-day volume ratio calculation completed: {len(combined_positions)} valid records")

        return combined_positions

    def analyze_volume_ratio_10d_distribution(self, position_data):
        """Analyze the distribution of 10-day volume ratio values"""
        print("📊 Analyzing 10-day volume ratio distribution...")

        volume_ratios = position_data.select('volume_ratio_10d').to_numpy().flatten()

        # Basic statistics
        stats_dict = {
            'mean': np.mean(volume_ratios),
            'median': np.median(volume_ratios),
            'std': np.std(volume_ratios),
            'min': np.min(volume_ratios),
            'max': np.max(volume_ratios),
            'skewness': stats.skew(volume_ratios),
            'kurtosis': stats.kurtosis(volume_ratios)
        }

        # Percentiles
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            stats_dict[f'p{p}'] = np.percentile(volume_ratios, p)

        print("   📈 10-Day Volume Ratio Statistics:")
        print(f"      Mean: {stats_dict['mean']:.4f}")
        print(f"      Median: {stats_dict['median']:.4f}")
        print(f"      Std: {stats_dict['std']:.4f}")
        print(f"      Range: [{stats_dict['min']:.4f}, {stats_dict['max']:.4f}]")
        print(f"      Skewness: {stats_dict['skewness']:.4f}")
        print(f"      Kurtosis: {stats_dict['kurtosis']:.4f}")

        return stats_dict

    def analyze_volume_ratio_10d_by_quantile(self, position_data, future_returns_data, n_quantiles=10):
        """Analyze returns by 10-day volume ratio quantiles"""
        print(f"📊 Analyzing returns by {n_quantiles} 10-day volume ratio quantiles...")

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

        volume_ratios = merged_data.select('volume_ratio_10d').to_numpy().flatten()
        returns = merged_data.select('5d_return').to_numpy().flatten()

        # Create quantiles
        quantile_labels = [f'Q{i+1}' for i in range(n_quantiles)]
        merged_data = merged_data.with_columns([
            pl.Series(np.digitize(volume_ratios, np.percentile(volume_ratios, np.linspace(0, 100, n_quantiles+1)[1:-1]))).alias('volume_ratio_10d_quantile')
        ])

        # Analyze each quantile
        quantile_stats = []
        for i in range(1, n_quantiles + 1):
            quantile_data = merged_data.filter(pl.col('volume_ratio_10d_quantile') == i)
            if len(quantile_data) > 0:
                q_returns = quantile_data.select('5d_return').to_numpy().flatten()
                q_volumes = quantile_data.select('volume_ratio_10d').to_numpy().flatten()

                stats_dict = {
                    'quantile': i,
                    'label': f'Q{i}',
                    'count': len(q_returns),
                    'mean_return': np.mean(q_returns),
                    'median_return': np.median(q_returns),
                    'std_return': np.std(q_returns),
                    'min_volume_ratio': np.min(q_volumes),
                    'max_volume_ratio': np.max(q_volumes),
                    'mean_volume_ratio': np.mean(q_volumes)
                }
                quantile_stats.append(stats_dict)

        # Print results
        print("   📈 10-Day Volume Ratio Quantile Analysis Results:")
        print("   " + "="*60)
        for stat in quantile_stats:
            print(f"   {stat['label']:>3}: Count={stat['count']:>5}, "
                  f"Mean Return={stat['mean_return']:>+7.4f}, "
                  f"Volume Ratio=[{stat['min_volume_ratio']:>+6.2f}, {stat['max_volume_ratio']:>+6.2f}]")

        return quantile_stats

    def create_volume_ratio_10d_visualization(self, position_data, quantile_stats):
        """Create comprehensive visualization of 10-day volume ratio distribution"""
        print("📊 Creating 10-day volume ratio distribution visualization...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('10-Day Volume Ratio Distribution Analysis', fontsize=16, fontweight='bold')

        volume_ratios = position_data.select('volume_ratio_10d').to_numpy().flatten()

        # 1. Histogram with KDE
        if HAS_SEABORN:
            sns.histplot(volume_ratios, bins=50, kde=True, ax=axes[0,0])
        else:
            axes[0,0].hist(volume_ratios, bins=50, alpha=0.7, density=True)
        axes[0,0].axvline(np.mean(volume_ratios), color='red', linestyle='--', label=f'Mean: {np.mean(volume_ratios):.2f}')
        axes[0,0].axvline(np.median(volume_ratios), color='green', linestyle='--', label=f'Median: {np.median(volume_ratios):.2f}')
        axes[0,0].axvline(1.0, color='orange', linestyle=':', label='Normal Volume (1.0)')
        axes[0,0].axvline(2.0, color='purple', linestyle=':', label='High Volume (2.0)')
        axes[0,0].set_xlabel('10-Day Volume Ratio')
        axes[0,0].set_ylabel('Density')
        axes[0,0].set_title('10-Day Volume Ratio Distribution')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)

        # 2. Quantile returns analysis
        if quantile_stats:
            quantiles = [stat['quantile'] for stat in quantile_stats]
            mean_returns = [stat['mean_return'] for stat in quantile_stats]
            quantile_labels = [stat['label'] for stat in quantile_stats]

            bars = axes[0,1].bar(quantiles, mean_returns, color='skyblue', alpha=0.7)
            axes[0,1].set_xlabel('10-Day Volume Ratio Quantile')
            axes[0,1].set_ylabel('Mean 5-Day Return')
            axes[0,1].set_title('Returns by 10-Day Volume Ratio Quantile')
            axes[0,1].set_xticks(quantiles)
            axes[0,1].set_xticklabels(quantile_labels)
            axes[0,1].grid(True, alpha=0.3)

            # Add value labels
            for bar, ret in zip(bars, mean_returns):
                axes[0,1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                             f'{ret:.4f}', ha='center', va='bottom', fontsize=9)

        # 3. Box plot of volume ratios
        if HAS_SEABORN:
            sns.boxplot(y=volume_ratios, ax=axes[1,0])
        else:
            axes[1,0].boxplot(volume_ratios)
        axes[1,0].set_ylabel('10-Day Volume Ratio')
        axes[1,0].set_title('10-Day Volume Ratio Box Plot')
        axes[1,0].grid(True, alpha=0.3)

        # 4. Q-Q plot for normality test
        stats.probplot(volume_ratios, dist="norm", plot=axes[1,1])
        axes[1,1].set_title('Q-Q Plot (Normality Test)')
        axes[1,1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('data/volume_ratio_10d_analysis.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/volume_ratio_10d_analysis.png")

        return fig

    def suggest_volume_ratio_10d_strategies(self, quantile_stats):
        """Suggest trading strategies based on 10-day volume ratio analysis"""
        print("\n🎯 10-Day Volume Ratio Trading Strategy Suggestions:")
        print("="*60)

        if not quantile_stats:
            print("   ⚠️  No quantile data available for strategy suggestions")
            return

        # Find best and worst performing quantiles
        sorted_quantiles = sorted(quantile_stats, key=lambda x: x['mean_return'], reverse=True)

        best_quantile = sorted_quantiles[0]
        worst_quantile = sorted_quantiles[-1]

        print("   📈 Best Performing Quantile:")
        print(f"      {best_quantile['label']}: Mean Return = {best_quantile['mean_return']:.4f}")
        print(f"      Volume Ratio Range: [{best_quantile['min_volume_ratio']:.2f}, {best_quantile['max_volume_ratio']:.2f}]")
        print(f"      Sample Size: {best_quantile['count']}")

        print("   📉 Worst Performing Quantile:")
        print(f"      {worst_quantile['label']}: Mean Return = {worst_quantile['mean_return']:.4f}")
        print(f"      Volume Ratio Range: [{worst_quantile['min_volume_ratio']:.2f}, {worst_quantile['max_volume_ratio']:.2f}]")
        print(f"      Sample Size: {worst_quantile['count']}")

        # 10-day volume ratio specific strategy suggestions
        print("\n   💡 10-Day Volume Ratio Strategy Recommendations:")

        # High volume strategy
        high_volume_quantiles = [q for q in quantile_stats if q['min_volume_ratio'] >= 1.5]
        if high_volume_quantiles:
            best_high_vol = max(high_volume_quantiles, key=lambda x: x['mean_return'])
            print(f"   📈 HIGH VOLUME Strategy: Focus on stocks with 10-day volume ratio in {best_high_vol['label']} range")
            print(f"      Volume Ratio Range: [{best_high_vol['min_volume_ratio']:.2f}, {best_high_vol['max_volume_ratio']:.2f}]")
            print(f"      Expected return: {best_high_vol['mean_return']:.2%} (5-day)")
            print("      Signal: Sustained high volume indicates strong institutional interest")

        # Low volume strategy
        low_volume_quantiles = [q for q in quantile_stats if q['max_volume_ratio'] <= 0.8]
        if low_volume_quantiles:
            best_low_vol = max(low_volume_quantiles, key=lambda x: x['mean_return'])
            print(f"   📉 LOW VOLUME Strategy: Consider stocks with 10-day volume ratio in {best_low_vol['label']} range")
            print(f"      Volume Ratio Range: [{best_low_vol['min_volume_ratio']:.2f}, {best_low_vol['max_volume_ratio']:.2f}]")
            print(f"      Expected return: {best_low_vol['mean_return']:.2%} (5-day)")
            print("      Signal: Low volume may indicate consolidation or lack of interest")

        # Volume breakout strategy
        extreme_high_quantiles = [q for q in quantile_stats if q['quantile'] >= 8]
        if extreme_high_quantiles:
            print("   🚀 VOLUME BREAKOUT Strategy:")
            print("      Buy stocks with extreme high 10-day volume (Q8-Q10)")
            print("      Look for volume ratio > 2.0 with price momentum")
            print("      Risk: May be overbought or false breakouts")

        # Mean reversion strategy
        extreme_low_quantiles = [q for q in quantile_stats if q['quantile'] <= 3]
        if extreme_low_quantiles:
            print("   🔄 MEAN REVERSION Strategy:")
            print("      Consider stocks with very low 10-day volume (Q1-Q3)")
            print("      May indicate oversold conditions or accumulation")
            print("      Risk: May continue to decline if fundamentals are weak")

        # Risk management
        print("\n   ⚠️ Risk Management:")
        print("   • Position sizing: Reduce size for extreme volume ratios")
        print("   • Stop loss: Set at 2-3 standard deviations from entry")
        print("   • Holding period: 3-5 days for sustained volume patterns")
        print("   • Diversification: Spread across different sectors")
        print("   • Confirmation: Use with price action and other indicators")

    def run_analysis(self):
        """Run complete 10-day volume ratio analysis"""
        print("🎯 Starting 10-day volume ratio indicator analysis...")
        print("=" * 60)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Calculate 10-day volume ratio positions
            position_data = self.calculate_volume_ratio_10d(scores_df)

            # 3. Calculate future returns (reuse from correlation analysis)
            from analyze_indicator_correlations import IndicatorCorrelationAnalyzer
            analyzer = IndicatorCorrelationAnalyzer()
            returns_data = analyzer.calculate_future_returns(scores_df, price_df, days_ahead=5)
            returns_data, discrete_col = analyzer.discretize_returns(returns_data, '5d_return', bin_size=0.1)

            # 4. Analyze distribution
            stats_dict = self.analyze_volume_ratio_10d_distribution(position_data)

            # 5. Analyze by quantiles
            quantile_stats = self.analyze_volume_ratio_10d_by_quantile(position_data, returns_data)

            # 6. Create visualization
            fig = self.create_volume_ratio_10d_visualization(position_data, quantile_stats)

            # 7. Suggest strategies
            self.suggest_volume_ratio_10d_strategies(quantile_stats)

            print("\n" + "=" * 60)
            print("🎉 Analysis completed!")

            return position_data, stats_dict, quantile_stats

        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None


def main():
    """Main function"""
    analyzer = VolumeRatio10DAnalyzer()
    position_data, stats_dict, quantile_stats = analyzer.run_analysis()

    if position_data is not None:
        print("\n✅ Analysis completed successfully!")
        print("📁 Charts saved to: data/volume_ratio_10d_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()