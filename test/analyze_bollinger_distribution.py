#!/usr/bin/env python3
"""
Analyze Bollinger Band position distribution and trading strategies
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

class BollingerBandAnalyzer:
    """Analyzer for Bollinger Band position distribution and strategies"""

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

    def calculate_bollinger_positions(self, scores_df, price_df):
        """Calculate Bollinger Band positions for all stocks"""
        print("📊 Calculating Bollinger Band positions...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        all_positions = []

        for score_date in score_dates_list:
            # Get data for current date
            current_scores = scores_df.filter(pl.col('score_date') == score_date)

            # Get price data for current date
            current_prices = price_df.filter(pl.col('date') == pl.lit(str(score_date)))

            if current_prices.is_empty():
                continue

            # Join data - get Bollinger indicators from scores data
            position_data = current_scores.select([
                'order_book_id', 'close', 'boll', 'boll_std', 'date'
            ])

            # Calculate Bollinger position
            position_data = position_data.with_columns([
                ((pl.col('close') - pl.col('boll')) / pl.col('boll_std')).alias('boll_position'),
                pl.lit(score_date).alias('date')
            ])

            # Filter valid data
            valid_position_data = position_data.filter(
                pl.col('boll_position').is_not_null() &
                pl.col('boll_position').is_finite() &
                (pl.col('boll_std') > 0) &
                pl.col('boll_std').is_finite()
            )

            if len(valid_position_data) > 0:
                all_positions.append(valid_position_data)
                print(f"   ✅ Processed {score_date}: {len(valid_position_data)} stocks")

        if not all_positions:
            raise ValueError("No valid Bollinger position data found")

        combined_positions = pl.concat(all_positions)
        print(f"   ✅ Bollinger position calculation completed: {len(combined_positions)} valid records")

        return combined_positions

    def analyze_position_distribution(self, position_data):
        """Analyze the distribution of Bollinger Band positions"""
        print("📊 Analyzing position distribution...")

        positions = position_data.select('boll_position').to_numpy().flatten()

        # Basic statistics
        stats_dict = {
            'mean': np.mean(positions),
            'median': np.median(positions),
            'std': np.std(positions),
            'min': np.min(positions),
            'max': np.max(positions),
            'skewness': stats.skew(positions),
            'kurtosis': stats.kurtosis(positions)
        }

        # Percentiles
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            stats_dict[f'p{p}'] = np.percentile(positions, p)

        print("   📈 Position Statistics:")
        print(f"      Mean: {stats_dict['mean']:.4f}")
        print(f"      Median: {stats_dict['median']:.4f}")
        print(f"      Std: {stats_dict['std']:.4f}")
        print(f"      Range: [{stats_dict['min']:.4f}, {stats_dict['max']:.4f}]")
        print(f"      Skewness: {stats_dict['skewness']:.4f}")
        print(f"      Kurtosis: {stats_dict['kurtosis']:.4f}")

        return stats_dict

    def analyze_position_by_quantile(self, position_data, future_returns_data, n_quantiles=10):
        """Analyze returns by Bollinger position quantiles"""
        print(f"📊 Analyzing returns by {n_quantiles} position quantiles...")

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

        positions = merged_data.select('boll_position').to_numpy().flatten()
        returns = merged_data.select('5d_return').to_numpy().flatten()

        # Create quantiles
        quantile_labels = [f'Q{i+1}' for i in range(n_quantiles)]
        merged_data = merged_data.with_columns([
            pl.Series(np.digitize(positions, np.percentile(positions, np.linspace(0, 100, n_quantiles+1)[1:-1]))).alias('position_quantile')
        ])

        # Analyze each quantile
        quantile_stats = []
        for i in range(1, n_quantiles + 1):
            quantile_data = merged_data.filter(pl.col('position_quantile') == i)
            if len(quantile_data) > 0:
                q_returns = quantile_data.select('5d_return').to_numpy().flatten()
                q_positions = quantile_data.select('boll_position').to_numpy().flatten()

                stats_dict = {
                    'quantile': i,
                    'label': f'Q{i}',
                    'count': len(q_returns),
                    'mean_return': np.mean(q_returns),
                    'median_return': np.median(q_returns),
                    'std_return': np.std(q_returns),
                    'min_position': np.min(q_positions),
                    'max_position': np.max(q_positions),
                    'mean_position': np.mean(q_positions)
                }
                quantile_stats.append(stats_dict)

        # Print results
        print("   📈 Quantile Analysis Results:")
        print("   " + "="*60)
        for stat in quantile_stats:
            print(f"   {stat['label']:>3}: Count={stat['count']:>5}, "
                  f"Mean Return={stat['mean_return']:>+7.4f}, "
                  f"Position=[{stat['min_position']:>+6.3f}, {stat['max_position']:>+6.3f}]")

        return quantile_stats

    def create_distribution_visualization(self, position_data, quantile_stats):
        """Create comprehensive visualization of Bollinger position distribution"""
        print("📊 Creating distribution visualization...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Bollinger Band Position Distribution Analysis', fontsize=16, fontweight='bold')

        positions = position_data.select('boll_position').to_numpy().flatten()

        # 1. Histogram with KDE
        if HAS_SEABORN:
            sns.histplot(positions, bins=50, kde=True, ax=axes[0,0])
        else:
            axes[0,0].hist(positions, bins=50, alpha=0.7, density=True)
        axes[0,0].axvline(np.mean(positions), color='red', linestyle='--', label=f'Mean: {np.mean(positions):.3f}')
        axes[0,0].axvline(np.median(positions), color='green', linestyle='--', label=f'Median: {np.median(positions):.3f}')
        axes[0,0].set_xlabel('Bollinger Position')
        axes[0,0].set_ylabel('Density')
        axes[0,0].set_title('Position Distribution')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)

        # 2. Quantile returns analysis
        if quantile_stats:
            quantiles = [stat['quantile'] for stat in quantile_stats]
            mean_returns = [stat['mean_return'] for stat in quantile_stats]
            quantile_labels = [stat['label'] for stat in quantile_stats]

            bars = axes[0,1].bar(quantiles, mean_returns, color='skyblue', alpha=0.7)
            axes[0,1].set_xlabel('Position Quantile')
            axes[0,1].set_ylabel('Mean 5-Day Return')
            axes[0,1].set_title('Returns by Position Quantile')
            axes[0,1].set_xticks(quantiles)
            axes[0,1].set_xticklabels(quantile_labels)
            axes[0,1].grid(True, alpha=0.3)

            # Add value labels
            for bar, ret in zip(bars, mean_returns):
                axes[0,1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                             f'{ret:.4f}', ha='center', va='bottom', fontsize=9)

        # 3. Box plot of positions
        if HAS_SEABORN:
            sns.boxplot(y=positions, ax=axes[1,0])
        else:
            axes[1,0].boxplot(positions)
        axes[1,0].set_ylabel('Bollinger Position')
        axes[1,0].set_title('Position Box Plot')
        axes[1,0].grid(True, alpha=0.3)

        # 4. Q-Q plot for normality test
        stats.probplot(positions, dist="norm", plot=axes[1,1])
        axes[1,1].set_title('Q-Q Plot (Normality Test)')
        axes[1,1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('data/bollinger_position_analysis.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/bollinger_position_analysis.png")

        return fig

    def suggest_trading_strategies(self, quantile_stats):
        """Suggest trading strategies based on analysis"""
        print("\n🎯 Trading Strategy Suggestions:")
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
        print(f"      Position Range: [{best_quantile['min_position']:.3f}, {best_quantile['max_position']:.3f}]")
        print(f"      Sample Size: {best_quantile['count']}")

        print("   📉 Worst Performing Quantile:")
        print(f"      {worst_quantile['label']}: Mean Return = {worst_quantile['mean_return']:.4f}")
        print(f"      Position Range: [{worst_quantile['min_position']:.3f}, {worst_quantile['max_position']:.3f}]")
        print(f"      Sample Size: {worst_quantile['count']}")

        # Strategy suggestions
        print("\n   💡 Strategy Recommendations:")

        # Long strategy
        if best_quantile['mean_return'] > 0:
            print(f"   🟢 LONG Strategy: Buy stocks with Bollinger position in {best_quantile['label']} range")
            print(f"      Expected return: {best_quantile['mean_return']:.2%} (5-day)")
            print("      Risk: Monitor for position normalization (mean reversion)")

        # Short strategy
        if worst_quantile['mean_return'] < 0:
            print(f"   🔴 SHORT Strategy: Sell/short stocks with Bollinger position in {worst_quantile['label']} range")
            print(f"      Expected return: {abs(worst_quantile['mean_return']):.2%} profit (5-day)")
            print("      Risk: Extreme positions may lead to sharp reversals")

        # Mean reversion strategy
        extreme_quantiles = [q for q in quantile_stats if q['quantile'] in [1, len(quantile_stats)]]
        if len(extreme_quantiles) >= 2:
            print("   🔄 MEAN REVERSION Strategy:")
            print("      Buy extreme low positions (Q1), Sell extreme high positions (Q10)")
            print("      Expected: Positions tend to revert to mean over time")

        # Risk management
        print("\n   ⚠️ Risk Management:")
        print("   • Position sizing: Reduce size for extreme quantiles")
        print("   • Stop loss: Set at 2-3 standard deviations from entry")
        print("   • Holding period: 3-5 days based on mean reversion time")
        print("   • Diversification: Spread across multiple stocks/industries")

    def run_analysis(self):
        """Run complete Bollinger Band analysis"""
        print("🎯 Starting Bollinger Band position analysis...")
        print("=" * 60)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Calculate Bollinger positions
            position_data = self.calculate_bollinger_positions(scores_df, price_df)

            # 3. Calculate future returns (reuse from correlation analysis)
            from analyze_indicator_correlations import IndicatorCorrelationAnalyzer
            analyzer = IndicatorCorrelationAnalyzer()
            returns_data = analyzer.calculate_future_returns(scores_df, price_df, days_ahead=5)
            returns_data, discrete_col = analyzer.discretize_returns(returns_data, '5d_return', bin_size=0.1)

            # 4. Analyze distribution
            stats_dict = self.analyze_position_distribution(position_data)

            # 5. Analyze by quantiles
            quantile_stats = self.analyze_position_by_quantile(position_data, returns_data)

            # 6. Create visualization
            fig = self.create_distribution_visualization(position_data, quantile_stats)

            # 7. Suggest strategies
            self.suggest_trading_strategies(quantile_stats)

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
    analyzer = BollingerBandAnalyzer()
    position_data, stats_dict, quantile_stats = analyzer.run_analysis()

    if position_data is not None:
        print("\n✅ Analysis completed successfully!")
        print("📁 Charts saved to: data/bollinger_position_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()