#!/usr/bin/env python3
"""
Analyze RSI indicator distribution and trading strategies
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

class RSIAnalyzer:
    """Analyzer for RSI indicator distribution and strategies"""

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

    def calculate_rsi_positions(self, scores_df):
        """Calculate RSI positions for all stocks"""
        print("📊 Calculating RSI positions...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        all_positions = []

        for score_date in score_dates_list:
            # Get data for current date
            current_scores = scores_df.filter(pl.col('score_date') == score_date)

            # Select RSI indicators
            position_data = current_scores.select([
                'order_book_id', 'rsi_14', 'rsi_6', 'rsi_10', 'close', 'date'
            ])

            # Filter valid data
            valid_position_data = position_data.filter(
                pl.col('rsi_14').is_not_null() &
                pl.col('rsi_14').is_finite() &
                pl.col('rsi_6').is_not_null() &
                pl.col('rsi_6').is_finite() &
                pl.col('rsi_10').is_not_null() &
                pl.col('rsi_10').is_finite() &
                (pl.col('rsi_14') >= 0) & (pl.col('rsi_14') <= 100) &
                (pl.col('rsi_6') >= 0) & (pl.col('rsi_6') <= 100) &
                (pl.col('rsi_10') >= 0) & (pl.col('rsi_10') <= 100)
            )

            if len(valid_position_data) > 0:
                all_positions.append(valid_position_data)
                print(f"   ✅ Processed {score_date}: {len(valid_position_data)} stocks")

        if not all_positions:
            raise ValueError("No valid RSI position data found")

        combined_positions = pl.concat(all_positions)
        print(f"   ✅ RSI position calculation completed: {len(combined_positions)} valid records")

        return combined_positions

    def analyze_rsi_distribution(self, position_data, rsi_period='rsi_14'):
        """Analyze the distribution of RSI values"""
        print(f"📊 Analyzing {rsi_period} distribution...")

        rsi_values = position_data.select(rsi_period).to_numpy().flatten()

        # Basic statistics
        stats_dict = {
            'mean': np.mean(rsi_values),
            'median': np.median(rsi_values),
            'std': np.std(rsi_values),
            'min': np.min(rsi_values),
            'max': np.max(rsi_values),
            'skewness': stats.skew(rsi_values),
            'kurtosis': stats.kurtosis(rsi_values)
        }

        # Percentiles
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            stats_dict[f'p{p}'] = np.percentile(rsi_values, p)

        print(f"   📈 {rsi_period.upper()} Statistics:")
        print(f"      Mean: {stats_dict['mean']:.4f}")
        print(f"      Median: {stats_dict['median']:.4f}")
        print(f"      Std: {stats_dict['std']:.4f}")
        print(f"      Range: [{stats_dict['min']:.4f}, {stats_dict['max']:.4f}]")
        print(f"      Skewness: {stats_dict['skewness']:.4f}")
        print(f"      Kurtosis: {stats_dict['kurtosis']:.4f}")

        return stats_dict

    def analyze_rsi_by_quantile(self, position_data, future_returns_data, rsi_period='rsi_14', n_quantiles=10):
        """Analyze returns by RSI quantiles"""
        print(f"📊 Analyzing returns by {n_quantiles} {rsi_period} quantiles...")

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

        rsi_values = merged_data.select(rsi_period).to_numpy().flatten()
        returns = merged_data.select('5d_return').to_numpy().flatten()

        # Create quantiles
        quantile_labels = [f'Q{i+1}' for i in range(n_quantiles)]
        merged_data = merged_data.with_columns([
            pl.Series(np.digitize(rsi_values, np.percentile(rsi_values, np.linspace(0, 100, n_quantiles+1)[1:-1]))).alias(f'{rsi_period}_quantile')
        ])

        # Analyze each quantile
        quantile_stats = []
        for i in range(1, n_quantiles + 1):
            quantile_data = merged_data.filter(pl.col(f'{rsi_period}_quantile') == i)
            if len(quantile_data) > 0:
                q_returns = quantile_data.select('5d_return').to_numpy().flatten()
                q_rsi = quantile_data.select(rsi_period).to_numpy().flatten()

                stats_dict = {
                    'quantile': i,
                    'label': f'Q{i}',
                    'count': len(q_returns),
                    'mean_return': np.mean(q_returns),
                    'median_return': np.median(q_returns),
                    'std_return': np.std(q_returns),
                    'min_rsi': np.min(q_rsi),
                    'max_rsi': np.max(q_rsi),
                    'mean_rsi': np.mean(q_rsi)
                }
                quantile_stats.append(stats_dict)

        # Print results
        print(f"   📈 {rsi_period.upper()} Quantile Analysis Results:")
        print("   " + "="*60)
        for stat in quantile_stats:
            print(f"   {stat['label']:>3}: Count={stat['count']:>5}, "
                  f"Mean Return={stat['mean_return']:>+7.4f}, "
                  f"RSI=[{stat['min_rsi']:>+6.2f}, {stat['max_rsi']:>+6.2f}]")

        return quantile_stats

    def create_rsi_visualization(self, position_data, quantile_stats, rsi_period='rsi_14'):
        """Create comprehensive visualization of RSI distribution"""
        print("📊 Creating RSI distribution visualization...")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(f'RSI ({rsi_period}) Distribution Analysis', fontsize=16, fontweight='bold')

        rsi_values = position_data.select(rsi_period).to_numpy().flatten()

        # 1. Histogram with KDE
        if HAS_SEABORN:
            sns.histplot(rsi_values, bins=50, kde=True, ax=axes[0,0])
        else:
            axes[0,0].hist(rsi_values, bins=50, alpha=0.7, density=True)
        axes[0,0].axvline(np.mean(rsi_values), color='red', linestyle='--', label=f'Mean: {np.mean(rsi_values):.2f}')
        axes[0,0].axvline(np.median(rsi_values), color='green', linestyle='--', label=f'Median: {np.median(rsi_values):.2f}')
        axes[0,0].axvline(30, color='orange', linestyle=':', label='Oversold (30)')
        axes[0,0].axvline(70, color='orange', linestyle=':', label='Overbought (70)')
        axes[0,0].set_xlabel(f'RSI ({rsi_period})')
        axes[0,0].set_ylabel('Density')
        axes[0,0].set_title('RSI Distribution')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)

        # 2. Quantile returns analysis
        if quantile_stats:
            quantiles = [stat['quantile'] for stat in quantile_stats]
            mean_returns = [stat['mean_return'] for stat in quantile_stats]
            quantile_labels = [stat['label'] for stat in quantile_stats]

            bars = axes[0,1].bar(quantiles, mean_returns, color='skyblue', alpha=0.7)
            axes[0,1].set_xlabel('RSI Quantile')
            axes[0,1].set_ylabel('Mean 5-Day Return')
            axes[0,1].set_title('Returns by RSI Quantile')
            axes[0,1].set_xticks(quantiles)
            axes[0,1].set_xticklabels(quantile_labels)
            axes[0,1].grid(True, alpha=0.3)

            # Add value labels
            for bar, ret in zip(bars, mean_returns):
                axes[0,1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                             f'{ret:.4f}', ha='center', va='bottom', fontsize=9)

        # 3. Box plot of RSI values
        if HAS_SEABORN:
            sns.boxplot(y=rsi_values, ax=axes[1,0])
        else:
            axes[1,0].boxplot(rsi_values)
        axes[1,0].set_ylabel(f'RSI ({rsi_period})')
        axes[1,0].set_title('RSI Box Plot')
        axes[1,0].grid(True, alpha=0.3)

        # 4. Q-Q plot for normality test
        stats.probplot(rsi_values, dist="norm", plot=axes[1,1])
        axes[1,1].set_title('Q-Q Plot (Normality Test)')
        axes[1,1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(f'data/rsi_{rsi_period}_analysis.png', dpi=300, bbox_inches='tight')
        print(f"   ✅ Chart saved to: data/rsi_{rsi_period}_analysis.png")

        return fig

    def suggest_rsi_strategies(self, quantile_stats, rsi_period='rsi_14'):
        """Suggest trading strategies based on RSI analysis"""
        print(f"\n🎯 RSI ({rsi_period.upper()}) Trading Strategy Suggestions:")
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
        print(f"      RSI Range: [{best_quantile['min_rsi']:.2f}, {best_quantile['max_rsi']:.2f}]")
        print(f"      Sample Size: {best_quantile['count']}")

        print("   📉 Worst Performing Quantile:")
        print(f"      {worst_quantile['label']}: Mean Return = {worst_quantile['mean_return']:.4f}")
        print(f"      RSI Range: [{worst_quantile['min_rsi']:.2f}, {worst_quantile['max_rsi']:.2f}]")
        print(f"      Sample Size: {worst_quantile['count']}")

        # RSI-specific strategy suggestions
        print("\n   💡 RSI Strategy Recommendations:")

        # Oversold strategy (RSI < 30)
        oversold_quantiles = [q for q in quantile_stats if q['max_rsi'] <= 35]
        if oversold_quantiles:
            best_oversold = max(oversold_quantiles, key=lambda x: x['mean_return'])
            print(f"   🟢 OVERSOLD Strategy: Buy stocks with RSI in {best_oversold['label']} range")
            print(f"      RSI Range: [{best_oversold['min_rsi']:.2f}, {best_oversold['max_rsi']:.2f}]")
            print(f"      Expected return: {best_oversold['mean_return']:.2%} (5-day)")
            print("      Signal: RSI indicates oversold condition")

        # Overbought strategy (RSI > 70)
        overbought_quantiles = [q for q in quantile_stats if q['min_rsi'] >= 65]
        if overbought_quantiles:
            worst_overbought = min(overbought_quantiles, key=lambda x: x['mean_return'])
            print(f"   🔴 OVERBOUGHT Strategy: Sell/short stocks with RSI in {worst_overbought['label']} range")
            print(f"      RSI Range: [{worst_overbought['min_rsi']:.2f}, {worst_overbought['max_rsi']:.2f}]")
            print(f"      Expected return: {abs(worst_overbought['mean_return']):.2%} profit (5-day)")
            print("      Signal: RSI indicates overbought condition")

        # Mean reversion strategy
        extreme_quantiles = [q for q in quantile_stats if q['quantile'] in [1, len(quantile_stats)]]
        if len(extreme_quantiles) >= 2:
            print("   🔄 MEAN REVERSION Strategy:")
            print("      Buy extreme low RSI (Q1), Sell extreme high RSI (Q10)")
            print("      Expected: RSI tends to revert to 50 over time")

        # Risk management
        print("\n   ⚠️ Risk Management:")
        print("   • Position sizing: Reduce size for extreme RSI values")
        print("   • Stop loss: Set at 2-3 standard deviations from entry")
        print("   • Holding period: 3-5 days based on RSI mean reversion")
        print("   • Diversification: Spread across multiple stocks/industries")
        print("   • Confirmation: Use with other indicators (volume, trend)")

    def run_analysis(self):
        """Run complete RSI analysis for all periods"""
        print("🎯 Starting RSI indicator analysis...")
        print("=" * 60)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Calculate RSI positions
            position_data = self.calculate_rsi_positions(scores_df)

            # 3. Calculate future returns (reuse from correlation analysis)
            from analyze_indicator_correlations import IndicatorCorrelationAnalyzer
            analyzer = IndicatorCorrelationAnalyzer()
            returns_data = analyzer.calculate_future_returns(scores_df, price_df, days_ahead=5)
            returns_data, discrete_col = analyzer.discretize_returns(returns_data, '5d_return', bin_size=0.1)

            # 4. Analyze each RSI period
            rsi_periods = ['rsi_6', 'rsi_10', 'rsi_14']

            for rsi_period in rsi_periods:
                print(f"\n🔍 Analyzing {rsi_period.upper()}...")
                print("-" * 40)

                # Distribution analysis
                stats_dict = self.analyze_rsi_distribution(position_data, rsi_period)

                # Quantile analysis
                quantile_stats = self.analyze_rsi_by_quantile(position_data, returns_data, rsi_period)

                # Visualization
                fig = self.create_rsi_visualization(position_data, quantile_stats, rsi_period)

                # Strategy suggestions
                self.suggest_rsi_strategies(quantile_stats, rsi_period)

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
    analyzer = RSIAnalyzer()
    position_data, stats_dict, quantile_stats = analyzer.run_analysis()

    if position_data is not None:
        print("\n✅ Analysis completed successfully!")
        print("📁 Charts saved to: data/rsi_*_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()