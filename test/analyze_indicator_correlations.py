#!/usr/bin/env python3
"""
Analyze correlation between various indicators and future 5-day returns
"""

import sys
import os
sys.path.append('.')

import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib.pyplot as plt
# import seaborn as sns  # Optional, will use matplotlib if not available
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

class IndicatorCorrelationAnalyzer:
    """Analyzer for correlation between indicators and future returns"""

    def __init__(self):
        self.scores_dir = Path('data/scores')
        self.data_dir = Path('data')
        self.results = {}

        # Define indicator categories
        self.indicator_categories = {
            'fundamental': [
                'pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio', 'market_cap',
                'gross_profit_margin', 'net_profit_margin', 'current_ratio'
            ],
            'technical': [
                'sma_5', 'sma_10', 'sma_20', 'sma_60',
                'rsi_14', 'rsi_6', 'rsi_10',
                'macd_diff', 'macd_dea', 'macd_hist',
                'boll', 'boll_std', 'boll_up', 'boll_down',
                'stoch_k', 'stoch_d', 'stoch_j'
            ],
            'momentum': [
                'ma5_angle', 'ma10_angle', 'ma20_angle', 'ma60_angle',
                'pct_change', 'volatility_5d', 'volatility_10d', 'volatility_20d'
            ],
            'volume': [
                'volume_ratio', 'volume_ma5', 'volume_ma20', 'volume_trend',
                'net_inflow', 'main_net_inflow_5d', 'main_net_inflow_ratio'
            ],
            'risk': [
                'drawdown_from_high', 'distance_to_support', 'risk_reward_ratio',
                'score_drawdown', 'score_support', 'score_rr_ratio', 'risk_score'
            ],
            'scores': [
                'score_arrangement', 'score_slope', 'score_position', 'score_stability',
                'trend_score', 'score_volume_price', 'score_inflow', 'score_institution',
                'score_volume_trend', 'capital_score', 'score_rsi', 'score_macd',
                'score_kdj', 'score_boll', 'technical_score', 'composite_score'
            ]
        }

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

    def calculate_future_returns(self, scores_df, price_df, days_ahead=5):
        """Calculate future returns for each date"""
        print(f"📈 Calculating {days_ahead}-day future returns...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        # Get price data date range
        price_dates = price_df.select('date').unique().sort('date')
        price_dates_list = price_dates.to_series().to_list()

        print(f"   📅 Scoring dates: {len(score_dates_list)} unique dates")
        print(f"   📅 Price dates: {len(price_dates_list)} unique dates")

        all_returns = []

        for i, score_date in enumerate(score_dates_list):
            # Find future date
            try:
                date_idx = price_dates_list.index(str(score_date))
                future_idx = date_idx + days_ahead

                if future_idx >= len(price_dates_list):
                    continue

                future_date = price_dates_list[future_idx]

                # Get data for current date
                current_scores = scores_df.filter(pl.col('score_date') == score_date)

                # Get current prices from price data (not from scores data)
                current_prices = price_df.filter(pl.col('date') == str(score_date))

                # Get future prices
                future_prices = price_df.filter(pl.col('date') == future_date)

                if current_prices.is_empty() or future_prices.is_empty():
                    continue

                # Calculate returns using correct price data
                returns_data = current_scores.join(
                    current_prices.select(['order_book_id', 'close']).rename({'close': 'current_close'}),
                    on='order_book_id',
                    how='inner'
                ).join(
                    future_prices.select(['order_book_id', 'close']).rename({'close': 'future_close'}),
                    on='order_book_id',
                    how='inner'
                ).with_columns([
                    ((pl.col('future_close') - pl.col('current_close')) / pl.col('current_close')).alias(f'{days_ahead}d_return')
                ])

                if len(returns_data) > 0:
                    all_returns.append(returns_data)
                    print(f"   ✅ Processed {score_date} -> {future_date}: {len(returns_data)} stocks")

            except ValueError:
                continue

        if not all_returns:
            raise ValueError("No valid return data found")

        combined_returns = pl.concat(all_returns)
        valid_returns = combined_returns.filter(pl.col(f'{days_ahead}d_return').is_not_null())

        print(f"   ✅ Returns calculation completed: {len(valid_returns)} valid records")
        return valid_returns

    def discretize_returns(self, data, return_col='5d_return', bin_size=0.1):
        """将收益率按照指定区间进行离散化"""
        print(f"📊 Discretizing returns into {bin_size*100}% bins...")

        # 获取收益率数据
        returns = data.select(return_col).to_numpy().flatten()

        # 计算分位数边界
        bins = []
        for i in range(int(1/bin_size) + 1):
            percentile = i * bin_size * 100
            if percentile <= 100:
                bins.append(np.percentile(returns, percentile))

        # 确保边界是唯一的
        bins = sorted(list(set(bins)))

        # 为每个区间分配值（使用区间的序号）
        discretized_returns = np.digitize(returns, bins[:-1])  # digitize返回区间索引

        # 创建新的列
        discretized_col = f'{return_col}_discrete_{int(bin_size*100)}pct'
        data_with_discrete = data.with_columns([
            pl.Series(discretized_returns).alias(discretized_col)
        ])

        print(f"   📊 Created {len(bins)-1} bins: {bins}")
        print(f"   📊 Discrete values range: {discretized_returns.min()} to {discretized_returns.max()}")

        return data_with_discrete, discretized_col

    def discretize_bollinger_bands(self, data, bin_size=0.1):
        """将布林带指标离散化，基于价格相对位置"""
        print(f"📊 Discretizing Bollinger Band indicators into {bin_size*100}% bins...")

        # 计算价格相对布林带的位置
        # boll_position = (close - boll) / boll_std
        # 这将给出价格在布林带中的相对位置

        if 'boll' not in data.columns or 'boll_std' not in data.columns or 'close' not in data.columns:
            print("   ⚠️  Required columns (boll, boll_std, close) not found")
            return data, None

        # 计算价格相对布林带中轨的位置（标准化）
        boll_position_data = data.with_columns([
            ((pl.col('close') - pl.col('boll')) / pl.col('boll_std')).alias('boll_position')
        ])

        # 过滤有效数据
        valid_data = boll_position_data.filter(
            pl.col('boll_position').is_not_null() &
            pl.col('boll_position').is_finite() &
            (pl.col('boll_std') > 0) &  # 避免除零错误
            pl.col('boll_std').is_finite()
        )

        if len(valid_data) == 0:
            print("   ⚠️  No valid Bollinger Band position data")
            return data, None

        # 获取位置数据
        positions = valid_data.select('boll_position').to_numpy().flatten()

        # 计算分位数边界
        bins = []
        for i in range(int(1/bin_size) + 1):
            percentile = i * bin_size * 100
            if percentile <= 100:
                bins.append(np.percentile(positions, percentile))

        # 确保边界是唯一的
        bins = sorted(list(set(bins)))

        # 为每个区间分配值
        discretized_positions = np.digitize(positions, bins[:-1])

        # 创建新的列
        discretized_col = f'boll_position_discrete_{int(bin_size*100)}pct'
        data_with_discrete = valid_data.with_columns([
            pl.Series(discretized_positions).alias(discretized_col)
        ])

        print(f"   📊 Created {len(bins)-1} bins for Bollinger position: {bins}")
        print(f"   📊 Discrete values range: {discretized_positions.min()} to {discretized_positions.max()}")
        print(f"   📊 Valid records: {len(valid_data)}")

        return data_with_discrete, discretized_col

    def calculate_indicator_correlations(self, data, return_col='5d_return', indicator_filter=None):
        """Calculate correlations between indicators and returns"""
        print("📊 Calculating indicator correlations...")

        correlations = {}

        for category, indicators in self.indicator_categories.items():
            print(f"   🔍 Analyzing {category} indicators...")
            category_correlations = {}

            # Apply indicator filter if provided
            if indicator_filter:
                indicators = [ind for ind in indicators if ind in indicator_filter]

            for indicator in indicators:
                if indicator not in data.columns:
                    print(f"     ⚠️  Indicator {indicator} not found in data")
                    continue

                # Filter valid data - handle different data types
                try:
                    # Check if column is numeric
                    col_dtype = data.select(indicator).dtypes[0]
                    if str(col_dtype) in ['Utf8', 'str', 'object']:
                        # For string columns, just check for nulls
                        valid_data = data.filter(
                            pl.col(indicator).is_not_null() &
                            pl.col(return_col).is_not_null() &
                            pl.col(return_col).is_finite()
                        )
                    else:
                        # For numeric columns, check for finite values
                        valid_data = data.filter(
                            pl.col(indicator).is_not_null() &
                            pl.col(return_col).is_not_null() &
                            pl.col(indicator).is_finite() &
                            pl.col(return_col).is_finite()
                        )
                except Exception as e:
                    print(f"     ⚠️  Error filtering data for {indicator}: {e}")
                    continue

                if len(valid_data) < 100:  # Need minimum sample size
                    continue

                # Calculate correlations
                indicator_values = valid_data.select(indicator).to_numpy().flatten()
                return_values = valid_data.select(return_col).to_numpy().flatten()

                try:
                    pearson_corr, pearson_p = stats.pearsonr(indicator_values, return_values)
                    spearman_corr, spearman_p = stats.spearmanr(indicator_values, return_values)

                    category_correlations[indicator] = {
                        'pearson_corr': pearson_corr,
                        'pearson_p': pearson_p,
                        'spearman_corr': spearman_corr,
                        'spearman_p': spearman_p,
                        'sample_size': len(valid_data),
                        'category': category
                    }

                    print(f"     ✅ {indicator}: Pearson={pearson_corr:.4f}, p={pearson_p:.4f}, n={len(valid_data)}")

                except Exception as e:
                    print(f"     ❌ {indicator}: Error calculating correlation - {e}")
                    continue

            correlations[category] = category_correlations

        return correlations

    def create_correlation_summary(self, correlations):
        """Create summary of top correlations"""
        print("📋 Creating correlation summary...")

        all_results = []
        for category, indicators in correlations.items():
            for indicator, stats in indicators.items():
                all_results.append({
                    'indicator': indicator,
                    'category': category,
                    'pearson_corr': stats['pearson_corr'],
                    'pearson_p': stats['pearson_p'],
                    'spearman_corr': stats['spearman_corr'],
                    'spearman_p': stats['spearman_p'],
                    'sample_size': stats['sample_size'],
                    'abs_pearson': abs(stats['pearson_corr'])
                })

        # Sort by absolute Pearson correlation
        all_results.sort(key=lambda x: x['abs_pearson'], reverse=True)

        return all_results

    def create_visualization(self, correlations, top_n=20):
        """Create visualization of top correlations"""
        print("📊 Creating correlation visualization...")

        # Get top correlations
        all_results = []
        for category, indicators in correlations.items():
            for indicator, stats in indicators.items():
                all_results.append({
                    'indicator': indicator,
                    'category': category,
                    'correlation': stats['pearson_corr'],
                    'p_value': stats['pearson_p'],
                    'abs_corr': abs(stats['pearson_corr'])
                })

        all_results.sort(key=lambda x: x['abs_corr'], reverse=True)
        top_results = all_results[:top_n]

        # Create visualization
        fig, axes = plt.subplots(2, 1, figsize=(15, 12))
        fig.suptitle('Top Indicator Correlations with Discretized 5-Day Returns (10% bins)', fontsize=16, fontweight='bold')

        # Bar chart of correlations
        indicators = [r['indicator'] for r in top_results]
        correlations = [r['correlation'] for r in top_results]
        categories = [r['category'] for r in top_results]

        # Create color mapping for categories
        unique_categories = list(set(categories))
        # Use a simple color cycle
        color_cycle = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        colors = color_cycle[:len(unique_categories)]
        category_colors = dict(zip(unique_categories, colors))

        bar_colors = [category_colors[cat] for cat in categories]

        bars = axes[0].barh(range(len(indicators)), correlations, color=bar_colors)
        axes[0].set_yticks(range(len(indicators)))
        axes[0].set_yticklabels(indicators)
        axes[0].set_xlabel('Pearson Correlation')
        axes[0].set_title('Top Correlations by Absolute Value')
        axes[0].grid(True, alpha=0.3)

        # Add correlation values
        for i, (bar, corr) in enumerate(zip(bars, correlations)):
            axes[0].text(corr + (0.01 if corr >= 0 else -0.01), i,
                        f'{corr:.3f}', va='center',
                        ha='left' if corr >= 0 else 'right')

        # Scatter plot of correlation vs p-value
        correlations_plot = [r['correlation'] for r in top_results]
        p_values = [-np.log10(r['p_value']) for r in top_results]  # -log10 for better visualization

        scatter = axes[1].scatter(correlations_plot, p_values, c=bar_colors, s=100, alpha=0.7)

        # Add significance line
        axes[1].axhline(y=-np.log10(0.05), color='red', linestyle='--', alpha=0.7, label='p=0.05')
        axes[1].axhline(y=-np.log10(0.01), color='orange', linestyle='--', alpha=0.7, label='p=0.01')

        axes[1].set_xlabel('Pearson Correlation')
        axes[1].set_ylabel('-log10(p-value)')
        axes[1].set_title('Correlation vs Statistical Significance')
        axes[1].grid(True, alpha=0.3)
        axes[1].legend()

        # Add indicator labels with better positioning
        for i, indicator in enumerate(indicators):
            x_pos = correlations_plot[i]
            y_pos = p_values[i]

            # Adjust label position based on quadrant
            if x_pos >= 0 and y_pos >= -np.log10(0.05):
                xytext = (5, 5)  # Top-right
                ha = 'left'
            elif x_pos >= 0 and y_pos < -np.log10(0.05):
                xytext = (5, -5)  # Bottom-right
                ha = 'left'
            elif x_pos < 0 and y_pos >= -np.log10(0.05):
                xytext = (-5, 5)  # Top-left
                ha = 'right'
            else:
                xytext = (-5, -5)  # Bottom-left
                ha = 'right'

            axes[1].annotate(indicator, (x_pos, y_pos),
                           xytext=xytext, textcoords='offset points',
                           fontsize=9, ha=ha, va='center',
                           bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8),
                           arrowprops=dict(arrowstyle='-', color='gray', alpha=0.5, lw=0.5))

        plt.tight_layout()
        plt.savefig('data/indicator_correlation_analysis.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/indicator_correlation_analysis.png")

        return fig

    def print_top_correlations(self, summary, top_n=20):
        """Print top correlations"""
        print(f"\n🏆 Top {top_n} Indicator Correlations with Discretized 5-Day Returns (10% bins):")
        print("=" * 80)

        for i, result in enumerate(summary[:top_n], 1):
            indicator = result['indicator']
            category = result['category']
            pearson = result['pearson_corr']
            p_value = result['pearson_p']
            sample_size = result['sample_size']

            significance = ""
            if p_value < 0.001:
                significance = "***"
            elif p_value < 0.01:
                significance = "**"
            elif p_value < 0.05:
                significance = "*"

            print(f"{i:2d}. {indicator:<25} ({category:<12}) | "
                  f"Pearson: {pearson:>+7.4f}{significance:<3} | "
                  f"p-value: {p_value:<8.4f} | "
                  f"n: {sample_size:,}")

        print("\nSignificance levels: * p<0.05, ** p<0.01, *** p<0.001")

    def run_analysis(self):
        """Run complete analysis"""
        print("🎯 Starting indicator correlation analysis...")
        print("=" * 60)

        try:
            # 1. Load data
            scores_df, price_df = self.load_data()

            # 2. Calculate future returns
            returns_data = self.calculate_future_returns(scores_df, price_df, days_ahead=5)

            # 2.5. Discretize returns into 10% bins
            returns_data, discrete_col = self.discretize_returns(returns_data, '5d_return', bin_size=0.1)

            # 2.6. Discretize Bollinger Band position
            boll_data, boll_discrete_col = self.discretize_bollinger_bands(returns_data, bin_size=0.1)

            # 3. Calculate correlations with discretized returns
            correlations = self.calculate_indicator_correlations(returns_data, discrete_col)

            # 3.5. Calculate correlations for discretized Bollinger Band position
            if boll_discrete_col:
                print("\n📊 Analyzing discretized Bollinger Band correlations...")
                # Create a simple correlation calculation for the discretized Bollinger position
                if boll_discrete_col in boll_data.columns and discrete_col in boll_data.columns:
                    valid_boll_data = boll_data.filter(
                        pl.col(boll_discrete_col).is_not_null() &
                        pl.col(discrete_col).is_not_null()
                    )

                    if len(valid_boll_data) > 100:
                        boll_values = valid_boll_data.select(boll_discrete_col).to_numpy().flatten()
                        return_values = valid_boll_data.select(discrete_col).to_numpy().flatten()

                        try:
                            pearson_corr, pearson_p = stats.pearsonr(boll_values, return_values)
                            spearman_corr, spearman_p = stats.spearmanr(boll_values, return_values)

                            # Add to correlations
                            if 'discretized' not in correlations:
                                correlations['discretized'] = {}

                            correlations['discretized'][boll_discrete_col] = {
                                'pearson_corr': pearson_corr,
                                'pearson_p': pearson_p,
                                'spearman_corr': spearman_corr,
                                'spearman_p': spearman_p,
                                'sample_size': len(valid_boll_data),
                                'category': 'discretized'
                            }

                            print(f"     ✅ {boll_discrete_col}: Pearson={pearson_corr:.4f}, p={pearson_p:.4f}, n={len(valid_boll_data)}")

                        except Exception as e:
                            print(f"     ⚠️  Error calculating correlation for {boll_discrete_col}: {e}")
                    else:
                        print(f"     ⚠️  Insufficient data for {boll_discrete_col} correlation analysis")

            # 4. Create summary
            summary = self.create_correlation_summary(correlations)

            # 5. Print results
            self.print_top_correlations(summary)

            # 6. Create visualization
            fig = self.create_visualization(correlations)

            print("\n" + "=" * 60)
            print("🎉 Analysis completed!")

            return correlations, summary

        except Exception as e:
            print(f"❌ Error during analysis: {e}")
            import traceback
            traceback.print_exc()
            return None, None


def main():
    """Main function"""
    analyzer = IndicatorCorrelationAnalyzer()
    correlations, summary = analyzer.run_analysis()

    if correlations and summary:
        print("\n✅ Analysis completed successfully!")
        print("📁 Result chart saved to: data/indicator_correlation_analysis.png")
    else:
        print("\n❌ Analysis failed, please check data and configuration")


if __name__ == "__main__":
    main()