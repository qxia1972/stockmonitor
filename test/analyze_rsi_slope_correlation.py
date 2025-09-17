#!/usr/bin/env python3
"""
Analyze correlation between RSI indicators and 5-day future returns
for stocks with 10-day moving average slope > 1%
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

class RSISlopeCorrelationAnalyzer:
    """Analyzer for RSI correlation with future returns for stocks with steep MA slope"""

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
        price_df = price_df.with_columns([
            pl.col('date').cast(pl.Date).alias('date')
        ])

        print(f"   ✅ Data loaded: {len(scores_df)} scoring records, {len(price_df)} price records")
        return scores_df, price_df

    def filter_steep_slope_stocks(self, scores_df):
        """Filter stocks with 10-day MA slope > 1%"""
        print("📊 Filtering stocks with 10-day MA slope > 1%...")

        # First, let's see what dates have non-zero ma10_angle values
        non_zero_dates = scores_df.filter(
            pl.col('ma10_angle').is_not_null() &
            pl.col('ma10_angle').is_finite() &
            (pl.col('ma10_angle') != 0.0)
        ).select('score_date').unique().sort('score_date')

        if len(non_zero_dates) > 0:
            print(f"   📅 Dates with non-zero ma10_angle: {non_zero_dates.head(5).to_series().to_list()}")

            # Filter to dates that have meaningful slope data
            recent_date = non_zero_dates.tail(1).item()
            scores_df = scores_df.filter(pl.col('score_date') >= recent_date - timedelta(days=30))

        # Filter stocks with ma10_angle > 1%
        steep_slope_data = scores_df.filter(
            pl.col('ma10_angle').is_not_null() &
            pl.col('ma10_angle').is_finite() &
            (pl.col('ma10_angle') > 1.0)  # 10日线斜率大于1%
        )

        print(f"   ✅ Found {len(steep_slope_data)} records with steep slope (>1%)")

        # If no steep slope stocks found, try with lower threshold
        if len(steep_slope_data) == 0:
            print("   ⚠️  No stocks found with ma10_angle > 1.0, trying lower threshold...")
            steep_slope_data = scores_df.filter(
                pl.col('ma10_angle').is_not_null() &
                pl.col('ma10_angle').is_finite() &
                (pl.col('ma10_angle') > 0.5)  # Try 0.5% threshold
            )
            print(f"   ✅ Found {len(steep_slope_data)} records with slope >0.5%")

        # If still no data, use all stocks with valid slope data
        if len(steep_slope_data) == 0:
            print("   ⚠️  Still no stocks found, using all stocks with valid slope data...")
            steep_slope_data = scores_df.filter(
                pl.col('ma10_angle').is_not_null() &
                pl.col('ma10_angle').is_finite()
            )
            print(f"   ✅ Found {len(steep_slope_data)} records with valid slope data")

        return steep_slope_data

    def calculate_future_returns(self, scores_df, price_df):
        """Calculate 3-day and 5-day future returns for filtered stocks"""
        print("📈 Calculating 3-day and 5-day future returns...")

        # Get unique dates from scoring data
        score_dates = scores_df.select('score_date').unique().sort('score_date')
        score_dates_list = score_dates.to_series().to_list()

        all_returns = []

        for i, score_date in enumerate(score_dates_list):
            # Get data for current date
            current_scores = scores_df.filter(pl.col('score_date') == score_date)

            if len(current_scores) == 0:
                continue

            # Calculate future date (use longer window for N=1 to 10)
            future_date = score_date + timedelta(days=30)  # Use 30 days to ensure we have enough data for N=10

            # Get future prices
            future_prices = price_df.filter(
                (pl.col('date').cast(pl.Date) >= score_date) &
                (pl.col('date').cast(pl.Date) <= future_date)
            )

            if len(future_prices) == 0:
                continue

            # Calculate returns for each stock
            for stock_data in current_scores.iter_rows(named=True):
                order_book_id = stock_data['order_book_id']
                current_close = stock_data['close']

                # Get future price for this stock
                stock_future_prices = future_prices.filter(
                    pl.col('order_book_id') == order_book_id
                ).sort('date')

                if len(stock_future_prices) >= 10:  # Need at least 10 days for N=1 to 10
                    # Get price 3 days later
                    future_price_3d_row = stock_future_prices.row(2)  # 0-indexed, 2 = 3rd day
                    future_close_3d = future_price_3d_row[5]  # close column index

                    # Get price 5 days later
                    future_price_5d_row = stock_future_prices.row(4)  # 0-indexed, 4 = 5th day
                    future_close_5d = future_price_5d_row[5]  # close column index

                    if (future_close_3d and future_close_5d and current_close and
                        current_close > 0):
                        future_return_3d = (future_close_3d - current_close) / current_close
                        future_return_5d = (future_close_5d - current_close) / current_close

                        # Create record for backward compatibility (3d and 5d)
                        return_record = {
                            'order_book_id': order_book_id,
                            'score_date': score_date,
                            'current_close': current_close,
                            'future_close_3d': future_close_3d,
                            'future_close_5d': future_close_5d,
                            '3d_return': future_return_3d,
                            '5d_return': future_return_5d,
                            'rsi_6': stock_data.get('rsi_6'),
                            'rsi_10': stock_data.get('rsi_10'),
                            'rsi_14': stock_data.get('rsi_14'),
                            'ma10_angle': stock_data.get('ma10_angle'),
                            'volume_ratio': stock_data.get('volume_ratio')
                        }
                        all_returns.append(return_record)

                        # Create records for N=1 to 10 for 3D visualization
                        for n in range(1, 11):  # N=1 to 10
                            if len(stock_future_prices) > n:
                                future_price_row = stock_future_prices.row(n)  # N days later
                                future_close = future_price_row[5]  # close column index

                                if future_close and current_close > 0:
                                    future_return = (future_close - current_close) / current_close

                                    n_return_record = {
                                        'order_book_id': order_book_id,
                                        'score_date': score_date,
                                        'current_close': current_close,
                                        'future_close': future_close,
                                        'n_days': n,
                                        'return': future_return,
                                        'rsi_6': stock_data.get('rsi_6'),
                                        'rsi_10': stock_data.get('rsi_10'),
                                        'rsi_14': stock_data.get('rsi_14'),
                                        'ma10_angle': stock_data.get('ma10_angle'),
                                        'volume_ratio': stock_data.get('volume_ratio')
                                    }
                                    all_returns.append(n_return_record)

            if (i + 1) % 10 == 0:
                print(f"   ✅ Processed {i+1}/{len(score_dates_list)} dates: {len(all_returns)} valid records")

        returns_df = pl.DataFrame(all_returns)
        print(f"   ✅ Future returns calculation completed: {len(returns_df)} valid records")
        if len(returns_df) > 0:
            print(f"   📊 Sample return record: {returns_df.head(1)}")
        return returns_df

    def analyze_rsi_correlations(self, returns_df):
        """Analyze correlations between RSI indicators and future returns (3-day and 5-day)"""
        print("📊 Analyzing RSI correlations with 3-day and 5-day future returns...")

        results = {}

        # Filter out invalid data
        valid_data = returns_df.filter(
            pl.col('3d_return').is_not_null() &
            pl.col('3d_return').is_finite() &
            pl.col('5d_return').is_not_null() &
            pl.col('5d_return').is_finite() &
            pl.col('rsi_14').is_not_null() &
            pl.col('rsi_14').is_finite()
        )

        print(f"   📊 Valid data points: {len(valid_data)}")

        # Analyze each RSI indicator
        rsi_indicators = ['rsi_14']

        for rsi_col in rsi_indicators:
            print(f"   🔍 Analyzing {rsi_col.upper()}...")

            rsi_values = valid_data.select(rsi_col).to_numpy().flatten()
            returns_3d = valid_data.select('3d_return').to_numpy().flatten()
            returns_5d = valid_data.select('5d_return').to_numpy().flatten()

            # Calculate correlations for both time periods
            corr_3d, p_value_3d = stats.pearsonr(rsi_values, returns_3d)
            corr_5d, p_value_5d = stats.pearsonr(rsi_values, returns_5d)
            spearman_corr_3d, spearman_p_3d = stats.spearmanr(rsi_values, returns_3d)
            spearman_corr_5d, spearman_p_5d = stats.spearmanr(rsi_values, returns_5d)

            # Calculate RSI quantiles and returns for both periods
            quantiles = [0.2, 0.4, 0.6, 0.8]
            rsi_quantiles = np.quantile(rsi_values, quantiles)

            quantile_returns_3d = []
            quantile_returns_5d = []
            for i in range(len(quantiles) + 1):
                if i == 0:
                    mask = rsi_values <= rsi_quantiles[0]
                    label = f"Q1 (≤{rsi_quantiles[0]:.1f})"
                elif i == len(quantiles):
                    mask = rsi_values > rsi_quantiles[-1]
                    label = f"Q5 (>{rsi_quantiles[-1]:.1f})"
                else:
                    mask = (rsi_values > rsi_quantiles[i-1]) & (rsi_values <= rsi_quantiles[i])
                    label = f"Q{i+1} ({rsi_quantiles[i-1]:.1f}-{rsi_quantiles[i]:.1f})"

                q_returns_3d = returns_3d[mask]
                q_returns_5d = returns_5d[mask]
                if len(q_returns_3d) > 0 and len(q_returns_5d) > 0:
                    quantile_returns_3d.append({
                        'quantile': label,
                        'count': len(q_returns_3d),
                        'mean_return': np.mean(q_returns_3d),
                        'median_return': np.median(q_returns_3d),
                        'std_return': np.std(q_returns_3d)
                    })
                    quantile_returns_5d.append({
                        'quantile': label,
                        'count': len(q_returns_5d),
                        'mean_return': np.mean(q_returns_5d),
                        'median_return': np.median(q_returns_5d),
                        'std_return': np.std(q_returns_5d)
                    })

            results[rsi_col] = {
                'correlation_3d': corr_3d,
                'p_value_3d': p_value_3d,
                'correlation_5d': corr_5d,
                'p_value_5d': p_value_5d,
                'spearman_correlation_3d': spearman_corr_3d,
                'spearman_p_value_3d': spearman_p_3d,
                'spearman_correlation_5d': spearman_corr_5d,
                'spearman_p_value_5d': spearman_p_5d,
                'quantile_analysis_3d': quantile_returns_3d,
                'quantile_analysis_5d': quantile_returns_5d,
                'rsi_stats': {
                    'mean': np.mean(rsi_values),
                    'median': np.median(rsi_values),
                    'std': np.std(rsi_values),
                    'min': np.min(rsi_values),
                    'max': np.max(rsi_values)
                },
                'return_stats_3d': {
                    'mean': np.mean(returns_3d),
                    'median': np.median(returns_3d),
                    'std': np.std(returns_3d),
                    'min': np.min(returns_3d),
                    'max': np.max(returns_3d)
                },
                'return_stats_5d': {
                    'mean': np.mean(returns_5d),
                    'median': np.median(returns_5d),
                    'std': np.std(returns_5d),
                    'min': np.min(returns_5d),
                    'max': np.max(returns_5d)
                }
            }

        return results, valid_data

    def create_visualization(self, results, valid_data):
        """Create comprehensive visualization comparing 3-day and 5-day returns"""
        print("📊 Creating correlation visualization...")

        fig, axes = plt.subplots(3, 1, figsize=(15, 15))
        fig.suptitle('RSI_14 vs 3-Day & 5-Day Returns Correlation Analysis\n(Stocks with 10-day MA Slope > 1%)', fontsize=16, fontweight='bold')

        rsi_indicators = ['rsi_14']

        for i, rsi_col in enumerate(rsi_indicators):
            rsi_values = valid_data.select(rsi_col).to_numpy().flatten()
            returns_3d = valid_data.select('3d_return').to_numpy().flatten()
            returns_5d = valid_data.select('5d_return').to_numpy().flatten()

            # Scatter plot for 3-day returns
            if HAS_SEABORN:
                import seaborn as sns
                sns.scatterplot(x=rsi_values, y=returns_3d, alpha=0.6, ax=axes[0], color='blue', label='3-Day')
                sns.scatterplot(x=rsi_values, y=returns_5d, alpha=0.6, ax=axes[0], color='red', label='5-Day')
            else:
                axes[0].scatter(rsi_values, returns_3d, alpha=0.6, color='blue', label='3-Day')
                axes[0].scatter(rsi_values, returns_5d, alpha=0.6, color='red', label='5-Day')

            # Add trend lines
            z_3d = np.polyfit(rsi_values, returns_3d, 1)
            p_3d = np.poly1d(z_3d)
            z_5d = np.polyfit(rsi_values, returns_5d, 1)
            p_5d = np.poly1d(z_5d)

            axes[0].plot(rsi_values, p_3d(rsi_values), "b--", alpha=0.8, linewidth=2)
            axes[0].plot(rsi_values, p_5d(rsi_values), "r--", alpha=0.8, linewidth=2)

            axes[0].set_xlabel(f'{rsi_col.upper()}')
            axes[0].set_ylabel('Returns')
            axes[0].set_title('.3f')
            axes[0].legend()
            axes[0].grid(True, alpha=0.3)

            # Quantile returns comparison for 3-day
            quantile_data_3d = results[rsi_col]['quantile_analysis_3d']
            quantile_data_5d = results[rsi_col]['quantile_analysis_5d']
            quantiles = [q['quantile'] for q in quantile_data_3d]
            returns_3d_data = [q['mean_return'] for q in quantile_data_3d]
            returns_5d_data = [q['mean_return'] for q in quantile_data_5d]

            x = np.arange(len(quantiles))
            width = 0.35

            axes[1].bar(x - width/2, returns_3d_data, width, alpha=0.7, label='3-Day', color='blue')
            axes[1].bar(x + width/2, returns_5d_data, width, alpha=0.7, label='5-Day', color='red')
            axes[1].set_xlabel('RSI Quantile')
            axes[1].set_ylabel('Mean Return')
            axes[1].set_title(f'{rsi_col.upper()} Quantile Returns Comparison')
            axes[1].set_xticks(x)
            axes[1].set_xticklabels(quantiles, rotation=45)
            axes[1].legend()
            axes[1].grid(True, alpha=0.3)

            # Returns distribution comparison
            axes[2].hist(returns_3d, bins=50, alpha=0.7, label='3-Day', color='blue', density=True)
            axes[2].hist(returns_5d, bins=50, alpha=0.7, label='5-Day', color='red', density=True)
            axes[2].axvline(np.mean(returns_3d), color='blue', linestyle='--', linewidth=2, label='.3f')
            axes[2].axvline(np.mean(returns_5d), color='red', linestyle='--', linewidth=2, label='.3f')
            axes[2].set_xlabel('Returns')
            axes[2].set_ylabel('Density')
            axes[2].set_title(f'{rsi_col.upper()} Returns Distribution')
            axes[2].legend()
            axes[2].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('data/rsi_slope_correlation_3d_5d_comparison.png', dpi=300, bbox_inches='tight')
        print("   ✅ Chart saved to: data/rsi_slope_correlation_3d_5d_comparison.png")

    def create_3d_rsi_n_returns_scatter(self, returns_df):
        """Create 3D visualization: RSI bins vs N days vs Returns with surface and error bars"""
        print("📊 Creating 3D RSI-N-Returns visualization...")

        try:
            from mpl_toolkits.mplot3d import Axes3D
        except ImportError:
            print("   ⚠️  mpl_toolkits.mplot3d not available, skipping 3D plot")
            return

        # Filter valid data
        valid_data = returns_df.filter(
            pl.col('return').is_not_null() &
            pl.col('return').is_finite() &
            pl.col('rsi_14').is_not_null() &
            pl.col('rsi_14').is_finite() &
            pl.col('n_days').is_not_null()
        )

        if len(valid_data) == 0:
            print("   ⚠️  No valid data for 3D plot")
            return

        # Create RSI bins (discrete values) - normal order (low to high)
        rsi_bins = np.linspace(0, 100, 11)  # 0, 10, 20, ..., 100

        # Prepare data for surface and error bars - normal N days (low to high)
        n_days = np.arange(1, 11)  # 1 to 10
        rsi_centers = [(rsi_bins[i] + rsi_bins[i+1]) / 2 for i in range(len(rsi_bins)-1)]

        # Create meshgrid for surface
        RSI, N = np.meshgrid(rsi_centers, n_days)

        # Calculate median surface
        median_surface = np.zeros_like(RSI)
        std_surface = np.zeros_like(RSI)

        for i, n in enumerate(n_days):
            n_data = valid_data.filter(pl.col('n_days') == n)

            for j, rsi_center in enumerate(rsi_centers):
                # For reverse order bins, ensure min < max for filtering
                rsi_min = min(rsi_bins[j], rsi_bins[j+1])
                rsi_max = max(rsi_bins[j], rsi_bins[j+1])

                bin_data = n_data.filter(
                    (pl.col('rsi_14') >= rsi_min) &
                    (pl.col('rsi_14') < rsi_max)
                )

                if len(bin_data) > 0:
                    returns = bin_data.select('return').to_numpy().flatten()
                    median_surface[i, j] = np.median(returns)
                    std_surface[i, j] = np.std(returns)
                else:
                    median_surface[i, j] = np.nan
                    std_surface[i, j] = 0

        # Create 3D plot
        fig = plt.figure(figsize=(15, 10))
        ax = fig.add_subplot(111, projection='3d')

        # Create surfaces for median, upper std, and lower std
        from matplotlib import cm
        from matplotlib.colors import Normalize

        # Create upper and lower surfaces
        upper_surface = median_surface + std_surface
        lower_surface = median_surface - std_surface

        # Normalize values for color mapping with enhanced contrast
        all_values = np.concatenate([median_surface.flatten(), upper_surface.flatten(), lower_surface.flatten()])
        all_values = all_values[~np.isnan(all_values)]

        # Calculate statistics for enhanced color mapping
        data_mean = np.mean(all_values)
        data_std = np.std(all_values)

        # Use 2 standard deviations for better contrast (instead of min/max)
        vmin = data_mean - 1.5 * data_std
        vmax = data_mean + 1.5 * data_std

        # Create custom normalization with gamma correction for enhanced contrast
        from matplotlib.colors import PowerNorm
        norm = PowerNorm(gamma=0.7, vmin=float(vmin), vmax=float(vmax))

        # Color maps with enhanced saturation
        blues_cmap = cm.get_cmap('Blues')
        reds_cmap = cm.get_cmap('Reds')
        greens_cmap = cm.get_cmap('Greens')

        # Plot median surface (blue gradient)
        median_colors = blues_cmap(norm(median_surface))
        surf_median = ax.plot_surface(RSI, N, median_surface,
                                     facecolors=median_colors,
                                     alpha=0.7,
                                     linewidth=0.5,
                                     shade=True,
                                     zorder=1)

        # Plot upper standard deviation surface (red)
        upper_colors = reds_cmap(norm(upper_surface))
        surf_upper = ax.plot_surface(RSI, N, upper_surface,
                                    facecolors=upper_colors,
                                    alpha=0.6,
                                    linewidth=0.5,
                                    shade=True,
                                    zorder=2)

        # Plot lower standard deviation surface (green)
        lower_colors = greens_cmap(norm(lower_surface))
        surf_lower = ax.plot_surface(RSI, N, lower_surface,
                                    facecolors=lower_colors,
                                    alpha=0.6,
                                    linewidth=0.5,
                                    shade=True,
                                    zorder=3)

        # Add wireframe grid on top (using median surface as reference)
        ax.plot_wireframe(RSI, N, median_surface,
                         color='black',
                         linewidth=0.5,
                         alpha=0.3,
                         zorder=4)

        # Set labels and title
        ax.set_xlabel('RSI 值', fontsize=12)
        ax.set_ylabel('持有天数 N', fontsize=12)
        ax.set_zlabel('收益率', fontsize=12)
        ax.set_title('RSI vs 持有天数 vs 收益率 3D可视化\n(蓝色:中位数, 红色:上标准差, 绿色:下标准差)', fontsize=14, fontweight='bold')

        # Set tick labels for RSI bins (normal order: low to high)
        rsi_labels = [f'{int(rsi_bins[i])}-{int(rsi_bins[i+1])}' for i in range(len(rsi_bins)-1)]
        ax.set_xticks(rsi_centers)
        ax.set_xticklabels(rsi_labels, rotation=45)

        # Set N days ticks (normal order: low to high)
        ax.set_yticks(n_days)
        ax.set_yticklabels([f'{i}天' for i in n_days])

        # Add grid and adjust view
        ax.grid(True, alpha=0.3)
        ax.view_init(elev=25, azim=45)

        # Add legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='lightblue', alpha=0.7, label='中位数收益曲面'),
            Patch(facecolor='red', alpha=0.6, label='上标准差曲面'),
            Patch(facecolor='green', alpha=0.6, label='下标准差曲面')
        ]
        ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(0.02, 0.98))

        plt.tight_layout()
        plt.savefig('data/rsi_n_returns_3d_visualization.png', dpi=300, bbox_inches='tight')
        print("   ✅ 3D visualization saved to: data/rsi_n_returns_3d_visualization.png")

        # Print summary statistics
        print("\n📊 3D可视化统计汇总:")
        valid_points = np.sum(~np.isnan(median_surface))
        print(f"   总数据点数: {valid_points}")
        print(f"   RSI区间数: {len(rsi_centers)}")
        print(f"   N天数范围: {n_days[0]}-{n_days[-1]}天")

        # Calculate overall statistics
        valid_medians = median_surface[~np.isnan(median_surface)]
        valid_stds = std_surface[~np.isnan(std_surface)]

        if len(valid_medians) > 0:
            print(".4f")
            print(".4f")
            print(".4f")

        # Add grid and legend
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left', bbox_to_anchor=(0.02, 0.98))

        # Adjust view angle for better visualization
        ax.view_init(elev=20, azim=45)

        plt.tight_layout()
        plt.savefig('data/rsi_n_returns_3d_visualization.png', dpi=300, bbox_inches='tight')
        print("   ✅ 3D visualization saved to: data/rsi_n_returns_3d_visualization.png")

        # Print summary statistics
        print("\n📊 3D可视化统计汇总:")
        valid_points = np.sum(~np.isnan(median_surface))
        print(f"   总数据点数: {valid_points}")
        print(f"   RSI区间数: {len(rsi_centers)}")
        print(f"   N天数范围: {n_days[0]}-{n_days[-1]}天")

        # Debug: Print axis labels for verification
        print(f"   RSI标签: {rsi_labels}")
        print(f"   N天标签: {[f'{i}天' for i in n_days]}")

        # Calculate overall statistics
        valid_medians = median_surface[~np.isnan(median_surface)]
        valid_stds = std_surface[~np.isnan(std_surface)]

        if len(valid_medians) > 0:
            print(".4f")
            print(".4f")
            print(".4f")

    def print_results(self, results):
        """Print comprehensive analysis results comparing 3-day and 5-day returns"""
        print("\n" + "="*90)
        print("🎯 RSI vs 3-DAY & 5-DAY RETURNS CORRELATION ANALYSIS")
        print("   (For stocks with 10-day MA slope > 1%)")
        print("="*90)

        for rsi_col, result in results.items():
            print(f"\n🔍 {rsi_col.upper()} Analysis:")
            print("-" * 50)

            print("   📊 3-Day Returns:")
            print(f"      Pearson Correlation: {result['correlation_3d']:.4f}")
            print(f"      P-value: {result['p_value_3d']:.2e}")
            print(f"      Spearman Correlation: {result['spearman_correlation_3d']:.4f}")
            print(f"      Spearman P-value: {result['spearman_p_value_3d']:.2e}")
            print("   📊 5-Day Returns:")
            print(f"      Pearson Correlation: {result['correlation_5d']:.4f}")
            print(f"      P-value: {result['p_value_5d']:.2e}")
            print(f"      Spearman Correlation: {result['spearman_correlation_5d']:.4f}")
            print(f"      Spearman P-value: {result['spearman_p_value_5d']:.2e}")
            print("\n📊 RSI Statistics:")
            rsi_stats = result['rsi_stats']
            print(f"      Mean: {rsi_stats['mean']:.2f}")
            print(f"      Median: {rsi_stats['median']:.2f}")
            print(f"      Std: {rsi_stats['std']:.2f}")
            print(f"      Min: {rsi_stats['min']:.2f}")
            print(f"      Max: {rsi_stats['max']:.2f}")

            print("\n� 3-Day Return Statistics:")
            ret_stats_3d = result['return_stats_3d']
            print(f"      Mean: {ret_stats_3d['mean']:.4f}")
            print(f"      Median: {ret_stats_3d['median']:.4f}")
            print(f"      Std: {ret_stats_3d['std']:.4f}")
            print(f"      Min: {ret_stats_3d['min']:.4f}")
            print(f"      Max: {ret_stats_3d['max']:.4f}")

            print("\n📈 5-Day Return Statistics:")
            ret_stats_5d = result['return_stats_5d']
            print(f"      Mean: {ret_stats_5d['mean']:.4f}")
            print(f"      Median: {ret_stats_5d['median']:.4f}")
            print(f"      Std: {ret_stats_5d['std']:.4f}")
            print(f"      Min: {ret_stats_5d['min']:.4f}")
            print(f"      Max: {ret_stats_5d['max']:.4f}")

            print("\n📊 3-Day Quantile Analysis:")
            for q in result['quantile_analysis_3d']:
                print(f"      {q['quantile']:6s}: Count={q['count']}, Mean Return={q['mean_return']:.4f}")

            print("\n📊 5-Day Quantile Analysis:")
            for q in result['quantile_analysis_5d']:
                print(f"      {q['quantile']:6s}: Count={q['count']}, Mean Return={q['mean_return']:.4f}")

            # Comparison summary
            print("\n🔄 Comparison Summary:")
            print(f"      3-Day vs 5-Day Correlation Difference: {result['correlation_3d'] - result['correlation_5d']:.4f}")
            print(f"      3-Day Mean Return: {result['return_stats_3d']['mean']:.4f}")
            print(f"      5-Day Mean Return: {result['return_stats_5d']['mean']:.4f}")
            print(f"      Return Difference (5D - 3D): {result['return_stats_5d']['mean'] - result['return_stats_3d']['mean']:.4f}")

    def run_analysis(self):
        """Run the complete analysis"""
        print("🎯 Starting RSI slope correlation analysis...")
        print("="*60)

        try:
            # Load data
            scores_df, price_df = self.load_data()

            # Filter steep slope stocks
            steep_slope_data = self.filter_steep_slope_stocks(scores_df)

            # Calculate future returns
            returns_df = self.calculate_future_returns(steep_slope_data, price_df)

            # Analyze correlations
            results, valid_data = self.analyze_rsi_correlations(returns_df)

            # Create visualization
            self.create_visualization(results, valid_data)

            # Create 3D scatter plot
            self.create_3d_rsi_n_returns_scatter(returns_df)

            # Print results
            self.print_results(results)

            print("\n" + "="*60)
            print("🎉 Analysis completed successfully!")
            print("📁 Charts saved to: data/rsi_slope_correlation_analysis.png")
            print("="*60)

        except Exception as e:
            print(f"❌ Analysis failed: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    analyzer = RSISlopeCorrelationAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()