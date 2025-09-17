#!/usr/bin/env python3
"""
Check future date availability for returns calculation
"""

import polars as pl
from datetime import timedelta

# Load data
scores_df = pl.read_parquet('data/scores/final_scores_20250916_231214.parquet')
price_df = pl.read_parquet('data/ohlcv_synced_20250917_013209.parquet')

# Convert dates
scores_df = scores_df.with_columns([pl.col('date').cast(pl.Date).alias('score_date')])
price_df = price_df.with_columns([pl.col('date').str.strptime(pl.Date, '%Y-%m-%d').alias('price_date')])

# Get unique dates
score_dates = sorted(scores_df.select('score_date').unique().to_series().to_list())
price_dates = set(price_df.select('price_date').unique().to_series().to_list())

print("🔍 CHECKING FUTURE DATE AVAILABILITY:")
print("=" * 50)

missing_count = 0
total_count = 0

for score_date in score_dates:
    future_date = score_date + timedelta(days=5)
    exists = future_date in price_dates
    total_count += 1

    if not exists:
        missing_count += 1
        print(f"❌ MISSING: {score_date} + 5 days = {future_date}")
    else:
        print(f"✅ EXISTS:  {score_date} + 5 days = {future_date}")

print(f"\n📊 SUMMARY:")
print(f"   Total dates checked: {total_count}")
print(f"   Missing future dates: {missing_count}")
print(".1f")

if missing_count > 0:
    print("\n🚨 CONCLUSION: Future price data is MISSING!")
    print("   This explains why expected returns are negative!")
else:
    print("\n✅ All future dates exist - issue is elsewhere")
    print("   Need to investigate return calculation logic")