import polars as pl
from pathlib import Path

# 查看最新的OHLCV数据文件
data_dir = Path('data')
ohlcv_files = list(data_dir.glob('ohlcv_synced_*.parquet'))
if ohlcv_files:
    latest_file = max(ohlcv_files, key=lambda f: f.stat().st_mtime)
    print(f'最新OHLCV文件: {latest_file.name}')

    # 读取数据
    df = pl.read_parquet(latest_file)
    print(f'数据形状: {df.shape}')
    print(f'列名: {df.columns}')

    # 检查数据质量
    print(f'\n数据质量检查:')
    print(f'总记录数: {len(df)}')
    print(f'唯一股票数: {df.select("order_book_id").n_unique()}')
    print(f'日期范围: {df.select("date").min()} 到 {df.select("date").max()}')

    # 检查缺失值
    null_counts = df.null_count()
    print(f'\n缺失值统计:')
    has_nulls = False
    for col in df.columns:
        null_count = null_counts[col].item()
        if null_count > 0:
            print(f'{col}: {null_count} 个缺失值')
            has_nulls = True
        else:
            print(f'{col}: 无缺失值')

    if has_nulls:
        print('\n⚠️ 数据存在缺失值，需要补全')
    else:
        print('\n✅ 数据无缺失值')

    # 检查数据完整性
    print(f'\n数据完整性检查:')
    total_records = len(df)
    unique_stocks = df.select("order_book_id").n_unique()
    unique_dates = df.select("date").n_unique()
    expected_records = unique_stocks * unique_dates

    print(f'唯一股票数: {unique_stocks}')
    print(f'唯一日期数: {unique_dates}')
    print(f'期望记录数: {expected_records}')
    print(f'实际记录数: {total_records}')
    print(f'完整性: {total_records/expected_records*100:.2f}%')

    if total_records < expected_records:
        missing_records = expected_records - total_records
        print(f'缺失记录数: {missing_records}')
        print('⚠️ 数据存在缺失，需要补全')
    else:
        print('✅ 数据完整')