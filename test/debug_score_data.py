import polars as pl
from pathlib import Path

def debug_score_data():
    """调试评分数据文件"""

    # 读取最新的评分文件
    data_dir = Path('data/scores')
    score_files = list(data_dir.glob('final_scores_*.parquet'))

    if not score_files:
        print('❌ 未找到评分文件')
        return

    latest_file = max(score_files, key=lambda x: x.stat().st_mtime)
    print(f'📊 读取文件: {latest_file}')

    df = pl.read_parquet(latest_file)
    print(f'📈 数据行数: {len(df)}')
    print(f'📊 列数: {len(df.columns)}')

    # 检查order_book_id列
    if 'order_book_id' in df.columns:
        print(f'✅ 找到order_book_id列')

        # 显示前10个股票代码（过滤掉null值）
        valid_codes = df.filter(pl.col('order_book_id').is_not_null()).select('order_book_id').head(10)
        print('\n🔍 前10个有效股票代码:')
        for i, code in enumerate(valid_codes['order_book_id']):
            print(f'  {i+1}. {code}')

        # 查找包含300290的股票（过滤掉null值）
        valid_df = df.filter(pl.col('order_book_id').is_not_null())
        contains_300290 = valid_df.filter(pl.col('order_book_id').str.contains('300290'))
        print(f'\n🎯 包含300290的股票数量: {len(contains_300290)}')

        if len(contains_300290) > 0:
            print('找到的股票:')
            for row in contains_300290.iter_rows(named=True):
                print(f'  ✅ {row["order_book_id"]}')

        # 检查创业板股票
        cyb_stocks = valid_df.filter(pl.col('order_book_id').str.contains('300'))
        print(f'\n🏢 创业板股票(300开头)数量: {len(cyb_stocks)}')

        if len(cyb_stocks) > 0:
            print('创业板股票示例:')
            sample_cyb = cyb_stocks.head(5)
            for row in sample_cyb.iter_rows(named=True):
                print(f'  📈 {row["order_book_id"]}')

        # 查找300290的具体情况
        exact_300290 = valid_df.filter(pl.col('order_book_id') == '300290.XSHE')
        if len(exact_300290) > 0:
            print(f'\n🎉 找到300290.XSHE!')
            row = exact_300290.row(0, named=True)
            print(f'   综合评分: {row.get("composite_score", "N/A")}')
            print(f'   评分等级: {row.get("score_level", "N/A")}')
        else:
            print('\n❌ 未找到300290.XSHE')

        # 显示数据质量信息
        null_count = df.filter(pl.col('order_book_id').is_null()).height
        print(f'\n📊 数据质量信息:')
        print(f'   总行数: {len(df)}')
        print(f'   null值行数: {null_count}')
        print(f'   有效行数: {len(df) - null_count}')

    else:
        print('❌ 数据中没有order_book_id列')
        print('可用的列:', list(df.columns)[:10])  # 只显示前10列

if __name__ == '__main__':
    debug_score_data()