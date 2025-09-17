import polars as pl
from pathlib import Path

def check_data_structure():
    """检查数据文件结构"""

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

    # 显示数据模式
    print('\n🔧 数据模式:')
    for col_name, col_type in df.schema.items():
        print(f'  {col_name}: {col_type}')

    # 检查order_book_id列
    if 'order_book_id' in df.columns:
        print(f'\n✅ 找到order_book_id列')

        # 检查null值
        null_count = df.select(pl.col('order_book_id').is_null().sum()).item()
        print(f'📊 null值数量: {null_count}')
        print(f'📊 有效值数量: {len(df) - null_count}')

        # 显示非null值的示例
        non_null_df = df.filter(pl.col('order_book_id').is_not_null())
        if len(non_null_df) > 0:
            sample = non_null_df.select('order_book_id').head(10)
            print('\n🔍 非null股票代码示例:')
            for i, code in enumerate(sample['order_book_id']):
                print(f'  {i+1}. {code}')

            # 查找300290
            stock_300290 = non_null_df.filter(pl.col('order_book_id').str.contains('300290'))
            print(f'\n🎯 包含300290的股票数量: {len(stock_300290)}')

            if len(stock_300290) > 0:
                print('找到的300290相关股票:')
                for row in stock_300290.iter_rows(named=True):
                    print(f'  ✅ {row["order_book_id"]}')

                    # 显示评分信息
                    if 'composite_score' in row:
                        print(f'     综合评分: {row["composite_score"]}')
                    if 'score_level' in row:
                        print(f'     评分等级: {row["score_level"]}')
                    break  # 只显示第一个
        else:
            print('❌ 没有找到有效的股票代码数据')
    else:
        print('❌ 数据中没有order_book_id列')

if __name__ == '__main__':
    check_data_structure()