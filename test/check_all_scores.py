import polars as pl
from pathlib import Path

def check_all_score_files():
    """检查所有评分相关文件"""

    data_dir = Path('data')
    all_score_files = []

    # 查找所有包含score的文件
    for pattern in ['*score*.parquet', '*final*.parquet']:
        files = list(data_dir.glob(f'**/{pattern}'))
        all_score_files.extend(files)

    print(f'📊 找到 {len(all_score_files)} 个评分相关文件:')

    valid_files = []
    found_300290 = False

    for file in sorted(all_score_files, key=lambda x: x.stat().st_mtime, reverse=True):
        size = file.stat().st_size / 1024 / 1024  # MB
        print(f'\n🔍 {file.name} ({size:.2f} MB)')

        try:
            df = pl.read_parquet(file)
            if 'order_book_id' in df.columns:
                null_count = df.select(pl.col('order_book_id').is_null().sum()).item()
                valid_count = len(df) - null_count
                print(f'   数据行数: {len(df)}, 有效股票代码: {valid_count}')

                if valid_count > 0:
                    valid_files.append((file, valid_count))

                    # 检查是否有300290
                    valid_df = df.filter(pl.col('order_book_id').is_not_null())
                    stock_300290 = valid_df.filter(pl.col('order_book_id').str.contains('300290'))

                    if len(stock_300290) > 0:
                        print(f'   🎯 找到300290!')
                        found_300290 = True
                        row = stock_300290.row(0, named=True)
                        stock_code = row.get('order_book_id', 'Unknown')
                        composite_score = row.get('composite_score', 'N/A')
                        score_level = row.get('score_level', 'N/A')

                        print(f'   📈 股票代码: {stock_code}')
                        print(f'   ⭐ 综合评分: {composite_score}')
                        print(f'   🏷️ 评分等级: {score_level}')

                        # 显示更多详细信息
                        if 'trend_score' in row:
                            print(f'   📊 趋势评分: {row["trend_score"]}')
                        if 'capital_score' in row:
                            print(f'   💰 资金评分: {row["capital_score"]}')
                        if 'technical_score' in row:
                            print(f'   🔧 技术评分: {row["technical_score"]}')
                        if 'risk_score' in row:
                            print(f'   ⚠️ 风险评分: {row["risk_score"]}')

                    # 显示文件中的股票示例
                    sample = valid_df.select('order_book_id').head(3)
                    print('   📋 股票示例:')
                    for code in sample['order_book_id']:
                        print(f'      {code}')
                else:
                    print('   ❌ 股票代码全为null')
            else:
                print('   ❌ 没有order_book_id列')
        except Exception as e:
            print(f'   ❌ 读取失败: {e}')

    print(f'\n📈 总结:')
    print(f'   有效文件数量: {len(valid_files)}')
    if found_300290:
        print('   ✅ 找到300290的评分数据')
    else:
        print('   ❌ 未找到300290的评分数据')

if __name__ == '__main__':
    check_all_score_files()