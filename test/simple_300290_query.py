import polars as pl

def main():
    # 尝试最早的评分文件
    score_files = [
        'data/scores/final_scores_20250916_184913.parquet',
        'data/scores/final_scores_20250916_185755.parquet',
        'data/scores/final_scores_20250916_192938.parquet',
        'data/scores/final_scores_20250916_193201.parquet'
    ]

    for file_path in score_files:
        try:
            print(f'\n尝试读取文件: {file_path}')
            df = pl.read_parquet(file_path)

            if 'order_book_id' not in df.columns:
                print('  ❌ 没有order_book_id列')
                continue

            # 检查null值
            null_count = df.select(pl.col('order_book_id').is_null().sum()).item()
            valid_count = len(df) - null_count
            print(f'  总行数: {len(df)}, 有效股票数: {valid_count}')

            if valid_count == 0:
                print('  ❌ 所有order_book_id都是null')
                continue

            # 查找300290
            result = df.filter(pl.col('order_book_id') == '300290.XSHE')

            if len(result) > 0:
                row = result.row(0, named=True)
                print('\n🎯 === 300290.XSHE 评分详情 ===')
                print(f'股票代码: {row["order_book_id"]}')
                print(f'综合评分: {row["composite_score"]}')
                print(f'评分等级: {row["score_level"]}')

                # 详细评分
                trend_score = row.get('trend_score', 'N/A')
                capital_score = row.get('capital_score', 'N/A')
                technical_score = row.get('technical_score', 'N/A')
                risk_score = row.get('risk_score', 'N/A')

                print(f'趋势评分: {trend_score}')
                print(f'资金评分: {capital_score}')
                print(f'技术评分: {technical_score}')
                print(f'风险评分: {risk_score}')

                # 计算排名
                all_scores = df.sort('composite_score', descending=True)
                rank = 1
                for r in all_scores.iter_rows(named=True):
                    if r['order_book_id'] == '300290.XSHE':
                        print(f'排名: 第{rank}名 (共{len(df)}只股票)')
                        break
                    rank += 1

                # 显示基本信息
                print(f'\n基本信息:')
                print(f'日期: {row.get("date", "N/A")}')
                print(f'收盘价: {row.get("close", "N/A")}')
                print(f'成交量: {row.get("volume", "N/A")}')

                return  # 找到后退出

            else:
                print('  ❌ 未找到300290.XSHE')

        except Exception as e:
            print(f'  ❌ 读取失败: {e}')

    print('\n❌ 在所有尝试的评分文件中都未找到300290.XSHE的评分数据')

if __name__ == '__main__':
    main()