import polars as pl
import os
from pathlib import Path

def get_stock_score(stock_code):
    """获取指定股票的评分信息"""

    # 查找最新的评分文件
    data_dir = Path('data/scores')
    score_files = list(data_dir.glob('final_scores_*.parquet'))

    if not score_files:
        print('❌ 未找到评分文件')
        return

    # 按时间戳排序，获取最新的文件
    latest_file = max(score_files, key=lambda x: x.stat().st_mtime)
    print(f'📊 使用最新的评分文件: {latest_file}')

    # 读取评分数据
    df = pl.read_parquet(latest_file)
    print(f'📈 评分数据包含 {len(df)} 只股票')

    # 尝试不同的股票代码格式
    code_formats = [
        f'{stock_code}.XSHE',  # 创业板格式
        f'{stock_code}.XSHG',  # 主板格式
        stock_code,            # 原始格式
    ]

    result = None
    found_code = None

    for code in code_formats:
        result = df.filter(pl.col('order_book_id') == code)
        if len(result) > 0:
            found_code = code
            break

    if result is None or len(result) == 0:
        print(f'❌ 未找到股票 {stock_code} 的评分数据')
        print('\n🔍 可用的股票代码示例:')
        sample_codes = df.select('order_book_id').unique().head(10)
        for code in sample_codes['order_book_id']:
            print(f'  {code}')
        return

    # 显示评分结果
    print(f'\n🎯 === 股票 {found_code} 评分详情 ===')

    row = result.row(0, named=True)

    print(f'📈 股票代码: {row["order_book_id"]}')
    print(f'⭐ 综合评分: {row.get("composite_score", "N/A")}')
    print(f'🏷️  评分等级: {row.get("score_level", "N/A")}')

    print(f'\n📊 详细评分分项:')
    print(f'  📈 趋势评分: {row.get("trend_score", "N/A")}')
    print(f'  💰 资金评分: {row.get("capital_score", "N/A")}')
    print(f'  🔧 技术评分: {row.get("technical_score", "N/A")}')
    print(f'  ⚠️  风险评分: {row.get("risk_score", "N/A")}')

    # 显示子评分项
    print(f'\n🔍 评分子项详情:')
    sub_scores = []
    for col in result.columns:
        if col.startswith('score_') and not col in ['score_level', 'composite_score', 'trend_score', 'capital_score', 'technical_score', 'risk_score']:
            value = row.get(col, "N/A")
            if isinstance(value, (int, float)) and not pl.Series([value]).is_null().any():
                sub_scores.append((col, value))

    for score_name, score_value in sorted(sub_scores, key=lambda x: x[1], reverse=True):
        print(f'  {score_name}: {score_value:.2f}')

    # 显示基本信息
    print(f'\n📋 基本信息:')
    basic_fields = ['date', 'close', 'volume', 'vwap', 'returns', 'volume_ratio']
    for field in basic_fields:
        if field in row:
            value = row[field]
            if isinstance(value, (int, float)):
                if field in ['close', 'vwap']:
                    print(f'  {field}: {value:.2f}')
                elif field in ['returns', 'volume_ratio']:
                    print(f'  {field}: {value:.4f}')
                else:
                    print(f'  {field}: {value}')
            else:
                print(f'  {field}: {value}')

if __name__ == '__main__':
    get_stock_score('300290')