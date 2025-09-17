# 详细比较600570的RQDatac原始数据和本地数据
import os
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 详细比较600570的RQDatac原始数据和本地数据...')

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=True)

    # 获取RQDatac原始数据
    print('\n📡 获取RQDatac原始数据...')
    order_book_id = '600570.XSHG'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=10)  # 获取最近10天的数据

    try:
        rq_data = loader.get_ohlcv_data(
            symbols=[order_book_id],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            frequency='1d',
            adjust_type='post'
        )

        if rq_data is not None and len(rq_data) > 0:
            print('✅ RQDatac原始数据获取成功')
            print(f'   数据行数: {len(rq_data)}')
            print(f'   日期范围: {rq_data["date"].min()} 到 {rq_data["date"].max()}')

            # 显示RQDatac的最新数据
            latest_rq = rq_data.sort('date', descending=True).head(3)
            print('\n📊 RQDatac最新3天数据:')
            for i, row in enumerate(latest_rq.to_pandas().itertuples(), 1):
                print(f'  {i}. {row.date}: 开{row.open:.2f} 收{row.close:.2f} 高{row.high:.2f} 低{row.low:.2f} 量{row.volume} 额{row.amount:.2f}')

        else:
            print('❌ RQDatac数据获取失败或为空')
            rq_data = None

    except Exception as e:
        print(f'❌ RQDatac数据获取异常: {e}')
        rq_data = None

    # 读取本地数据
    print('\n💾 读取本地数据...')
    data_dir = 'data'
    files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]
    if files:
        latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
        file_path = os.path.join(data_dir, latest_file)

        df = pl.read_parquet(file_path)

        # 过滤600570的数据
        local_data = df.filter(pl.col('order_book_id') == order_book_id)

        if len(local_data) > 0:
            print('✅ 本地数据读取成功')
            print(f'   数据行数: {len(local_data)}')
            print(f'   日期范围: {local_data["date"].min()} 到 {local_data["date"].max()}')

            # 显示本地数据的最新数据
            latest_local = local_data.sort('date', descending=True).head(3)
            print('\n📊 本地最新3天数据:')
            for i, row in enumerate(latest_local.to_pandas().itertuples(), 1):
                print(f'  {i}. {row.date}: 开{row.open:.2f} 收{row.close:.2f} 高{row.high:.2f} 低{row.low:.2f} 量{row.volume} 额{row.amount:.2f}')

        else:
            print('❌ 本地数据中未找到600570')
            local_data = None
    else:
        print('❌ 未找到本地数据文件')
        local_data = None

    # 比较数据
    if rq_data is not None and local_data is not None:
        print('\n🔍 数据比较分析...')

        # 转换为pandas进行比较
        rq_df = rq_data.to_pandas()
        local_df = local_data.to_pandas()

        # 按日期合并
        merged = rq_df.merge(local_df, on='date', suffixes=('_rq', '_local'))

        if len(merged) > 0:
            print(f'✅ 找到 {len(merged)} 个共同日期进行比较')

            # 检查价格差异
            price_fields = ['open', 'close', 'high', 'low']
            has_diff = False

            for field in price_fields:
                rq_col = f'{field}_rq'
                local_col = f'{field}_local'

                if rq_col in merged.columns and local_col in merged.columns:
                    diff = (merged[local_col] - merged[rq_col]).abs()
                    max_diff = diff.max()

                    if max_diff > 0.01:  # 允许0.01的误差
                        has_diff = True
                        print(f'⚠️  {field.upper()} 价格存在差异，最大差异: {max_diff:.4f}')
                        # 显示差异最大的记录
                        max_diff_idx = diff.idxmax()
                        row = merged.iloc[max_diff_idx]
                        print(f'     日期: {row["date"]}, RQ: {row[rq_col]:.2f}, 本地: {row[local_col]:.2f}')
                    else:
                        print(f'✅ {field.upper()} 价格完全匹配')

            if not has_diff:
                print('\n🎉 所有价格数据完全匹配！数据是正确的。')

                # 显示最新的匹配数据
                latest_match = merged.sort_values('date', ascending=False).head(1).iloc[0]
                print('\n📊 最新数据验证:')
                print(f'  日期: {latest_match["date"]}')
                print(f'  开盘价: RQ={latest_match["open_rq"]:.2f}, 本地={latest_match["open_local"]:.2f}')
                print(f'  收盘价: RQ={latest_match["close_rq"]:.2f}, 本地={latest_match["close_local"]:.2f}')
                print(f'  最高价: RQ={latest_match["high_rq"]:.2f}, 本地={latest_match["high_local"]:.2f}')
                print(f'  最低价: RQ={latest_match["low_rq"]:.2f}, 本地={latest_match["low_local"]:.2f}')

        else:
            print('❌ 没有共同的日期可以比较')

    print('\n🔍 可能的问题分析:')
    print('1. 如果数据匹配但用户觉得不对，可能是显示格式问题')
    print('2. 或者是用户记忆中的价格与实际价格不同')
    print('3. 或者是数据源的问题（但从比较看数据是正确的）')
    print('4. 或者是时间点不同导致的价格差异')

except Exception as e:
    print(f'❌ 脚本执行异常: {e}')
    import traceback
    traceback.print_exc()