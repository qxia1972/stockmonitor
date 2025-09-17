"""
验证RQDatac复权数据获取的详细测试脚本
检查不同adjust_type参数下的实际数据差异
"""

import sys
sys.path.append('.')

from networks.rqdatac_data_loader import RQDatacDataLoader
import polars as pl
from datetime import datetime, timedelta

def main():
    print('=== RQDatac复权数据验证 ===')
    print('开始时间:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=False)

    # 测试股票
    test_stock = '600570.XSHG'
    end_date = '2025-09-16'
    start_date = '2025-09-10'

    print(f'\n测试股票: {test_stock}')
    print(f'时间范围: {start_date} 到 {end_date}')

    # 1. 获取默认数据（应该使用adjust_type="post"）
    print('\n1. 获取默认数据 (adjust_type默认值)...')
    default_data = loader.get_ohlcv_data(
        symbols=[test_stock],
        start_date=start_date,
        end_date=end_date
    )

    if not default_data.is_empty():
        print('   默认数据获取成功')
        print('   最新收盘价:', default_data.select('close').tail(1).item())
        print('   _adjust_type字段:', default_data['_adjust_type'].unique().to_list())
    else:
        print('   ❌ 默认数据获取失败')
        return

    # 2. 明确指定后复权
    print('\n2. 获取后复权数据 (adjust_type="post")...')
    post_data = loader.get_ohlcv_data(
        symbols=[test_stock],
        start_date=start_date,
        end_date=end_date,
        adjust_type="post"
    )

    if not post_data.is_empty():
        print('   后复权数据获取成功')
        print('   最新收盘价:', post_data.select('close').tail(1).item())
        print('   _adjust_type字段:', post_data['_adjust_type'].unique().to_list())
    else:
        print('   ❌ 后复权数据获取失败')

    # 3. 获取前复权数据
    print('\n3. 获取前复权数据 (adjust_type="pre")...')
    pre_data = loader.get_ohlcv_data(
        symbols=[test_stock],
        start_date=start_date,
        end_date=end_date,
        adjust_type="pre"
    )

    if not pre_data.is_empty():
        print('   前复权数据获取成功')
        print('   最新收盘价:', pre_data.select('close').tail(1).item())
        print('   _adjust_type字段:', pre_data['_adjust_type'].unique().to_list())
    else:
        print('   ❌ 前复权数据获取失败')

    # 4. 获取不复权数据
    print('\n4. 获取不复权数据 (adjust_type="none")...')
    none_data = loader.get_ohlcv_data(
        symbols=[test_stock],
        start_date=start_date,
        end_date=end_date,
        adjust_type="none"
    )

    if not none_data.is_empty():
        print('   不复权数据获取成功')
        print('   最新收盘价:', none_data.select('close').tail(1).item())
        print('   _adjust_type字段:', none_data['_adjust_type'].unique().to_list())
    else:
        print('   ❌ 不复权数据获取失败')

    # 5. 比较结果
    print('\n5. 数据比较分析:')
    if not default_data.is_empty() and not post_data.is_empty():
        default_close = default_data.select('close').tail(1).item()
        post_close = post_data.select('close').tail(1).item()
        if abs(default_close - post_close) < 0.01:
            print('   ✅ 默认数据与后复权数据一致')
        else:
            print(f'   ❌ 默认数据({default_close})与后复权数据({post_close})不一致')

    if not pre_data.is_empty():
        pre_close = pre_data.select('close').tail(1).item()
        post_close = post_data.select('close').tail(1).item() if not post_data.is_empty() else 0
        print(f'   前复权价格: {pre_close}')
        print(f'   后复权价格: {post_close}')
        if post_close > 0:
            ratio = post_close / pre_close
            print(f'   复权倍数: {ratio:.2f}x')

    # 6. 检查RQDatac的实际调用
    print('\n6. 检查RQDatac API调用细节...')
    try:
        # 直接调用RQDatac的get_price方法来验证
        if hasattr(loader, '_rqdatac') and loader._rqdatac is not None:
            print('   直接调用RQDatac.get_price()进行验证...')

            # 调用RQDatac的原始API
            raw_post_data = loader._rqdatac.get_price(
                order_book_ids=[test_stock],
                start_date=start_date,
                end_date=end_date,
                frequency='1d',
                adjust_type='post'
            )

            raw_pre_data = loader._rqdatac.get_price(
                order_book_ids=[test_stock],
                start_date=start_date,
                end_date=end_date,
                frequency='1d',
                adjust_type='pre'
            )

            if not raw_post_data.empty:
                raw_post_close = raw_post_data['close'].iloc[-1]
                print(f'   RQDatac原始post数据收盘价: {raw_post_close}')

            if not raw_pre_data.empty:
                raw_pre_close = raw_pre_data['close'].iloc[-1]
                print(f'   RQDatac原始pre数据收盘价: {raw_pre_close}')

                if not raw_post_data.empty:
                    ratio = raw_post_close / raw_pre_close
                    print(f'   RQDatac原始数据复权倍数: {ratio:.2f}x')

        else:
            print('   ❌ RQDatac未初始化，无法进行原始API验证')

    except Exception as e:
        print(f'   ❌ RQDatac原始API调用失败: {e}')

    print('\n=== 验证完成 ===')

if __name__ == '__main__':
    main()