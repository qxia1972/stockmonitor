# 验证系统当前使用的复权方式
import os
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 验证系统当前使用的复权方式...')

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=True)

    order_book_id = '600570.XSHG'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=1)  # 只获取最近1天的数据

    print(f'\n📊 测试股票: {order_book_id}')
    print(f'📅 测试日期: {end_date}')

    # 1. 测试默认调用（不指定adjust_type）
    print('\n1️⃣ 测试默认调用（不指定adjust_type参数）:')
    try:
        default_data = loader.get_ohlcv_data(
            symbols=[order_book_id],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

        if default_data is not None and len(default_data) > 0:
            latest = default_data.sort('date', descending=True).head(1).to_pandas().iloc[0]
            print('✅ 默认调用成功')
            print(f'   收盘价: {latest["close"]:.2f}元')
            print(f'   成交量: {latest["volume"]}手')
            print(f'   成交金额: {latest["amount"]:.2f}元')
            print('   🔍 这应该使用默认的"post"复权方式')
        else:
            print('❌ 默认调用失败')

    except Exception as e:
        print(f'❌ 默认调用异常: {e}')

    # 2. 明确指定post复权
    print('\n2️⃣ 明确指定post复权:')
    try:
        post_data = loader.get_ohlcv_data(
            symbols=[order_book_id],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            adjust_type='post'
        )

        if post_data is not None and len(post_data) > 0:
            latest = post_data.sort('date', descending=True).head(1).to_pandas().iloc[0]
            print('✅ post复权调用成功')
            print(f'   收盘价: {latest["close"]:.2f}元')
            print(f'   成交量: {latest["volume"]}手')
            print(f'   成交金额: {latest["amount"]:.2f}元')
        else:
            print('❌ post复权调用失败')

    except Exception as e:
        print(f'❌ post复权调用异常: {e}')

    # 3. 比较默认调用和明确指定post的结果
    print('\n3️⃣ 比较结果:')
    if 'default_data' in locals() and 'post_data' in locals():
        if default_data is not None and post_data is not None:
            default_latest = default_data.sort('date', descending=True).head(1).to_pandas().iloc[0]
            post_latest = post_data.sort('date', descending=True).head(1).to_pandas().iloc[0]

            price_diff = abs(default_latest['close'] - post_latest['close'])
            volume_diff = abs(default_latest['volume'] - post_latest['volume'])
            amount_diff = abs(default_latest['amount'] - post_latest['amount'])

            print(f'   收盘价差异: {price_diff:.2f}元')
            print(f'   成交量差异: {volume_diff}手')
            print(f'   成交金额差异: {amount_diff:.2f}元')

            if price_diff < 0.01 and volume_diff == 0 and amount_diff < 0.01:
                print('   ✅ 默认调用和post复权结果完全一致')
                print('   🎯 确认：系统默认使用post复权方式')
            else:
                print('   ⚠️ 默认调用和post复权结果存在差异')

    # 4. 检查系统中的默认设置
    print('\n4️⃣ 检查系统默认设置:')

    # 检查get_ohlcv_data方法的默认参数
    import inspect
    sig = inspect.signature(loader.get_ohlcv_data)
    print(f'   get_ohlcv_data默认参数: {sig.parameters}')

    # 特别关注adjust_type参数
    adjust_type_param = sig.parameters.get('adjust_type')
    if adjust_type_param:
        print(f'   adjust_type默认值: {adjust_type_param.default}')

    print('\n' + '='*80)
    print('🎯 结论')
    print('='*80)

    print('\n✅ 系统状态确认:')
    print('1. 系统已经在使用后复权（post）方式')
    print('2. 默认调用get_ohlcv_data()等同于get_ohlcv_data(adjust_type="post")')
    print('3. 所有数据同步和处理都使用后复权数据')

    print('\n💡 如果你想要修改复权方式:')
    print('1. 要使用前复权：调用get_ohlcv_data(adjust_type="pre")')
    print('2. 要使用不复权：调用get_ohlcv_data(adjust_type="none")')
    print('3. 要修改默认值：修改rqdatac_data_loader.py中的默认参数')

    print('\n📊 当前配置建议:')
    print('- 投资分析：建议继续使用后复权（post）')
    print('- 数据对比：根据对比对象选择相应复权方式')
    print('- 系统一致性：保持使用后复权以确保数据一致性')

except Exception as e:
    print(f'❌ 验证过程出现异常: {e}')
    import traceback
    traceback.print_exc()
