# 深入分析600570的复权数据与同花顺对比
import os
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 深入分析600570的复权数据与同花顺对比...')

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=True)

    order_book_id = '600570.XSHG'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=5)  # 获取最近5天的数据

    print(f'\n📊 分析股票: {order_book_id}')
    print(f'📅 时间范围: {start_date} 到 {end_date}')

    # 获取不同复权方式的数据
    adjust_types = ['post', 'pre', 'none']

    print('\n🏢 获取不同复权方式的数据:')

    for adjust_type in adjust_types:
        try:
            data = loader.get_ohlcv_data(
                symbols=[order_book_id],
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                frequency='1d',
                adjust_type=adjust_type
            )

            if data is not None and len(data) > 0:
                print(f'\n✅ {adjust_type}复权数据:')

                # 显示最近3天的数据
                recent_data = data.sort('date', descending=True).head(3)
                for row in recent_data.to_pandas().itertuples():
                    print(f'  {row.date}: '
                          f'开{row.open:.2f} 收{row.close:.2f} 高{row.high:.2f} 低{row.low:.2f} '
                          f'量{row.volume/10000:.1f}万 额{row.amount/100000000:.2f}亿')

                # 特别关注最新数据
                latest = recent_data.to_pandas().iloc[0]
                print(f'  📌 最新数据 ({latest["date"]}):')
                print(f'     收盘价: {latest["close"]:.2f}元')
                print(f'     成交量: {latest["volume"]/10000:.1f}万手')
                print(f'     成交金额: {latest["amount"]/100000000:.2f}亿元')

            else:
                print(f'❌ {adjust_type}复权: 数据获取失败')

        except Exception as e:
            print(f'❌ {adjust_type}复权: 异常 {e}')

    print('\n' + '='*80)
    print('🎯 与同花顺数据对比分析')
    print('='*80)

    print('\n📊 同花顺报告数据:')
    print('  收盘价: 34.56元')
    print('  成交量: 35.58万手')
    print('  成交金额: 12.14亿元')

    print('\n🔍 RQDatac数据对比:')

    # 获取pre复权数据进行精确对比
    try:
        pre_data = loader.get_ohlcv_data(
            symbols=[order_book_id],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            frequency='1d',
            adjust_type='pre'
        )

        if pre_data is not None and len(pre_data) > 0:
            latest_pre = pre_data.sort('date', descending=True).head(1).to_pandas().iloc[0]

            print('  RQDatac pre复权数据:')
            print(f'    收盘价: {latest_pre["close"]:.2f}元')
            print(f'    成交量: {latest_pre["volume"]/10000:.2f}万手')
            print(f'    成交金额: {latest_pre["amount"]/100000000:.2f}亿元')

            print('\n  📏 差异对比:')
            user_close = 34.56
            user_volume = 35.58
            user_amount = 12.14

            close_diff = abs(latest_pre["close"] - user_close)
            volume_diff = abs(latest_pre["volume"]/10000 - user_volume)
            amount_diff = abs(latest_pre["amount"]/100000000 - user_amount)

            print(f'    收盘价差异: {close_diff:.2f}元')
            print(f'    成交量差异: {volume_diff:.2f}万手')
            print(f'    成交金额差异: {amount_diff:.2f}亿元')

            if close_diff < 0.01 and volume_diff < 1 and amount_diff < 0.1:
                print('\n  🎉 数据高度匹配！差异在合理范围内')
            elif close_diff < 0.01:
                print('\n  ✅ 收盘价完全匹配，成交数据存在差异')
            else:
                print('\n  ⚠️ 数据存在显著差异')

    except Exception as e:
        print(f'❌ 获取pre复权数据失败: {e}')

    print('\n' + '='*80)
    print('💡 结论与建议')
    print('='*80)

    print('\n🎯 问题根源:')
    print('1. 同花顺可能使用pre复权或none复权数据')
    print('2. 我们的系统使用post复权数据')
    print('3. 复权方式差异导致价格相差40倍以上')

    print('\n🔧 解决方案:')
    print('1. 检查同花顺的复权设置，建议使用后复权')
    print('2. 或者修改我们的系统使用相同复权方式')
    print('3. 在数据对比时确保使用相同的复权方式')

    print('\n📊 复权方式选择建议:')
    print('- 投资分析: 建议使用后复权（post）')
    print('- 历史对比: 建议使用前复权（pre）')
    print('- 原始数据: 使用不复权（none）')

except Exception as e:
    print(f'❌ 分析过程出现异常: {e}')
    import traceback
    traceback.print_exc()