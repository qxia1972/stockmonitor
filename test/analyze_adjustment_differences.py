# 验证不同股票的复权差异分析
import os
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 验证不同股票的复权差异分析...')

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=True)

    # 选择几只代表性股票
    test_stocks = [
        '600570.XSHG',  # 恒生电子 - 我们已经分析过的
        '000001.XSHE',  # 平安银行 - 大盘股
        '300001.XSHE',  # 特锐德 - 创业板
        '600036.XSHG',  # 招商银行 - 蓝筹股
    ]

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=10)  # 获取最近10天的数据

    print(f'\n📊 分析时间范围: {start_date} 到 {end_date}')
    print(f'📈 测试股票: {test_stocks}')

    # 存储不同复权方式的结果
    results = {}

    for order_book_id in test_stocks:
        print(f'\n🏢 分析股票: {order_book_id}')

        # 获取不同复权方式的数据
        adjust_types = ['post', 'pre', 'none']  # 后复权、前复权、不复权

        stock_results = {}
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
                    latest_record = data.sort('date', descending=True).head(1).to_pandas().iloc[0]
                    stock_results[adjust_type] = {
                        'close': latest_record['close'],
                        'high': latest_record['high'],
                        'low': latest_record['low'],
                        'volume': latest_record['volume'],
                        'amount': latest_record['amount'],
                        'date': latest_record['date']
                    }
                    print(f'  ✅ {adjust_type}复权: 收盘价 {latest_record["close"]:.2f}元')
                else:
                    print(f'  ❌ {adjust_type}复权: 数据获取失败')

            except Exception as e:
                print(f'  ❌ {adjust_type}复权: 异常 {e}')

        results[order_book_id] = stock_results

    # 分析复权差异
    print('\n' + '='*80)
    print('📊 复权差异分析结果')
    print('='*80)

    for order_book_id, stock_data in results.items():
        print(f'\n🏢 {order_book_id}:')

        if len(stock_data) >= 2:
            # 计算不同复权方式之间的差异
            prices = {}
            for adjust_type, data in stock_data.items():
                prices[adjust_type] = data['close']

            # 显示价格差异
            print(f'  收盘价对比:')
            for adjust_type, price in prices.items():
                print(f'    {adjust_type}复权: {price:.2f}元')

            # 计算最大差异
            if len(prices) > 1:
                price_values = list(prices.values())
                max_diff = max(price_values) - min(price_values)
                print(f'  最大价格差异: {max_diff:.2f}元 ({max_diff/max(price_values)*100:.2f}%)')

            # 分析成交量和成交金额的一致性
            volumes = {}
            amounts = {}
            for adjust_type, data in stock_data.items():
                volumes[adjust_type] = data['volume']
                amounts[adjust_type] = data['amount']

            # 检查成交量是否一致（应该不受复权影响）
            volume_values = list(volumes.values())
            volume_diff = max(volume_values) - min(volume_values)
            print(f'  成交量差异: {volume_diff} 手')

            # 检查成交金额是否一致（应该不受复权影响）
            amount_values = list(amounts.values())
            amount_diff = max(amount_values) - min(amount_values)
            print(f'  成交金额差异: {amount_diff:.0f} 元')

        else:
            print('  ❌ 数据不足，无法进行差异分析')

    # 总结分析
    print('\n' + '='*80)
    print('🎯 总结分析')
    print('='*80)

    print('\n💡 复权方式说明:')
    print('- post（后复权）: 保持当前价格不变，向前调整历史价格')
    print('- pre（前复权）: 保持历史价格不变，向后调整当前价格')
    print('- none（不复权）: 原始价格，不进行任何调整')

    print('\n📊 分析结果:')
    print('1. 价格差异主要由复权方式造成')
    print('2. 成交量和成交金额通常不受复权影响')
    print('3. 同花顺可能使用不同的复权方式或数据源')

    print('\n🔍 验证建议:')
    print('1. 检查同花顺的复权设置（通常默认为后复权）')
    print('2. 对比同一复权方式下的数据')
    print('3. 关注数据的时间戳和更新频率')
    print('4. 考虑数据源的差异（Wind vs RQDatac）')

    print('\n💡 如果你能提供同花顺中具体股票的数据，我可以进一步分析差异的具体原因。')

except Exception as e:
    print(f'❌ 分析过程出现异常: {e}')
    import traceback
    traceback.print_exc()