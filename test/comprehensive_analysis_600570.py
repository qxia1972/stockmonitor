# 全面分析数据流程，查找可能的错误点
import os
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 全面分析600570数据流程，查找可能的错误点...')

    # 1. 获取RQDatac原始数据
    print('\n1️⃣ 获取RQDatac原始数据...')
    loader = RQDatacDataLoader(allow_mock_data=True)
    order_book_id = '600570.XSHG'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=1)  # 只获取最近1天的数据

    rq_data = loader.get_ohlcv_data(
        symbols=[order_book_id],
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        frequency='1d',
        adjust_type='post'
    )

    if rq_data is not None and len(rq_data) > 0:
        print('✅ RQDatac数据获取成功')
        rq_record = rq_data.to_pandas().iloc[0]
        print(f'   收盘价: {rq_record["close"]:.2f}元')
        print(f'   成交量: {rq_record["volume"]}手')
        print(f'   成交金额: {rq_record["amount"]:.2f}元')

    # 2. 检查本地数据文件
    print('\n2️⃣ 检查本地数据文件...')
    data_dir = 'data'
    files = [f for f in os.listdir(data_dir) if f.startswith('ohlcv_synced_') and f.endswith('.parquet')]
    if files:
        latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(data_dir, x)))
        file_path = os.path.join(data_dir, latest_file)

        df = pl.read_parquet(file_path)
        local_data = df.filter(pl.col('order_book_id') == order_book_id)

        if len(local_data) > 0:
            print('✅ 本地数据读取成功')
            local_record = local_data.to_pandas().iloc[0]
            print(f'   收盘价: {local_record["close"]:.2f}元')
            print(f'   成交量: {local_record["volume"]}手')
            print(f'   成交金额: {local_record["amount"]:.2f}元')

            # 3. 比较RQDatac和本地数据
            print('\n3️⃣ 比较RQDatac和本地数据差异...')
            if rq_data is not None:
                price_diff = abs(rq_record["close"] - local_record["close"])
                volume_diff = abs(rq_record["volume"] - local_record["volume"])
                amount_diff = abs(rq_record["amount"] - local_record["amount"])

                print(f'   收盘价差异: {price_diff:.2f}元')
                print(f'   成交量差异: {volume_diff}手')
                print(f'   成交金额差异: {amount_diff:.2f}元')

                if price_diff < 0.01 and volume_diff == 0 and amount_diff < 0.01:
                    print('✅ RQDatac和本地数据完全一致')
                else:
                    print('⚠️ RQDatac和本地数据存在差异')

        # 4. 检查数据处理后的结果
        print('\n4️⃣ 检查数据处理流程...')

        # 检查是否有计算后的数据文件
        processed_files = [f for f in os.listdir(data_dir) if 'processed' in f and f.endswith('.parquet')]
        if processed_files:
            print('   发现处理后的数据文件:')
            for pf in processed_files[:3]:  # 只显示前3个
                print(f'   - {pf}')
        else:
            print('   未发现处理后的数据文件')

        # 5. 检查评分计算结果
        print('\n5️⃣ 检查评分计算结果...')
        try:
            from modules.compute.stock_scorer import stock_scorer
            from modules.compute.indicator_calculator import IndicatorCalculator

            indicator_calc = IndicatorCalculator()

            # 计算指标
            indicators_df = indicator_calc.calculate_indicators(local_data)
            if indicators_df is not None and len(indicators_df) > 0:
                print('✅ 指标计算成功')
                # 显示一些指标
                indicator_record = indicators_df.to_pandas().iloc[0]
                print(f'   MA5: {indicator_record.get("ma_5", "N/A")}')
                print(f'   RSI: {indicator_record.get("rsi", "N/A")}')
            else:
                print('❌ 指标计算失败')

        except Exception as e:
            print(f'❌ 评分计算检查失败: {e}')

    # 6. 分析用户报告的数据
    print('\n6️⃣ 分析用户报告的数据...')
    print('   用户报告:')
    print('   - 收盘价: 34.56元')
    print('   - 成交量: 35.58万手')
    print('   - 成交金额: 12.14亿元')

    if 'local_record' in locals():
        print('   实际数据:')
        print(f'   - 收盘价: {local_record["close"]:.2f}元')
        print(f'   - 成交量: {local_record["volume"]/10000:.2f}万手')
        print(f'   - 成交金额: {local_record["amount"]/100000000:.2f}亿元')

        print('   差异分析:')
        user_close = 34.56
        user_volume = 35.58 * 10000  # 转换为手
        user_amount = 12.14 * 100000000  # 转换为元

        print(f'   - 收盘价差异: {abs(local_record["close"] - user_close):.2f}元')
        print(f'   - 成交量差异: {abs(local_record["volume"] - user_volume):.0f}手')
        print(f'   - 成交金额差异: {abs(local_record["amount"] - user_amount):.0f}元')

    print('\n💡 总结分析:')
    print('1. 如果成交金额差异很小，可能是正常的四舍五入')
    print('2. 如果成交量差异很大，可能有其他数据源或时间点')
    print('3. 如果收盘价差异巨大，可能是查看了不同的股票')
    print('4. 建议检查用户使用的"股票软件"是否是我们开发的系统')

except Exception as e:
    print(f'❌ 分析过程出现异常: {e}')
    import traceback
    traceback.print_exc()
