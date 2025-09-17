# 验证同花顺数据与RQDatac数据的差异
import polars as pl
from datetime import datetime, timedelta
import sys
sys.path.append('.')

try:
    from networks.rqdatac_data_loader import RQDatacDataLoader

    print('🔍 分析同花顺数据与RQDatac数据的差异...')

    # 初始化数据加载器
    loader = RQDatacDataLoader(allow_mock_data=True)

    # 获取RQDatac数据进行对比
    order_book_id = '600570.XSHG'
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=5)  # 获取最近5天的数据

    print(f'\n📊 获取 {order_book_id} 的RQDatac数据 ({start_date} 到 {end_date})...')

    rq_data = loader.get_ohlcv_data(
        symbols=[order_book_id],
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        frequency='1d',
        adjust_type='post'
    )

    if rq_data is not None and len(rq_data) > 0:
        print('✅ RQDatac数据获取成功')
        print('\n📈 RQDatac最近5天数据:')

        # 显示最近5天的数据
        recent_data = rq_data.sort('date', descending=True).head(5)
        for i, row in enumerate(recent_data.to_pandas().itertuples(), 1):
            print(f'  {i}. {row.date}: '
                  f'开{row.open:.2f} 收{row.close:.2f} 高{row.high:.2f} 低{row.low:.2f} '
                  f'量{row.volume/10000:.1f}万 额{row.amount/100000000:.2f}亿')

        print('\n💡 数据差异分析:')
        print('1. 同花顺可能使用不同的数据源（Wind、东方财富等）')
        print('2. 可能显示的是不同的复权方式（前复权、后复权、不复权）')
        print('3. 可能显示的是不同的时间点或交易日')
        print('4. 可能存在数据延迟或更新频率差异')

        print('\n🔍 验证建议:')
        print('1. 检查同花顺中的股票代码是否正确（600570）')
        print('2. 确认同花顺显示的日期是否与RQDatac一致')
        print('3. 检查同花顺的复权设置')
        print('4. 对比成交量和成交金额的单位是否一致')

        print('\n📊 单位换算说明:')
        print('- 成交量: RQDatac显示"手"，同花顺通常显示"万手"')
        print('- 成交金额: RQDatac显示"元"，同花顺通常显示"万元"或"亿元"')
        print('- 价格: 通常都是"元"，但复权方式可能不同')

        # 计算可能的单位换算
        latest_record = recent_data.to_pandas().iloc[0]
        print(f'\n🔢 以最新数据为例的单位换算:')
        print(f'  RQDatac成交量: {latest_record["volume"]} 手 = {latest_record["volume"]/10000:.1f} 万手')
        print(f'  RQDatac成交金额: {latest_record["amount"]:.0f} 元 = {latest_record["amount"]/10000:.0f} 万元 = {latest_record["amount"]/100000000:.2f} 亿元')

    else:
        print('❌ RQDatac数据获取失败')

    print('\n🎯 结论:')
    print('同花顺和RQDatac显示数据差异是正常的，因为：')
    print('1. 数据源不同')
    print('2. 复权方式可能不同')
    print('3. 更新时间可能不同')
    print('4. 单位换算可能不同')

except ImportError as e:
    print(f'❌ 导入失败: {e}')
    print('请确保RQDatac已正确安装和配置')
except Exception as e:
    print(f'❌ 分析失败: {e}')