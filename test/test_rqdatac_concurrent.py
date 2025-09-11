#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RQDatac并发测试脚本
测试RQDatac是否支持并发数据获取
"""

import sys
import time
import concurrent.futures
from datetime import datetime, timedelta

# 添加项目路径
sys.path.insert(0, '/home/xiaqing/projects/stockman')

try:
    import rqdatac
    rqdatac.init()
    print("✅ RQDatac初始化成功")
except Exception as e:
    print(f"❌ RQDatac初始化失败: {e}")
    sys.exit(1)

def test_single_stock(stock_code):
    """测试单只股票的数据获取"""
    try:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        start_time = time.time()
        data = rqdatac.get_price(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            frequency='1d',
            fields=['open', 'close', 'high', 'low', 'volume']
        )
        end_time = time.time()

        if data is not None and not data.empty:
            return f"✅ {stock_code}: {len(data)}条数据, 耗时: {end_time - start_time:.2f}秒"
        else:
            return f"❌ {stock_code}: 无数据"
    except Exception as e:
        return f"❌ {stock_code}: 错误 - {e}"

def test_concurrent_fetch():
    """测试并发数据获取"""
    print("\n🔄 测试并发数据获取...")

    # 测试股票列表
    test_stocks = ['000001.XSHE', '000002.XSHE', '600000.XSHG', '600036.XSHG']

    print(f"📊 测试股票: {test_stocks}")

    # 1. 串行获取
    print("\n📈 串行获取测试:")
    serial_start = time.time()
    serial_results = []
    for stock in test_stocks:
        result = test_single_stock(stock)
        serial_results.append(result)
        print(result)

    serial_time = time.time() - serial_start
    print(f"⏱️ 串行总耗时: {serial_time:.2f}秒")

    # 2. 并发获取
    print("\n⚡ 并发获取测试:")
    concurrent_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        concurrent_results = list(executor.map(test_single_stock, test_stocks))

    concurrent_time = time.time() - concurrent_start

    for result in concurrent_results:
        print(result)

    print(f"⏱️ 并发总耗时: {concurrent_time:.2f}秒")

    # 3. 批量获取测试
    print("\n📦 批量获取测试:")
    try:
        batch_start = time.time()
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        batch_data = rqdatac.get_price(
            test_stocks,
            start_date=start_date,
            end_date=end_date,
            frequency='1d',
            fields=['open', 'close', 'high', 'low', 'volume']
        )
        batch_time = time.time() - batch_start

        if batch_data is not None:
            print(f"✅ 批量获取成功: {len(batch_data)}条数据, 耗时: {batch_time:.2f}秒")
            print(f"📊 数据类型: {type(batch_data)}")
            print(f"📊 数据索引: {batch_data.index.names if hasattr(batch_data.index, 'names') else '单索引'}")
        else:
            print("❌ 批量获取失败: 返回None")
    except Exception as e:
        print(f"❌ 批量获取错误: {e}")

if __name__ == "__main__":
    test_concurrent_fetch()
