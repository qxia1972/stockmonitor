#!/usr/bin/env python3
"""
分阶段测试股票池处理流水线
测试每个阶段的实际数据输出和处理结果
"""

import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    """主测试函数"""
    print("🎯 开始分阶段测试股票池处理流水线")

    # 导入必要的模块
    try:
        from stockpool import StockPoolManager
        print("✅ 成功导入 StockPoolManager")
    except ImportError as e:
        print(f"❌ 导入 StockPoolManager 失败: {e}")
        return

    # 初始化管理器
    try:
        manager = StockPoolManager()
        print("✅ StockPoolManager初始化成功")
    except Exception as e:
        print(f"❌ StockPoolManager初始化失败: {e}")
        return

    # 第一阶段：获取股票列表
    print("\n🚀 测试第一阶段：获取股票列表")
    try:
        stock_list_df = manager.data_store.fetch_stock_list()

        if stock_list_df is None or stock_list_df.empty:
            print("❌ 无法获取股票列表")
            return

        stock_codes = stock_list_df['order_book_id'].tolist()
        print("✅ 第一阶段测试结果:")
        print(f"   📊 总股票数量: {len(stock_codes)}")
        print(f"   📋 股票列表样例: {stock_codes[:5]}")
        print(f"   📋 DataFrame形状: {stock_list_df.shape}")

    except Exception as e:
        print(f"❌ 第一阶段测试失败: {e}")
        return

    # 第二阶段：批量获取基本面数据
    print("\n🚀 测试第二阶段：批量获取基本面数据")
    try:
        # 限制测试股票数量
        test_stock_codes = stock_codes[:20]  # 只测试前20只股票
        print(f"   📊 测试股票数量: {len(test_stock_codes)}")

        # 获取目标日期
        target_date = manager._get_latest_trading_date()
        print(f"   📅 目标分析日期: {target_date}")

        # 批量获取估值数据
        valuation_data = manager._batch_get_valuation_data(test_stock_codes, target_date)

        print("✅ 第二阶段测试结果:")
        print(f"   📊 获取到估值数据的股票数量: {len(valuation_data)}")

        if valuation_data:
            # 显示前3只股票的估值数据
            print("   📋 前3只股票的估值数据样例:")
            for i, stock_info in enumerate(valuation_data[:3]):
                print(f"      股票{i+1}: {stock_info['stock_code']}")
                print(f"         市值: {stock_info.get('market_cap', 'N/A')}")
                print(f"         PE: {stock_info.get('pe_ratio', 'N/A')}")
                print(f"         PB: {stock_info.get('pb_ratio', 'N/A')}")

    except Exception as e:
        print(f"❌ 第二阶段测试失败: {e}")
        return

    # 第三阶段：批量获取价格序列数据
    print("\n🚀 测试第三阶段：批量获取价格序列数据")
    try:
        # 限制测试股票数量
        test_stock_codes = stock_codes[:10]  # 只测试前10只股票
        print(f"   📊 测试股票数量: {len(test_stock_codes)}")

        # 计算日期范围
        target_date = manager._get_latest_trading_date()
        from datetime import datetime, timedelta
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') -
                     timedelta(days=30)).strftime('%Y-%m-%d')  # 30天数据

        print(f"   📅 日期范围: {start_date} 至 {target_date}")

        # 批量获取价格数据
        price_data = manager._batch_get_price_data(test_stock_codes, start_date, target_date)

        print("✅ 第三阶段测试结果:")
        print(f"   📊 获取到价格数据的股票数量: {len(price_data)}")

        if price_data:
            # 显示第一只股票的价格数据详情
            first_stock = list(price_data.keys())[0]
            price_df = price_data[first_stock]

            print(f"   📋 样例股票 {first_stock} 的价格数据:")
            print(f"      数据形状: {price_df.shape}")
            print(f"      列名: {list(price_df.columns)}")

            # 显示最新的3行数据
            print("      最新3行价格数据:")
            latest_data = price_df.tail(3)
            for date, row in latest_data.iterrows():
                print(f"         {date.strftime('%Y-%m-%d')}: O={row.get('open', 'N/A'):.2f}, "
                      f"C={row.get('close', 'N/A'):.2f}, V={row.get('volume', 'N/A'):.0f}")

    except Exception as e:
        print(f"❌ 第三阶段测试失败: {e}")
        return

    # 第四阶段：计算技术指标
    print("\n🚀 测试第四阶段：计算技术指标")
    try:
        if not price_data:
            print("⚠️ 没有价格数据，跳过技术指标计算")
            return

        # 选择第一只股票进行技术指标计算测试
        test_stock = list(price_data.keys())[0]
        test_price_df = price_data[test_stock]

        print(f"   📊 测试股票: {test_stock}")
        print(f"   📊 价格数据点数: {len(test_price_df)}")

        # 计算技术指标
        technical_indicators = manager.calculate_technical_indicators(test_price_df, test_stock)

        print("✅ 第四阶段测试结果:")
        if technical_indicators:
            print(f"   📊 计算得到的技术指标数量: {len(technical_indicators)}")

            # 显示部分技术指标
            sample_indicators = ['RSI_14', 'MACD', 'BB_UPPER', 'SMA_20']
            print("   📋 部分技术指标值:")
            for indicator in sample_indicators:
                if indicator in technical_indicators:
                    value = technical_indicators[indicator]
                    if isinstance(value, (int, float)):
                        print(f"      {indicator}: {value:.4f}")
                    else:
                        print(f"      {indicator}: {value}")
                else:
                    print(f"      {indicator}: 未计算")
        else:
            print("⚠️ 未计算到技术指标")

    except Exception as e:
        print(f"❌ 第四阶段测试失败: {e}")
        return

    print("\n� 分阶段测试完成！")
    print("所有阶段都成功执行，数据流水线工作正常。")

if __name__ == "__main__":
    main()

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/stage_test.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

def test_stage_1_stock_list(manager: StockPoolManager, logger) -> Optional[pd.DataFrame]:
    """测试第一阶段：获取股票列表"""
    logger.info("🚀 测试第一阶段：获取股票列表")

    try:
        # 获取股票列表
        stock_list_df = manager.data_store.fetch_stock_list()

        if stock_list_df is None or stock_list_df.empty:
            logger.error("❌ 无法获取股票列表")
            return None

        # 提取股票代码
        stock_codes = stock_list_df['order_book_id'].tolist()

        logger.info("✅ 第一阶段测试结果:")
        logger.info(f"   📊 总股票数量: {len(stock_codes)}")
        logger.info(f"   📋 股票列表样例: {stock_codes[:10]}")
        logger.info(f"   📋 DataFrame列: {list(stock_list_df.columns)}")
        logger.info(f"   📋 DataFrame形状: {stock_list_df.shape}")

        # 显示前5行数据
        logger.info("   📋 前5行股票数据:")
        for idx, row in stock_list_df.head().iterrows():
            logger.info(f"      {row['order_book_id']} - {row.get('abbrev_symbol', 'N/A')}")

        return stock_list_df

    except Exception as e:
        logger.error(f"❌ 第一阶段测试失败: {e}")
        return None

def test_stage_2_valuation_data(manager: StockPoolManager, stock_codes: List[str], logger) -> Optional[List[Dict]]:
    """测试第二阶段：批量获取基本面数据"""
    logger.info("🚀 测试第二阶段：批量获取基本面数据")

    try:
        # 限制测试股票数量，避免测试时间过长
        test_stock_codes = stock_codes[:50]  # 只测试前50只股票
        logger.info(f"   📊 测试股票数量: {len(test_stock_codes)}")

        # 获取目标日期
        target_date = manager._get_latest_trading_date()
        logger.info(f"   📅 目标分析日期: {target_date}")

        # 批量获取估值数据
        valuation_data = manager._batch_get_valuation_data(test_stock_codes, target_date)

        logger.info("✅ 第二阶段测试结果:"        logger.info(f"   📊 获取到估值数据的股票数量: {len(valuation_data)}")

        if valuation_data:
            # 显示前3只股票的估值数据
            logger.info("   📋 前3只股票的估值数据样例:")
            for i, stock_info in enumerate(valuation_data[:3]):
                logger.info(f"      股票{i+1}: {stock_info['stock_code']}")
                logger.info(f"         市值: {stock_info.get('market_cap', 'N/A')}")
                logger.info(f"         PE: {stock_info.get('pe_ratio', 'N/A')}")
                logger.info(f"         PB: {stock_info.get('pb_ratio', 'N/A')}")
                logger.info(f"         PS: {stock_info.get('ps_ratio', 'N/A')}")

            # 统计数据完整性
            complete_count = sum(1 for item in valuation_data
                               if item.get('market_cap') is not None and item.get('pe_ratio') is not None)
            logger.info(f"   📊 数据完整性: {complete_count}/{len(valuation_data)} 股票有完整估值数据")

        return valuation_data

    except Exception as e:
        logger.error(f"❌ 第二阶段测试失败: {e}")
        return None

def test_stage_3_price_data(manager: StockPoolManager, stock_codes: List[str], logger) -> Optional[Dict[str, pd.DataFrame]]:
    """测试第三阶段：批量获取价格序列数据"""
    logger.info("🚀 测试第三阶段：批量获取价格序列数据")

    try:
        # 限制测试股票数量
        test_stock_codes = stock_codes[:30]  # 只测试前30只股票
        logger.info(f"   📊 测试股票数量: {len(test_stock_codes)}")

        # 计算日期范围
        target_date = manager._get_latest_trading_date()
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') -
                     timedelta(days=manager.config['history_days'])).strftime('%Y-%m-%d')

        logger.info(f"   📅 日期范围: {start_date} 至 {target_date}")

        # 批量获取价格数据
        price_data = manager._batch_get_price_data(test_stock_codes, start_date, target_date)

        logger.info("✅ 第三阶段测试结果:"        logger.info(f"   📊 获取到价格数据的股票数量: {len(price_data)}")

        if price_data:
            # 显示第一只股票的价格数据详情
            first_stock = list(price_data.keys())[0]
            price_df = price_data[first_stock]

            logger.info(f"   📋 样例股票 {first_stock} 的价格数据:")
            logger.info(f"      数据形状: {price_df.shape}")
            logger.info(f"      列名: {list(price_df.columns)}")
            logger.info(f"      数据时间范围: {price_df.index.min()} 至 {price_df.index.max()}")

            # 显示最新的5行数据
            logger.info("      最新5行价格数据:")
            latest_data = price_df.tail()
            for date, row in latest_data.iterrows():
                logger.info(f"         {date.strftime('%Y-%m-%d')}: O={row.get('open', 'N/A'):.2f}, "
                           f"H={row.get('high', 'N/A'):.2f}, L={row.get('low', 'N/A'):.2f}, "
                           f"C={row.get('close', 'N/A'):.2f}, V={row.get('volume', 'N/A'):.0f}")

            # 统计数据质量
            valid_stocks = [code for code, df in price_data.items() if df is not None and not df.empty]
            logger.info(f"   📊 数据质量: {len(valid_stocks)}/{len(price_data)} 股票有有效价格数据")

        return price_data

    except Exception as e:
        logger.error(f"❌ 第三阶段测试失败: {e}")
        return None

def test_stage_4_technical_indicators(manager: StockPoolManager, price_data: Dict[str, pd.DataFrame], logger) -> Optional[Dict]:
    """测试第四阶段：计算技术指标"""
    logger.info("🚀 测试第四阶段：计算技术指标")

    try:
        if not price_data:
            logger.warning("⚠️ 没有价格数据，跳过技术指标计算")
            return None

        # 选择第一只股票进行技术指标计算测试
        test_stock = list(price_data.keys())[0]
        test_price_df = price_data[test_stock]

        logger.info(f"   📊 测试股票: {test_stock}")
        logger.info(f"   📊 价格数据点数: {len(test_price_df)}")

        # 计算技术指标
        technical_indicators = manager.calculate_technical_indicators(test_price_df, test_stock)

        logger.info("✅ 第四阶段测试结果:"        if technical_indicators:
            logger.info(f"   📊 计算得到的技术指标数量: {len(technical_indicators)}")

            # 显示部分技术指标
            sample_indicators = ['RSI_14', 'MACD', 'BB_UPPER', 'BB_LOWER', 'SMA_20', 'EMA_12']
            logger.info("   📋 部分技术指标值:")

            for indicator in sample_indicators:
                if indicator in technical_indicators:
                    value = technical_indicators[indicator]
                    if isinstance(value, (int, float)) and not np.isnan(value):
                        logger.info(f"      {indicator}: {value:.4f}")
                    else:
                        logger.info(f"      {indicator}: {value}")
                else:
                    logger.info(f"      {indicator}: 未计算")

            # 显示所有指标名称
            logger.info("   📋 所有技术指标列表:"            all_indicators = sorted(technical_indicators.keys())
            for i in range(0, len(all_indicators), 8):
                logger.info(f"      {', '.join(all_indicators[i:i+8])}")
        else:
            logger.warning("⚠️ 未计算到技术指标")

        return technical_indicators

    except Exception as e:
        logger.error(f"❌ 第四阶段测试失败: {e}")
        return None

def test_stage_5_scoring(manager: StockPoolManager, valuation_data: List[Dict],
                        price_data: Dict[str, pd.DataFrame], logger) -> Optional[List[Dict]]:
    """测试第五阶段：计算评分"""
    logger.info("🚀 测试第五阶段：计算评分")

    try:
        if not valuation_data or not price_data:
            logger.warning("⚠️ 缺少必要数据，跳过评分计算")
            return None

        # 准备测试数据 - 只使用有完整数据的股票
        test_stocks = []
        for stock_info in valuation_data[:10]:  # 测试前10只股票
            stock_code = stock_info['stock_code']
            if stock_code in price_data and price_data[stock_code] is not None:
                # 计算技术指标
                technical_indicators = manager.calculate_technical_indicators(
                    price_data[stock_code], stock_code
                )

                if technical_indicators:
                    stock_data = {
                        'stock_code': stock_code,
                        **stock_info,
                        'technical_indicators': technical_indicators
                    }
                    test_stocks.append(stock_data)

        logger.info(f"   📊 用于评分测试的股票数量: {len(test_stocks)}")

        if not test_stocks:
            logger.warning("⚠️ 没有有效的测试数据")
            return None

        # 计算评分
        scored_stocks = []
        for i, stock_data in enumerate(test_stocks):
            try:
                # 计算基础层评分
                basic_score = manager.calculate_basic_layer_score(stock_data)
                stock_data['basic_score'] = basic_score

                # 计算观察层评分
                watch_score = manager.calculate_watch_layer_score(stock_data)
                stock_data['watch_score'] = watch_score

                # 计算核心层评分
                core_score = manager.calculate_core_layer_score(stock_data)
                stock_data['core_score'] = core_score

                scored_stocks.append(stock_data)

                if (i + 1) % 5 == 0:
                    logger.info(f"   ⏳ 已评分 {i + 1}/{len(test_stocks)} 只股票")

            except Exception as e:
                logger.warning(f"⚠️ 评分股票 {stock_data['stock_code']} 失败: {e}")
                continue

        logger.info("✅ 第五阶段测试结果:"        logger.info(f"   📊 成功评分的股票数量: {len(scored_stocks)}")

        if scored_stocks:
            # 显示评分结果
            logger.info("   📋 评分结果样例 (前5只股票):")
            for i, stock in enumerate(scored_stocks[:5]):
                logger.info(f"      股票{i+1}: {stock['stock_code']}")
                logger.info(f"         基础评分: {stock.get('basic_score', 'N/A'):.2f}")
                logger.info(f"         观察评分: {stock.get('watch_score', 'N/A'):.2f}")
                logger.info(f"         核心评分: {stock.get('core_score', 'N/A'):.2f}")

            # 统计评分分布
            basic_scores = [s.get('basic_score', 0) for s in scored_stocks]
            watch_scores = [s.get('watch_score', 0) for s in scored_stocks]
            core_scores = [s.get('core_score', 0) for s in scored_stocks]

            logger.info("   📊 评分统计:"            logger.info(f"      基础评分 - 平均: {np.mean(basic_scores):.2f}, 最高: {max(basic_scores):.2f}")
            logger.info(f"      观察评分 - 平均: {np.mean(watch_scores):.2f}, 最高: {max(watch_scores):.2f}")
            logger.info(f"      核心评分 - 平均: {np.mean(core_scores):.2f}, 最高: {max(core_scores):.2f}")

        return scored_stocks

    except Exception as e:
        logger.error(f"❌ 第五阶段测试失败: {e}")
        return None

def test_stage_6_pool_building(manager: StockPoolManager, scored_stocks: List[Dict], logger) -> Optional[Dict[str, pd.DataFrame]]:
    """测试第六阶段：构建股票池"""
    logger.info("🚀 测试第六阶段：构建股票池")

    try:
        if not scored_stocks:
            logger.warning("⚠️ 没有评分数据，跳过股票池构建")
            return None

        # 构建股票池
        pools = manager.build_stock_pool(scored_stocks)

        logger.info("✅ 第六阶段测试结果:"        if pools:
            for pool_name, pool_df in pools.items():
                logger.info(f"   📊 {pool_name}: {len(pool_df)} 只股票")

                if not pool_df.empty:
                    # 显示前3只股票
                    logger.info(f"      前3只股票 ({pool_name}):")
                    for i, (_, row) in enumerate(pool_df.head(3).iterrows()):
                        stock_code = row.get('stock_code', 'N/A')
                        basic_score = row.get('basic_score', 'N/A')
                        watch_score = row.get('watch_score', 'N/A')
                        core_score = row.get('core_score', 'N/A')
                        logger.info(f"         {i+1}. {stock_code} - 基础:{basic_score}, 观察:{watch_score}, 核心:{core_score}")

        return pools

    except Exception as e:
        logger.error(f"❌ 第六阶段测试失败: {e}")
        return None

def main():
    """主测试函数"""
    logger = setup_logging()
    logger.info("🎯 开始分阶段测试股票池处理流水线")

    # 初始化管理器
    try:
        manager = StockPoolManager()
        logger.info("✅ StockPoolManager初始化成功")
    except Exception as e:
        logger.error(f"❌ StockPoolManager初始化失败: {e}")
        return

    # 分阶段测试
    results = {}

    # 第一阶段：获取股票列表
    stock_list_df = test_stage_1_stock_list(manager, logger)
    results['stage_1'] = stock_list_df is not None

    if stock_list_df is not None:
        stock_codes = stock_list_df['order_book_id'].tolist()

        # 第二阶段：批量获取基本面数据
        valuation_data = test_stage_2_valuation_data(manager, stock_codes, logger)
        results['stage_2'] = valuation_data is not None

        # 第三阶段：批量获取价格序列数据
        price_data = test_stage_3_price_data(manager, stock_codes, logger)
        results['stage_3'] = price_data is not None

        # 第四阶段：计算技术指标
        technical_indicators = test_stage_4_technical_indicators(manager, price_data, logger)
        results['stage_4'] = technical_indicators is not None

        # 第五阶段：计算评分
        scored_stocks = test_stage_5_scoring(manager, valuation_data, price_data, logger)
        results['stage_5'] = scored_stocks is not None

        # 第六阶段：构建股票池
        pools = test_stage_6_pool_building(manager, scored_stocks, logger)
        results['stage_6'] = pools is not None

    # 输出测试总结
    logger.info("\n" + "="*60)
    logger.info("📊 测试总结:")
    logger.info("="*60)

    stage_names = {
        'stage_1': '第一阶段：获取股票列表',
        'stage_2': '第二阶段：批量获取基本面数据',
        'stage_3': '第三阶段：批量获取价格序列数据',
        'stage_4': '第四阶段：计算技术指标',
        'stage_5': '第五阶段：计算评分',
        'stage_6': '第六阶段：构建股票池'
    }

    for stage_key, success in results.items():
        status = "✅ 通过" if success else "❌ 失败"
        logger.info(f"   {stage_names[stage_key]}: {status}")

    successful_stages = sum(results.values())
    total_stages = len(results)

    logger.info(f"\n🎯 总体结果: {successful_stages}/{total_stages} 个阶段测试通过")

    if successful_stages == total_stages:
        logger.info("🎉 所有阶段测试通过！流水线工作正常")
    else:
        logger.warning("⚠️ 部分阶段测试失败，请检查相关配置和数据源")

if __name__ == "__main__":
    main()</content>
<parameter name="filePath">/home/xiaqing/projects/stockman/stage_test.py
