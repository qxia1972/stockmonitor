# -*- coding: utf-8 -*-
"""
Stock Pool Calculation Tool Manager

Command-line interface for managing stock pool calculations and data synchronization.
Provides comprehensive tools for building, managing and monitoring three-layer stock pools.
"""

import sys
import os
import argparse
from datetime import datetime

# Add project path to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Environment management and auto-switching
from modules.python_manager import EnvironmentManager

# Auto environment check and switch
env_manager = EnvironmentManager()

# 使用公共的环境检查方法，支持命令行模式fallback
env_manager.ensure_environment_with_fallback()

# Now safe to import rqdatac and other modules
import rqdatac
from stockpool import PoolManager
from modules.log_manager import get_tool_logger

# Initialize tool logger
logger = get_tool_logger()

# Create stock pool manager instance
sync_tool = PoolManager()

def init_rqdatac():
    """
    Initialize rqdatac data source connection
    
    Returns:
        bool: True if initialization successful, False otherwise
    """
    try:
        logger.info("🔄 Initializing rqdatac connection...")
        rqdatac.init()
        logger.info("✅ rqdatac initialization successful")
        return True
    except Exception as e:
        logger.error(f"❌ rqdatac initialization failed: {e}")
        return False

def show_status():
    """
    Display comprehensive stock pool calculation status
    
    Shows current sync status, data file availability, and pool statistics.
    """
    logger.info("=" * 60)
    logger.info("Stock Pool Calculation Tool Status")
    logger.info("=" * 60)
    
    status = sync_tool.get_sync_status()
    
    logger.info("📊 Stock Pool Calculation Status:")
    logger.info(f"  🔄 Currently Calculating: {'Yes' if status['is_syncing'] else 'No'}")
    logger.info(f"  🕐 Last Calculation: {status['last_sync_time'] or 'None'}")
    logger.info(f"  📅 Data Date: {status['data_date'] or 'None'}")
    
    logger.info("📁 Data Files Status:")
    logger.info(f"  📋 Basic Pool File: {'✅' if status['basic_pool_exists'] else '❌'}")
    logger.info(f"  👀 Watch Pool File: {'✅' if status['watch_pool_exists'] else '❌'}")
    logger.info(f"  ⭐ Core Pool File: {'✅' if status['core_pool_exists'] else '❌'}")
    
    logger.info("📈 Pool Statistics:")
    logger.info(f"  📋 Basic Layer Count: {status['basic_pool_count']} stocks")
    logger.info(f"  观察层数量: {status['watch_pool_count']}只")
    logger.info(f"  核心层数量: {status['core_pool_count']}只")

def run_sync():
    """执行股票池计算"""
    if not init_rqdatac():
        return
    
    logger.info("正在执行股票池计算...")
    success = sync_tool.process_daily_sync_caculate_buildpool()
    
    if success:
        logger.info("✓ 股票池计算成功")
        show_status()
    else:
        logger.error("✗ 股票池计算失败")

def show_data():
    """显示数据内容"""
    logger.info("=" * 60)
    logger.info("当日股票数据")
    logger.info("=" * 60)
    
    # 显示股票池
    pools = sync_tool.load_all_pools()
    if pools and pools['basic_layer']:
        logger.info("基础层股票池 (前10只):")
        for i, stock in enumerate(pools['basic_layer'][:10], 1):
            logger.info(f"  {i:2d}. {stock['stock_code']} - 评分: {stock['score']:.1f} - 价格: {stock.get('current_price', 0):.2f}")
    else:
        logger.warning("✗ 未找到基础层股票池数据")
    
    if pools and pools['core_layer']:
        logger.info("核心层股票池 (前10只):")
        for i, stock in enumerate(pools['core_layer'][:10], 1):
            logger.info(f"  {i:2d}. {stock['stock_code']} - 评分: {stock['score']:.1f} - 价格: {stock.get('current_price', 0):.2f}")
    else:
        logger.warning("✗ 未找到核心层股票池数据")

def clear_data():
    """清除数据"""
    logger.info("正在清除所有数据文件...")
    success = sync_tool.clear_all_data()
    
    if success:
        logger.info("✓ 数据清除成功")
    else:
        logger.error("✗ 数据清除失败")

def setup_scheduler():
    """设置定时任务"""
    logger.info("定时任务功能暂未实现")
    logger.info("请使用crontab或其他工具设置定时执行: python stockpool_tool.py sync")
    
    try:
        while True:
            import time
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("定时任务已停止")

def test_data_flow():
    """测试数据流程"""
    logger.info("=" * 60)
    logger.info("测试数据流程")
    logger.info("=" * 60)
    
    if not init_rqdatac():
        return
    
    # 1. 执行股票池计算
    print("1. 执行股票池计算...")
    success = sync_tool.process_daily_sync_caculate_buildpool()
    
    if not success:
        print("✗ 股票池计算失败，无法继续测试")
        return
    
    # 2. 测试数据加载
    print("2. 测试数据加载...")
    
    pools = sync_tool.load_all_pools()
    
    if pools and pools['basic_layer']:
        print(f"✓ 基础层股票池加载成功: {len(pools['basic_layer'])}只")
    else:
        print("✗ 基础层股票池加载失败")
    
    if pools and pools['core_layer']:
        print(f"✓ 核心层股票池加载成功: {len(pools['core_layer'])}只")
    else:
        print("✗ 核心层股票池加载失败")
    
    # 3. 显示结果统计
    print("3. 结果统计:")
    if pools and pools['basic_layer'] and pools['core_layer']:
        basic_scores = [s['score'] for s in pools['basic_layer']]
        core_scores = [s['score'] for s in pools['core_layer']]
        
        print(f"  基础层平均评分: {sum(basic_scores)/len(basic_scores):.1f}")
        print(f"  核心层平均评分: {sum(core_scores)/len(core_scores):.1f}")
        print(f"  筛选比例: {len(pools['core_layer'])/len(pools['basic_layer'])*100:.1f}%")
    
    print("✓ 数据流程测试完成")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='股票池计算工具管理器')
    parser.add_argument('action', choices=['status', 'sync', 'data', 'clear', 'schedule', 'test'], 
                       help='操作类型: status(查看状态), sync(执行股票池计算), data(显示数据), clear(清除数据), schedule(设置定时任务), test(测试流程)')
    
    args = parser.parse_args()
    
    if args.action == 'status':
        show_status()
    elif args.action == 'sync':
        run_sync()
    elif args.action == 'data':
        show_data()
    elif args.action == 'clear':
        clear_data()
    elif args.action == 'schedule':
        setup_scheduler()
    elif args.action == 'test':
        test_data_flow()

if __name__ == "__main__":
    main()
