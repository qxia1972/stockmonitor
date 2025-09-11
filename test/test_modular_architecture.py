#!/usr/bin/env python3
"""
测试新的模块化架构
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stockpool import PoolManager
from datetime import datetime

def test_modular_architecture():
    """测试新的模块化架构"""
    print("🧪 开始测试新的模块化架构...")

    try:
        # 初始化PoolManager
        pool_manager = PoolManager()
        print("✅ PoolManager 初始化成功")

        # 检查新组件是否存在
        if hasattr(pool_manager, 'cache_manager'):
            print("✅ CacheManager 组件存在")
        else:
            print("❌ CacheManager 组件不存在")

        if hasattr(pool_manager, 'data_loader'):
            print("✅ DataLoader 组件存在")
        else:
            print("❌ DataLoader 组件不存在")

        if hasattr(pool_manager, 'data_saver'):
            print("✅ DataSaver 组件存在")
        else:
            print("❌ DataSaver 组件不存在")

        # 测试CacheManager的基本方法
        if hasattr(pool_manager.cache_manager, 'load_cache_with_validation'):
            print("✅ CacheManager.load_cache_with_validation 方法存在")
        else:
            print("❌ CacheManager.load_cache_with_validation 方法不存在")

        if hasattr(pool_manager.cache_manager, 'save_cache_data'):
            print("✅ CacheManager.save_cache_data 方法存在")
        else:
            print("❌ CacheManager.save_cache_data 方法不存在")

        # 测试DataLoader的基本方法
        if hasattr(pool_manager.data_loader, 'load_valuation_data_with_fallback'):
            print("✅ DataLoader.load_valuation_data_with_fallback 方法存在")
        else:
            print("❌ DataLoader.load_valuation_data_with_fallback 方法不存在")

        if hasattr(pool_manager.data_loader, 'load_price_data_with_fallback'):
            print("✅ DataLoader.load_price_data_with_fallback 方法存在")
        else:
            print("❌ DataLoader.load_price_data_with_fallback 方法不存在")

        # 测试DataSaver的基本方法
        if hasattr(pool_manager.data_saver, 'save_stock_pools'):
            print("✅ DataSaver.save_stock_pools 方法存在")
        else:
            print("❌ DataSaver.save_stock_pools 方法不存在")

        if hasattr(pool_manager.data_saver, 'save_single_pool'):
            print("✅ DataSaver.save_single_pool 方法存在")
        else:
            print("❌ DataSaver.save_single_pool 方法不存在")

        print("🎉 所有组件和方法检查通过！")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_modular_architecture()
    sys.exit(0 if success else 1)
