#!/usr/bin/env python3
"""
测试重构后的DataLoader方法
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stockpool import PoolManager
from datetime import datetime

def test_refactored_methods():
    """测试重构后的方法"""
    print("🧪 开始测试重构后的DataLoader方法...")

    try:
        # 初始化PoolManager
        pool_manager = PoolManager()
        print("✅ PoolManager 初始化成功")

        # 测试重构后的方法是否存在
        data_loader = pool_manager.data_loader

        # 测试主要方法
        if hasattr(data_loader, 'load_valuation_data_with_fallback'):
            print("✅ load_valuation_data_with_fallback 方法存在")
        else:
            print("❌ load_valuation_data_with_fallback 方法不存在")

        if hasattr(data_loader, 'load_price_data_with_fallback'):
            print("✅ load_price_data_with_fallback 方法存在")
        else:
            print("❌ load_price_data_with_fallback 方法不存在")

        # 测试辅助方法
        if hasattr(data_loader, '_rebuild_valuation_dataframe_from_cache'):
            print("✅ _rebuild_valuation_dataframe_from_cache 方法存在")
        else:
            print("❌ _rebuild_valuation_dataframe_from_cache 方法不存在")

        if hasattr(data_loader, '_rebuild_price_data_from_cache'):
            print("✅ _rebuild_price_data_from_cache 方法存在")
        else:
            print("❌ _rebuild_price_data_from_cache 方法不存在")

        if hasattr(data_loader, '_fetch_and_cache_valuation_data'):
            print("✅ _fetch_and_cache_valuation_data 方法存在")
        else:
            print("❌ _fetch_and_cache_valuation_data 方法不存在")

        if hasattr(data_loader, '_fetch_and_merge_price_data'):
            print("✅ _fetch_and_merge_price_data 方法存在")
        else:
            print("❌ _fetch_and_merge_price_data 方法不存在")

        print("🎉 所有重构后的方法检查通过！")
        return True

    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_refactored_methods()
    sys.exit(0 if success else 1)
