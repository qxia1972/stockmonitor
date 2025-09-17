#!/usr/bin/env python3
"""
模块导入验证脚本
验证所有重构后的模块都能正常导入
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_module_imports():
    """测试所有模块的导入"""
    print("🔍 开始验证模块导入...")

    test_cases = [
        # 核心模块
        ("modules.rqdatac_data_loader", "RQDatacDataLoader"),
        ("modules.data_model", "data_model"),
        ("modules.business_model", "BusinessModel"),

        # 计算模块
        ("modules.compute.data_processor", "DataProcessor"),
        ("modules.compute.indicator_calculator", "IndicatorCalculator"),
        ("modules.compute.stock_scorer", "StockScorer"),
        ("modules.compute.parallel_processor", "ParallelProcessor"),

        # 编排模块
        ("modules.orchestration.pipeline_manager", "PipelineManager"),
        ("modules.orchestration.pipeline_definitions", "daily_data_sync_job"),

        # 查询模块
        ("modules.query.query_engine", "QueryEngine"),

        # 存储模块
        ("modules.storage.parquet_manager", "ParquetManager"),
        ("modules.storage.schema_manager", "SchemaManager"),
    ]

    success_count = 0
    total_count = len(test_cases)

    for module_name, class_name in test_cases:
        try:
            module = __import__(module_name, fromlist=[class_name] if class_name else [])
            if class_name:
                obj = getattr(module, class_name, None)
                if obj:
                    print(f"✅ {module_name}.{class_name} - 导入成功")
                    success_count += 1
                else:
                    print(f"❌ {module_name}.{class_name} - 类不存在")
            else:
                print(f"✅ {module_name} - 模块导入成功")
                success_count += 1
        except ImportError as e:
            print(f"❌ {module_name} - 导入失败: {e}")
        except Exception as e:
            print(f"⚠️  {module_name} - 其他错误: {e}")

    print(f"\n📊 导入验证结果: {success_count}/{total_count} 成功")
    return success_count == total_count

def test_tools_scripts():
    """测试tools脚本的基本导入"""
    print("\n🔧 开始验证tools脚本导入...")

    tools_scripts = [
        "tools.run_dual_adjustment_scoring",
        "tools.run_scoring_task",
        "tools.sync_all_data",
        "tools.sync_data_partitioned",
        "tools.sync_pre_adjusted_data",
        "tools.rqdatac_adjustment_guide",
    ]

    success_count = 0
    total_count = len(tools_scripts)

    for script_name in tools_scripts:
        try:
            __import__(script_name)
            print(f"✅ {script_name} - 导入成功")
            success_count += 1
        except ImportError as e:
            print(f"❌ {script_name} - 导入失败: {e}")
        except Exception as e:
            print(f"⚠️  {script_name} - 其他错误: {e}")

    print(f"\n📊 Tools脚本验证结果: {success_count}/{total_count} 成功")
    return success_count == total_count

if __name__ == "__main__":
    print("🚀 开始模块和脚本验证...\n")

    modules_ok = test_module_imports()
    tools_ok = test_tools_scripts()

    print("\n🎯 验证总结:")
    print(f"模块导入: {'✅ 通过' if modules_ok else '❌ 失败'}")
    print(f"Tools脚本: {'✅ 通过' if tools_ok else '❌ 失败'}")

    if modules_ok and tools_ok:
        print("\n🎉 所有验证通过！项目结构梳理完成！")
        sys.exit(0)
    else:
        print("\n⚠️  部分验证失败，请检查相关模块")
        sys.exit(1)
