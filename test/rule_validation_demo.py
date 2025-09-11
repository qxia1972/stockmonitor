#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AI规则验证系统演示脚本
演示如何使用ai_config.py中的验证功能
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent  # 上级目录
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

def demo_basic_validation():
    """演示基本验证功能"""
    print("=== AI规则验证系统演示 ===\n")

    try:
        import ai_config
        print("1. 显示环境信息和规则...")
        ai_config.inform_ai_environment_and_rules()
    except ImportError as e:
        print(f"导入失败: {e}")
        return

def demo_specific_checks():
    """演示专项检查功能"""
    print("\n2. 演示专项检查功能...")

    import ai_config
    import json

    # 检查文件路径问题
    print("\n--- 检查文件路径问题 ---")
    path_results = ai_config.check_specific_issue('paths')
    print(f"发现问题: {sum(1 for v in path_results.values() if isinstance(v, bool) and v)}")
    if path_results.get('recommendations'):
        print("建议:")
        for rec in path_results['recommendations']:
            print(f"  - {rec}")

    # 检查PowerShell问题
    print("\n--- 检查PowerShell问题 ---")
    ps_results = ai_config.check_specific_issue('powershell')
    print(f"发现问题: {sum(1 for v in ps_results.values() if isinstance(v, bool) and v)}")

def demo_comprehensive_validation():
    """演示全面验证功能"""
    print("\n3. 运行全面验证...")

    import ai_config

    print("开始全面验证（这可能需要一些时间）...")
    results = ai_config.validate_rules()

    print("\n验证完成！结果已保存到 test/validation_results.json")
    # 显示汇总信息
    total_issues = 0
    for category, result in results.items():
        if isinstance(result, dict) and 'error' not in result:
            issues = sum(1 for k, v in result.items()
                        if k != 'recommendations' and isinstance(v, bool) and v)
            total_issues += issues
            print(f"{category}: {issues} 个问题")

    print(f"\n总计发现问题: {total_issues}")

def show_usage_examples():
    """显示使用示例"""
    print("\n4. 使用示例")
    print("=" * 50)

    examples = [
        "# 基本环境和规则通知",
        "from ai_config import inform_ai_environment_and_rules",
        "inform_ai_environment_and_rules()",
        "",
        "# 快速验证所有规则",
        "from ai_config import validate_rules",
        "results = validate_rules()",
        "",
        "# 检查特定问题类型",
        "from ai_config import check_specific_issue",
        "path_issues = check_specific_issue('paths')  # 'powershell', 'encoding', 'paths'",
        "",
        "# 专项检测",
        "from ai_config import detect_file_path_issues",
        "issues = detect_file_path_issues()",
        "",
        "# 在代码中使用验证结果",
        "if issues['wrong_test_directory']:",
        "    print('警告：测试文件位置错误')",
    ]

    for example in examples:
        print(example)

def main():
    """主演示函数"""
    print("AI规则验证系统演示")
    print("=" * 50)

def main():
    """主演示函数"""
    print("AI规则验证系统演示")
    print("=" * 50)

    try:
        # 演示各个功能
        demo_basic_validation()
        demo_specific_checks()
        demo_comprehensive_validation()
        show_usage_examples()

        print("\n" + "=" * 50)
        print("演示完成！")
        print("\n提示：")
        print("- 检测结果保存在 test/validation_results.json")
        print("- 可以定期运行验证来确保规则遵守")
        print("- 建议在每次重大修改后运行全面验证")

    except Exception as e:
        print(f"\n演示过程中出现错误: {e}")
        print("请确保 ai_config.py 文件存在且格式正确")

if __name__ == "__main__":
    main()
