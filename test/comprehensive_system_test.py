#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
复杂Python代码测试脚本
测试各种潜在问题：编码、路径、导入、文件操作、网络、异常处理等
"""

import os
import sys
import json
import tempfile
import urllib.request
from pathlib import Path
import subprocess

def test_encoding_issues():
    """测试编码问题"""
    print("=== 测试编码问题 ===")

    # 中文字符串处理
    chinese_text = "测试中文字符串处理"
    print(f"中文字符串: {chinese_text}")

    # 文件读写编码
    try:
        with open('test_encoding.txt', 'w', encoding='utf-8') as f:
            f.write(chinese_text)
        print("✓ UTF-8文件写入成功")

        with open('test_encoding.txt', 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"✓ UTF-8文件读取成功: {content}")

        # 尝试用错误编码读取
        try:
            with open('test_encoding.txt', 'r', encoding='ascii') as f:
                content = f.read()
        except UnicodeDecodeError as e:
            print(f"⚠ 预期的编码错误: {e}")

    except Exception as e:
        print(f"✗ 编码测试失败: {e}")
    finally:
        if os.path.exists('test_encoding.txt'):
            os.remove('test_encoding.txt')

def test_path_issues():
    """测试路径问题"""
    print("\n=== 测试路径问题 ===")

    # 长路径测试
    try:
        long_path = Path.cwd() / "very" / "long" / "path" / "test" / "file.txt"
        long_path.parent.mkdir(parents=True, exist_ok=True)

        with open(long_path, 'w', encoding='utf-8') as f:
            f.write("长路径测试")
        print("✓ 长路径创建成功")

        # 读取测试
        with open(long_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"✓ 长路径读取成功: {content}")

    except Exception as e:
        print(f"✗ 长路径测试失败: {e}")
    finally:
        if long_path.exists():
            long_path.unlink()
            # 清理空目录
            for parent in long_path.parents:
                if parent != Path.cwd() and not any(parent.iterdir()):
                    parent.rmdir()

    # 特殊字符路径测试
    try:
        special_path = Path.cwd() / "test folder (特殊字符)" / "file[1].txt"
        special_path.parent.mkdir(parents=True, exist_ok=True)

        with open(special_path, 'w', encoding='utf-8') as f:
            f.write("特殊字符路径测试")
        print("✓ 特殊字符路径创建成功")

    except Exception as e:
        print(f"✗ 特殊字符路径测试失败: {e}")
    finally:
        if special_path.exists():
            special_path.unlink()
        if special_path.parent.exists():
            try:
                special_path.parent.rmdir()
            except OSError:
                # 目录不为空，跳过删除
                pass

def test_import_issues():
    """测试导入问题"""
    print("\n=== 测试导入问题 ===")

    # 尝试导入可能不存在的模块
    try:
        import nonexistent_module
        print("✓ 非预期模块导入成功")
    except ImportError as e:
        print(f"⚠ 预期的导入错误: {e}")

    # 相对导入测试
    try:
        # 创建临时模块
        temp_dir = Path(tempfile.mkdtemp())
        module_path = temp_dir / "test_module.py"

        with open(module_path, 'w', encoding='utf-8') as f:
            f.write('def test_function():\n    return "测试模块"')

        # 添加到路径并导入
        sys.path.insert(0, str(temp_dir))
        import test_module
        result = test_module.test_function()
        print(f"✓ 动态模块导入成功: {result}")

    except Exception as e:
        print(f"✗ 动态导入测试失败: {e}")
    finally:
        if 'test_module' in sys.modules:
            del sys.modules['test_module']
        if temp_dir in sys.path:
            sys.path.remove(str(temp_dir))
        if module_path.exists():
            module_path.unlink()
        # 安全删除临时目录
        if temp_dir.exists():
            try:
                # 递归清理目录中的所有文件和子目录
                import shutil
                shutil.rmtree(temp_dir)
                print(f"✓ 临时目录已清理: {temp_dir}")
            except Exception as e:
                print(f"⚠ 临时目录清理警告: {e}")
                # 如果shutil失败，尝试手动递归删除
                try:
                    def remove_tree(path):
                        """递归删除目录树"""
                        for item in path.iterdir():
                            if item.is_file():
                                item.unlink()
                            elif item.is_dir():
                                remove_tree(item)
                                item.rmdir()
                        path.rmdir()
                    
                    remove_tree(temp_dir)
                    print(f"✓ 临时目录已手动清理: {temp_dir}")
                except Exception as e2:
                    print(f"❌ 临时目录清理失败: {e2}")

def test_network_issues():
    """测试网络问题"""
    print("\n=== 测试网络问题 ===")

    # HTTP请求测试
    try:
        with urllib.request.urlopen('http://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"✓ HTTP请求成功: {data['url']}")
    except Exception as e:
        print(f"✗ HTTP请求失败: {e}")

    # HTTPS请求测试
    try:
        with urllib.request.urlopen('https://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"✓ HTTPS请求成功: {data['url']}")
    except Exception as e:
        print(f"✗ HTTPS请求失败: {e}")

def test_subprocess_issues():
    """测试子进程问题"""
    print("\n=== 测试子进程问题 ===")

    # PowerShell命令执行
    try:
        result = subprocess.run(['powershell', '-Command', 'Get-Date'],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode == 0:
            print(f"✓ PowerShell命令执行成功: {result.stdout.strip()}")
        else:
            print(f"⚠ PowerShell命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"✗ PowerShell测试失败: {e}")

    # 系统命令执行
    try:
        result = subprocess.run(['cmd', '/c', 'echo', 'Hello World'],
                              capture_output=True, text=True, encoding='cp936', timeout=10)
        if result.returncode == 0:
            print(f"✓ 系统命令执行成功: {result.stdout.strip()}")
        else:
            print(f"⚠ 系统命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"✗ 系统命令测试失败: {e}")

def test_memory_performance():
    """测试内存和性能问题"""
    print("\n=== 测试内存和性能问题 ===")

    # 大数据处理测试
    try:
        large_list = list(range(1000000))  # 100万元素
        result = sum(large_list)
        print(f"✓ 大数据处理成功: 总和 = {result}")
    except Exception as e:
        print(f"✗ 大数据处理失败: {e}")

    # 递归深度测试
    try:
        def recursive_function(depth=0):
            if depth > 100:
                return depth
            return recursive_function(depth + 1)

        result = recursive_function()
        print(f"✓ 递归测试成功: 深度 = {result}")
    except RecursionError as e:
        print(f"⚠ 预期的递归深度错误: {e}")

def main():
    """主测试函数"""
    print("开始复杂Python代码测试...")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")

    test_encoding_issues()
    test_path_issues()
    test_import_issues()
    test_network_issues()
    test_subprocess_issues()
    test_memory_performance()

    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()
