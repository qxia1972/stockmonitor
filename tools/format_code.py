#!/usr/bin/env python3
"""
代码格式化和清理工具
"""
import os
import sys
import subprocess
import shutil
from datetime import datetime

def create_backup(file_path):
    """创建备份"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup/{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_path = f"{backup_dir}/{os.path.basename(file_path)}"
    shutil.copy2(file_path, backup_path)
    print(f"✅ 备份创建: {backup_path}")
    return backup_path

def format_with_black(file_path):
    """使用black格式化代码"""
    print("🎨 使用black格式化代码...")
    home_dir = os.path.expanduser("~")
    cmd = [f"{home_dir}/.local/bin/black", "--line-length=100", file_path]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ black格式化完成")
        else:
            print("⚠️  black格式化失败:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ black执行失败: {e}")

def sort_imports(file_path):
    """使用isort整理导入"""
    print("📦 使用isort整理导入...")
    home_dir = os.path.expanduser("~")
    cmd = [f"{home_dir}/.local/bin/isort", "--profile=black", file_path]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ isort整理完成")
        else:
            print("⚠️  isort整理失败:")
            print(result.stderr)
    except Exception as e:
        print(f"❌ isort执行失败: {e}")

def main():
    if len(sys.argv) < 2:
        print("用法: python format_code.py <文件名>")
        return
    
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return
    
    print(f"🔧 格式化文件: {file_path}")
    
    # 创建备份
    create_backup(file_path)
    
    # 格式化代码
    format_with_black(file_path)
    sort_imports(file_path)
    
    print("🎉 代码格式化完成！")

if __name__ == "__main__":
    main()
