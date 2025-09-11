#!/usr/bin/env python3
"""
安全代码编辑工具
自动备份和验证代码完整性
"""
import os
import sys
import shutil
from datetime import datetime
import subprocess

def create_backup(file_path):
    """创建带时间戳的备份"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"backup/{timestamp}"
    os.makedirs(backup_dir, exist_ok=True)
    
    backup_path = f"{backup_dir}/{os.path.basename(file_path)}"
    shutil.copy2(file_path, backup_path)
    print(f"✅ 备份创建: {backup_path}")
    return backup_path

def validate_syntax(file_path):
    """验证Python语法"""
    try:
        result = subprocess.run([sys.executable, '-m', 'py_compile', file_path], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ 语法验证通过")
            return True
        else:
            print("❌ 语法错误:")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        return False

def run_code_quality_checks(file_path):
    """运行代码质量检查"""
    print("🔍 运行代码质量检查...")
    
    home_dir = os.path.expanduser("~")
    checks = [
        ("flake8", [f"{home_dir}/.local/bin/flake8", "--max-line-length=100", file_path]),
        ("black", [f"{home_dir}/.local/bin/black", "--check", "--diff", "--line-length=100", file_path]),
        ("isort", [f"{home_dir}/.local/bin/isort", "--check-only", "--diff", file_path])
    ]
    
    for name, cmd in checks:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ {name}: 通过")
            else:
                print(f"⚠️  {name}: 发现问题")
                # 只显示前几行输出
                lines = result.stdout.split('\n')[:10]
                for line in lines:
                    if line.strip():
                        print(f"   {line}")
                if len(result.stdout.split('\n')) > 10:
                    print("   ... (更多输出被截断)")
        except FileNotFoundError:
            print(f"⚠️  {name}: 工具未找到")
        except Exception as e:
            print(f"⚠️  {name}: 检查失败 - {e}")

def main():
    if len(sys.argv) < 2:
        print("用法: python safe_edit.py <文件名>")
        return
    
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return
    
    print(f"🔧 处理文件: {file_path}")
    
    # 创建备份
    backup_path = create_backup(file_path)
    
    # 验证语法
    if validate_syntax(file_path):
        print("🎉 文件准备就绪，可以安全编辑！")
        # 运行代码质量检查
        run_code_quality_checks(file_path)
    else:
        print("⚠️  请先修复语法错误")

if __name__ == "__main__":
    main()
