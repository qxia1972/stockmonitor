# -*- coding: utf-8 -*-
"""
Stock Monitoring System Startup Script

Integrated startup script for stock monitoring system that coordinates
independent sync tools and main program execution. Provides automated
environment checking, data validation and system initialization.
"""

import sys
import os
import subprocess
import json
import argparse
from pathlib import Path
from datetime import date

# Add project path to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Environment management and auto-switching
from modules.python_manager import EnvironmentManager

# Initialize environment manager first
env_manager = EnvironmentManager()

# 使用公共的环境检查方法，支持命令行模式fallback
env_manager.ensure_environment_with_fallback()

from modules.log_manager import get_system_logger

# Initialize system logger
logger = get_system_logger()

logger.info("🔍 Checking Python environment...")

def check_data():
    """
    Check if today's data files exist and are valid
    
    Validates the presence of required daily data files including
    stock pool data, candidate stocks and filtered results.
    
    Returns:
        bool: True if all required data files exist, False otherwise
    """
    today = date.today().isoformat()
    data_dir = Path("data")
    
    # Define required data files for system operation
    required_files = [
        data_dir / "basic_pool.json",
        data_dir / "watch_pool.json", 
        data_dir / "core_pool.json"
    ]
    
    all_exist = all(f.exists() for f in required_files)
    
    if all_exist:
        # 检查数据日期
        try:
            with open(data_dir / "daily_stock_pool.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
            data_date = data.get('date')
            if data_date == today:
                return True, "今日数据已存在"
            else:
                return False, f"数据日期不匹配: {data_date} (今日: {today})"
        except Exception as e:
            return False, f"数据文件读取失败: {e}"
    else:
        missing_files = [f.name for f in required_files if not f.exists()]
        return False, f"缺少数据文件: {', '.join(missing_files)}"

def run_daily_sync():
    """运行每日同步"""
    logger.info("正在运行每日同步...")
    try:
        result = subprocess.run([
            sys.executable, "stockpool_tool.py", "sync"
        ], capture_output=True, text=True, encoding='utf-8', errors='ignore')

        if result.returncode == 0:
            logger.info("✓ 每日同步完成")
            logger.debug(f"同步输出: {result.stdout.strip()}")
            return True
        else:
            logger.error("✗ 每日同步失败")
            logger.error(f"错误信息: {result.stderr.strip()}")
            return False
    except Exception as e:
        logger.error(f"✗ 同步执行失败: {e}")
        return False

def start_main_program():
    """启动主程序"""
    logger.info("正在启动主程序...")
    try:
        subprocess.run([sys.executable, "stockmonitor.py"])
        return True
    except Exception as e:
        logger.error(f"✗ 主程序启动失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="股票监控系统启动器")
    parser.add_argument("--force-sync", action="store_true", 
                       help="强制执行每日同步（即使今日数据已存在）")
    parser.add_argument("--sync-only", action="store_true",
                       help="仅执行同步，不启动主程序")
    parser.add_argument("--no-sync", action="store_true",
                       help="跳过同步检查，直接启动主程序")
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("股票监控系统启动器")
    logger.info("=" * 60)
    
    # 检查数据状态
    if not args.no_sync:
        data_exists, message = check_data()
        logger.info(f"数据状态检查: {message}")
        
        # 决定是否需要同步
        need_sync = args.force_sync or not data_exists
        
        if need_sync:
            success = run_daily_sync()
            if not success and not args.force_sync:
                logger.warning("⚠ 同步失败，但将尝试使用现有数据启动主程序")
        else:
            logger.info("✓ 使用现有数据")
    
    # 如果只是同步，则结束
    if args.sync_only:
        logger.info("仅同步模式，任务完成")
        return
    
    # 启动主程序
    logger.info("=" * 60)
    start_main_program()

if __name__ == "__main__":
    main()
