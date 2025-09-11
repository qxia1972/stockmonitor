# -*- coding: utf-8 -*-
"""
Stock Monitor Program Management Tool

Command-line interface for monitoring and managing running stockmonitor.py processes.
Provides comprehensive process monitoring, resource tracking and system management capabilities.
"""

import sys
import argparse
import psutil
import time
import os
import threading
from datetime import datetime
import matplotlib.pyplot as plt
from modules.log_manager import get_monitor_logger

# Initialize monitor logger
logger = get_monitor_logger()

class StockMonitorTool:
    """
    Stock monitor program management and monitoring tool
    
    Provides comprehensive monitoring capabilities for stockmonitor.py processes
    including resource tracking, performance analysis and process management.
    """
    
    def __init__(self):
        self.target_process_name = "python"
        self.memory_data = []
        self.time_data = []
        self.is_monitoring = False
        
    def find_stockmonitor_process(self):
        """
        Find running stockmonitor.py process
        
        Searches through all running Python processes to locate
        stockmonitor.py instances.
        
        Returns:
            psutil.Process: Process object if found, None otherwise
        """
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and any('stockmonitor.py' in cmd for cmd in cmdline):
                    return proc
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None
    
    def get_process_info(self):
        """
        Get detailed process information
        
        Retrieves comprehensive information about the stockmonitor process
        including PID, status, resource usage and system metrics.
        
        Returns:
            dict: Process information dictionary or None if not found
        """
        process = self.find_stockmonitor_process()
        if not process:
            return None
            
        try:
            info = {
                'pid': process.pid,
                'name': process.name(),
                'status': process.status(),
                'create_time': datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S'),
                'cpu_percent': process.cpu_percent(),
                'memory_percent': process.memory_percent(),
                'memory_info': process.memory_info(),
                'num_threads': process.num_threads(),
                'connections': len(process.connections()) if hasattr(process, 'connections') else 0
            }
            return info
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None

def show_status():
    """显示股票监控程序状态"""
    logger.info("=" * 60)
    logger.info("股票监控程序状态")
    logger.info("=" * 60)
    
    tool = StockMonitorTool()
    process_info = tool.get_process_info()
    
    if not process_info:
        logger.warning("✗ 未找到正在运行的stockmonitor.py程序")
        logger.info("启动建议:")
        logger.info("  1. 运行命令: python stockmonitor.py")
        logger.info("  2. 或使用: python start_system.py")
        return
    
    logger.info("程序信息:")
    logger.info(f"  进程ID: {process_info['pid']}")
    logger.info(f"  程序状态: {process_info['status']}")
    logger.info(f"  启动时间: {process_info['create_time']}")
    logger.info(f"  线程数: {process_info['num_threads']}")
    logger.info(f"  网络连接: {process_info['connections']}")
    
    logger.info("内存使用:")
    memory_info = process_info['memory_info']
    logger.info(f"  物理内存(RSS): {memory_info.rss / 1024 / 1024:.1f} MB")
    logger.info(f"  虚拟内存(VMS): {memory_info.vms / 1024 / 1024:.1f} MB")
    logger.info(f"  内存占用率: {process_info['memory_percent']:.1f}%")
    
    logger.info("CPU使用:")
    logger.info(f"  CPU占用率: {process_info['cpu_percent']:.1f}%")

def monitor_memory():
    """实时监控内存使用"""
    logger.info("=" * 60)
    logger.info("股票程序内存实时监控")
    logger.info("=" * 60)
    
    tool = StockMonitorTool()
    process = tool.find_stockmonitor_process()
    
    if not process:
        logger.warning("✗ 未找到正在运行的stockmonitor.py程序")
        return
    
    logger.info(f"找到进程 PID: {process.pid}")
    logger.info("按 Ctrl+C 停止监控")
    
    try:
        start_time = time.time()
        
        logger.info(f"{'时间':>8} {'RSS(MB)':>10} {'VMS(MB)':>10} {'占用率%':>8} {'CPU%':>6} {'状态':>12}")
        logger.info("-" * 70)
        
        while True:
            try:
                memory_info = process.memory_info()
                memory_percent = process.memory_percent()
                cpu_percent = process.cpu_percent()
                status = process.status()
                
                elapsed = time.time() - start_time
                rss_mb = memory_info.rss / 1024 / 1024
                vms_mb = memory_info.vms / 1024 / 1024
                
                logger.info(f"{elapsed:8.0f} {rss_mb:10.1f} {vms_mb:10.1f} {memory_percent:8.1f} {cpu_percent:6.1f} {status:>12}")
                
                time.sleep(5)  # 每5秒更新一次
                
            except psutil.NoSuchProcess:
                logger.warning("进程已结束")
                break
            except KeyboardInterrupt:
                logger.info("监控已停止")
                break
                
    except psutil.NoSuchProcess:
        logger.error(f"进程不存在")
    except Exception as e:
        logger.error(f"监控过程中出错: {e}")

def analyze_program():
    """分析正在运行的程序"""
    logger.info("=" * 60)
    logger.info("股票程序运行分析")
    logger.info("=" * 60)
    
    tool = StockMonitorTool()
    process = tool.find_stockmonitor_process()
    
    if not process:
        logger.warning("✗ 未找到正在运行的stockmonitor.py程序")
        return
    
    try:
        # 基本信息
        logger.info("基本信息:")
        logger.info(f"  进程ID: {process.pid}")
        logger.info(f"  进程名: {process.name()}")
        logger.info(f"  状态: {process.status()}")
        logger.info(f"  启动时间: {datetime.fromtimestamp(process.create_time()).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 资源使用
        memory_info = process.memory_info()
        logger.info("资源使用:")
        logger.info(f"  物理内存(RSS): {memory_info.rss / 1024 / 1024:.1f} MB")
        logger.info(f"  虚拟内存(VMS): {memory_info.vms / 1024 / 1024:.1f} MB")
        logger.info(f"  内存占用率: {process.memory_percent():.1f}%")
        logger.info(f"  CPU占用率: {process.cpu_percent():.1f}%")
        
        # 线程信息
        logger.info(f"线程信息:")
        logger.info(f"  线程数: {process.num_threads()}")
        
        # 文件句柄
        try:
            open_files = process.open_files()
            logger.info(f"  打开文件数: {len(open_files)}")
            if open_files:
                logger.info("  主要文件:")
                for f in open_files[:5]:  # 显示前5个文件
                    logger.info(f"    {f.path}")
        except psutil.AccessDenied:
            logger.warning("  文件信息: 无权限访问")
        
        # 网络连接
        try:
            connections = process.connections()
            logger.info(f"  网络连接数: {len(connections)}")
            if connections:
                logger.info("  连接信息:")
                for conn in connections[:3]:  # 显示前3个连接
                    logger.info(f"    {conn.laddr} -> {conn.raddr if conn.raddr else 'N/A'} ({conn.status})")
        except psutil.AccessDenied:
            logger.warning("  网络信息: 无权限访问")
        
        # 环境变量
        try:
            environ = process.environ()
            logger.info(f"环境变量 (关键):")
            key_vars = ['PYTHONPATH', 'PATH', 'CONDA_DEFAULT_ENV']
            for var in key_vars:
                if var in environ:
                    value = environ[var]
                    if len(value) > 60:
                        value = value[:60] + "..."
                    logger.info(f"  {var}: {value}")
        except psutil.AccessDenied:
            logger.warning("  环境变量: 无权限访问")
            
    except psutil.NoSuchProcess:
        logger.warning("进程已结束")
    except Exception as e:
        logger.error(f"分析过程中出错: {e}")

def profile_memory():
    """内存使用性能分析"""
    logger.info("=" * 60)
    logger.info("内存使用性能分析")
    logger.info("=" * 60)
    
    tool = StockMonitorTool()
    process = tool.find_stockmonitor_process()
    
    if not process:
        logger.warning("✗ 未找到正在运行的stockmonitor.py程序")
        return
    
    logger.info(f"找到进程 PID: {process.pid}")
    logger.info("开始性能分析，持续60秒...")
    
    try:
        # 收集数据
        memory_data = []
        cpu_data = []
        time_data = []
        
        start_time = time.time()
        duration = 60  # 分析60秒
        interval = 2   # 每2秒采样
        
        while time.time() - start_time < duration:
            try:
                current_time = time.time() - start_time
                memory_info = process.memory_info()
                cpu_percent = process.cpu_percent()
                
                memory_mb = memory_info.rss / 1024 / 1024
                
                time_data.append(current_time)
                memory_data.append(memory_mb)
                cpu_data.append(cpu_percent)
                
                logger.info(f"[{current_time:5.1f}s] 内存: {memory_mb:6.1f}MB, CPU: {cpu_percent:5.1f}%")
                
                time.sleep(interval)
                
            except psutil.NoSuchProcess:
                logger.warning("进程已结束")
                break
        
        # 生成分析报告
        if memory_data:
            logger.info("分析报告:")
            logger.info(f"  采样点数: {len(memory_data)}")
            logger.info(f"  内存使用:")
            logger.info(f"    平均值: {sum(memory_data)/len(memory_data):.1f} MB")
            logger.info(f"    最大值: {max(memory_data):.1f} MB")
            logger.info(f"    最小值: {min(memory_data):.1f} MB")
            logger.info(f"    变化幅度: {max(memory_data) - min(memory_data):.1f} MB")
            
            logger.info(f"  CPU使用:")
            logger.info(f"    平均值: {sum(cpu_data)/len(cpu_data):.1f}%")
            logger.info(f"    最大值: {max(cpu_data):.1f}%")
            
    except Exception as e:
        logger.error(f"性能分析失败: {e}")

def kill_process():
    """终止股票监控程序"""
    logger.info("=" * 60)
    logger.info("终止股票监控程序")
    logger.info("=" * 60)
    
    tool = StockMonitorTool()
    process = tool.find_stockmonitor_process()
    
    if not process:
        logger.warning("✗ 未找到正在运行的stockmonitor.py程序")
        return
    
    try:
        logger.info(f"找到进程 PID: {process.pid}")
        
        # 确认操作
        logger.warning("⚠️  警告: 这将强制终止股票监控程序")
        confirm = input("确认执行? (y/N): ").strip().lower()
        
        if confirm in ['y', 'yes']:
            process.terminate()
            logger.info("✓ 终止信号已发送")
            
            # 等待进程结束
            try:
                process.wait(timeout=10)
                logger.info("✓ 程序已正常终止")
            except psutil.TimeoutExpired:
                logger.warning("程序未响应，执行强制杀死...")
                process.kill()
                logger.info("✓ 程序已强制终止")
        else:
            logger.info("操作已取消")
            
    except psutil.NoSuchProcess:
        logger.warning("进程已结束")
    except Exception as e:
        logger.error(f"终止程序失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='股票监控程序管理工具')
    parser.add_argument('action', choices=['status', 'monitor', 'analyze', 'profile', 'kill'], 
                       help='操作类型: status(查看状态), monitor(实时监控), analyze(程序分析), profile(性能分析), kill(终止程序)')
    
    args = parser.parse_args()
    
    if args.action == 'status':
        show_status()
    elif args.action == 'monitor':
        monitor_memory()
    elif args.action == 'analyze':
        analyze_program()
    elif args.action == 'profile':
        profile_memory()
    elif args.action == 'kill':
        kill_process()

if __name__ == "__main__":
    main()
