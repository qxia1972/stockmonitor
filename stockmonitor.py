"""
Stock Monitor System - MVP Architecture Version
Real-time stock monitoring and analysis system using Model-View-Presenter pattern.
Provides comprehensive technical analysis with interactive GUI interface.
"""

# First perform environment check and auto-switching
import sys
import os

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

# Main module imports
import sys
import os
import tkinter as tk
from tkinter import ttk
from datetime import datetime
from typing import Dict, List, Optional, Protocol, Set
import argparse
import psutil
import time
import threading

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.patches import Rectangle

# Import simplified data interface modules
from modules.data_interface import (
    get_stock_kline, calculate_basic_indicators
)

# Import logger
from modules.log_manager import get_monitor_logger

# Initialize monitor logger
logger = get_monitor_logger()


# ========================================================
#               Stock Monitor Tool Class
# ========================================================

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
        logger.info("  2. 或使用: python stockmonitor.py --monitor")
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


# ========================================================
#                    Model Layer - Data Models
# ========================================================

class StockData:
    """
    Stock data model for storing essential stock information
    
    Encapsulates basic stock attributes including code, name, price, 
    market capitalization and analysis score.
    """
    def __init__(self, code: str, name: str = "", price: float = 0.0, 
                 market_cap: float = 0.0, score: float = 0.0):
        self.code = code
        self.name = name or code
        self.price = price
        self.market_cap = market_cap
        self.score = score


class EventData:
    """
    Event data model for system events and notifications
    
    Stores event information including timestamp, type and message.
    """
    def __init__(self, time: str, code: str, event: str):
        self.time = time
        self.code = code
        self.event = event


class KLineData:
    """K线数据模型"""
    def __init__(self, timestamp: datetime, open_price: float, 
                 high_price: float, low_price: float, close_price: float, volume: int):
        self.timestamp = timestamp
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume


class StockMonitorModel:
    """股票监控数据模型"""
    
    def __init__(self):
        self._watch_stocks: List[StockData] = []  # 观察层股票
        self._core_stocks: List[StockData] = []   # 核心层股票
        self._basic_stocks: List[StockData] = []  # 基础层股票
        self._events: List[EventData] = []
        self._current_stock_events: List[EventData] = []
        self._current_stock_code: Optional[str] = None
        
    def load_stock_pools(self):
        """加载股票池数据"""
        try:
            from stockpool import PoolManager
            stock_manager = PoolManager()
            pools = stock_manager.load_all_pools()  # type: ignore
            
            # 清空现有数据
            self._watch_stocks.clear()
            self._core_stocks.clear()
            self._basic_stocks.clear()
            
            # 加载基础层股票
            if pools and isinstance(pools, dict) and 'basic_layer' in pools:
                basic_layer = pools['basic_layer']
                if hasattr(basic_layer, 'iterrows') and hasattr(basic_layer, 'empty') and not isinstance(basic_layer, list):  # DataFrame
                    for _, stock in basic_layer.iterrows():  # type: ignore
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._basic_stocks.append(stock_data)
                elif isinstance(basic_layer, list):  # List of dicts
                    for stock in basic_layer:
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._basic_stocks.append(stock_data)
                    
            # 加载观察层股票
            if pools and isinstance(pools, dict) and 'watch_layer' in pools:
                watch_layer = pools['watch_layer']
                if hasattr(watch_layer, 'iterrows') and hasattr(watch_layer, 'empty') and not isinstance(watch_layer, list):  # DataFrame
                    for _, stock in watch_layer.iterrows():  # type: ignore
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._watch_stocks.append(stock_data)
                elif isinstance(watch_layer, list):  # List of dicts
                    for stock in watch_layer:
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._watch_stocks.append(stock_data)
                    
            # 加载核心层股票
            if pools and isinstance(pools, dict) and 'core_layer' in pools:
                core_layer = pools['core_layer']
                if hasattr(core_layer, 'iterrows') and hasattr(core_layer, 'empty') and not isinstance(core_layer, list):  # DataFrame
                    for _, stock in core_layer.iterrows():  # type: ignore
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._core_stocks.append(stock_data)
                elif isinstance(core_layer, list):  # List of dicts
                    for stock in core_layer:
                        stock_data = StockData(
                            code=str(stock['stock_code']),
                            name=str(stock.get('name', stock['stock_code'])),
                            price=float(stock.get('current_price', 0)),
                            market_cap=float(stock.get('market_cap', 0)),
                            score=float(stock['score'])
                        )
                        self._core_stocks.append(stock_data)
                    
            return True
        except Exception as e:
            print(f"加载股票池数据失败: {e}")
            return False
    
    def get_monitor_pool(self) -> Set[str]:
        """获取监控股票池（返回核心池股票代码）"""
        return {stock.code for stock in self._core_stocks}
    
    def get_pool_by_type(self, pool_type: str) -> Set[str]:
        """根据池类型获取股票池"""
        if pool_type == "core":
            return {stock.code for stock in self._core_stocks}
        elif pool_type == "watch":
            return {stock.code for stock in self._watch_stocks}
        elif pool_type == "basic":
            return {stock.code for stock in self._basic_stocks}
        else:
            return set()
    
    def get_watch_stocks(self) -> List[StockData]:
        """获取观察层股票"""
        return self._watch_stocks.copy()
    
    def get_basic_stocks(self) -> List[StockData]:
        """获取基础层股票"""
        return self._basic_stocks.copy()
    
    def get_core_stocks(self) -> List[StockData]:
        """获取核心层股票"""
        return self._core_stocks.copy()
    
    def get_stock_kline_data(self, stock_code: str, timeframe: str = '60min', periods: int = 200) -> Optional[List[KLineData]]:
        """获取K线数据"""
        try:
            raw_data = get_stock_kline(stock_code, timeframe, periods)
            if raw_data is None or raw_data.empty:
                return None
                
            kline_data = []
            for index, row in raw_data.iterrows():
                # 确保index是datetime类型
                if isinstance(index, str):
                    from datetime import datetime
                    timestamp = datetime.fromisoformat(index.replace('Z', '+00:00'))
                elif hasattr(index, 'to_pydatetime'):  # pandas Timestamp
                    timestamp = index.to_pydatetime()  # type: ignore
                else:
                    timestamp = index if isinstance(index, datetime) else datetime.now()
                kline = KLineData(
                    timestamp=timestamp,  # type: ignore
                    open_price=row['open'],
                    high_price=row['high'],
                    low_price=row['low'],
                    close_price=row['close'],
                    volume=row['volume']
                )
                kline_data.append(kline)
            return kline_data
        except Exception as e:
            print(f"获取K线数据失败: {e}")
            return None
    
    def get_volatility_data(self, stock_code: str) -> Optional[List[float]]:
        """获取波动性数据"""
        try:
            indicators = calculate_basic_indicators(stock_code)
            if indicators and 'volatility' in indicators:
                return indicators['volatility']
            return None
        except Exception as e:
            print(f"获取波动性数据失败: {e}")
            return None
    
    def load_all_events(self, stock_pool: Optional[Set[str]] = None):
        """加载所有股票池事件"""
        try:
            from modules.data_interface import get_indicators
            self._events.clear()
            
            # 如果没有指定股票池，使用核心池作为默认
            target_pool = stock_pool if stock_pool is not None else {stock.code for stock in self._core_stocks}
            
            indicators = get_indicators()
            if indicators is not None:
                all_events = indicators.get_all_events(target_pool)
                for event in all_events:  # type: ignore
                    event_data = EventData(
                        time=event.get('time', ''),
                        code=event.get('code', ''),
                        event=event.get('description', '')
                    )
                    self._events.append(event_data)
        except Exception as e:
            print(f"加载事件数据失败: {e}")
    
    def load_stock_events(self, stock_code: str):
        """加载指定股票的事件"""
        try:
            from modules.data_interface import get_indicators
            self._current_stock_events.clear()
            self._current_stock_code = stock_code
            indicators = get_indicators()
            if indicators is not None:
                stock_events = indicators.get_stock_events(stock_code)
                for event in stock_events:  # type: ignore
                    event_data = EventData(
                        time=event.get('time', ''),
                        code=stock_code,
                        event=event.get('description', '')
                    )
                    self._current_stock_events.append(event_data)
        except Exception as e:
            print(f"加载股票事件失败: {e}")
    
    def get_all_events(self) -> List[EventData]:
        """获取所有事件"""
        return self._events.copy()
    
    def get_current_stock_events(self) -> List[EventData]:
        """获取当前股票事件"""
        return self._current_stock_events.copy()
    
    def get_current_stock_code(self) -> Optional[str]:
        """获取当前选中的股票代码"""
        return self._current_stock_code


# ========================================================
#                        View 层
# ========================================================

class IStockMonitorView(Protocol):
    """股票监控视图接口"""
    
    def show_stock_list(self, stocks: Dict[str, int]) -> None:
        """显示股票列表"""
        ...
    
    def update_chart(self, stock_code: str, kline_data: List[KLineData], 
                    volatility_data: Optional[List[float]]) -> None:
        """更新图表"""
        ...
    
    def show_all_events(self, events: List[EventData]) -> None:
        """显示所有事件"""
        ...
    
    def show_stock_events(self, events: List[EventData]) -> None:
        """显示股票事件"""
        ...
    
    def set_stock_selection_callback(self, callback) -> None:
        """设置股票选择回调"""
        ...
    
    def set_pool_change_callback(self, callback) -> None:
        """设置池改变回调"""
        ...
    
    def show_status(self, message: str) -> None:
        """显示状态信息"""
        ...
    
    def start(self) -> None:
        """启动视图"""
        ...


class StockMonitorView(IStockMonitorView):
    """股票监控视图实现"""
    
    def __init__(self):
        self._root: Optional[tk.Tk] = None
        self._listbox: Optional[tk.Listbox] = None
        self._fig: Optional[Figure] = None
        self._canvas: Optional[FigureCanvasTkAgg] = None
        self._event_tree: Optional[ttk.Treeview] = None
        self._stock_event_tree: Optional[ttk.Treeview] = None
        self._status_label: Optional[tk.Label] = None
        self._stock_selection_callback = None
        self._pool_change_callback = None
        self._current_data = []
        self._pool_type = None
        self._setup_ui()
    
    def _setup_ui(self):
        """设置用户界面"""
        # 创建主窗口
        self._root = tk.Tk()
        self._root.title('股票监控系统 - MVP架构')
        self._root.geometry('1600x1200')
        
        # 三列布局
        self._setup_left_panel()
        self._setup_center_panel()
        self._setup_right_panel()
        self._setup_status_bar()
    
    def _setup_left_panel(self):
        """设置左侧面板"""
        # 左侧frame：股票池列表(宽度180)
        left_frame = tk.Frame(self._root, width=180, bg='lightgray')
        left_frame.pack(side=tk.LEFT, fill=tk.Y)
        left_frame.pack_propagate(False)
        
        # 股票池标题
        tk.Label(left_frame, text="股票池", font=('SimHei', 12, 'bold'), bg='lightgray').pack(pady=5)
        
        # 池类型选择
        pool_frame = tk.Frame(left_frame, bg='lightgray')
        pool_frame.pack(pady=5)
        
        self._pool_type = tk.StringVar(value="core")
        tk.Radiobutton(pool_frame, text="核心池(5)", variable=self._pool_type, value="core", 
                       bg='lightgray', font=('SimHei', 9), command=self._on_pool_change).pack(anchor='w')
        tk.Radiobutton(pool_frame, text="观察池(50)", variable=self._pool_type, value="watch", 
                       bg='lightgray', font=('SimHei', 9), command=self._on_pool_change).pack(anchor='w')
        tk.Radiobutton(pool_frame, text="基础池(500)", variable=self._pool_type, value="basic", 
                       bg='lightgray', font=('SimHei', 9), command=self._on_pool_change).pack(anchor='w')
        
        # 股票列表
        self._listbox = tk.Listbox(left_frame, width=22, font=('SimHei', 10))
        self._listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self._listbox.bind('<<ListboxSelect>>', self._on_stock_selection)
    
    def _setup_center_panel(self):
        """设置中间面板"""
        # 中间frame：K线图
        center_frame = tk.Frame(self._root, bg='white')
        center_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # matplotlib图表
        self._fig = Figure(figsize=(10, 8))
        self._canvas = FigureCanvasTkAgg(self._fig, master=center_frame)
        self._canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def _setup_right_panel(self):
        """设置右侧面板"""
        # 右侧frame：事件区(宽度600)
        right_frame = tk.Frame(self._root, width=600, bg='lightblue')
        right_frame.pack(side=tk.LEFT, fill=tk.Y)
        right_frame.pack_propagate(False)
        
        # 全股票池事件区域
        tk.Label(right_frame, text="全股票池事件", font=('SimHei', 11, 'bold'), bg='lightblue').pack(pady=5)
        
        event_tree_frame = tk.Frame(right_frame)
        event_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self._event_tree = ttk.Treeview(event_tree_frame, columns=('time', 'code', 'event'), show='headings', height=8)
        self._event_tree.heading('time', text='时间')
        self._event_tree.heading('code', text='代码')
        self._event_tree.heading('event', text='事件')
        self._event_tree.column('time', width=120)
        self._event_tree.column('code', width=120)
        self._event_tree.column('event', width=320)
        
        event_scrollbar = ttk.Scrollbar(event_tree_frame, orient=tk.VERTICAL, command=self._event_tree.yview)
        self._event_tree.configure(yscrollcommand=event_scrollbar.set)
        self._event_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        event_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 当前股票事件区域
        tk.Label(right_frame, text="当前股票事件", font=('SimHei', 11, 'bold'), bg='lightblue').pack(pady=(10,5))
        
        stock_event_tree_frame = tk.Frame(right_frame)
        stock_event_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self._stock_event_tree = ttk.Treeview(stock_event_tree_frame, columns=('time', 'event'), show='headings', height=6)
        self._stock_event_tree.heading('time', text='时间')
        self._stock_event_tree.heading('event', text='事件')
        self._stock_event_tree.column('time', width=120)
        self._stock_event_tree.column('event', width=440)
        
        stock_event_scrollbar = ttk.Scrollbar(stock_event_tree_frame, orient=tk.VERTICAL, command=self._stock_event_tree.yview)
        self._stock_event_tree.configure(yscrollcommand=stock_event_scrollbar.set)
        self._stock_event_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stock_event_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    def _setup_status_bar(self):
        """设置状态栏"""
        self._status_label = tk.Label(self._root, text="就绪", relief=tk.SUNKEN, anchor=tk.W)
        self._status_label.pack(side=tk.BOTTOM, fill=tk.X)
    
    def _on_stock_selection(self, event):
        """股票选择事件处理"""
        if not self._listbox:
            return
        selection = self._listbox.curselection()
        if selection and self._stock_selection_callback:
            index = selection[0]
            stock_text = self._listbox.get(index)
            # 从显示文本中提取股票代码
            stock_code = stock_text.split(' ')[0]
            self._stock_selection_callback(stock_code)
    
    def _on_pool_change(self):
        """池类型改变事件处理"""
        if hasattr(self, '_pool_change_callback') and self._pool_change_callback:
            if self._pool_type:
                pool_type = self._pool_type.get()
                self._pool_change_callback(pool_type)
    
    def show_stock_list(self, stocks: Dict[str, int]) -> None:
        """显示股票列表"""
        if not self._listbox:
            return
        self._listbox.delete(0, tk.END)
        for code, days in stocks.items():
            self._listbox.insert(tk.END, f"{code} ({days}天)")
    
    def update_chart(self, stock_code: str, kline_data: List[KLineData], 
                    volatility_data: Optional[List[float]]) -> None:
        """更新图表"""
        try:
            if not self._fig or not self._canvas:
                return
                
            self._fig.clear()
            
            if not kline_data:
                return
            
            # 创建子图
            ax_k = self._fig.add_subplot(2, 1, 1)
            ax_vol = self._fig.add_subplot(2, 1, 2)
            
            # 绘制K线图
            self._draw_kline_chart(ax_k, stock_code, kline_data)
            
            # 绘制波动性指标
            if volatility_data:
                self._draw_volatility_chart(ax_vol, volatility_data)
            
            self._canvas.draw()
            
        except Exception as e:
            print(f"更新图表时出错: {e}")
    
    def _draw_kline_chart(self, ax, stock_code: str, kline_data: List[KLineData]):
        """绘制K线图"""
        if not kline_data:
            return
            
        x_data = range(len(kline_data))
        
        for i, kline in enumerate(kline_data):
            # K线颜色
            color = 'red' if kline.close_price >= kline.open_price else 'green'
            
            # 绘制影线
            ax.vlines(i, kline.low_price, kline.high_price, colors=color, linewidth=1, alpha=0.8)
            
            # 绘制实体
            if kline.close_price != kline.open_price:
                body_height = abs(kline.close_price - kline.open_price)
                body_bottom = min(kline.close_price, kline.open_price)
                rect = Rectangle((i-0.4, body_bottom), 0.8, body_height, 
                               facecolor=color, alpha=0.8, edgecolor=color)
                ax.add_patch(rect)
            else:
                # 一字线
                ax.hlines(kline.open_price, i-0.4, i+0.4, colors=color, linewidth=2, alpha=0.9)
        
        ax.set_title(f'{stock_code} 60分钟K线图', fontsize=14, fontweight='bold')
        ax.set_xlim(-0.5, len(kline_data)-0.5)
        ax.grid(True, alpha=0.3)
        ax.set_ylabel('价格', fontsize=12)
    
    def _draw_volatility_chart(self, ax, volatility_data: List[float]):
        """绘制波动性图表"""
        if not volatility_data:
            return
            
        vol_x = np.arange(len(volatility_data))
        ax.plot(vol_x, volatility_data, color='blue', linewidth=2, label='波动性指标')
        
        # 零轴
        ax.axhline(y=0, color='black', linestyle='-', alpha=0.8, linewidth=1)
        
        # 保护区
        ax.axhspan(0.15, 0.30, alpha=0.2, color='red', label='上保护区')
        ax.axhspan(-0.30, -0.15, alpha=0.2, color='green', label='下保护区')
        
        # 十周期均线
        if len(volatility_data) >= 10:
            ma10 = np.convolve(volatility_data, np.ones(10)/10, mode='valid')
            ma10_x = np.arange(9, len(volatility_data))
            ax.plot(ma10_x, ma10, color='orange', linewidth=1.5, alpha=0.8, label='10周期均线')
        
        ax.set_title('波动性指标', fontsize=14, fontweight='bold')
        ax.set_xlim(-0.5, len(volatility_data)-0.5)
        ax.grid(True, alpha=0.3)
        ax.set_ylabel('波动性', fontsize=12)
        ax.set_xlabel('时间', fontsize=12)
        ax.legend(fontsize=10)
    
    def show_all_events(self, events: List[EventData]) -> None:
        """显示所有事件"""
        if not self._event_tree:
            return
            
        # 清空现有事件
        for item in self._event_tree.get_children():
            self._event_tree.delete(item)
        
        # 添加新事件
        for event in events:
            self._event_tree.insert('', 'end', values=(
                event.time,
                event.code,
                event.event
            ))
    
    def show_stock_events(self, events: List[EventData]) -> None:
        """显示股票事件"""
        if not self._stock_event_tree:
            return
            
        # 清空现有事件
        for item in self._stock_event_tree.get_children():
            self._stock_event_tree.delete(item)
        
        # 添加新事件
        for event in events:
            self._stock_event_tree.insert('', 'end', values=(
                event.time,
                event.event
            ))
    
    def set_stock_selection_callback(self, callback) -> None:
        """设置股票选择回调"""
        self._stock_selection_callback = callback
    
    def set_pool_change_callback(self, callback) -> None:
        """设置池改变回调"""
        self._pool_change_callback = callback
    
    def show_status(self, message: str) -> None:
        """显示状态信息"""
        if self._status_label:
            self._status_label.config(text=message)
    
    def start(self) -> None:
        """启动视图"""
        if self._root:
            self._root.mainloop()


# ========================================================
#                      Presenter 层
# ========================================================

class StockMonitorPresenter:
    """股票监控展示器"""
    
    def __init__(self, model: StockMonitorModel, view: IStockMonitorView):
        self._model = model
        self._view = view
        self._setup_view_callbacks()
    
    def _setup_view_callbacks(self):
        """设置视图回调"""
        self._view.set_stock_selection_callback(self._on_stock_selected)
        self._view.set_pool_change_callback(self._on_pool_changed)
    
    def start(self):
        """启动应用程序"""
        try:
            self._view.show_status("正在加载股票数据...")
            
            # 加载数据
            if self._model.load_stock_pools():
                self._view.show_status("股票数据加载成功")
                self._refresh_stock_list()
                self._refresh_all_events("core")  # 指定使用核心池
            else:
                self._view.show_status("股票数据加载失败")
            
            # 启动视图
            self._view.start()
            
        except Exception as e:
            self._view.show_status(f"启动失败: {e}")
            print(f"启动应用程序时出错: {e}")
    
    def _refresh_stock_list(self, pool_type: str = "core"):
        """刷新股票列表"""
        try:
            # 根据池类型获取股票
            stock_pool_set = self._model.get_pool_by_type(pool_type)
            # 转换为字典格式以兼容现有界面
            stock_pool = {code: 0 for code in stock_pool_set}  # 值设为0，表示无池龄概念
            self._view.show_stock_list(stock_pool)
            
            # 如果有股票，默认选择第一个
            if stock_pool_set:
                first_stock = next(iter(stock_pool_set))
                self._on_stock_selected(first_stock)
                
        except Exception as e:
            self._view.show_status(f"刷新股票列表失败: {e}")
            print(f"刷新股票列表时出错: {e}")
    
    def _refresh_all_events(self, pool_type: str = "core"):
        """刷新所有事件"""
        try:
            # 根据池类型获取对应的股票池
            target_pool = self._model.get_pool_by_type(pool_type)
            self._model.load_all_events(target_pool)
            events = self._model.get_all_events()
            self._view.show_all_events(events)
        except Exception as e:
            self._view.show_status(f"刷新事件失败: {e}")
            print(f"刷新事件时出错: {e}")
    
    def _on_stock_selected(self, stock_code: str):
        """处理股票选择事件"""
        try:
            self._view.show_status(f"正在加载 {stock_code} 的数据...")
            
            # 加载K线数据
            kline_data = self._model.get_stock_kline_data(stock_code)
            
            # 加载波动性数据
            volatility_data = self._model.get_volatility_data(stock_code)
            
            # 更新图表
            self._view.update_chart(stock_code, kline_data or [], volatility_data)
            
            # 加载股票事件
            self._model.load_stock_events(stock_code)
            stock_events = self._model.get_current_stock_events()
            self._view.show_stock_events(stock_events)
            
            self._view.show_status(f"已选择股票: {stock_code}")
            
        except Exception as e:
            self._view.show_status(f"加载股票数据失败: {e}")
            print(f"处理股票选择时出错: {e}")
    
    def _on_pool_changed(self, pool_type: str):
        """处理池类型改变事件"""
        try:
            self._view.show_status(f"正在切换到{pool_type}池...")
            self._refresh_stock_list(pool_type)
            self._refresh_all_events(pool_type)  # 同时刷新事件
            self._view.show_status(f"已切换到{pool_type}池")
        except Exception as e:
            self._view.show_status(f"切换股票池失败: {e}")
            print(f"处理池切换时出错: {e}")


# ========================================================
#                      应用程序入口
# ========================================================

def create_mvp_application():
    """创建MVP架构的应用程序"""
    # 创建组件
    model = StockMonitorModel()
    view = StockMonitorView()
    presenter = StockMonitorPresenter(model, view)
    
    return presenter


def main_mvp():
    """MVP版本的主函数 - 支持命令行参数"""
    parser = argparse.ArgumentParser(description='股票监控程序管理工具')
    parser.add_argument('action', nargs='?', choices=['status', 'monitor', 'analyze', 'profile', 'kill'],
                       help='操作类型: status(查看状态), monitor(实时监控), analyze(程序分析), profile(性能分析), kill(终止程序)')
    parser.add_argument('--gui', action='store_true', help='启动GUI界面（默认行为）')

    args = parser.parse_args()

    # 如果没有提供action参数或指定了--gui，启动GUI
    if args.action is None or args.gui:
        try:
            import sys

            # 初始化rqdatac
            print("正在初始化系统...")

            # 创建并启动MVP应用
            app = create_mvp_application()
            app.start()

        except KeyboardInterrupt:
            print("\n程序被用户中断")
        except Exception as e:
            print(f"程序运行出错: {e}")
            import traceback
            traceback.print_exc()
    else:
        # 执行工具命令
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
    main_mvp()
