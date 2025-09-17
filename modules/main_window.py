"""
新架构主窗口GUI模块 (New Architecture Main Window GUI)
基于新架构组件的股票监控系统主窗口

职责:
- 实现主窗口UI布局
- 集成新架构的BusinessModel和数据访问
- 处理窗口事件和生命周期
- 管理子视图切换
"""

import logging
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Dict, Any, Optional
from datetime import datetime

from modules.new_business_model import new_business_model
from modules.new_data_model import new_data_model
from modules.new_processor_manager import new_processor_manager

logger = logging.getLogger(__name__)


class NewArchitectureMainWindow:
    """新架构主窗口类"""

    def __init__(self):
        # 初始化Tkinter窗口
        self.root = tk.Tk()
        self.root.title("股票监控系统 - 新架构")
        self.root.geometry("1400x900")
        self.root.minsize(1200, 800)

        # 初始化新架构组件
        self.business_model = new_business_model
        self.data_model = new_data_model
        self.processor_manager = new_processor_manager

        # UI状态
        self.current_view = "dashboard"
        self.is_loading = False

        # 设置窗口关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        # 创建UI
        self._setup_ui()

        logger.info("🎯 新架构主窗口初始化完成")

    def _setup_ui(self):
        """设置UI界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 创建菜单栏
        self._create_menu_bar()

        # 创建工具栏
        self._create_toolbar(main_frame)

        # 创建内容区域
        self._create_content_area(main_frame)

        # 创建状态栏
        self._create_status_bar(main_frame)

        # 初始化显示仪表板视图
        self._switch_to_view("dashboard")

    def _create_menu_bar(self):
        """创建菜单栏"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # 文件菜单
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="退出", command=self._on_closing)

        # 视图菜单
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="视图", menu=view_menu)
        view_menu.add_command(
            label="仪表板",
            command=lambda: self._switch_to_view("dashboard"),
        )
        view_menu.add_command(
            label="股票池管理",
            command=lambda: self._switch_to_view("stock_pool"),
        )
        view_menu.add_command(
            label="市场分析",
            command=lambda: self._switch_to_view("market_analysis"),
        )
        view_menu.add_command(
            label="数据管理",
            command=lambda: self._switch_to_view("data_management"),
        )
        view_menu.add_command(
            label="系统监控",
            command=lambda: self._switch_to_view("system_monitor"),
        )

        # 工具菜单
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="工具", menu=tools_menu)
        tools_menu.add_command(label="刷新数据", command=self._refresh_data)
        tools_menu.add_command(label="计算指标", command=self._calculate_indicators)
        tools_menu.add_command(label="运行健康检查", command=self._run_health_check)

        # 帮助菜单
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self._show_about)

    def _create_toolbar(self, parent):
        """创建工具栏"""
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill=tk.X, pady=(0, 10))

        # 视图切换按钮
        ttk.Button(
            toolbar,
            text="仪表板",
            command=lambda: self._switch_to_view("dashboard"),
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            toolbar,
            text="股票池",
            command=lambda: self._switch_to_view("stock_pool"),
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            toolbar,
            text="市场分析",
            command=lambda: self._switch_to_view("market_analysis"),
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            toolbar,
            text="数据管理",
            command=lambda: self._switch_to_view("data_management"),
        ).pack(side=tk.LEFT, padx=5)

        # 分隔符
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(
            side=tk.LEFT, fill=tk.Y, padx=10
        )

        # 操作按钮
        ttk.Button(toolbar, text="刷新", command=self._refresh_current_view).pack(
            side=tk.LEFT, padx=5
        )

        ttk.Button(toolbar, text="同步", command=self._sync_data).pack(
            side=tk.LEFT, padx=5
        )

    def _create_content_area(self, parent):
        """创建内容区域"""
        # 创建内容框架
        self.content_frame = ttk.Frame(parent)
        self.content_frame.pack(fill=tk.BOTH, expand=True)

        # 创建视图容器
        self.view_container = ttk.Frame(self.content_frame)
        self.view_container.pack(fill=tk.BOTH, expand=True)

        # 初始化各个视图
        self._create_views()

    def _create_views(self):
        """创建所有视图"""
        # 仪表板视图
        self.dashboard_view = self._create_dashboard_view()

        # 股票池视图
        self.stock_pool_view = self._create_stock_pool_view()

        # 市场分析视图
        self.market_analysis_view = self._create_market_analysis_view()

        # 数据管理视图
        self.data_management_view = self._create_data_management_view()

        # 系统监控视图
        self.system_monitor_view = self._create_system_monitor_view()

        # 默认隐藏所有视图
        self._hide_all_views()

    def _create_dashboard_view(self) -> ttk.Frame:
        """创建仪表板视图"""
        frame = ttk.Frame(self.view_container)

        # 标题
        ttk.Label(frame, text="系统仪表板", font=("Arial", 18, "bold")).pack(pady=10)

        # 统计卡片区域
        stats_frame = ttk.Frame(frame)
        stats_frame.pack(fill=tk.X, padx=10, pady=5)

        # 创建统计卡片
        self.stats_cards = {}
        stats_config = [
            ("总股票数", "total_stocks", "0"),
            ("基础池", "basic_pool", "0"),
            ("观察池", "watch_pool", "0"),
            ("核心池", "core_pool", "0"),
            ("数据质量", "data_quality", "0%"),
            ("系统状态", "system_status", "正常")
        ]

        for i, (label, key, default) in enumerate(stats_config):
            card_dict = self._create_stats_card(stats_frame, label, default)
            self.stats_cards[key] = card_dict

            # 网格布局
            row = i // 3
            col = i % 3
            card_dict["frame"].grid(row=row, column=col, padx=5, pady=5, sticky="ew")

        # 配置网格权重
        for i in range(3):
            stats_frame.grid_columnconfigure(i, weight=1)

        # 最近活动区域
        activity_frame = ttk.LabelFrame(frame, text="最近活动")
        activity_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.activity_text = tk.Text(activity_frame, height=10, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(
            activity_frame, orient=tk.VERTICAL, command=self.activity_text.yview
        )
        self.activity_text.configure(yscrollcommand=scrollbar.set)

        self.activity_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return frame

    def _create_stock_pool_view(self) -> ttk.Frame:
        """创建股票池视图"""
        frame = ttk.Frame(self.view_container)

        # 标题
        ttk.Label(frame, text="股票池管理", font=("Arial", 16, "bold")).pack(pady=10)

        # 控制面板
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(control_frame, text="股票池类型:").pack(side=tk.LEFT)
        self.pool_type_var = tk.StringVar(value="basic")
        pool_combo = ttk.Combobox(
            control_frame,
            textvariable=self.pool_type_var,
            values=["basic", "watch", "core"],
            state="readonly",
        )
        pool_combo.pack(side=tk.LEFT, padx=5)
        pool_combo.bind("<<ComboboxSelected>>", self._on_pool_type_changed)

        ttk.Button(control_frame, text="刷新", command=self._refresh_stock_pool).pack(side=tk.RIGHT, padx=5)

        # 股票列表
        list_frame = ttk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # 创建Treeview
        columns = ("代码", "评分", "排名", "池类型")
        self.stock_tree = ttk.Treeview(
            list_frame, columns=columns, show="headings", height=20
        )

        for col in columns:
            self.stock_tree.heading(col, text=col)
            self.stock_tree.column(col, width=120)

        scrollbar = ttk.Scrollbar(
            list_frame, orient=tk.VERTICAL, command=self.stock_tree.yview
        )
        self.stock_tree.configure(yscrollcommand=scrollbar.set)

        self.stock_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return frame

    def _create_market_analysis_view(self) -> ttk.Frame:
        """创建市场分析视图"""
        frame = ttk.Frame(self.view_container)

        # 标题
        ttk.Label(frame, text="市场分析", font=("Arial", 16, "bold")).pack(pady=10)

        # 分析选项
        options_frame = ttk.Frame(frame)
        options_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(options_frame, text="计算技术指标", command=self._calculate_indicators).pack(side=tk.LEFT, padx=5)
        ttk.Button(options_frame, text="生成评分", command=self._calculate_scores).pack(side=tk.LEFT, padx=5)

        # 结果显示区域
        result_frame = ttk.LabelFrame(frame, text="分析结果")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.analysis_text = tk.Text(result_frame, height=15, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(
            result_frame, orient=tk.VERTICAL, command=self.analysis_text.yview
        )
        self.analysis_text.configure(yscrollcommand=scrollbar.set)

        self.analysis_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return frame

    def _create_data_management_view(self) -> ttk.Frame:
        """创建数据管理视图"""
        frame = ttk.Frame(self.view_container)

        # 标题
        ttk.Label(frame, text="数据管理", font=("Arial", 16, "bold")).pack(pady=10)

        # 操作按钮
        button_frame = ttk.Frame(frame)
        button_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(button_frame, text="同步市场数据", command=self._sync_data).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="刷新缓存", command=self._refresh_cache).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="数据质量检查", command=self._check_data_quality).pack(side=tk.LEFT, padx=5)

        # 状态显示
        status_frame = ttk.Frame(frame)
        status_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(status_frame, text="同步状态:").grid(row=0, column=0, sticky="w")
        self.sync_status_label = ttk.Label(status_frame, text="未同步")
        self.sync_status_label.grid(row=0, column=1, sticky="w", padx=10)

        ttk.Label(status_frame, text="最后更新:").grid(row=1, column=0, sticky="w")
        self.last_update_label = ttk.Label(status_frame, text="无")
        self.last_update_label.grid(row=1, column=1, sticky="w", padx=10)

        # 数据统计
        stats_frame = ttk.LabelFrame(frame, text="数据统计")
        stats_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.data_stats_text = tk.Text(stats_frame, height=15, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(
            stats_frame, orient=tk.VERTICAL, command=self.data_stats_text.yview
        )
        self.data_stats_text.configure(yscrollcommand=scrollbar.set)

        self.data_stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return frame

    def _create_system_monitor_view(self) -> ttk.Frame:
        """创建系统监控视图"""
        frame = ttk.Frame(self.view_container)

        # 标题
        ttk.Label(frame, text="系统监控", font=("Arial", 16, "bold")).pack(pady=10)

        # 监控选项
        monitor_frame = ttk.Frame(frame)
        monitor_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Button(monitor_frame, text="健康检查", command=self._run_health_check).pack(side=tk.LEFT, padx=5)
        ttk.Button(monitor_frame, text="处理器状态", command=self._show_processor_status).pack(side=tk.LEFT, padx=5)
        ttk.Button(monitor_frame, text="缓存状态", command=self._show_cache_status).pack(side=tk.LEFT, padx=5)

        # 监控结果
        result_frame = ttk.LabelFrame(frame, text="监控结果")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.monitor_text = tk.Text(result_frame, height=20, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(
            result_frame, orient=tk.VERTICAL, command=self.monitor_text.yview
        )
        self.monitor_text.configure(yscrollcommand=scrollbar.set)

        self.monitor_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        return frame

    def _create_stats_card(self, parent, title: str, value: str) -> Dict[str, Any]:
        """创建统计卡片"""
        frame = ttk.Frame(parent, relief="groove", borderwidth=2)

        ttk.Label(frame, text=title, font=("Arial", 10, "bold")).pack(pady=2)
        value_label = ttk.Label(frame, text=value, font=("Arial", 14))
        value_label.pack(pady=2)

        # 返回包含frame和value_label的字典
        return {"frame": frame, "value_label": value_label}

    def _create_status_bar(self, parent):
        """创建状态栏"""
        self.status_bar = ttk.Frame(parent)
        self.status_bar.pack(fill=tk.X, pady=(10, 0))

        self.status_label = ttk.Label(self.status_bar, text="新架构系统就绪")
        self.status_label.pack(side=tk.LEFT)

        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            self.status_bar, variable=self.progress_var, maximum=100
        )
        self.progress_bar.pack(side=tk.RIGHT, padx=10)

    def _switch_to_view(self, view_name: str):
        """切换视图"""
        self.current_view = view_name
        self._hide_all_views()

        # 显示目标视图
        if view_name == "dashboard":
            self.dashboard_view.pack(fill=tk.BOTH, expand=True)
            self._refresh_dashboard()
        elif view_name == "stock_pool":
            self.stock_pool_view.pack(fill=tk.BOTH, expand=True)
            self._refresh_stock_pool()
        elif view_name == "market_analysis":
            self.market_analysis_view.pack(fill=tk.BOTH, expand=True)
        elif view_name == "data_management":
            self.data_management_view.pack(fill=tk.BOTH, expand=True)
            self._refresh_data_management()
        elif view_name == "system_monitor":
            self.system_monitor_view.pack(fill=tk.BOTH, expand=True)

        self.status_label.config(text=f"当前视图: {view_name}")

    def _hide_all_views(self):
        """隐藏所有视图"""
        self.dashboard_view.pack_forget()
        self.stock_pool_view.pack_forget()
        self.market_analysis_view.pack_forget()
        self.data_management_view.pack_forget()
        self.system_monitor_view.pack_forget()

    def _refresh_current_view(self):
        """刷新当前视图"""
        if self.current_view == "dashboard":
            self._refresh_dashboard()
        elif self.current_view == "stock_pool":
            self._refresh_stock_pool()
        elif self.current_view == "data_management":
            self._refresh_data_management()

    def _refresh_dashboard(self):
        """刷新仪表板"""
        try:
            self._set_loading(True)

            # 获取股票池统计
            pool_stats = self.business_model.get_pool_statistics()

            # 更新统计卡片
            updates = {
                "total_stocks": str(pool_stats.get("total_unique_stocks", 0)),
                "basic_pool": str(pool_stats.get("basic_pool_size", 0)),
                "watch_pool": str(pool_stats.get("watch_pool_size", 0)),
                "core_pool": str(pool_stats.get("core_pool_size", 0)),
            }

            for key, value in updates.items():
                if key in self.stats_cards:
                    self.stats_cards[key]["value_label"].config(text=value)

            # 获取数据质量报告
            quality_report = self.data_model.get_data_quality_report()
            quality_score = quality_report.get("quality_score", 0)
            self.stats_cards["data_quality"]["value_label"].config(text=f"{quality_score:.1f}%")

            # 更新活动日志
            self.activity_text.delete(1.0, tk.END)
            self.activity_text.insert(tk.END, f"仪表板刷新完成 - {datetime.now().strftime('%H:%M:%S')}\n")
            self.activity_text.insert(tk.END, f"股票池统计: {pool_stats}\n")
            self.activity_text.insert(tk.END, f"数据质量评分: {quality_score:.1f}%\n")

            self._set_loading(False)

        except Exception as e:
            logger.exception("刷新仪表板失败: %s", e)
            self._set_loading(False)

    def _refresh_stock_pool(self):
        """刷新股票池"""
        try:
            self._set_loading(True)

            pool_type = self.pool_type_var.get()

            # 获取对应池的数据
            if pool_type == "basic":
                stocks = self.business_model.get_basic_pool()
            elif pool_type == "watch":
                stocks = self.business_model.get_watch_pool()
            else:  # core
                stocks = self.business_model.get_core_pool()

            # 清空现有数据
            for item in self.stock_tree.get_children():
                self.stock_tree.delete(item)

            # 添加新数据
            for stock in stocks[:100]:  # 限制显示前100个
                values = (
                    stock.get("stock_code", ""),
                    f"{stock.get('score', 0):.4f}",
                    str(stock.get("rank", 0)),
                    pool_type
                )
                self.stock_tree.insert("", tk.END, values=values)

            self._set_loading(False)

        except Exception as e:
            logger.exception("刷新股票池失败: %s", e)
            self._set_loading(False)

    def _refresh_data_management(self):
        """刷新数据管理视图"""
        try:
            # 获取数据质量报告
            quality_report = self.data_model.get_data_quality_report()

            # 更新状态标签
            self.sync_status_label.config(text="已同步")
            self.last_update_label.config(text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            # 更新统计信息
            self.data_stats_text.delete(1.0, tk.END)
            self.data_stats_text.insert(tk.END, "数据质量报告:\n")
            self.data_stats_text.insert(tk.END, f"总记录数: {quality_report.get('total_records', 0)}\n")
            self.data_stats_text.insert(tk.END, f"质量评分: {quality_report.get('quality_score', 0):.1f}%\n")

            if "data_sources" in quality_report:
                self.data_stats_text.insert(tk.END, "\n数据源详情:\n")
                for source in quality_report["data_sources"]:
                    self.data_stats_text.insert(tk.END,
                        f"- {source['name']}: {source['records']} 条记录\n")

        except Exception as e:
            logger.exception("刷新数据管理失败: %s", e)

    def _sync_data(self):
        """同步数据"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在同步数据...")

            # 执行数据同步
            result = self.business_model.sync_and_build_pools(force=True)

            if result:
                self.status_label.config(text="数据同步完成")
                self._refresh_current_view()
            else:
                self.status_label.config(text="数据同步失败")

            self._set_loading(False)

        except Exception as e:
            logger.exception("数据同步失败: %s", e)
            self.status_label.config(text="数据同步出错")
            self._set_loading(False)

    def _refresh_data(self):
        """刷新数据"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在刷新数据...")

            # 更新数据缓存
            self.data_model.update_data_cache()

            self.status_label.config(text="数据刷新完成")
            self._refresh_current_view()
            self._set_loading(False)

        except Exception as e:
            logger.exception("数据刷新失败: %s", e)
            self.status_label.config(text="数据刷新出错")
            self._set_loading(False)

    def _calculate_indicators(self):
        """计算技术指标"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在计算技术指标...")

            # 获取股票池中的股票代码
            basic_pool = self.business_model.get_basic_pool()
            stock_codes = [stock["stock_code"] for stock in basic_pool[:10]]  # 限制前10个

            # 计算指标
            indicators = self.business_model.calculate_technical_indicators(stock_codes)

            # 显示结果
            self.analysis_text.delete(1.0, tk.END)
            self.analysis_text.insert(tk.END, f"技术指标计算完成 - {datetime.now().strftime('%H:%M:%S')}\n\n")
            self.analysis_text.insert(tk.END, f"处理的股票数量: {len(indicators)}\n")

            for stock_code, data in indicators.items():
                self.analysis_text.insert(tk.END, f"\n股票 {stock_code}:\n")
                if not data.is_empty():
                    self.analysis_text.insert(tk.END, f"  数据行数: {len(data)}\n")
                    self.analysis_text.insert(tk.END, f"  列数: {len(data.columns)}\n")
                    self.analysis_text.insert(tk.END, f"  指标: {', '.join(data.columns)}\n")

            self.status_label.config(text="技术指标计算完成")
            self._set_loading(False)

        except Exception as e:
            logger.exception("计算技术指标失败: %s", e)
            self.status_label.config(text="技术指标计算出错")
            self._set_loading(False)

    def _calculate_scores(self):
        """计算股票评分"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在计算股票评分...")

            # 获取股票池中的股票代码
            basic_pool = self.business_model.get_basic_pool()
            stock_codes = [stock["stock_code"] for stock in basic_pool[:10]]  # 限制前10个

            # 计算评分
            scores = self.business_model.calculate_scores(stock_codes)

            # 显示结果
            self.analysis_text.delete(1.0, tk.END)
            self.analysis_text.insert(tk.END, f"股票评分计算完成 - {datetime.now().strftime('%H:%M:%S')}\n\n")

            if not scores.is_empty():
                self.analysis_text.insert(tk.END, f"评分股票数量: {len(scores)}\n")
                self.analysis_text.insert(tk.END, f"评分列: {', '.join(scores.columns)}\n\n")

                # 显示前几个股票的评分
                for row in scores.head(5).rows():
                    self.analysis_text.insert(tk.END, f"股票 {row[0]}: 评分 = {row[1]:.4f}\n")

            self.status_label.config(text="股票评分计算完成")
            self._set_loading(False)

        except Exception as e:
            logger.exception("计算股票评分失败: %s", e)
            self.status_label.config(text="股票评分计算出错")
            self._set_loading(False)

    def _run_health_check(self):
        """运行健康检查"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在运行健康检查...")

            # 获取系统健康状态
            health_status = self.business_model.get_health_status()

            # 显示结果
            self.monitor_text.delete(1.0, tk.END)
            self.monitor_text.insert(tk.END, f"系统健康检查 - {datetime.now().strftime('%H:%M:%S')}\n\n")

            overall_score = health_status.get("overall_health_score", 0)
            self.monitor_text.insert(tk.END, f"整体健康评分: {overall_score:.1f}%\n\n")

            # 显示各组件状态
            if "data_model" in health_status:
                dm = health_status["data_model"]
                self.monitor_text.insert(tk.END, f"数据模型:\n")
                self.monitor_text.insert(tk.END, f"  质量评分: {dm.get('quality_score', 0):.1f}%\n")
                self.monitor_text.insert(tk.END, f"  总记录数: {dm.get('total_records', 0)}\n\n")

            if "processor_manager" in health_status:
                pm = health_status["processor_manager"]
                self.monitor_text.insert(tk.END, f"处理器管理器:\n")
                self.monitor_text.insert(tk.END, f"  处理器数量: {pm.get('total_processors', 0)}\n")
                self.monitor_text.insert(tk.END, f"  就绪处理器: {pm.get('ready_processors', 0)}\n")
                self.monitor_text.insert(tk.END, f"  健康评分: {pm.get('health_score', 0):.1f}%\n\n")

            self.status_label.config(text="健康检查完成")
            self._set_loading(False)

        except Exception as e:
            logger.exception("健康检查失败: %s", e)
            self.status_label.config(text="健康检查出错")
            self._set_loading(False)

    def _show_processor_status(self):
        """显示处理器状态"""
        try:
            status = self.processor_manager.get_processor_status()

            self.monitor_text.delete(1.0, tk.END)
            self.monitor_text.insert(tk.END, f"处理器状态 - {datetime.now().strftime('%H:%M:%S')}\n\n")

            for name, info in status.items():
                self.monitor_text.insert(tk.END, f"处理器: {name}\n")
                self.monitor_text.insert(tk.END, f"  状态: {info['status']}\n")
                self.monitor_text.insert(tk.END, f"  处理次数: {info['process_count']}\n")
                self.monitor_text.insert(tk.END, f"  错误次数: {info['error_count']}\n")
                self.monitor_text.insert(tk.END, f"  最后使用: {info['last_used'] or '从未使用'}\n\n")

        except Exception as e:
            logger.exception("获取处理器状态失败: %s", e)

    def _show_cache_status(self):
        """显示缓存状态"""
        try:
            # 显示数据缓存状态
            cache_info = {
                "缓存条目数": len(self.data_model._data_cache),
                "缓存过期条目数": len(self.data_model._cache_expiry)
            }

            self.monitor_text.delete(1.0, tk.END)
            self.monitor_text.insert(tk.END, f"缓存状态 - {datetime.now().strftime('%H:%M:%S')}\n\n")

            for key, value in cache_info.items():
                self.monitor_text.insert(tk.END, f"{key}: {value}\n")

        except Exception as e:
            logger.exception("获取缓存状态失败: %s", e)

    def _check_data_quality(self):
        """检查数据质量"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在检查数据质量...")

            # 获取数据质量报告
            quality_report = self.data_model.get_data_quality_report()

            # 更新数据管理视图
            self.data_stats_text.delete(1.0, tk.END)
            self.data_stats_text.insert(tk.END, "数据质量检查结果:\n\n")
            self.data_stats_text.insert(tk.END, f"总记录数: {quality_report.get('total_records', 0)}\n")
            self.data_stats_text.insert(tk.END, f"质量评分: {quality_report.get('quality_score', 0):.1f}%\n\n")

            if "data_sources" in quality_report:
                self.data_stats_text.insert(tk.END, "各数据源详情:\n")
                for source in quality_report["data_sources"]:
                    self.data_stats_text.insert(tk.END,
                        f"• {source['name']}: {source['records']} 条记录, {source['columns']} 列\n")

            self.status_label.config(text="数据质量检查完成")
            self._set_loading(False)

        except Exception as e:
            logger.exception("数据质量检查失败: %s", e)
            self.status_label.config(text="数据质量检查出错")
            self._set_loading(False)

    def _refresh_cache(self):
        """刷新缓存"""
        try:
            self._set_loading(True)
            self.status_label.config(text="正在刷新缓存...")

            # 更新数据缓存
            self.data_model.update_data_cache()

            self.status_label.config(text="缓存刷新完成")
            self._set_loading(False)

        except Exception as e:
            logger.exception("缓存刷新失败: %s", e)
            self.status_label.config(text="缓存刷新出错")
            self._set_loading(False)

    def _on_pool_type_changed(self, event):
        """池类型变化事件处理"""
        self._refresh_stock_pool()

    def _set_loading(self, loading: bool):
        """设置加载状态"""
        self.is_loading = loading
        if loading:
            self.progress_var.set(50)
            self.root.config(cursor="wait")
        else:
            self.progress_var.set(100)
            self.root.config(cursor="")

    def _show_about(self):
        """显示关于对话框"""
        messagebox.showinfo("关于", "股票监控系统 - 新架构版本\n基于Polars和Dagster实现\n© 2024")

    def _on_closing(self):
        """处理窗口关闭"""
        try:
            logger.info("🛑 正在关闭新架构主窗口...")

            # 清理资源
            if hasattr(self, 'business_model'):
                # 这里可以添加清理逻辑
                pass

            # 销毁窗口
            self.root.destroy()

            logger.info("✅ 新架构主窗口已关闭")

        except Exception as e:
            logger.exception("关闭窗口时出错: %s", e)

    def run(self):
        """运行GUI应用"""
        try:
            self.root.mainloop()
        except Exception as e:
            logger.exception("GUI运行时出错: %s", e)
            raise


# 创建全局实例
def create_main_window():
    """创建主窗口"""
    return NewArchitectureMainWindow()
