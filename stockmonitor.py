"""
股票监控系统主程序 (Stock Monitor Main Program)
简化版 - 专注用户交互，依赖查询层获取数据

核心功能:
- 🎯 初始化系统组件（依赖查询层）
- 🚀 提供GUI运行模式
- 📊 实时监控系统健康状态
- 🔧 支持数据更新操作（通过编排层）

架构优势:
- 应用层简化: 不再直接处理数据源
- 查询层抽象: 统一的查询接口
- 编排层主导: 数据处理流程自动化
- 用户界面优化: 专注用户体验

使用说明:
- 默认启动GUI模式: python stockmonitor.py
- 命令行数据更新: python stockmonitor.py --update
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import argparse

# 确保项目根目录在sys.path中
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 系统组件导入 - 简化版
from modules.main_window import create_main_window

# 基础设施组件导入 - 只导入必要的
from modules.query.query_engine import query_engine
from modules.orchestration.pipeline_manager import pipeline_manager

# 日志配置
from modules.util.log_manager import get_stockpool_logger
logger = get_stockpool_logger()


class SimplifiedStockMonitor:
    """简化版股票监控系统主控制器"""

    def __init__(self):
        """初始化系统"""
        self.components_initialized = False
        self.gui_window = None

        logger.info("🎯 简化版股票监控系统初始化中...")

    def initialize_components(self) -> bool:
        """初始化系统组件

        Returns:
            初始化是否成功
        """
        try:
            logger.info("🔧 开始初始化简化版系统组件...")

            # 1. 验证查询层
            logger.info("📊 验证查询层组件...")
            if not hasattr(query_engine, 'execute_query'):
                logger.error("❌ QueryEngine 未正确初始化")
                return False
            logger.info("✅ 查询层 (QueryEngine) 初始化正常")

            # 2. 验证编排层
            logger.info("🎭 验证编排层组件...")
            if not hasattr(pipeline_manager, 'execute_job'):
                logger.error("❌ PipelineManager 未正确初始化")
                return False
            logger.info("✅ 编排层 (PipelineManager) 初始化正常")

            self.components_initialized = True
            logger.info("🎉 简化版系统组件初始化完成！")

            return True

        except Exception as e:
            logger.exception(f"❌ 组件初始化失败: {e}")
            return False

    def run_health_check(self) -> Dict[str, Any]:
        """执行系统健康检查

        Returns:
            健康检查结果
        """
        try:
            logger.info("🏥 开始系统健康检查...")

            health_report = {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "unknown",
                "components": {
                    "query_engine": "QueryEngine",
                    "pipeline_manager": "PipelineManager",
                    "gui_system": "GUI System",
                },
                "checks": {}
            }

            # 检查各组件状态
            health_report["checks"]["query_engine"] = hasattr(query_engine, 'execute_query')
            health_report["checks"]["pipeline_manager"] = hasattr(pipeline_manager, 'execute_job')
            health_report["checks"]["gui_system"] = True  # GUI系统总是可用的

            # 计算整体状态
            all_checks_passed = all(health_report["checks"].values())
            health_report["overall_status"] = "healthy" if all_checks_passed else "unhealthy"

            logger.info(f"🏥 健康检查完成: {health_report['overall_status']}")
            return health_report

        except Exception as e:
            logger.exception(f"❌ 健康检查失败: {e}")
            return {"error": str(e), "overall_status": "error"}

    def update_data(self, force_refresh: bool = False) -> bool:
        """更新数据（通过编排层）

        Args:
            force_refresh: 是否强制刷新

        Returns:
            更新是否成功
        """
        try:
            logger.info("🔄 开始数据更新（通过编排层）...")

            # 通过编排层执行数据更新作业
            result = pipeline_manager.run_job("daily_full_pipeline", force_refresh=force_refresh)

            if result and result.get("success", False):
                logger.info("✅ 数据更新完成")
                return True
            else:
                logger.error("❌ 数据更新失败")
                return False

        except Exception as e:
            logger.exception(f"❌ 数据更新异常: {e}")
            return False

    def run_gui(self) -> None:
        """启动GUI模式"""
        try:
            logger.info("🎨 启动GUI界面...")

            if not self.components_initialized and not self.initialize_components():
                logger.error("❌ 组件初始化失败，无法启动GUI")
                return

            # 创建主窗口
            self.gui_window = create_main_window()

            logger.info("✅ GUI界面启动完成")

        except Exception as e:
            logger.exception(f"❌ GUI启动失败: {e}")


def setup_environment() -> None:
    """设置运行环境"""
    try:
        logger.info("🔧 设置Python环境...")

        # 确保所有必要的目录存在
        data_dir = project_root / "data"
        logs_dir = project_root / "logs"
        cache_dir = project_root / "cache"

        data_dir.mkdir(exist_ok=True)
        logs_dir.mkdir(exist_ok=True)
        cache_dir.mkdir(exist_ok=True)

        logger.info("✅ 环境设置完成")

    except Exception as e:
        logger.exception(f"❌ 环境设置失败: {e}")
        raise


def main():
    """主函数"""
    # 设置环境
    setup_environment()

    # 创建简化版主控制器
    monitor = SimplifiedStockMonitor()

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="股票监控系统（简化版）")
    parser.add_argument("--update", action="store_true", help="更新数据（通过编排层）")
    parser.add_argument("--gui", action="store_true", help="启动GUI模式")
    parser.add_argument("--health-check", action="store_true", help="运行健康检查")
    parser.add_argument("--diagnostics", action="store_true", help="运行系统诊断")
    parser.add_argument("--force-refresh", action="store_true", help="强制刷新缓存")

    args = parser.parse_args()

    # 根据参数执行相应操作
    if args.update:
        success = monitor.update_data(force_refresh=args.force_refresh)
        sys.exit(0 if success else 1)

    elif args.gui:
        monitor.run_gui()

    elif args.health_check:
        health_report = monitor.run_health_check()
        print(json.dumps(health_report, indent=2, ensure_ascii=False))

    elif args.diagnostics:
        # 运行诊断
        health_report = monitor.run_health_check()
        print("=== 系统诊断报告 ===")
        print(json.dumps(health_report, indent=2, ensure_ascii=False))

    else:
        # 默认启动GUI
        monitor.run_gui()


if __name__ == "__main__":
    main()
