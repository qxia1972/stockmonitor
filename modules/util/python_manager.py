"""
Python环境管理工具
自动检查和切换到stock monitor python environment venv环境
"""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

# 添加项目根路径到sys.path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


# 直接在此处定义环境配置,放弃unified_config.py
PYTHON_ENV_CONFIG: dict[str, Any] = {
    "preferred_env": "stockmonitor",
    "auto_switch": True,
    "required_packages": [
        "rqdatac",
        "pandas",
        "numpy",
        "matplotlib",
        "tkinter",
        "talib",
        "psutil",
    ],
    "fallback_env": "stockmonitor",
}


class EnvironmentManager:
    """Python环境管理器 (使用venv)"""

    def __init__(self):
        self.config: dict[str, Any] = PYTHON_ENV_CONFIG
        self.current_python: str = sys.executable
        self.is_stockmonitor_env: bool = self._check_if_stockmonitor_env()
        self.venv_info: dict[str, Any] = self._detect_venv_info()

    def _detect_venv_info(self) -> dict[str, Any]:
        """检测venv环境信息"""
        venv_info: dict[str, Any] = {
            "venv_path": None,
            "stockmonitor_python_path": None,
            "stockmonitor_env_exists": False,
        }

        # 检查项目目录下是否有venv
        project_root = Path(__file__).parent.parent
        venv_dir = project_root / "venv"
        if venv_dir.exists():
            venv_info["venv_path"] = str(venv_dir)
            if os.name == "nt":
                python_exe = venv_dir / "Scripts" / "python.exe"
            else:
                python_exe = venv_dir / "bin" / "python"

            if python_exe.exists():
                venv_info["stockmonitor_python_path"] = str(python_exe)
                venv_info["stockmonitor_env_exists"] = True

        return venv_info

    def _check_if_stockmonitor_env(self) -> bool:
        """检查当前是否在stock monitor python environment中"""
        env_name = self.config["preferred_env"]
        return (
            env_name in self.current_python.lower()
            or "venv" in self.current_python.lower()
        )

    def check_required_packages(self) -> dict[str, bool]:
        """检查必需包的安装状态"""
        results: dict[str, bool] = {}

        packages_to_check: list[str] = self.config["required_packages"]

        for package in packages_to_check:
            try:
                if package == "tkinter":
                    # tkinter是标准库的一部分,特殊处理
                    try:
                        importlib.import_module("tkinter")
                        results[package] = True
                    except ImportError:
                        results[package] = False
                else:
                    spec = importlib.util.find_spec(package)
                    results[package] = spec is not None
            except ImportError:
                results[package] = False
        return results

    def get_stockmonitor_python_command(self) -> list[str]:
        """获取stock monitor python environment的Python命令"""
        # 优先使用检测到的venv路径
        if self.venv_info["stockmonitor_python_path"]:
            return [self.venv_info["stockmonitor_python_path"]]

        # 如果没有venv,使用当前Python
        return [sys.executable]

    def create_venv_environment(self):
        """创建venv环境"""
        project_root = Path(__file__).parent.parent
        venv_dir = project_root / "venv"

        if venv_dir.exists():
            print(f"✅ venv环境已存在: {venv_dir}")
            return True

        print("🔄 创建venv环境...")
        try:
            subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], check=True)
            print(f"✅ venv环境创建成功: {venv_dir}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ venv环境创建失败: {e}")
            return False

    def install_missing_packages(self, missing_packages: list[str]) -> bool:
        """自动安装缺少的包"""
        if not missing_packages:
            return True

        print(f"🔄 自动安装缺少的包: {', '.join(missing_packages)}")

        try:
            import subprocess

            # 使用pip安装
            cmd = [sys.executable, "-m", "pip", "install"] + missing_packages
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )

            if result.returncode == 0:
                print("✅ 包安装成功")
                return True
            print(f"❌ 包安装失败: {result.stderr}")
            return False
        except Exception as e:
            print(f"❌ 包安装异常: {e}")
            return False

    def ensure_environment_with_fallback(self):
        """确保环境正确,带fallback机制"""

        print("🔍 检查Python环境...")
        print(f"当前Python: {self.current_python}")  # 检查必需包
        package_results = self.check_required_packages()
        missing_packages = [
            pkg for pkg, installed in package_results.items() if not installed
        ]

        if missing_packages:
            print(f"⚠️ 缺少必需包: {', '.join(missing_packages)}")
            # 自动安装缺少的包
            if self.install_missing_packages(missing_packages):
                # 重新检查
                package_results = self.check_required_packages()
                missing_packages = [
                    pkg for pkg, installed in package_results.items() if not installed
                ]
                if missing_packages:
                    print(f"❌ 安装后仍有缺少的包: {', '.join(missing_packages)}")
                    print("🔧 Environment check failed, program exit")
                    return False
            else:
                print("🔧 Environment check failed, program exit")
                return False

        print("✅ 当前在stock monitor python environment中")
        print("✅ 所有必需包已安装")
        return True


if __name__ == "__main__":
    # 测试环境管理器
    env_manager = EnvironmentManager()
    env_manager.ensure_environment_with_fallback()
