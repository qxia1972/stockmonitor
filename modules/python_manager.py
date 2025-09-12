"""
Python环境管理工具
自动检查和切换到base环境
"""

import sys
import os
import subprocess
import importlib.util
from pathlib import Path

# 添加项目根路径到sys.path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


# 直接在此处定义环境配置，放弃unified_config.py
PYTHON_ENV_CONFIG = {
    'preferred_env': 'base',
    'auto_switch': True,
    'required_packages': ['rqdatac', 'pandas', 'numpy', 'matplotlib', 'tkinter', 'talib', 'psutil'],
    'fallback_env': 'base'
}


class EnvironmentManager:
    """Python环境管理器"""
    
    def __init__(self):
        self.config = PYTHON_ENV_CONFIG
        self.current_python = sys.executable
        self.is_base_env = self._check_if_base_env()
        self.conda_info = self._detect_conda_info()
    
    def _detect_conda_info(self) -> dict:
        """动态检测conda环境信息"""
        conda_info = {
            'conda_executable': None,
            'conda_base_path': None,
            'base_python_path': None,
            'base_env_exists': False
        }
        
        # 尝试多种方式找到conda
        conda_candidates = []
        
        # 1. 从当前Python路径推断
        if 'anaconda' in self.current_python.lower() or 'miniconda' in self.current_python.lower():
            python_dir = Path(self.current_python).parent
            if 'envs' in str(python_dir):
                # 当前在某个环境中
                conda_base = python_dir.parent.parent
            else:
                # 当前在base环境中
                conda_base = python_dir.parent
            conda_candidates.append(conda_base)
        
        # 2. 从环境变量获取
        conda_prefix = os.environ.get('CONDA_PREFIX')
        if conda_prefix:
            conda_candidates.append(Path(conda_prefix))
        
        # 3. 尝试常见安装位置
        if os.name == 'nt':  # Windows
            user_home = Path.home()
            conda_candidates.extend([
                user_home / 'anaconda3',
                user_home / 'miniconda3',
                Path('C:/ProgramData/Anaconda3'),
                Path('C:/ProgramData/Miniconda3')
            ])
        else:  # Linux/Mac
            user_home = Path.home()
            conda_candidates.extend([
                user_home / 'anaconda3',
                user_home / 'miniconda3',
                Path('/opt/anaconda3'),
                Path('/opt/miniconda3')
            ])
        
        # 4. 尝试从PATH中找到conda
        try:
            result = subprocess.run(['conda', '--version'], capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=5)
            if result.returncode == 0:
                # conda在PATH中可用
                conda_info['conda_executable'] = 'conda'
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # 验证conda候选路径
        for candidate in conda_candidates:
            if candidate and candidate.exists():
                # 检查是否有conda可执行文件
                conda_exe_candidates = []
                if os.name == 'nt':
                    conda_exe_candidates = [
                        candidate / 'Scripts' / 'conda.exe',
                        candidate / 'condabin' / 'conda.bat',
                        candidate / 'Scripts' / 'conda.bat'
                    ]
                else:
                    conda_exe_candidates = [
                        candidate / 'bin' / 'conda',
                        candidate / 'condabin' / 'conda'
                    ]
                
                for conda_exe in conda_exe_candidates:
                    if conda_exe.exists():
                        conda_info['conda_executable'] = str(conda_exe)
                        conda_info['conda_base_path'] = str(candidate)
                        break
                
                if conda_info['conda_base_path']:
                    # 检查base环境是否存在
                    envs_dir = candidate / 'envs' / self.config['preferred_env']
                    base_dir = candidate  # base环境通常在conda根目录
                    
                    # 检查base环境（可能在envs目录下，也可能在根目录下）
                    if envs_dir.exists():
                        conda_info['base_env_exists'] = True
                        if os.name == 'nt':
                            base_python = envs_dir / 'python.exe'
                        else:
                            base_python = envs_dir / 'bin' / 'python'
                    elif base_dir.exists() and self.config['preferred_env'] == 'base':
                        # 检查conda根目录下的base环境
                        conda_info['base_env_exists'] = True
                        if os.name == 'nt':
                            base_python = base_dir / 'python.exe'
                        else:
                            base_python = base_dir / 'python'
                    
                    if conda_info['base_env_exists'] and 'base_python' in locals() and base_python.exists():
                        conda_info['base_python_path'] = str(base_python)
                    break
        
        return conda_info
    
    def _check_if_base_env(self) -> bool:
        """检查当前是否在base环境中"""
        env_name = self.config['preferred_env']
        return env_name in self.current_python.lower() or env_name in os.environ.get('CONDA_DEFAULT_ENV', '')
    
    def check_required_packages(self) -> dict:
        """检查必需包的安装状态"""
        results = {}
        for package in self.config['required_packages']:
            try:
                if package == 'tkinter':
                    # tkinter是Python标准库的一部分，但可能在某些系统上不可用
                    import tkinter
                    results[package] = True
                elif package in ['rqdatac', 'talib']:
                    # 这些包可能需要特殊处理
                    spec = importlib.util.find_spec(package)
                    if spec is None:
                        # 尝试直接导入
                        __import__(package)
                        results[package] = True
                    else:
                        results[package] = True
                else:
                    # 标准包检测
                    spec = importlib.util.find_spec(package)
                    results[package] = spec is not None
            except (ImportError, ModuleNotFoundError, AttributeError):
                results[package] = False
            except Exception as e:
                # 处理其他可能的异常
                print(f"⚠️ 检测包 {package} 时出现异常: {e}")
                results[package] = False
        return results
    
    def get_base_python_command(self) -> list:
        """获取base环境的Python命令"""
        # 优先使用检测到的路径
        if self.conda_info['base_python_path']:
            return [self.conda_info['base_python_path']]
        
        # 备选方案：使用conda run命令
        if self.conda_info['conda_executable']:
            return [
                self.conda_info['conda_executable'], 
                'run', '-n', self.config['preferred_env'], 
                'python'
            ]
        
        # 最后的备选方案：尝试激活环境
        if os.name == 'nt':
            return ['conda', 'run', '-n', self.config['preferred_env'], 'python']
        else:
            return ['conda', 'run', '-n', self.config['preferred_env'], 'python']
    
    def restart_in_base_env(self, script_args=None):
        """在base环境中重启当前脚本"""
        if self.is_base_env:
            print("✅ 已在base环境中")
            return False
        
        if not self.config.get('auto_switch', False):
            print("⚠️ 自动切换被禁用，请手动切换到base环境")
            print(f"运行命令: conda activate {self.config['preferred_env']}")
            return False
        
        # 检查base环境是否存在
        if not self.conda_info['base_env_exists']:
            print(f"❌ {self.config['preferred_env']} 环境不存在")
            print(f"请创建环境: conda create -n {self.config['preferred_env']} python")
            return False
        
        print("🔄 正在切换到base环境...")
        
        # 构建命令
        python_cmd = self.get_base_python_command()
        current_script = sys.argv[0]
        # 检查是否为 -c 或空（交互式/代码模式无法自动重启）
        if current_script in ('-c', ''):
            print("❌ 当前脚本无法自动重启（-c模式或无脚本路径），请手动切换环境并重新运行主脚本。")
            print(f"建议命令：conda activate {self.config['preferred_env']} && python <your_script.py>")
            return False
        script_args = script_args or sys.argv[1:]
        cmd = python_cmd + [current_script] + script_args

        try:
            # 重启脚本
            print(f"执行命令: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 环境切换失败: {e}")
            print("请尝试手动激活环境:")
            print(f"conda activate {self.config['preferred_env']}")
            return False
        except FileNotFoundError:
            print(f"❌ 找不到Python可执行文件: {python_cmd[0]}")
            print("请检查conda安装或手动激活环境:")
            print(f"conda activate {self.config['preferred_env']}")
            return False
    
    def ensure_base_environment(self, auto_restart=True):
        """确保在base环境中运行"""
        print("🔍 检查Python环境...")
        print(f"当前Python: {self.current_python}")
        
        if self.is_base_env:
            print("✅ 当前在base环境中")
            
            # 检查必需包
            package_status = self.check_required_packages()
            missing_packages = [pkg for pkg, status in package_status.items() if not status]
            
            if missing_packages:
                print(f"⚠️ 缺少必需包: {', '.join(missing_packages)}")
                return False
            else:
                print("✅ 所有必需包已安装")
                return True
        else:
            print(f"⚠️ 当前不在base环境中 (当前: {os.path.basename(os.path.dirname(self.current_python))})")
            
            if auto_restart:
                success = self.restart_in_base_env()
                if success:
                    # 重启成功，当前进程应该退出
                    sys.exit(0)
                else:
                    print("❌ 无法切换到base环境")
                    return False
            else:
                return False
    
    def print_environment_info(self):
        """打印环境信息"""
        print("\n📊 环境信息:")
        print(f"当前Python: {self.current_python}")
        print(f"是否{self.config['preferred_env']}环境: {self.is_base_env}")
        
        print(f"\n🔍 Conda检测结果:")
        print(f"Conda可执行文件: {self.conda_info['conda_executable'] or '未找到'}")
        print(f"Conda基础路径: {self.conda_info['conda_base_path'] or '未找到'}")
        print(f"{self.config['preferred_env']}环境存在: {self.conda_info['base_env_exists']}")
        if self.conda_info['base_python_path']:
            print(f"{self.config['preferred_env']} Python路径: {self.conda_info['base_python_path']}")
        
        package_status = self.check_required_packages()
        print("\n📦 包状态:")
        for package, status in package_status.items():
            status_icon = "✅" if status else "❌"
            print(f"  {status_icon} {package}")


    def ensure_environment_with_fallback(self):
        """
        确保在base环境中运行，带命令行模式fallback处理
        
        这个方法是公共的，可以被任何主程序使用。
        在-c模式下会给出警告但不强制退出，在正常脚本模式下会尝试自动切换环境。
        
        Returns:
            bool: 是否成功（在base环境中且包完整）
        """
        # 检查是否为命令行模式（-c）
        is_command_mode = sys.argv[0] == '-c' if len(sys.argv) > 0 else False
        
        if not self.ensure_base_environment(auto_restart=not is_command_mode):
            if is_command_mode:
                print("⚠️ 警告: 当前不在base环境中运行")
                print("建议: conda activate base && python -c \"<your_command>\"")
                print("或者直接运行脚本文件: python <your_script.py>")
                # 在命令行模式下不退出，让程序继续运行
                return False
            else:
                print("🔧 Environment check failed, program exit")
                sys.exit(1)
        
        return True


if __name__ == "__main__":
    # 测试环境管理器
    env_manager = EnvironmentManager()
    env_manager.print_environment_info()
    env_manager.ensure_base_environment(auto_restart=True)
