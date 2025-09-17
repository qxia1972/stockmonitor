"""
Pytest配置文件和共享fixtures
"""

import pytest
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 添加src目录到Python路径（如果存在）
src_dir = project_root / "src"
if src_dir.exists():
    sys.path.insert(0, str(src_dir))

@pytest.fixture(scope="session")
def project_root_path():
    """项目根目录路径"""
    return Path(__file__).parent.parent

@pytest.fixture(scope="session")
def test_data_dir(project_root_path):
    """测试数据目录"""
    return project_root_path / "test" / "fixtures"

@pytest.fixture(autouse=True)
def setup_test_environment():
    """设置测试环境"""
    # 这里可以添加测试前的环境设置
    yield
    # 这里可以添加测试后的清理工作

# 可以在这里添加更多的共享fixtures和配置
