"""
网络配置模块 (Network Configuration Module)

包含所有网络相关的配置和API客户端设置
"""

import os
from typing import Dict, Any


class RQDatacConfig:
    """RQDatac配置类"""

    def __init__(self):
        self.api_key = os.getenv("RQDATAC_API_KEY", "")
        self.base_url = os.getenv("RQDATAC_BASE_URL", "https://api.ricequant.com")
        self.timeout = 60
        self.retry_attempts = 3

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "api_key": self.api_key,
            "base_url": self.base_url,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts
        }
