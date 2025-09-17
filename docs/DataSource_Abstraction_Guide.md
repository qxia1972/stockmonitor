# 数据源抽象层使用指南

## 概述

数据源抽象层 (`modules/data_sources.py`) 提供统一的数据访问接口，支持多种数据提供商的插件化集成。目前支持：

- **RQDatac**: 专业金融数据平台 (优先级: 100)
- **LocalFile**: 本地文件数据源 (优先级: 50)

## 基本使用

### 1. 获取数据源管理器

```python
from modules.data_sources import get_data_source_manager

# 获取全局数据源管理器实例
manager = get_data_source_manager()
```

### 2. 检查数据源状态

```python
# 获取所有数据源状态
status_list = manager.get_sources_status()
for status in status_list:
    print(f"{status['name']}: {'可用' if status['available'] else '不可用'}")

# 获取可用数据源
available_sources = manager.get_available_sources()
print(f"可用数据源: {[s.name for s in available_sources]}")
```

### 3. 获取数据

使用便捷函数 `get_data()` 从最佳可用数据源获取数据：

```python
from modules.data_sources import get_data

# 获取交易日历
trading_dates = get_data('get_trading_dates', '2023-01-01', '2023-01-31')
print(f"交易日历: {trading_dates}")

# 获取股票基本信息
stock_codes = ['000001.XSHE', '000002.XSHE']
basic_info = get_data('get_stock_basic_info', stock_codes)
print(basic_info)

# 获取价格数据
price_data = get_data('get_price_data', stock_codes, '2023-01-01', '2023-01-31')
for code, df in price_data.items():
    print(f"{code} 价格数据: {len(df)} 条记录")
```

## 高级使用

### 1. 直接使用数据源

```python
# 获取特定数据源
rqdatac_source = manager.get_source('rqdatac')
if rqdatac_source and rqdatac_source.is_available():
    # 直接使用RQDatac数据源
    basic_info = rqdatac_source.get_stock_basic_info(['000001.XSHE'])
    print(basic_info)
```

### 2. 注册自定义数据源

```python
from modules.data_sources import DataSource

class CustomDataSource(DataSource):
    name = "custom"
    priority = 75
    description = "自定义数据源"

    def is_available(self) -> bool:
        return True  # 实现可用性检查

    def get_stock_basic_info(self, stock_codes):
        # 实现数据获取逻辑
        pass

    # 实现其他抽象方法...

# 注册自定义数据源
custom_source = CustomDataSource()
manager.register_source(custom_source)
```

## 数据源接口

所有数据源必须实现以下接口：

### 核心属性
- `name`: 数据源名称
- `priority`: 优先级 (越高越优先)
- `description`: 描述信息

### 核心方法
- `is_available()`: 检查数据源是否可用
- `get_stock_basic_info(stock_codes)`: 获取股票基本信息
- `get_price_data(stock_codes, start_date, end_date, frequency)`: 获取价格数据
- `get_trading_dates(start_date, end_date)`: 获取交易日历
- `get_latest_trading_date()`: 获取最新交易日
- `get_valuation_data(stock_codes, target_date)`: 获取估值数据

## 配置说明

### RQDatac数据源
需要设置环境变量：
```bash
export RQDATAC_USERNAME="your_username"
export RQDATAC_PASSWORD="your_password"
```

### 本地文件数据源
默认使用 `data/` 目录存储缓存文件：
- `stock_basic_info.json`: 股票基本信息缓存
- `price_{stock_code}_{frequency}.json`: 价格数据缓存
- `trading_dates.json`: 交易日历缓存
- `valuation_{date}.json`: 估值数据缓存

## 错误处理

数据源抽象层提供统一的异常处理：

```python
from modules.data_sources import DataSourceException

try:
    data = get_data('get_stock_basic_info', ['000001.XSHE'])
except DataSourceException as e:
    print(f"数据获取失败: {e}")
```

异常类型：
- `DataSourceException`: 通用数据源异常
- `DataSourceUnavailableException`: 数据源不可用
- `DataSourceTimeoutException`: 数据源超时

## 最佳实践

1. **优先级管理**: 高优先级数据源优先使用，低优先级作为后备
2. **错误处理**: 总是处理 `DataSourceException`
3. **缓存策略**: 本地文件数据源可用于缓存和离线测试
4. **并发安全**: 数据源管理器是线程安全的
5. **资源管理**: 数据源会自动管理连接和资源

## 测试

运行测试脚本验证功能：

```bash
python test_data_sources.py
```

## 扩展开发

要添加新的数据源：

1. 继承 `DataSource` 抽象基类
2. 实现所有抽象方法
3. 设置合适的优先级
4. 注册到 `DataSourceManager`

示例见 `RQDatacDataSource` 和 `LocalFileDataSource` 的实现。