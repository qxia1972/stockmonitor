# Stock Pool Management System

股票池管理系统 - ```
stockmonitor/
├── stockmonitor.py           # 🚀 主程序
├── ARCH.md                   # 📖 架构设计文档
├── README.md                 # 📋 项目说明
├── TODO.md                   # 📋 任务清单
│
├── modules/                  # 📚 核心模块
│   ├── data_model.py         # 💾 数据模型
│   ├── business_model.py     # 🎯 业务模型
│   ├── processor_manager.py  # ⚙️ 处理器管理器
│   ├── main_window.py        # 🎨 GUI界面
│   └── util/                 # 🛠️ 工具模块数据处理平台

## 项目概述

本系统采用分层架构设计，实现股票池管理、数据分析和可视化功能。新架构包含：
- **编排层**: Dagster轻量级任务编排
- **计算层**: Polars高性能数据处理
- **存储层**: Parquet列式数据存储
- **查询层**: DuckDB嵌入式SQL查询

## 核心特性

- 🚀 **高性能**: Polars数据处理引擎，亚秒级查询响应
- 📊 **现代化存储**: Parquet列式存储，高效压缩
- 🔍 **强大查询**: DuckDB SQL查询引擎
- 🎯 **分层架构**: 清晰的职责分离和模块化设计
- 🛡️ **生产就绪**: 完善的错误处理和稳定性保障

## 快速开始

### 环境要求
- Python 3.13+
- 依赖包：polars, duckdb, pyarrow, tkinter

### 安装和运行

```bash
# 启动GUI模式
python stockmonitor.py

# 数据更新模式
python stockmonitor.py --update

# 后台服务模式
python stockmonitor.py --service

# 健康检查
python stockmonitor.py --health-check
```

## 项目结构

```
stockmonitor/
├── new_stockmonitor.py       # 🚀 新架构主程序
├── ARCH.md                   # 📖 架构设计文档
├── README.md                 # � 项目说明
├── TODO.md                   # 📋 任务清单
│
├── modules/                  # 📚 核心模块
│   ├── new_data_model.py     # 💾 新架构数据模型
│   ├── new_business_model.py # 🎯 新架构业务模型
│   ├── new_processor_manager.py # ⚙️ 新架构处理器管理器
│   ├── new_main_window.py    # 🎨 新架构GUI界面
│   └── util/                 # 🛠️ 工具模块
│       ├── log_manager.py    # � 日志管理
│       └── python_manager.py # 🐍 Python环境管理
│
├── storage/                  # � 存储层
│   ├── parquet_manager.py    # � Parquet文件管理
│   ├── schema_manager.py     # � 数据模式管理
│   └── file_manager.py       # � 文件操作管理
│
├── query/                    # � 查询层
│   ├── query_engine.py       # 🚀 查询引擎
│   └── query_builder.py      # �️ 查询构建器
│
├── compute/                  # ⚡ 计算层
│   ├── data_processor.py     # � 数据处理器
│   ├── indicator_calculator.py # 📈 技术指标计算器
│   └── score_calculator.py   # 🧮 评分计算器
│
├── orchestration/            # 🎭 编排层
│   ├── pipeline_manager.py   # 🔄 流水线管理器
│   └── task_scheduler.py     # ⏰ 任务调度器
│
├── data/                     # 📊 数据文件
│   ├── factors.parquet       # 因子数据
│   ├── instruments.parquet   # 合约数据
│   ├── market_data.parquet   # 市场数据
│   └── technical_indicators.parquet # 技术指标
│
├── config/                   # ⚙️ 配置
│   ├── new_architecture_config.py # 新架构配置
│   └── pyrightconfig.json    # 类型检查配置
│
├── logs/                     # 📝 日志文件
│   └── stockmonitor.log      # 主日志文件
│
└── stubs/                    # 📋 类型存根
    ├── psutil.pyi           # psutil类型定义
    └── rqdatac.pyi          # rqdatac类型定义
```

## 架构优势

### 性能提升
- **数据处理**: Polars比pandas快5-10倍
- **存储效率**: Parquet压缩节省60-80%存储空间
- **查询性能**: DuckDB提供毫秒级SQL查询
- **内存优化**: 优化的数据结构减少内存占用

### 可维护性
- **模块化**: 清晰的职责分离
- **类型安全**: 完整的类型注解
- **错误处理**: 完善的异常处理机制
- **文档齐全**: 详细的代码注释和文档

### 扩展性
- **插件架构**: 支持自定义指标和处理器
- **配置驱动**: 灵活的配置管理系统
- **事件驱动**: 基于事件的松耦合设计
- **API友好**: 标准化的接口设计

## 使用指南

### 数据更新
```python
from modules.data_model import data_model

# 更新所有数据
data_model.load_instruments()
data_model.load_factors()
data_model.load_market_data()
```

### 查询数据
```python
from modules.data_model import data_model

# SQL查询
result = data_model.execute_query("""
    SELECT * FROM instruments
    WHERE market_cap > 1000000000
    ORDER BY market_cap DESC
    LIMIT 10
""")
```

### 计算指标
```python
from compute.indicator_calculator import indicator_calculator

# 计算技术指标
indicators = indicator_calculator.calculate_all_indicators(price_data)
```

## 开发和部署

### 开发环境
1. 克隆项目
2. 安装依赖：`pip install -r requirements.txt`
3. 运行测试：`python -m pytest`
4. 启动开发服务器：`python new_stockmonitor.py --dev`

### 生产部署
1. 配置环境变量
2. 设置数据源连接
3. 运行健康检查
4. 启动服务：`python new_stockmonitor.py --service`

## 许可证

本项目采用MIT许可证。
│
├── data/                     # 💾 静态数据文件
│   ├── basic_pool.json      # 基础股票池
│   ├── core_pool.json       # 核心股票池
│   ├── watch_pool.json      # 观察股票池
│   └── *.json               # 其他数据文件
│
└── stockpool_data/           # 📈 动态数据文件（程序生成）
    └── kline_*.json         # K线数据文件
```

## 功能特性

### 🎯 核心功能
- **三层股票池管理**: 基础池 → 观察池 → 核心池
- **实时数据处理**: RQDatac数据集成
- **技术指标计算**: TA-Lib技术分析
- **智能评分系统**: 多维度股票评估
- **数据质量监控**: 完整性验证和异常检测
- **交互式GUI界面**: K线图、技术指标、实时监控

### 🏗️ 架构特性
- **MVVM架构**: 完整的Model-View-ViewModel模式实现
- **数据绑定**: 自动UI更新，支持单向/双向绑定
- **命令模式**: DelegateCommand、AsyncDelegateCommand等命令实现
- **模块化设计**: 清晰的四层架构分离
- **类型安全**: 完整的类型注解和静态检查

### 🛠️ 开发工具
- **安全编辑**: 自动备份和语法检查
- **代码格式化**: Black + isort自动化格式化
- **中文支持**: 完整的中文字符集处理
- **版本控制**: Git版本管理
- **MVVM测试**: 完整的框架测试套件

### 📊 数据处理
- **多格式支持**: JSON, CSV, Excel
- **编码检测**: 自动检测文件编码
- **数据验证**: 完整性和一致性检查
- **性能优化**: 缓存和批量处理

## 快速开始

### 1. 环境准备
```bash
# 确保Python 3.8+ 已安装
python --version

# 安装依赖（如果有requirements.txt）
pip install -r requirements.txt
```

### 2. 运行主程序
```bash
# 启动股票池管理系统（默认执行同步操作）
python stockpool.py

# 同步和构建股票池
python stockpool.py --sync

# 启动股票监控
python stockpool.py --monitor

# 运行技术分析
python stockpool.py --analyze

# 启动GUI界面 (K线图、技术指标、交互式分析)
python stockpool.py --gui

# 查看帮助信息
python stockpool.py --help

# 或使用启动脚本
python scripts/start_system.py
```

### 4. 开发工具使用
```bash
# 安全编辑（自动备份）
python tools/safe_edit.py stockpool.py

# 代码格式化
python tools/format_code.py stockpool.py

# 中文处理测试
python tools/chinese_quick.py "测试文本"
```

## 开发规范

### 📝 代码风格
- 使用 **Black** 进行代码格式化
- 使用 **isort** 整理导入语句
- 使用 **flake8** 进行代码检查
- 使用 **mypy** 进行类型检查

### 🔄 工作流程
1. **编辑前**: `python tools/safe_edit.py <file>`
2. **开发中**: 遵循代码规范
3. **提交前**: `python tools/format_code.py <file>`
4. **提交**: `git add . && git commit -m "提交说明"`

### 📁 文件组织
- **核心代码**: 放在根目录或 `modules/`
- **工具脚本**: 放在 `tools/`
- **配置文件**: 放在 `config/`
- **测试文件**: 放在 `test/`
- **文档**: 放在 `docs/`
- **临时文件**: 放在 `temp/`

## 配置说明

### 环境配置
- Python 3.8+
- RQDatac API访问权限
- TA-Lib技术分析库

### 数据源
- RQDatac: 量化数据平台
- Wind: 金融数据（可选）
- 自定义数据源

## 注意事项

⚠️ **重要提醒**:
- `stockpool_data/` 目录下的文件会被 `.gitignore` 忽略
- 使用 `tools/safe_edit.py` 进行安全编辑
- 定期提交代码变更到Git
- 备份重要数据文件

## 🎯 统一每日处理管道 - 重构完成

### ✅ 重构成果

1. **移除冗余作业**
   - 删除了 `scoring_and_save_job`（与完整管道重复）
   - 删除了 `data_completion_job`（已集成到数据同步中）

2. **统一工作流程**
   - `daily_full_pipeline_job` 现在是唯一的完整处理管道
   - 包含：数据同步补全 → 指标计算保存 → 评分存盘

3. **优化作业职责**
   - 数据同步：增量更新 + 缺失补全
   - 指标计算：技术指标 + 基本面指标 + 自动保存
   - 评分存盘：最终评分计算 + 数据保存

4. **更新监控和调度**
   - 传感器监听 `daily_full_pipeline_job`
   - 调度器触发统一的完整管道
   - 移除了对冗余作业的引用

### 🚀 使用方法

#### 推荐启动方式（消除警告）

```powershell
# 使用项目启动脚本（推荐）- 自动设置环境变量消除警告
.\start_dagster.ps1

# 或者使用批处理文件
.\start_dagster.bat
```

#### 手动启动方式

```bash
# 启动Dagster UI（需要手动设置环境变量）
set PYTHONLEGACYWINDOWSSTDIO=1
dagster dev -f modules/orchestration/repository.py

# 或者直接执行完整管道
dagster job execute -f modules/orchestration/repository.py -j daily_full_pipeline_job

# 查看可用作业
python run_jobs.py --list
```

### 📋 可用作业

- **`full`**: ⭐ 推荐 - 完整每日处理管道
- **`sync`**: 数据同步补全作业
- **`calculation`**: 指标计算并保存作业

### 🔧 启动脚本说明

项目提供了专门的启动脚本，用于自动设置环境变量以消除 Windows 上的 Dagster 计算日志警告：

- **`start_dagster.ps1`**: PowerShell 启动脚本（推荐）
- **`start_dagster.bat`**: 批处理启动脚本（兼容性选项）

**脚本功能：**
- ✅ 自动设置 `PYTHONLEGACYWINDOWSSTDIO=1` 环境变量
- ✅ 启动 Dagster 开发服务器在端口 3001
- ✅ 显示启动状态和服务器地址
- ✅ 无需修改系统环境变量

### 🔧 技术架构

- **框架**: Dagster + Polars + RQDatac
- **存储**: Parquet文件系统
- **调度**: 工作日18:00自动执行
- **监控**: 成功/失败状态传感器

## 许可证

本项目仅供学习和研究使用，请遵守相关法律法规。

---

*最后更新: 2025年9月11日*
