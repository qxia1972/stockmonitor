# Stock ├── stockpool.py        ├── tools/                    # 🛠️ 开发工具
│   ├── safe_edit.py         # 安全编辑工具
│   ├── format_code.py       # 代码格式化
│   └── chinese_quick.py     # 中文处理工具 # 🚀 主程序文件 (支持命令行参数)
├── stockmonitor.py           # 📊 股票监控模块
├── __init__.py              # 📦 包初始化 Management System

股票池管理系统 - 一个完整的量化投资数据处理平台

## 项目结构

```
stockman/
├── stockpool.py              # 🚀 主程序文件
├── stockmonitor.py           # 📊 股票监控模块 (支持命令行工具)
├── __init__.py              # 📦 包初始化
│
├── modules/                  # 📚 核心模块
│   ├── data_manager.py      # 数据管理
│   ├── indicator_manager.py # 技术指标
│   ├── event_manager.py     # 事件处理
│   ├── log_manager.py       # 日志管理
│   ├── python_manager.py    # Python环境管理
│   ├── data_formats.py      # 数据格式定义
│   ├── data_interface.py    # 数据接口
│   └── cache/               # 模块缓存
│
├── config/                   # ⚙️ 配置文件
│   ├── unified_config.py    # 统一配置
│   ├── ai_config.py         # AI配置
│   ├── ai_config_backup.py  # 配置备份
│   ├── pyrightconfig.json   # Python类型检查配置
│   └── README.md            # 配置说明
│
├── tools/                    # �� 开发工具
│   ├── safe_edit.py         # 安全编辑工具
│   ├── format_code.py       # 代码格式化
│   ├── chinese_quick.py     # 中文处理工具
│   └── stockpool_tool.py    # 股票池工具
│
├── scripts/                  # 📜 自动化脚本
│   ├── start_system.py      # 系统启动脚本
│   └── start_system.bat     # Windows启动脚本
│
├── test/                     # 🧪 测试文件
│   ├── comprehensive_system_test.py
│   ├── requirements_validation.py
│   ├── rule_validation_demo.py
│   └── reflection_reports/   # 测试报告
│
├── docs/                     # 📖 文档
│   └── DATA_OPTIMIZATION_SUMMARY.md
│
├── temp/                     # 🗂️ 临时文件
│   ├── 备份文件...          # 自动备份
│   └── 临时文件...          # 开发过程中的临时文件
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

### 🛠️ 开发工具
- **安全编辑**: 自动备份和语法检查
- **代码格式化**: Black + isort自动化格式化
- **中文支持**: 完整的中文字符集处理
- **版本控制**: Git版本管理

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

# 查看帮助信息
python stockpool.py --help

# 或使用启动脚本
python scripts/start_system.py
```

### 3. 股票监控工具
```bash
# 启动GUI监控界面（默认）
python stockmonitor.py

# 查看监控程序状态
python stockmonitor.py status

# 实时监控内存使用
python stockmonitor.py monitor

# 分析程序运行状态
python stockmonitor.py analyze

# 性能分析（60秒）
python stockmonitor.py profile

# 终止监控程序
python stockmonitor.py kill

# 查看帮助信息
python stockmonitor.py --help
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

## 许可证

本项目仅供学习和研究使用，请遵守相关法律法规。

---

*最后更新: 2025年9月11日*
