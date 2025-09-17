# Dagster每日数据处理作业

基于Dagster框架实现的完整每日数据处理流程，包括数据同步、补全、指标计算、评分和存盘操作。

## 📋 功能概述

- **数据同步**: 从RQDatac获取OHLCV和基本面数据
- **数据补全**: 处理缺失数据值
- **指标计算**: 计算技术指标和基本面指标
- **评分存盘**: 计算综## 📅 调度配置

系统预配置了以下调度：

- **数据同步补全**: 每周一至周五 09:00（核心调度）
- **指标计算**: 每周一至周五 14:00
- **评分存盘**: 每周一至周五 15:00

### 作业依赖关系

```
数据同步补全 → 指标计算 → 评分存盘
```

### 传感器配置

- **sync_completion_trigger**: 监听同步补全作业成功后自动触发指标计算
- **indicator_scoring_trigger**: 监听指标计算作业成功后自动触发评分存盘**: 支持定时自动执行
- **监控告警**: 作业状态监控和错误处理

## 🏗️ 架构设计

### 核心理念

**同步 = 拉取 + 补全**

根据您的建议，我们将"拉取"和"补全"合并为一个统一的"同步"操作：
- **拉取**: 从RQDatac获取最新数据
- **补全**: 检查并补全历史数据的缺失值
- **同步**: 一次性完成拉取和补全，确保数据完整性

### 作业类型

1. **同步补全作业** (推荐用于生产环境)
   - `daily_data_sync_job`: 数据同步补全作业（核心）
   - 一次性完成拉取和补全操作

2. **独立作业** (用于开发和调试)
   - `data_completion_job`: 数据补全作业（兼容性）
   - `indicator_calculation_job`: 指标计算作业
   - `scoring_and_save_job`: 评分存盘作业

3. **完整管道作业**
   - `daily_full_pipeline_job`: 端到端完整处理流程

### 核心操作

- **sync_and_complete_ohlcv_data**: 同步并补全OHLCV数据
- **sync_and_complete_fundamental_data**: 同步并补全基本面数据
- **calculate_technical_indicators**: 计算技术指标
- **calculate_fundamental_indicators**: 计算基本面指标
- **merge_all_data**: 合并所有数据
- **calculate_final_scores**: 计算最终评分
- **save_processed_data**: 保存处理后的数据

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install dagster dagster-webserver polars

# 设置环境变量
export RQDATAC_API_KEY="your_api_key"
export DB_HOST="localhost"
export DB_NAME="stockmonitor"
export DB_USER="postgres"
export DB_PASSWORD="your_password"
```

### 2. 验证配置

```bash
# 验证配置完整性
python run_jobs.py --validate
```

### 3. 运行作业

```bash
# 运行数据同步补全作业（推荐）
python run_jobs.py sync

# 运行完整管道
python run_jobs.py full

# 查看所有可用作业
python run_jobs.py --list
```

### 推荐使用流程

1. **生产环境**: 使用 `sync` 作业（同步补全一体化）
2. **开发调试**: 使用独立作业进行分步调试
3. **完整流程**: 使用 `full` 作业进行端到端测试

### 4. 查看作业配置

```bash
# 查看同步作业配置
python run_jobs.py sync --show-config
```

## 📊 作业详情

### 数据同步补全作业 (sync)

**功能**: 从RQDatac拉取数据并补全缺失值（核心操作）

**操作流程**:
1. 获取目标股票列表和日期范围
2. 从RQDatac拉取指定日期范围的数据
3. 检查数据完整性，识别缺失值
4. 对缺失值进行智能补全（插值、前向填充等）
5. 返回完整的数据集

**优势**: 一次性完成拉取和补全，提高效率和数据一致性

**输出**: 完整补全后的数据DataFrame

### 数据补全作业 (completion)

**功能**: 独立的补全操作（兼容性保留）

**说明**: 主要用于特殊场景下的独立补全需求，生产环境推荐使用同步补全作业

### 指标计算作业 (calculation)

**功能**: 计算技术指标和基本面指标

**操作流程**:
1. 使用同步补全后的OHLCV数据
2. 计算技术指标 (SMA, RSI, MACD)
3. 使用同步补全后的基本面数据
4. 计算基本面指标 (PE, PB, ROE)

**输出**: 包含指标的数据DataFrame

### 指标计算作业 (calculation)

**功能**: 计算技术指标和基本面指标

**操作流程**:
1. 执行数据补全
2. 计算技术指标 (SMA, RSI, MACD)
3. 计算基本面指标 (PE, PB, ROE)

**输出**: 包含指标的数据DataFrame

### 评分存盘作业 (scoring)

**功能**: 计算综合评分并保存结果

**操作流程**:
1. 执行指标计算
2. 合并所有数据
3. 计算综合评分
4. 保存到Parquet文件

**输出**: 处理结果文件路径

### 完整管道作业 (full)

**功能**: 端到端完整处理流程

**操作流程**: 按顺序执行上述所有步骤

## ⚙️ 配置说明

### 环境变量

```bash
# RQDatac配置
RQDATAC_API_KEY=your_api_key
RQDATAC_BASE_URL=https://api.ricequant.com

# 数据库配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stockmonitor
DB_USER=postgres
DB_PASSWORD=your_password

# 处理配置
DATA_DIR=data
CACHE_DIR=cache
LOG_DIR=logs
```

### 自定义配置

```bash
# 使用自定义配置运行
python run_jobs.py sync --config target_stocks=000001.XSHE,000002.XSHE --config days_back=60
```

## 📅 调度配置

系统预配置了以下调度：

- **数据同步**: 每周一至五 09:00
- **数据补全**: 每周一至五 10:00
- **指标计算**: 每周一至五 14:00
- **评分存盘**: 每周一至五 15:00

### 启动调度器

```bash
# 启动Dagster调度器
dagster-daemon run

# 启动Web UI
dagster-webserver
```

## 🔧 开发和调试

### 本地开发

```bash
# 运行单个操作进行调试
python -c "
from orchestration.daily_processing_jobs import get_target_stocks_op
from dagster import build_op_context

context = build_op_context()
result = get_target_stocks_op(context)
print('目标股票:', result)
"
```

### 错误处理

- **重试机制**: 所有操作都配置了重试策略
- **日志记录**: 详细的执行日志和错误信息
- **状态监控**: 通过Dagster UI监控作业状态

### 性能优化

- **并行处理**: 数据同步操作支持并行执行
- **批处理**: 大数据量采用批处理策略
- **缓存机制**: 重复数据自动缓存

## 📈 监控和告警

### 作业状态监控

```python
from dagster import run_status_sensor, DagsterRunStatus

@run_status_sensor(
    monitored_jobs=[daily_data_sync_job],
    run_status=DagsterRunStatus.SUCCESS
)
def sync_success_handler(context):
    # 处理成功完成的通知
    pass
```

### 性能指标

- 执行时间统计
- 数据处理量监控
- 错误率跟踪
- 资源使用情况

## 🔍 故障排除

### 常见问题

1. **配置错误**
   ```bash
   python run_jobs.py --validate
   ```

2. **依赖缺失**
   ```bash
   pip install -r requirements.txt
   ```

3. **权限问题**
   - 检查文件系统权限
   - 验证数据库连接权限
   - 确认RQDatac API密钥有效性

4. **内存不足**
   - 调整批处理大小
   - 增加系统内存
   - 使用数据分片处理

### 日志查看

```bash
# 查看应用日志
tail -f logs/stockmonitor.log

# 查看Dagster日志
tail -f logs/dagster.log
```

## 📚 扩展开发

### 添加新指标

```python
@op
def calculate_custom_indicator_op(context, data):
    # 实现自定义指标计算逻辑
    return processed_data
```

### 添加新数据源

```python
@resource
def custom_data_source_resource(context):
    # 实现自定义数据源连接
    return connection
```

### 自定义调度

```python
@schedule(
    cron_schedule="0 */2 * * *",  # 每2小时
    job=custom_job
)
def custom_schedule():
    return RunConfig()
```

## 📄 许可证

本项目采用 MIT 许可证。