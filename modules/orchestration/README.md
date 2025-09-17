# 编排层使用指南

## 📋 概述

编排层是股票监控系统的新架构核心，基于Dagster提供强大的工作流编排能力。它集成了计算层、存储层和查询层的所有组件，提供端到端的数据处理管道。

## 🏗️ 核心组件

### PipelineManager (`orchestration/pipeline_manager.py`)
- **功能**: 统一管理所有组件和管道
- **特性**:
  - 组件依赖注入
  - 作业注册和管理
  - 资产注册和管理
  - Dagster定义生成

### 作业定义 (`orchestration/job_definitions.py`)
预定义的作业包括：
- `stock_analysis_job`: 完整的股票分析管道
- 数据加载、指标计算、评分计算等操作

## 🚀 快速开始

### 1. 运行演示脚本
```bash
python scripts/orchestration_demo.py
```

### 2. 基本使用示例
```python
from orchestration.pipeline_manager import pipeline_manager

# 加载数据
data = pipeline_manager.data_processor.load_data("data/market_data.parquet")

# 计算技术指标
indicators = pipeline_manager.indicator_calculator.calculate_indicators(data)

# 计算评分
scores = pipeline_manager.score_calculator.calculate_technical_score(indicators)

# 保存结果
pipeline_manager.data_processor.save_data(scores, "data/results.parquet")
```

### 3. 使用查询层
```python
# 加载数据到查询引擎
pipeline_manager.query_engine.load_parquet_table("stocks", "data/market_data.parquet")

# 执行查询
result = pipeline_manager.query_engine.execute_query("""
    SELECT symbol, close, rsi_14
    FROM stocks
    WHERE rsi_14 < 30
    ORDER BY rsi_14 ASC
""")
```

## 📊 主要特性

### ✅ 工作流编排
- 基于Dagster的声明式管道定义
- 自动依赖管理和执行顺序
- 可视化的管道监控界面

### ✅ 错误处理
- 自动重试机制（默认3次）
- 完善的异常捕获和日志记录
- 失败恢复和资产物化

### ✅ 并行处理
- 多股票并行计算
- 线程池管理
- 高性能批量处理

### ✅ 资产物化
- 数据处理结果的持久化
- 元数据跟踪和版本管理
- 数据血缘分析

### ✅ 监控和日志
- 完整的执行日志
- 性能指标收集
- 错误统计和告警

## 🔧 配置和扩展

### 添加新的作业
```python
from dagster import job, op
from orchestration.pipeline_manager import pipeline_manager

@op
def my_custom_operation(context):
    # 自定义操作逻辑
    return "result"

@job
def my_custom_job():
    my_custom_operation()

# 注册到管道管理器
pipeline_manager.register_job("my_custom_job", my_custom_job)
```

### 自定义评分规则
```python
from compute.score_calculator import ScoreCalculator

calculator = ScoreCalculator()
calculator.add_scoring_rule("custom_indicator", {
    "field": "my_indicator",
    "ideal_range": (0.5, 1.0),
    "ideal_score": 10,
    "weight": 0.2
})
```

## 📈 性能优化

### 并行处理配置
```python
# 配置并行处理器
pipeline_manager.parallel_processor.configure(
    max_workers=8,
    chunk_size=100
)
```

### 内存优化
- 使用Polars的延迟计算
- 分批处理大数据集
- 及时释放不需要的数据

## 🔍 故障排除

### 常见问题

1. **技术指标计算失败**
   - 检查输入数据格式
   - 确认必要的列存在（open, high, low, close, volume）
   - 查看日志中的具体错误信息

2. **查询执行慢**
   - 确保已创建适当的索引
   - 考虑数据分区策略
   - 检查查询语句的复杂度

3. **内存不足**
   - 减少并行处理的worker数量
   - 使用数据流式处理
   - 增加系统内存或使用更大的实例

### 调试技巧
- 查看详细日志：设置 `logging.level = DEBUG`
- 使用Dagster UI监控管道执行
- 检查资产物化的中间结果

## 📚 相关文档

- [系统架构概述](../ARCH.md)
- [数据处理指南](../docs/DataSource_Abstraction_Guide.md)
- [存储层设计](../docs/DataStore_Architecture_Design.md)
- [API参考](../docs/)

## 🎯 最佳实践

1. **数据验证**: 在管道开始时验证输入数据质量
2. **错误处理**: 为关键操作添加适当的重试策略
3. **监控告警**: 设置关键指标的监控和告警
4. **性能调优**: 根据数据规模调整并行度和批处理大小
5. **测试覆盖**: 为所有管道操作编写单元测试

## 🤝 贡献

欢迎提交Issue和Pull Request来改进编排层功能！

---

*编排层为系统提供了强大的工作流管理能力，是连接各组件的桥梁。*</content>
<parameter name="filePath">c:\Users\qxia1\Desktop\交易\项目代码\stockmonitor\orchestration\README.md