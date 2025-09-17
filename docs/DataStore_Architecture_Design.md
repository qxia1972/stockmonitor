# DataStore 架构重构设计方案

## 🎯 设计理念讨论

基于您提出的架构思想，我完全同意以下核心观点，并在此基础上补充更详细的设计方案：

### ✅ 用户提出的核心架构思想

#### 1. **数据分类与存储策略**
- **三类核心数据**：股票基本信息、价格序列、指标序列
- **文件缓存策略**：减少网络访问，复用已完成的计算
- **数据生命周期管理**：从网络获取→缓存存储→内存提供

#### 2. **数据源抽象化**
- **RQDatac作为基础数据源**：当前的主要数据提供者
- **抽象通用接口**：便于将来更换数据服务商
- **插件化架构**：支持多种数据源的动态切换

#### 3. **职责重新分配**
- **股票池管理上移**：从DataStore上移到PoolManager，成为业务数据的核心单元
- **评分引擎工具化**：作为PoolManager的工具类，抽象评分规则与计算过程

## 🏗️ 详细架构设计

### 1. **数据层架构 (Data Layer)**

#### 数据分类体系
```
数据层 (Data Layer)
├── 基础数据 (Foundation Data)
│   ├── 股票基本信息 (Stock Basic Info)
│   │   ├── 静态信息：代码、名称、行业、地区等
│   │   ├── 动态信息：市值、流通股本、财务指标等
│   │   └── 更新频率：每日/每周
│   ├── 价格序列 (Price Series)
│   │   ├── OHLCV数据：开高低收成交量
│   │   ├── 复权数据：前复权、后复权
│   │   └── 更新频率：实时/每日
│   └── 指标序列 (Indicator Series)
│       ├── 技术指标：MA、RSI、MACD等
│       ├── 复合指标：自定义组合指标
│       └── 更新频率：实时计算/定时计算
├── 业务数据 (Business Data)
│   ├── 股票池数据 (Stock Pool Data)
│   │   ├── 基础池 (Basic Pool)
│   │   ├── 观察池 (Watch Pool)
│   │   └── 核心池 (Core Pool)
│   └── 评分数据 (Scoring Data)
│       ├── 评分结果缓存
│       └── 评分历史记录
└── 元数据 (Metadata)
    ├── 数据版本信息
    ├── 时间戳和校验和
    └── 数据血缘关系
```

#### 数据存储策略
```
数据存储层次
├── 内存层 (Memory Layer)
│   ├── 热数据缓存 (Hot Cache)
│   │   ├── 最近访问数据
│   │   └── 高频使用数据
│   └── 计算结果缓存 (Computation Cache)
│       ├── 技术指标计算结果
│       └── 评分计算结果
├── 文件层 (File Layer)
│   ├── 本地文件缓存 (Local File Cache)
│   │   ├── JSON格式：结构化数据
│   │   ├── Pickle格式：复杂对象
│   │   └── CSV/Parquet：大数据集
│   └── 压缩归档 (Archive)
│       └── 历史数据归档存储
└── 网络层 (Network Layer)
    ├── 实时数据源 (Real-time Data Source)
    └── 批量数据源 (Batch Data Source)
```

### 2. **数据源抽象架构 (Data Source Abstraction)**

#### 数据源接口设计
```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
import pandas as pd

class DataSource(ABC):
    """数据源抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        pass

    @property
    @abstractmethod
    def priority(self) -> int:
        """数据源优先级 (越高越优先)"""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        pass

    @abstractmethod
    def get_stock_basic_info(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取股票基本信息"""
        pass

    @abstractmethod
    def get_price_data(self, stock_codes: List[str],
                      start_date: str, end_date: str,
                      frequency: str = '1d') -> Dict[str, pd.DataFrame]:
        """获取价格数据"""
        pass

    @abstractmethod
    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日历"""
        pass

    @abstractmethod
    def get_latest_trading_date(self) -> Optional[str]:
        """获取最新交易日"""
        pass

class RQDatacDataSource(DataSource):
    """RQDatac数据源实现"""
    name = "rqdatac"
    priority = 100

    def __init__(self, username: str = None, password: str = None):
        self._init_connection(username, password)

    def is_available(self) -> bool:
        try:
            import rqdatac
            return rqdatac.is_authenticed()
        except:
            return False

    # 实现所有抽象方法...

class LocalFileDataSource(DataSource):
    """本地文件数据源实现"""
    name = "local_file"
    priority = 50

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir

    def is_available(self) -> bool:
        return os.path.exists(self.data_dir)

    # 实现所有抽象方法...
```

#### 数据源管理器
```python
class DataSourceManager:
    """数据源管理器"""

    def __init__(self):
        self._data_sources: Dict[str, DataSource] = {}
        self._register_builtin_sources()

    def register_source(self, source: DataSource):
        """注册数据源"""
        self._data_sources[source.name] = source
        logger.info(f"注册数据源: {source.name} (优先级: {source.priority})")

    def get_available_sources(self) -> List[DataSource]:
        """获取可用的数据源，按优先级排序"""
        available = [source for source in self._data_sources.values()
                    if source.is_available()]
        return sorted(available, key=lambda x: x.priority, reverse=True)

    def get_data(self, method_name: str, *args, **kwargs):
        """从最佳可用数据源获取数据"""
        sources = self.get_available_sources()

        for source in sources:
            try:
                method = getattr(source, method_name)
                result = method(*args, **kwargs)
                logger.debug(f"从数据源 {source.name} 成功获取数据")
                return result
            except Exception as e:
                logger.warning(f"数据源 {source.name} 获取失败: {e}")
                continue

        raise DataSourceException(f"所有数据源都无法获取数据: {method_name}")
```

### 3. **BusinessModel 业务架构 (Business Architecture)**

#### BusinessModel 职责重新设计
```python
class BusinessModel:
    """
    股票池管理器 - 业务逻辑核心

    职责：
    1. 股票池生命周期管理 (创建、更新、删除、查询)
    2. 业务规则执行 (筛选、评分、排序)
    3. 数据加工协调 (调用各种处理器)
    4. 业务流程编排 (数据获取→处理→评分→池构建)
    5. 业务状态管理 (池状态、更新时间、统计信息)
    """

    def __init__(self, data_store: DataStore, scoring_engine: ScoringEngine):
        self.data_store = data_store
        self.scoring_engine = scoring_engine

        # 业务数据管理
        self._pools: Dict[str, StockPool] = {}
        self._pool_metadata: Dict[str, PoolMetadata] = {}

    def create_pool(self, pool_config: PoolConfig) -> str:
        """创建新的股票池"""
        pool_id = self._generate_pool_id()
        pool = StockPool(pool_id, pool_config)

        self._pools[pool_id] = pool
        self._pool_metadata[pool_id] = PoolMetadata(
            created_at=datetime.now(),
            last_updated=datetime.now(),
            status=PoolStatus.EMPTY
        )

        return pool_id

    def update_pool(self, pool_id: str, force_refresh: bool = False) -> bool:
        """更新股票池"""
        if pool_id not in self._pools:
            raise PoolNotFoundException(f"股票池不存在: {pool_id}")

        pool = self._pools[pool_id]

        # 业务流程编排
        try:
            # 1. 获取基础数据
            basic_data = self._fetch_basic_data(pool.config.filters)

            # 2. 计算技术指标
            indicator_data = self._calculate_indicators(basic_data)

            # 3. 执行评分
            scored_data = self.scoring_engine.score_stocks(
                basic_data, indicator_data, pool.config.scoring_rules
            )

            # 4. 构建池
            pool.update_stocks(scored_data)

            # 5. 更新元数据
            self._pool_metadata[pool_id].last_updated = datetime.now()
            self._pool_metadata[pool_id].status = PoolStatus.ACTIVE

            return True

        except Exception as e:
            logger.error(f"更新股票池失败 {pool_id}: {e}")
            self._pool_metadata[pool_id].status = PoolStatus.ERROR
            return False

    def get_pool(self, pool_id: str) -> Optional[StockPool]:
        """获取股票池"""
        return self._pools.get(pool_id)

    def list_pools(self) -> List[Dict]:
        """列出所有股票池"""
        return [{
            'pool_id': pool_id,
            'config': pool.config,
            'metadata': metadata,
            'stock_count': len(pool.stocks) if pool.stocks else 0
        } for pool_id, (pool, metadata) in self._pools.items()]

    def delete_pool(self, pool_id: str) -> bool:
        """删除股票池"""
        if pool_id in self._pools:
            del self._pools[pool_id]
            del self._pool_metadata[pool_id]
            return True
        return False
```

#### 评分引擎工具化
```python
class ScoringEngine:
    """
    评分引擎 - 评分规则与计算过程的抽象

    职责：
    1. 评分规则管理 (注册、配置、验证)
    2. 评分计算执行 (单股票、多股票批量)
    3. 评分结果处理 (排序、过滤、统计)
    4. 评分策略扩展 (插件化评分规则)
    """

    def __init__(self):
        self._scoring_rules: Dict[str, ScoringRule] = {}
        self._register_builtin_rules()

    def register_rule(self, rule: ScoringRule):
        """注册评分规则"""
        self._scoring_rules[rule.name] = rule

    def score_stocks(self, basic_data: pd.DataFrame,
                    indicator_data: Dict[str, pd.DataFrame],
                    scoring_config: ScoringConfig) -> pd.DataFrame:
        """对股票进行评分"""
        scored_stocks = []

        for _, stock_info in basic_data.iterrows():
            stock_code = stock_info['stock_code']

            # 获取该股票的指标数据
            stock_indicators = {}
            for indicator_name, indicator_df in indicator_data.items():
                if stock_code in indicator_df.index:
                    stock_indicators[indicator_name] = indicator_df.loc[stock_code]

            # 计算综合评分
            score = self._calculate_composite_score(
                stock_info, stock_indicators, scoring_config
            )

            scored_stocks.append({
                'stock_code': stock_code,
                'score': score,
                'basic_info': stock_info.to_dict(),
                'indicators': stock_indicators
            })

        return pd.DataFrame(scored_stocks).sort_values('score', ascending=False)

    def _calculate_composite_score(self, stock_info: pd.Series,
                                 indicators: Dict[str, Any],
                                 config: ScoringConfig) -> float:
        """计算综合评分"""
        total_score = 0.0
        total_weight = 0.0

        for rule_config in config.rules:
            rule = self._scoring_rules.get(rule_config.name)
            if rule:
                score = rule.calculate(stock_info, indicators, rule_config.params)
                total_score += score * rule_config.weight
                total_weight += rule_config.weight

        return total_score / total_weight if total_weight > 0 else 0.0
```

### 4. **数据加工器架构 (Data Processor Architecture)**

#### 处理器接口抽象
```python
class DataProcessor(ABC):
    """数据处理器抽象基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """处理器名称"""
        pass

    @property
    @abstractmethod
    def input_schema(self) -> Dict[str, Any]:
        """输入数据格式规范"""
        pass

    @property
    @abstractmethod
    def output_schema(self) -> Dict[str, Any]:
        """输出数据格式规范"""
        pass

    @abstractmethod
    def process(self, input_data: Any, **kwargs) -> Any:
        """处理数据"""
        pass

    @abstractmethod
    def validate_input(self, input_data: Any) -> bool:
        """验证输入数据"""
        pass

    @abstractmethod
    def validate_output(self, output_data: Any) -> bool:
        """验证输出数据"""
        pass

class IndicatorCalculator(DataProcessor):
    """技术指标计算器"""
    name = "indicator_calculator"

    def __init__(self, indicators: List[str]):
        self.indicators = indicators

    def process(self, price_data: pd.DataFrame, **kwargs) -> Dict[str, pd.Series]:
        """计算技术指标"""
        results = {}
        for indicator in self.indicators:
            results[indicator] = self._calculate_indicator(price_data, indicator)
        return results

class DataValidator(DataProcessor):
    """数据验证器"""
    name = "data_validator"

    def process(self, data: Any, **kwargs) -> ValidationResult:
        """验证数据质量"""
        return self._validate_data_quality(data)
```

## 💡 我的补充观点

### 1. **数据生命周期管理**
您的三类数据分类非常合理，我建议增加数据生命周期管理：
- **热数据**：内存缓存，实时访问
- **温数据**：文件缓存，定期访问
- **冷数据**：归档存储，历史查询

### 2. **数据血缘追踪**
建议增加数据血缘追踪功能：
- 记录数据的来源、处理过程、版本信息
- 支持数据质量问题回溯
- 便于调试和审计

### 3. **配置化评分引擎**
评分引擎工具化思路很好，建议进一步配置化：
- 评分规则配置外部化
- 支持A/B测试不同评分策略
- 评分结果的可解释性

### 4. **异步处理架构**
考虑引入异步处理：
- 长时间计算任务异步执行
- 实时数据更新推送
- 任务队列和状态管理

### 5. **监控和可观测性**
增加监控能力：
- 数据访问性能监控
- 缓存命中率统计
- 错误率和异常监控

## 🎯 实施建议

### Phase 1: 核心架构搭建 (2周)
1. 实现数据源抽象层
2. 重构DataStore职责分离
3. 创建BusinessModel业务框架

### Phase 2: 数据处理优化 (2周)
1. 实现数据加工器架构
2. 优化缓存策略
3. 建立数据质量监控

### Phase 3: 业务功能完善 (2周)
1. 完善评分引擎配置化
2. 实现异步处理机制
3. 建立监控和告警系统

这个架构设计既保持了您提出的核心思想，又增加了必要的扩展性和可维护性。您觉得这个设计方案如何？是否需要对某个部分进行更详细的设计？</content>
<parameter name="filePath">c:\Users\qxia1\Desktop\交易\项目代码\stockmonitor\docs\DataStore_Architecture_Design.md