"""
股票日线级别综合评分系统 - 优化版
基于Polars的高性能评分计算引擎

版本: 2.1.0
更新日期: 2025年9月16日
"""

import polars as pl
import numpy as np
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class StockScorer:
    """股票综合评分器 - 优化版"""

    VERSION = "2.1.0"
    CALCULATION_TIMESTAMP = None

    # 默认权重配置（可根据市场环境动态调整）
    WEIGHT_CONFIG = {
        "trend_strength": 0.45,  # 趋势强度 - 激进提高权重
        "capital_power": 0.20,   # 资金动能 - 激进降低权重
        "technical": 0.20,       # 技术指标
        "risk_control": 0.15     # 风险控制
    }

    # 行业调整系数
    INDUSTRY_ADJUSTMENT = {
        "科技": 1.15,
        "新能源": 1.15,
        "医药": 1.05,
        "消费": 1.05,
        "金融": 0.95,
        "资源": 0.95,
        "其他": 1.00
    }

    # 市值调整系数
    MARKET_CAP_ADJUSTMENT = {
        "大盘股": 0.95,   # >500亿
        "中盘股": 1.00,   # 100-500亿
        "小盘股": 1.10    # <100亿
    }

    # 市场环境权重调整因子
    MARKET_ENV_ADJUSTMENT = {
        "bull": {  # 牛市
            "trend_strength": 1.0,
            "capital_power": 1.4,    # 牛市资金更重要
            "technical": 0.9,
            "risk_control": 0.8      # 牛市降低风险权重
        },
        "bear": {  # 熊市
            "trend_strength": 0.8,
            "capital_power": 1.3,    # 熊市资金活跃度更重要
            "technical": 0.9,
            "risk_control": 1.3      # 熊市提高风险权重
        },
        "volatile": {  # 震荡市
            "trend_strength": 0.9,
            "capital_power": 1.5,    # 震荡市资金活跃度最重要
            "technical": 1.0,
            "risk_control": 1.0      # 震荡市保持基准风险权重
        },
        "normal": {  # 正常市
            "trend_strength": 1.0,
            "capital_power": 1.2,    # 正常市仍重视资金
            "technical": 0.9,
            "risk_control": 1.0      # 正常市保持基准风险权重
        }
    }

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._metadata: Dict[str, Any] = {
            "version": self.VERSION,
            "calculation_count": 0,
            "last_calculation_time": None,
            "performance_stats": {},
            "data_quality_warnings": []
        }

    def score_stocks(self, 
                    df: pl.DataFrame,
                    market_env: str = "normal") -> pl.DataFrame:
        """计算股票综合评分
        
        Args:
            df: 包含股票日线数据的DataFrame
            market_env: 市场环境（bull/bear/volatile/normal）
            
        Returns:
            包含评分结果的DataFrame
        """
        if df.is_empty():
            logger.warning("⚠️ 输入数据为空")
            return df
            
        # 记录计算开始时间
        start_time = datetime.now()
        self._metadata["last_calculation_time"] = start_time
        
        # 数据质量检查
        quality_warnings = self._validate_input_data(df)
        self._metadata["data_quality_warnings"] = quality_warnings
        
        if quality_warnings:
            logger.warning(f"⚠️ 数据质量警告: {len(quality_warnings)} 项")
            for warning in quality_warnings:
                logger.warning(f"  - {warning}")
        
        # 计算各维度得分
        df = self._calculate_trend_strength(df)
        df = self._calculate_capital_power(df)
        df = self._calculate_technical_indicators(df)
        df = self._calculate_risk_control(df)
        
        # 计算综合得分
        weights = self._get_dynamic_weights(market_env)
        df = self._calculate_composite_score(df, weights)
        
        # 应用行业和市值调整
        df = self._apply_adjustments(df)
        
        # 添加评分等级
        df = df.with_columns(
            pl.when(pl.col("composite_score") >= 85).then(pl.lit("强势股"))
            .when(pl.col("composite_score") >= 70).then(pl.lit("潜力股"))
            .when(pl.col("composite_score") >= 55).then(pl.lit("震荡股"))
            .when(pl.col("composite_score") >= 40).then(pl.lit("弱势股"))
            .otherwise(pl.lit("高风险股"))
            .alias("score_level")
        )
        
        # 添加元数据
        df = self._add_calculation_metadata(df, market_env, weights, start_time)
        
        # 更新性能统计
        end_time = datetime.now()
        calculation_time = (end_time - start_time).total_seconds()
        self._metadata["calculation_count"] += 1
        self._metadata["performance_stats"]["last_calculation_time"] = calculation_time
        self._metadata["performance_stats"]["avg_calculation_time"] = (
            (self._metadata["performance_stats"].get("avg_calculation_time", 0) * 
             (self._metadata["calculation_count"] - 1) + calculation_time) / 
            self._metadata["calculation_count"]
        )
        
        # logger.info(f"✅ 评分计算完成: {len(df)} 只股票, 耗时 {calculation_time:.3f}秒")
        return df

    def _validate_input_data(self, df: pl.DataFrame) -> List[str]:
        """验证输入数据的质量"""
        warnings = []
        
        # 检查必需字段
        required_fields = ["close"]
        for field in required_fields:
            if field not in df.columns:
                warnings.append(f"缺少必需字段: {field}")
        
        # 检查数据完整性
        if df.height == 0:
            warnings.append("数据为空")
        else:
            # 检查数值字段的空值比例
            numeric_fields = ["close", "high", "low", "open", "volume"]
            for field in numeric_fields:
                if field in df.columns:
                    null_ratio = df[field].null_count() / df.height
                    if null_ratio > 0.1:  # 超过10%的空值
                        warnings.append(f"字段 {field} 空值比例过高: {null_ratio:.1%}")
        
        # 检查行业名称是否与本系统定义匹配
        expected_industries = set(self.INDUSTRY_ADJUSTMENT.keys())
        industry_col = None
        if "industry" in df.columns:
            industry_col = "industry"
        elif "industry_name" in df.columns:
            industry_col = "industry_name"

        if industry_col is not None and df.height > 0:
            try:
                unique_industries = (
                    df.select(pl.col(industry_col))
                      .drop_nulls()
                      .unique()
                      .to_series()
                      .to_list()
                )
                unknown = [x for x in unique_industries if x not in expected_industries]
                if unknown:
                    sample = ", ".join(map(str, unknown[:10]))
                    more = "" if len(unknown) <= 10 else f" 等共{len(unknown)}种"
                    warnings.append(
                        f"行业名称不匹配：{sample}{more}；期望集合：{sorted(expected_industries)}"
                    )
            except Exception as _:
                warnings.append("行业名称校验失败（数据解析异常）")

        return warnings

    def _add_calculation_metadata(self, df: pl.DataFrame, market_env: str, 
                                weights: Dict[str, float], start_time: datetime) -> pl.DataFrame:
        """添加计算元数据"""
        metadata = {
            "scorer_version": self.VERSION,
            "calculation_timestamp": start_time.isoformat(),
            "market_environment": market_env,
            "weights_used": weights,
            "data_quality_warnings": len(self._metadata["data_quality_warnings"]),
            "calculation_count": self._metadata["calculation_count"]
        }
        
        # 将元数据添加到DataFrame的属性中
        df = df.with_columns([
            pl.lit(metadata["scorer_version"]).alias("_scorer_version"),
            pl.lit(metadata["calculation_timestamp"]).alias("_calculation_timestamp"),
            pl.lit(metadata["market_environment"]).alias("_market_environment"),
            pl.lit(str(metadata["weights_used"])).alias("_weights_used"),
            pl.lit(metadata["data_quality_warnings"]).alias("_data_quality_warnings"),
            pl.lit(metadata["calculation_count"]).alias("_calculation_count")
        ])
        
        return df

    def _calculate_trend_strength(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算趋势强度维度得分（满分100分）"""
        return df.with_columns([
            # 多空排列评分 - 使用sma字段，移除SMA60判断 (满分30分)
            pl.when(
                (pl.col("sma_5") > pl.col("sma_10")) & 
                (pl.col("sma_10") > pl.col("sma_20"))
            ).then(30.0)
            .when(pl.col("sma_5") > pl.col("sma_10"))
            .then(21.0)
            .when(pl.col("sma_5") > pl.col("sma_20"))
            .then(12.0)
            .otherwise(3.0)
            .alias("score_arrangement"),
            
            # 趋势斜率评分（10日线角度）- 移除陡峭角度惩罚 (满分30分)
            pl.when(pl.col("ma10_angle") >= 45).then(30.0)  # 良好上涨趋势
            .when(pl.col("ma10_angle") >= 15).then(25.0)  # 适中上涨趋势
            .when(pl.col("ma10_angle") >= 5).then(20.0)   # 温和上涨趋势
            .when(pl.col("ma10_angle").is_between(-5, 5)).then(15.0)   # 平稳趋势
            .when(pl.col("ma10_angle") >= -15).then(10.0)  # 温和下跌趋势
            .otherwise(5.0)  # 明显下跌趋势
            .alias("score_slope"),
            
            # 位置强度评分（相对于历史最高价）(满分25分)
            pl.when(pl.col("close") > pl.col("year_high")).then(25.0)
            .when(pl.col("close") > pl.col("year_high") * 0.95).then(20.0)
            .when(pl.col("close") > pl.col("year_high") * 0.9).then(15.0)
            .when(pl.col("close") < pl.col("year_low")).then(0.0)
            .otherwise(8.0)
            .alias("score_position"),
            
            # 趋势稳定性评分（20日波动率）(满分15分)
            pl.when(pl.col("volatility_20d") < 0.15).then(15.0)
            .when(pl.col("volatility_20d").is_between(0.15, 0.25)).then(10.0)
            .when(pl.col("volatility_20d").is_between(0.25, 0.35)).then(5.0)
            .otherwise(0.0)
            .alias("score_stability")
        ]).with_columns(
            # 趋势强度总分 (满分100分)
            (pl.col("score_arrangement") + 
             pl.col("score_slope") + 
             pl.col("score_position") + 
             pl.col("score_stability"))
            .alias("trend_score")
        )

    def _calculate_capital_power(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算资金动能维度得分（满分100分）"""
        return df.with_columns([
            # 量价配合评分 (满分30分) - 考虑价格趋势方向
            pl.when(
                ((pl.col("pct_change") > 0) & (pl.col("volume_ratio") > 1.5) & (pl.col("ma10_angle") > 0)) |
                ((pl.col("pct_change") < 0) & (pl.col("volume_ratio") < 0.7) & (pl.col("ma10_angle") < 0))
            ).then(30.0)
            .when(
                ((pl.col("pct_change") > 0) & (pl.col("volume_ratio") > 1.2) & (pl.col("ma10_angle") > 0)) |
                ((pl.col("pct_change") < 0) & (pl.col("volume_ratio") < 0.8) & (pl.col("ma10_angle") < 0))
            ).then(25.0)
            .when(
                (pl.col("pct_change") > 0) & (pl.col("volume_ratio").is_between(1.0, 1.2)) & (pl.col("ma10_angle") > 0)
            ).then(20.0)
            .when(
                (pl.col("pct_change") < 0) & (pl.col("volume_ratio").is_between(0.8, 1.0)) & (pl.col("ma10_angle") < 0)
            ).then(15.0)
            .when(
                (pl.col("pct_change") > 0) & (pl.col("volume_ratio").is_between(0.8, 1.0)) & (pl.col("ma10_angle") > 0)
            ).then(12.0)
            .when(
                (pl.col("pct_change") < 0) & (pl.col("volume_ratio") > 1.0) & (pl.col("ma10_angle") < 0)
            ).then(9.0)
            .otherwise(3.0)
            .alias("score_volume_price"),

            # 资金流入评分 (满分25分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("main_net_inflow_ratio") > 0.08) & (pl.col("ma10_angle") > -10)
            ).then(25.0)
            .when(
                (pl.col("main_net_inflow_ratio") > 0.08) & (pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.05, 0.08)) & (pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.05, 0.08)) & (pl.col("ma10_angle") <= -10)
            ).then(12.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.03, 0.05)) & (pl.col("ma10_angle") > -10)
            ).then(15.0)
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.03, 0.05)) & (pl.col("ma10_angle") <= -10)
            ).then(9.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.01, 0.03)) & (pl.col("ma10_angle") > -10)
            ).then(12.0)
            .when(
                (pl.col("main_net_inflow_ratio").is_between(0.01, 0.03)) & (pl.col("ma10_angle") <= -10)
            ).then(6.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("main_net_inflow_ratio").is_between(-0.005, 0.01)) & (pl.col("ma10_angle") > -10)
            ).then(6.0)
            .when(
                (pl.col("main_net_inflow_ratio").is_between(-0.005, 0.01)) & (pl.col("ma10_angle") <= -10)
            ).then(3.0)  # 下跌趋势时进一步降低
            .when(
                (pl.col("main_net_inflow_ratio").is_between(-0.01, -0.005)) & (pl.col("ma10_angle") > -10)
            ).then(3.0)
            .when(
                (pl.col("main_net_inflow_ratio").is_between(-0.01, -0.005)) & (pl.col("ma10_angle") <= -10)
            ).then(1.0)  # 下跌趋势时大幅降低
            .otherwise(0.0)
            .alias("score_inflow"),

            # 机构参与评分 (满分25分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("large_order_ratio") > 0.4) & (pl.col("ma10_angle") > -10)
            ).then(25.0)
            .when(
                (pl.col("large_order_ratio") > 0.4) & (pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("large_order_ratio").is_between(0.3, 0.4)) & (pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("large_order_ratio").is_between(0.3, 0.4)) & (pl.col("ma10_angle") <= -10)
            ).then(12.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("large_order_ratio").is_between(0.2, 0.3)) & (pl.col("ma10_angle") > -10)
            ).then(15.0)
            .when(
                (pl.col("large_order_ratio").is_between(0.2, 0.3)) & (pl.col("ma10_angle") <= -10)
            ).then(9.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("large_order_ratio").is_between(0.15, 0.2)) & (pl.col("ma10_angle") > -10)
            ).then(12.0)
            .when(
                (pl.col("large_order_ratio").is_between(0.15, 0.2)) & (pl.col("ma10_angle") <= -10)
            ).then(6.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("large_order_ratio").is_between(0.1, 0.15)) & (pl.col("ma10_angle") > -10)
            ).then(9.0)
            .when(
                (pl.col("large_order_ratio").is_between(0.1, 0.15)) & (pl.col("ma10_angle") <= -10)
            ).then(3.0)  # 下跌趋势时大幅降低
            .when(
                (pl.col("large_order_ratio").is_between(0.05, 0.1)) & (pl.col("ma10_angle") > -10)
            ).then(6.0)
            .when(
                (pl.col("large_order_ratio").is_between(0.05, 0.1)) & (pl.col("ma10_angle") <= -10)
            ).then(2.0)  # 下跌趋势时大幅降低
            .otherwise(1.0)
            .alias("score_institution"),

            # 量能趋势评分 (满分20分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("volume_trend") == "increasing") & (pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("volume_trend") == "increasing") & (pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("volume_trend") == "intermittent") & (pl.col("ma10_angle") > -10)
            ).then(15.0)
            .when(
                (pl.col("volume_trend") == "intermittent") & (pl.col("ma10_angle") <= -10)
            ).then(8.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("volume_trend") == "stable") & (pl.col("ma10_angle") > -10)
            ).then(10.0)
            .when(
                (pl.col("volume_trend") == "stable") & (pl.col("ma10_angle") <= -10)
            ).then(5.0)  # 下跌趋势时降低分数
            .otherwise(3.0)
            .alias("score_volume_trend")
        ]).with_columns(
            # 资金动能总分 (满分100分)
            (pl.col("score_volume_price") +
             pl.col("score_inflow") +
             pl.col("score_institution") +
             pl.col("score_volume_trend"))
            .alias("capital_score")
        )

    def _calculate_technical_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算技术指标维度得分（满分100分）"""
        return df.with_columns([
            # RSI评分（满分30分）- 下跌趋势时降低权重
            pl.when(
                (pl.col("rsi_14") > 55).and_(pl.col("ma10_angle") > -10)
            ).then(30.0)
            .when(
                (pl.col("rsi_14") > 55).and_(pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("rsi_14") > 50).and_(pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("rsi_14") > 50).and_(pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("rsi_14") < 40).and_(pl.col("ma10_angle") > -10)
            ).then(5.0)
            .when(
                (pl.col("rsi_14") < 40).and_(pl.col("ma10_angle") <= -10)
            ).then(2.0)  # 下跌趋势时进一步降低
            .otherwise(15.0)
            .alias("score_rsi"),
            
            # MACD状态评分 (满分30分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("macd_diff") > 0).and_(pl.col("macd_diff") > pl.col("macd_dea")).and_(pl.col("macd_hist") > 0).and_(pl.col("ma10_angle") > -10)
            ).then(30.0)
            .when(
                (pl.col("macd_diff") > 0).and_(pl.col("macd_diff") > pl.col("macd_dea")).and_(pl.col("macd_hist") > 0).and_(pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("macd_diff") > 0).and_(pl.col("macd_diff") > pl.col("macd_dea")).and_(pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("macd_diff") > 0).and_(pl.col("macd_diff") > pl.col("macd_dea")).and_(pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("macd_diff") < 0).and_(pl.col("macd_diff") < pl.col("macd_dea")).and_(pl.col("ma10_angle") > -10)
            ).then(5.0)
            .when(
                (pl.col("macd_diff") < 0).and_(pl.col("macd_diff") < pl.col("macd_dea")).and_(pl.col("ma10_angle") <= -10)
            ).then(2.0)  # 下跌趋势时进一步降低
            .otherwise(10.0)
            .alias("score_macd"),
            
            # KDJ位置评分 (满分20分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("stoch_k") > pl.col("stoch_d")).and_(pl.col("stoch_k") < 80).and_(pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("stoch_k") > pl.col("stoch_d")).and_(pl.col("stoch_k") < 80).and_(pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("stoch_k") > pl.col("stoch_d")).and_(pl.col("stoch_k") >= 80).and_(pl.col("ma10_angle") > -10)
            ).then(10.0)
            .when(
                (pl.col("stoch_k") > pl.col("stoch_d")).and_(pl.col("stoch_k") >= 80).and_(pl.col("ma10_angle") <= -10)
            ).then(5.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("stoch_k") < pl.col("stoch_d")).and_(pl.col("ma10_angle") > -10)
            ).then(5.0)
            .when(
                (pl.col("stoch_k") < pl.col("stoch_d")).and_(pl.col("ma10_angle") <= -10)
            ).then(2.0)  # 下跌趋势时进一步降低
            .otherwise(0.0)
            .alias("score_kdj"),
            
            # 布林带位置评分 (满分20分) - 下跌趋势时降低权重
            pl.when(
                (pl.col("close") > pl.col("boll")).and_(pl.col("boll_up") - pl.col("boll_down") > pl.col("boll_std") * 2).and_(pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("close") > pl.col("boll")).and_(pl.col("boll_up") - pl.col("boll_down") > pl.col("boll_std") * 2).and_(pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("close") > pl.col("boll")).and_(pl.col("ma10_angle") > -10)
            ).then(15.0)
            .when(
                (pl.col("close") > pl.col("boll")).and_(pl.col("ma10_angle") <= -10)
            ).then(8.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("close") > pl.col("boll_down")).and_(pl.col("ma10_angle") > -10)
            ).then(10.0)
            .when(
                (pl.col("close") > pl.col("boll_down")).and_(pl.col("ma10_angle") <= -10)
            ).then(5.0)  # 下跌趋势时降低分数
            .otherwise(3.0)
            .alias("score_boll")
        ]).with_columns(
            # 技术指标总分 (满分100分)
            (pl.col("score_rsi") + 
             pl.col("score_macd") + 
             pl.col("score_kdj") + 
             pl.col("score_boll"))
            .alias("technical_score")
        )

    def _calculate_risk_control(self, df: pl.DataFrame) -> pl.DataFrame:
        """计算风险控制维度得分（满分100分）"""
        return df.with_columns([
            # 回调幅度评分 (满分40分) - 下跌趋势时更加严格
            pl.when(
                (pl.col("drawdown_from_high") < 0.10) & (pl.col("ma10_angle") > -10)
            ).then(40.0)
            .when(
                (pl.col("drawdown_from_high") < 0.10) & (pl.col("ma10_angle") <= -10)
            ).then(20.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("drawdown_from_high").is_between(0.10, 0.20)) & (pl.col("ma10_angle") > -10)
            ).then(25.0)
            .when(
                (pl.col("drawdown_from_high").is_between(0.10, 0.20)) & (pl.col("ma10_angle") <= -10)
            ).then(10.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("drawdown_from_high").is_between(0.20, 0.30)) & (pl.col("ma10_angle") > -10)
            ).then(10.0)
            .when(
                (pl.col("drawdown_from_high").is_between(0.20, 0.30)) & (pl.col("ma10_angle") <= -10)
            ).then(5.0)  # 下跌趋势时进一步降低
            .otherwise(0.0)
            .alias("score_drawdown"),
            
            # 支撑强度评分 (满分35分) - 下跌趋势时更加严格
            pl.when(
                (pl.col("distance_to_support") > 0.15) & (pl.col("ma10_angle") > -10)
            ).then(35.0)
            .when(
                (pl.col("distance_to_support") > 0.15) & (pl.col("ma10_angle") <= -10)
            ).then(20.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("distance_to_support").is_between(0.05, 0.15)) & (pl.col("ma10_angle") > -10)
            ).then(25.0)
            .when(
                (pl.col("distance_to_support").is_between(0.05, 0.15)) & (pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("distance_to_support") < 0.05) & (pl.col("ma10_angle") > -10)
            ).then(10.0)
            .when(
                (pl.col("distance_to_support") < 0.05) & (pl.col("ma10_angle") <= -10)
            ).then(5.0)  # 下跌趋势时进一步降低
            .when(
                pl.col("support_broken") == True
            ).then(0.0)  # 支撑破位，无论趋势都给0分
            .otherwise(15.0)
            .alias("score_support"),
            
            # 风险收益比评分 (满分25分) - 下跌趋势时更加严格
            pl.when(
                (pl.col("risk_reward_ratio") > 3.0) & (pl.col("ma10_angle") > -10)
            ).then(25.0)
            .when(
                (pl.col("risk_reward_ratio") > 3.0) & (pl.col("ma10_angle") <= -10)
            ).then(15.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("risk_reward_ratio").is_between(2.0, 3.0)) & (pl.col("ma10_angle") > -10)
            ).then(20.0)
            .when(
                (pl.col("risk_reward_ratio").is_between(2.0, 3.0)) & (pl.col("ma10_angle") <= -10)
            ).then(12.0)  # 下跌趋势时降低分数
            .when(
                (pl.col("risk_reward_ratio").is_between(1.0, 2.0)) & (pl.col("ma10_angle") > -10)
            ).then(15.0)
            .when(
                (pl.col("risk_reward_ratio").is_between(1.0, 2.0)) & (pl.col("ma10_angle") <= -10)
            ).then(8.0)  # 下跌趋势时降低分数
            .otherwise(5.0)
            .alias("score_rr_ratio")
        ]).with_columns(
            # 风险控制总分 (满分100分)
            (pl.col("score_drawdown") + 
             pl.col("score_support") + 
             pl.col("score_rr_ratio"))
            .alias("risk_score")
        )

    def _calculate_composite_score(self, 
                                  df: pl.DataFrame, 
                                  weights: Dict[str, float]) -> pl.DataFrame:
        """计算综合评分"""
        # 确保各维度分数不超过上限
        capped_scores = [
            pl.when(pl.col("trend_score") > 100).then(100).otherwise(pl.col("trend_score")).alias("trend_score"),
            pl.when(pl.col("capital_score") > 100).then(100).otherwise(pl.col("capital_score")).alias("capital_score"),
            pl.when(pl.col("technical_score") > 100).then(100).otherwise(pl.col("technical_score")).alias("technical_score"),
            pl.when(pl.col("risk_score") > 100).then(100).otherwise(pl.col("risk_score")).alias("risk_score")
        ]
        
        # 计算加权总分
        return df.with_columns(capped_scores).with_columns(
            (pl.col("trend_score") * weights["trend_strength"] +
             pl.col("capital_score") * weights["capital_power"] +
             pl.col("technical_score") * weights["technical"] +
             pl.col("risk_score") * weights["risk_control"])
            .clip(0, 100)  # 确保在0-100范围内
            .alias("composite_score")
        )

    def _get_dynamic_weights(self, market_env: str) -> Dict[str, float]:
        """根据市场环境获取动态权重"""
        base_weights = self.WEIGHT_CONFIG.copy()
        adjustments = self.MARKET_ENV_ADJUSTMENT.get(market_env, self.MARKET_ENV_ADJUSTMENT["normal"])
        
        return {
            "trend_strength": base_weights["trend_strength"] * adjustments["trend_strength"],
            "capital_power": base_weights["capital_power"] * adjustments["capital_power"],
            "technical": base_weights["technical"] * adjustments["technical"],
            "risk_control": base_weights["risk_control"] * adjustments["risk_control"]
        }

    def _apply_adjustments(self, df: pl.DataFrame) -> pl.DataFrame:
        """应用行业和市值调整"""
        # 行业调整 - 如果没有行业列，使用默认值1.0
        if "industry" in df.columns:
            industry_adjust = pl.when(pl.col("industry") == "科技").then(1.15)
            for industry, factor in self.INDUSTRY_ADJUSTMENT.items():
                if industry != "科技":  # 跳过第一个，因为已经在when中处理
                    industry_adjust = industry_adjust.when(pl.col("industry") == industry).then(factor)
            industry_adjust = industry_adjust.otherwise(1.0)
        else:
            industry_adjust = pl.lit(1.0)

        # 市值调整 - 如果没有市值列，使用默认值1.0
        if "market_cap" in df.columns:
            cap_adjust = pl.when(pl.col("market_cap") > 5e10).then(0.95) \
                .when(pl.col("market_cap").is_between(1e10, 5e10)).then(1.00) \
                .otherwise(1.10)
        else:
            cap_adjust = pl.lit(1.0)

        # 应用调整系数
        return df.with_columns(
            (pl.col("composite_score") * industry_adjust * cap_adjust)
            .clip(0, 100)
            .alias("composite_score")
        )

    def filter_by_score(self, df: pl.DataFrame, min_score: float = 65) -> pl.DataFrame:
        """根据评分过滤股票"""
        if df.is_empty():
            return df
            
        return df.filter(pl.col("composite_score") >= min_score)

    def get_metadata(self) -> Dict[str, Any]:
        """获取评分器元数据"""
        return self._metadata.copy()

    def reset_metadata(self):
        """重置元数据统计"""
        self._metadata.update({
            "calculation_count": 0,
            "last_calculation_time": None,
            "performance_stats": {},
            "data_quality_warnings": []
        })
        logger.info("🔄 评分器元数据已重置")


# 全局评分器实例
stock_scorer = StockScorer()