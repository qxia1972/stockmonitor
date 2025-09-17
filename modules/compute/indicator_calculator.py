"""
技术指标计算器 (Technical Indicator Calculator)

职责：
- 纯技术指标算法实现
- 不涉及数据加载、验证或转换
- 提供高性能的指标计算函数
- 支持批量和单次计算
"""

import polars as pl
from typing import Dict, List, Optional, Union, Any
import numpy as np
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class IndicatorConfig:
    """指标计算配置"""
    period: int = 14
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    upper_band: float = 2.0
    lower_band: float = 2.0

class IndicatorCalculator:
    """
    纯技术指标计算器

    提供各种技术指标的计算算法，不涉及数据处理逻辑
    """

    def __init__(self, config: Optional[IndicatorConfig] = None):
        self.config = config or IndicatorConfig()

    def calculate_indicators(self, data: pl.DataFrame, indicators: List[str]) -> pl.DataFrame:
        """
        计算指定的技术指标

        Args:
            data: 包含价格数据的DataFrame
            indicators: 要计算的指标列表

        Returns:
            包含计算结果的DataFrame
        """
        result = data.clone()

        for indicator in indicators:
            try:
                if indicator == 'SMA':
                    result = self._calculate_sma(result)
                elif indicator == 'EMA':
                    result = self._calculate_ema(result)
                elif indicator == 'RSI':
                    result = self._calculate_rsi(result)
                elif indicator == 'MACD':
                    result = self._calculate_macd(result)
                elif indicator == 'Bollinger':
                    result = self._calculate_bollinger_bands(result)
                elif indicator == 'Volume_Ratio':
                    result = self._calculate_volume_ratio(result)
                elif indicator == 'Price_Angle':
                    result = self._calculate_price_angle(result)
                elif indicator == 'Score':
                    result = self._calculate_score(result)
                else:
                    logger.warning(f"Unknown indicator: {indicator}")
            except Exception as e:
                logger.error(f"Error calculating {indicator}: {e}")
                continue

        return result

    def _calculate_sma(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算简单移动平均线"""
        if 'close' not in data.columns:
            return data

        return data.with_columns([
            pl.col('close').rolling_mean(window_size=self.config.period).alias('SMA')
        ])

    def _calculate_ema(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算指数移动平均线"""
        if 'close' not in data.columns:
            return data

        return data.with_columns([
            pl.col('close').ewm_mean(alpha=2/(self.config.period+1)).alias('EMA')
        ])

    def _calculate_rsi(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算相对强弱指数"""
        if 'close' not in data.columns:
            return data

        # 计算价格变化
        data = data.with_columns([
            pl.col('close').diff().alias('price_change')
        ])

        # 计算上涨和下跌
        data = data.with_columns([
            pl.when(pl.col('price_change') > 0)
            .then(pl.col('price_change'))
            .otherwise(0)
            .alias('gain'),
            pl.when(pl.col('price_change') < 0)
            .then(-pl.col('price_change'))
            .otherwise(0)
            .alias('loss')
        ])

        # 计算平均涨幅和跌幅
        data = data.with_columns([
            pl.col('gain').rolling_mean(window_size=self.config.period).alias('avg_gain'),
            pl.col('loss').rolling_mean(window_size=self.config.period).alias('avg_loss')
        ])

        # 计算RS和RSI
        data = data.with_columns([
            (pl.col('avg_gain') / pl.col('avg_loss')).alias('rs'),
            (100 - (100 / (1 + (pl.col('avg_gain') / pl.col('avg_loss'))))).alias('RSI')
        ])

        return data

    def _calculate_macd(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算MACD指标"""
        if 'close' not in data.columns:
            return data

        # 计算快线和慢线
        fast_ema = data.select([
            pl.col('close').ewm_mean(alpha=2/(self.config.fast_period+1)).alias('fast_ema')
        ])

        slow_ema = data.select([
            pl.col('close').ewm_mean(alpha=2/(self.config.slow_period+1)).alias('slow_ema')
        ])

        # 计算DIF
        dif = fast_ema.select([
            (pl.col('fast_ema') - slow_ema['slow_ema']).alias('DIF')
        ])

        # 计算DEA
        dea = dif.select([
            pl.col('DIF').ewm_mean(alpha=2/(self.config.signal_period+1)).alias('DEA')
        ])

        # 计算MACD
        macd = dif.select([
            ((pl.col('DIF') - dea['DEA']) * 2).alias('MACD')
        ])

        return data.with_columns([
            dif['DIF'],
            dea['DEA'],
            macd['MACD']
        ])

    def _calculate_bollinger_bands(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算布林带"""
        if 'close' not in data.columns:
            return data

        # 计算中轨（SMA）
        sma = data.select([
            pl.col('close').rolling_mean(window_size=self.config.period).alias('BB_Middle')
        ])

        # 计算标准差
        std = data.select([
            pl.col('close').rolling_std(window_size=self.config.period).alias('std')
        ])

        # 计算上轨和下轨
        upper = sma.select([
            (pl.col('BB_Middle') + (std['std'] * self.config.upper_band)).alias('BB_Upper')
        ])

        lower = sma.select([
            (pl.col('BB_Middle') - (std['std'] * self.config.lower_band)).alias('BB_Lower')
        ])

        return data.with_columns([
            sma['BB_Middle'],
            upper['BB_Upper'],
            lower['BB_Lower']
        ])

    def _calculate_volume_ratio(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算量比"""
        if 'volume' not in data.columns:
            return data

        # 计算平均成交量
        avg_volume = data.select([
            pl.col('volume').rolling_mean(window_size=10).alias('avg_volume')
        ])

        # 计算量比
        volume_ratio = data.select([
            (pl.col('volume') / avg_volume['avg_volume']).alias('Volume_Ratio')
        ])

        return data.with_columns(volume_ratio['Volume_Ratio'])

    def _calculate_price_angle(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算价格角度"""
        if 'close' not in data.columns:
            return data

        # 计算价格变化率
        price_change_pct = data.select([
            pl.col('close').pct_change().alias('price_change_pct')
        ])

        # 计算角度（弧度转角度）
        angle = price_change_pct.select([
            (pl.col('price_change_pct').arctan() * (180 / np.pi)).alias('Price_Angle')
        ])

        return data.with_columns(angle['Price_Angle'])

    def _calculate_score(self, data: pl.DataFrame) -> pl.DataFrame:
        """计算综合得分"""
        score_components = []

        # RSI得分
        if 'RSI' in data.columns:
            rsi_score = data.select([
                pl.when(pl.col('RSI') > 70).then(1)
                .when(pl.col('RSI') < 30).then(-1)
                .otherwise(0)
                .alias('rsi_score')
            ])
            score_components.append(rsi_score['rsi_score'])

        # MACD得分
        if 'MACD' in data.columns:
            macd_score = data.select([
                pl.when(pl.col('MACD') > 0).then(1)
                .otherwise(-1)
                .alias('macd_score')
            ])
            score_components.append(macd_score['macd_score'])

        # 布林带得分
        if 'BB_Upper' in data.columns and 'BB_Lower' in data.columns and 'close' in data.columns:
            bb_score = data.select([
                pl.when(pl.col('close') > pl.col('BB_Upper')).then(1)
                .when(pl.col('close') < pl.col('BB_Lower')).then(-1)
                .otherwise(0)
                .alias('bb_score')
            ])
            score_components.append(bb_score['bb_score'])

        # 计算综合得分
        if score_components:
            total_score = score_components[0]
            for component in score_components[1:]:
                total_score = total_score + component

            # 归一化得分
            normalized_score = total_score.select([
                (pl.col(total_score.columns[0]) / len(score_components)).alias('Score')
            ])

            return data.with_columns(normalized_score['Score'])

        return data

    def _calculate_single_indicators(self, data: pl.DataFrame, indicator: str) -> pl.DataFrame:
        """
        计算单个指标（用于并行处理）

        Args:
            data: 输入数据
            indicator: 指标名称

        Returns:
            包含计算结果的DataFrame
        """
        try:
            if indicator == 'SMA':
                return self._calculate_sma(data)
            elif indicator == 'EMA':
                return self._calculate_ema(data)
            elif indicator == 'RSI':
                return self._calculate_rsi(data)
            elif indicator == 'MACD':
                return self._calculate_macd(data)
            elif indicator == 'Bollinger':
                return self._calculate_bollinger_bands(data)
            elif indicator == 'Volume_Ratio':
                return self._calculate_volume_ratio(data)
            elif indicator == 'Price_Angle':
                return self._calculate_price_angle(data)
            elif indicator == 'Score':
                return self._calculate_score(data)
            else:
                logger.warning(f"Unknown indicator: {indicator}")
                return data
        except Exception as e:
            logger.error(f"Error calculating {indicator}: {e}")
            return data
