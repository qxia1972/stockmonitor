"""
事件管理器模块 (Event Manager)
负责技术分析事件的检测和管理

职责：
- 价格事件检测（突破、反转等）
- 成交量事件检测（放量、缩量等）
- 波动性事件检测（异常波动等）
- 事件过滤和优化
- 事件历史记录和管理
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Union, Tuple
from datetime import datetime, timedelta
import sys
import os

# 导入指标管理器
from .indicator_manager import IndicatorManager, create_indicator_manager

# 导入配置
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.unified_config import *
except ImportError:
    # 提供默认配置
    print("⚠️ 配置文件导入失败，使用默认配置")
    VOLATILITY_MODE = "tonghuashun"
    TONGHUASHUN_CONFIG = {
        'enable_volume_enhancement': True,
        'volume_weight_limit': [0.1, 5.0],
        'N': 10,
    }
    ADVANCED_CONFIG = {
        "N": 10,
        "M": 5,
        "Extra": 20,
        "trend_type": 1,
    }

# 事件类型定义
EVENT_TYPES = {
    'PRICE_EVENTS': {
        'BREAKOUT_UP': '向上突破',
        'BREAKOUT_DOWN': '向下突破',
        'SUPPORT_TEST': '支撑测试',
        'RESISTANCE_TEST': '阻力测试',
        'REVERSAL_UP': '向上反转',
        'REVERSAL_DOWN': '向下反转',
        'GAP_UP': '向上跳空',
        'GAP_DOWN': '向下跳空'
    },
    'VOLUME_EVENTS': {
        'VOLUME_SPIKE': '成交量激增',
        'VOLUME_DRY': '成交量萎缩',
        'VOLUME_BREAKOUT': '放量突破',
        'VOLUME_DIVERGENCE': '量价背离'
    },
    'VOLATILITY_EVENTS': {
        'HIGH_VOLATILITY': '高波动',
        'LOW_VOLATILITY': '低波动',
        'VOLATILITY_EXPANSION': '波动性扩张',
        'VOLATILITY_CONTRACTION': '波动性收缩',
        'ABNORMAL_MOVEMENT': '异常波动'
    },
    'TECHNICAL_EVENTS': {
        'OVERSOLD': '超卖',
        'OVERBOUGHT': '超买',
        'BULLISH_CROSSOVER': '多头交叉',
        'BEARISH_CROSSOVER': '空头交叉',
        'DIVERGENCE': '背离信号'
    }
}


class EventDetector:
    """
    事件检测器
    负责各种技术分析事件的检测
    """
    
    def __init__(self, data_processor, indicator_manager: IndicatorManager):
        """
        初始化事件检测器
        
        Args:
            data_processor: 数据处理器 (现在是 DataStore)
            indicator_manager: 指标管理器
        """
        self.data_processor = data_processor
        self.indicator_manager = indicator_manager
        
        # 检测参数
        self.detection_params = {
            'breakout_threshold': 0.02,  # 突破阈值 2%
            'volume_spike_threshold': 2.0,  # 成交量激增阈值
            'volatility_threshold': 0.03,  # 波动性阈值 3%
            'rsi_oversold': 30,  # RSI超卖
            'rsi_overbought': 70,  # RSI超买
            'gap_threshold': 0.01,  # 跳空阈值 1%
            'lookback_period': 20  # 回看周期
        }
        
        print("🔍 事件检测器初始化完成")
    
    def detect_price_events(self, key: str, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        检测价格事件
        
        Args:
            key: 数据键值
            count: 检测的数据点数量
            
        Returns:
            价格事件列表
        """
        events = []
        
        try:
            # 获取OHLCV数据
            data = self.data_processor.get_ohlcv_data(key, count=count)
            if data is None or len(data) < self.detection_params['lookback_period']:
                return events
            
            # 计算所需指标
            sma_20 = self.indicator_manager.calculate_indicator(key, 'SMA', period=20, count=count)
            
            if sma_20 is None:
                return events
            
            # 确保SMA返回的是Series类型
            if isinstance(sma_20, dict):
                sma_20 = sma_20.get('sma', None)
                if sma_20 is None:
                    return events
            
            # 检测突破事件
            events.extend(self._detect_breakout_events(data, sma_20))
            
            # 检测跳空事件
            events.extend(self._detect_gap_events(data))
            
            # 检测支撑阻力测试
            events.extend(self._detect_support_resistance_events(data, sma_20))
            
        except Exception as e:
            print(f"❌ 价格事件检测失败: {e}")
        
        return events
    
    def detect_volume_events(self, key: str, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        检测成交量事件
        
        Args:
            key: 数据键值
            count: 检测的数据点数量
            
        Returns:
            成交量事件列表
        """
        events = []
        
        try:
            # 获取OHLCV数据
            data = self.data_processor.get_ohlcv_data(key, count=count)
            if data is None or 'volume' not in data.columns or len(data) < self.detection_params['lookback_period']:
                return events
            
            # 计算成交量移动平均
            volume_sma = data['volume'].rolling(window=20).mean()
            
            # 检测成交量激增
            events.extend(self._detect_volume_spike_events(data, volume_sma))
            
            # 检测量价背离
            events.extend(self._detect_volume_price_divergence(data))
            
        except Exception as e:
            print(f"❌ 成交量事件检测失败: {e}")
        
        return events
    
    def detect_volatility_events(self, key: str, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        检测波动性事件
        
        Args:
            key: 数据键值
            count: 检测的数据点数量
            
        Returns:
            波动性事件列表
        """
        events = []
        
        try:
            # 获取数据
            data = self.data_processor.get_ohlcv_data(key, count=count)
            if data is None or len(data) < self.detection_params['lookback_period']:
                return events
            
            # 计算ATR指标
            atr = self.indicator_manager.calculate_indicator(key, 'ATR', period=14, count=count)
            
            if atr is None:
                return events
            
            # 确保ATR返回的是Series类型
            if isinstance(atr, dict):
                atr = atr.get('atr', None)
                if atr is None:
                    return events
            
            # 检测高低波动事件
            events.extend(self._detect_volatility_level_events(data, atr))
            
            # 检测波动性扩张收缩
            events.extend(self._detect_volatility_expansion_events(atr))
            
            # 检测异常波动
            events.extend(self._detect_abnormal_movement_events(data))
            
        except Exception as e:
            print(f"❌ 波动性事件检测失败: {e}")
        
        return events
    
    def detect_technical_events(self, key: str, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        检测技术指标事件
        
        Args:
            key: 数据键值
            count: 检测的数据点数量
            
        Returns:
            技术指标事件列表
        """
        events = []
        
        try:
            # 计算技术指标
            rsi = self.indicator_manager.calculate_indicator(key, 'RSI', period=14, count=count)
            macd = self.indicator_manager.calculate_indicator(key, 'MACD', count=count)
            
            # 检测RSI超买超卖
            if rsi is not None:
                # 确保RSI返回的是Series类型
                if isinstance(rsi, dict):
                    rsi = rsi.get('rsi', None)
                    if rsi is None:
                        return events
                events.extend(self._detect_rsi_events(rsi))
            
            # 检测MACD交叉
            if macd is not None and isinstance(macd, dict):
                events.extend(self._detect_macd_crossover_events(macd))
            
        except Exception as e:
            print(f"❌ 技术指标事件检测失败: {e}")
        
        return events
    
    def _detect_breakout_events(self, data: pd.DataFrame, sma: pd.Series) -> List[Dict[str, Any]]:
        """检测突破事件"""
        events = []
        
        if len(data) < 2 or len(sma) < 2:
            return events
        
        # 检测向上突破
        current_close = data['close'].iloc[-1]
        prev_close = data['close'].iloc[-2]
        current_sma = sma.iloc[-1]
        
        if prev_close <= current_sma and current_close > current_sma * (1 + self.detection_params['breakout_threshold']):
            events.append({
                'type': 'BREAKOUT_UP',
                'timestamp': data.index[-1],
                'price': current_close,
                'description': f"向上突破SMA20，突破幅度: {((current_close/current_sma - 1) * 100):.2f}%",
                'strength': min(abs(current_close/current_sma - 1) * 10, 1.0)
            })
        
        # 检测向下突破
        elif prev_close >= current_sma and current_close < current_sma * (1 - self.detection_params['breakout_threshold']):
            events.append({
                'type': 'BREAKOUT_DOWN',
                'timestamp': data.index[-1],
                'price': current_close,
                'description': f"向下突破SMA20，突破幅度: {((1 - current_close/current_sma) * 100):.2f}%",
                'strength': min(abs(1 - current_close/current_sma) * 10, 1.0)
            })
        
        return events
    
    def _detect_gap_events(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """检测跳空事件"""
        events = []
        
        if len(data) < 2:
            return events
        
        current_open = data['open'].iloc[-1]
        prev_close = data['close'].iloc[-2]
        
        gap_ratio = (current_open - prev_close) / prev_close
        
        if gap_ratio > self.detection_params['gap_threshold']:
            events.append({
                'type': 'GAP_UP',
                'timestamp': data.index[-1],
                'price': current_open,
                'description': f"向上跳空 {(gap_ratio * 100):.2f}%",
                'strength': min(abs(gap_ratio) * 20, 1.0)
            })
        elif gap_ratio < -self.detection_params['gap_threshold']:
            events.append({
                'type': 'GAP_DOWN',
                'timestamp': data.index[-1],
                'price': current_open,
                'description': f"向下跳空 {(abs(gap_ratio) * 100):.2f}%",
                'strength': min(abs(gap_ratio) * 20, 1.0)
            })
        
        return events
    
    def _detect_support_resistance_events(self, data: pd.DataFrame, sma: pd.Series) -> List[Dict[str, Any]]:
        """检测支撑阻力测试事件"""
        events = []
        
        if len(data) < 3 or len(sma) < 3:
            return events
        
        current_low = data['low'].iloc[-1]
        current_high = data['high'].iloc[-1]
        current_sma = sma.iloc[-1]
        
        # 支撑测试：价格接近但未突破支撑位
        if abs(current_low - current_sma) / current_sma < 0.01 and current_low > current_sma:
            events.append({
                'type': 'SUPPORT_TEST',
                'timestamp': data.index[-1],
                'price': current_low,
                'description': f"测试SMA20支撑位",
                'strength': 0.5
            })
        
        # 阻力测试：价格接近但未突破阻力位
        elif abs(current_high - current_sma) / current_sma < 0.01 and current_high < current_sma:
            events.append({
                'type': 'RESISTANCE_TEST',
                'timestamp': data.index[-1],
                'price': current_high,
                'description': f"测试SMA20阻力位",
                'strength': 0.5
            })
        
        return events
    
    def _detect_volume_spike_events(self, data: pd.DataFrame, volume_sma: pd.Series) -> List[Dict[str, Any]]:
        """检测成交量激增事件"""
        events = []
        
        if len(data) < 1 or len(volume_sma) < 1:
            return events
        
        current_volume = data['volume'].iloc[-1]
        avg_volume = volume_sma.iloc[-1]
        
        if current_volume > avg_volume * self.detection_params['volume_spike_threshold']:
            spike_ratio = current_volume / avg_volume
            events.append({
                'type': 'VOLUME_SPIKE',
                'timestamp': data.index[-1],
                'volume': current_volume,
                'description': f"成交量激增 {spike_ratio:.1f} 倍",
                'strength': min((spike_ratio - 1) / 3, 1.0)
            })
        
        return events
    
    def _detect_volume_price_divergence(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """检测量价背离事件"""
        events = []
        
        if len(data) < 5:
            return events
        
        # 计算价格和成交量的趋势
        price_trend = data['close'].iloc[-5:].pct_change().sum()
        volume_trend = data['volume'].iloc[-5:].pct_change().sum()
        
        # 量价背离：价格上涨但成交量下降，或价格下跌但成交量增加
        if price_trend > 0.02 and volume_trend < -0.1:
            events.append({
                'type': 'VOLUME_DIVERGENCE',
                'timestamp': data.index[-1],
                'description': "价格上涨但成交量下降，可能存在量价背离",
                'strength': min(abs(price_trend) + abs(volume_trend), 1.0)
            })
        elif price_trend < -0.02 and volume_trend > 0.1:
            events.append({
                'type': 'VOLUME_DIVERGENCE',
                'timestamp': data.index[-1],
                'description': "价格下跌但成交量增加，可能存在量价背离",
                'strength': min(abs(price_trend) + abs(volume_trend), 1.0)
            })
        
        return events
    
    def _detect_volatility_level_events(self, data: pd.DataFrame, atr: pd.Series) -> List[Dict[str, Any]]:
        """检测波动性水平事件"""
        events = []
        
        if len(atr) < 20:
            return events
        
        current_atr = atr.iloc[-1]
        avg_atr = atr.iloc[-20:].mean()
        current_price = data['close'].iloc[-1]
        
        atr_ratio = current_atr / current_price
        
        if atr_ratio > self.detection_params['volatility_threshold']:
            events.append({
                'type': 'HIGH_VOLATILITY',
                'timestamp': data.index[-1],
                'atr': current_atr,
                'description': f"高波动状态，ATR比例: {(atr_ratio * 100):.2f}%",
                'strength': min(atr_ratio / 0.05, 1.0)
            })
        elif atr_ratio < self.detection_params['volatility_threshold'] / 3:
            events.append({
                'type': 'LOW_VOLATILITY',
                'timestamp': data.index[-1],
                'atr': current_atr,
                'description': f"低波动状态，ATR比例: {(atr_ratio * 100):.2f}%",
                'strength': min((self.detection_params['volatility_threshold'] / 3 - atr_ratio) / 0.01, 1.0)
            })
        
        return events
    
    def _detect_volatility_expansion_events(self, atr: pd.Series) -> List[Dict[str, Any]]:
        """检测波动性扩张收缩事件"""
        events = []
        
        if len(atr) < 5:
            return events
        
        # 计算ATR变化趋势
        atr_change = (atr.iloc[-1] - atr.iloc[-5]) / atr.iloc[-5]
        
        if atr_change > 0.3:
            events.append({
                'type': 'VOLATILITY_EXPANSION',
                'timestamp': atr.index[-1],
                'description': f"波动性扩张 {(atr_change * 100):.1f}%",
                'strength': min(atr_change / 0.5, 1.0)
            })
        elif atr_change < -0.3:
            events.append({
                'type': 'VOLATILITY_CONTRACTION',
                'timestamp': atr.index[-1],
                'description': f"波动性收缩 {(abs(atr_change) * 100):.1f}%",
                'strength': min(abs(atr_change) / 0.5, 1.0)
            })
        
        return events
    
    def _detect_abnormal_movement_events(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """检测异常波动事件"""
        events = []
        
        if len(data) < 2:
            return events
        
        # 计算当日振幅
        current_range = (data['high'].iloc[-1] - data['low'].iloc[-1]) / data['open'].iloc[-1]
        
        # 计算历史平均振幅
        if len(data) >= 20:
            historical_ranges = []
            for i in range(max(1, len(data) - 20), len(data) - 1):
                range_val = (data['high'].iloc[i] - data['low'].iloc[i]) / data['open'].iloc[i]
                historical_ranges.append(range_val)
            
            if historical_ranges:
                avg_range = np.mean(historical_ranges)
                
                if current_range > avg_range * 2:
                    events.append({
                        'type': 'ABNORMAL_MOVEMENT',
                        'timestamp': data.index[-1],
                        'range': current_range,
                        'description': f"异常大幅波动，振幅: {(current_range * 100):.2f}%",
                        'strength': min(current_range / (avg_range * 3), 1.0)
                    })
        
        return events
    
    def _detect_rsi_events(self, rsi: pd.Series) -> List[Dict[str, Any]]:
        """检测RSI事件"""
        events = []
        
        if len(rsi) < 1:
            return events
        
        current_rsi = rsi.iloc[-1]
        
        if current_rsi < self.detection_params['rsi_oversold']:
            events.append({
                'type': 'OVERSOLD',
                'timestamp': rsi.index[-1],
                'rsi': current_rsi,
                'description': f"RSI超卖信号，RSI: {current_rsi:.1f}",
                'strength': (self.detection_params['rsi_oversold'] - current_rsi) / 20
            })
        elif current_rsi > self.detection_params['rsi_overbought']:
            events.append({
                'type': 'OVERBOUGHT',
                'timestamp': rsi.index[-1],
                'rsi': current_rsi,
                'description': f"RSI超买信号，RSI: {current_rsi:.1f}",
                'strength': (current_rsi - self.detection_params['rsi_overbought']) / 20
            })
        
        return events
    
    def _detect_macd_crossover_events(self, macd: Dict[str, pd.Series]) -> List[Dict[str, Any]]:
        """检测MACD交叉事件"""
        events = []
        
        if 'macd' not in macd or 'signal' not in macd or len(macd['macd']) < 2:
            return events
        
        macd_line = macd['macd']
        signal_line = macd['signal']
        
        # 检测金叉和死叉
        if len(macd_line) >= 2 and len(signal_line) >= 2:
            prev_diff = macd_line.iloc[-2] - signal_line.iloc[-2]
            curr_diff = macd_line.iloc[-1] - signal_line.iloc[-1]
            
            if prev_diff <= 0 and curr_diff > 0:
                events.append({
                    'type': 'BULLISH_CROSSOVER',
                    'timestamp': macd_line.index[-1],
                    'description': "MACD金叉信号",
                    'strength': min(abs(curr_diff) * 10, 1.0)
                })
            elif prev_diff >= 0 and curr_diff < 0:
                events.append({
                    'type': 'BEARISH_CROSSOVER',
                    'timestamp': macd_line.index[-1],
                    'description': "MACD死叉信号",
                    'strength': min(abs(curr_diff) * 10, 1.0)
                })
        
        return events


class EventFilter:
    """
    事件过滤器
    负责事件的去重、过滤和优化
    """
    
    def __init__(self):
        """初始化事件过滤器"""
        self.filter_params = {
            'min_strength': 0.1,  # 最小强度阈值
            'time_window': 300,   # 时间窗口（秒）
            'max_events_per_type': 5  # 每种类型最大事件数
        }
        
        print("🔧 事件过滤器初始化完成")
    
    def filter_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        过滤事件
        
        Args:
            events: 原始事件列表
            
        Returns:
            过滤后的事件列表
        """
        if not events:
            return events
        
        # 按强度过滤
        filtered_events = [e for e in events if e.get('strength', 0) >= self.filter_params['min_strength']]
        
        # 去重（同类型、短时间内的事件）
        filtered_events = self._remove_duplicates(filtered_events)
        
        # 限制每种类型的事件数量
        filtered_events = self._limit_events_per_type(filtered_events)
        
        # 按强度和时间排序
        filtered_events.sort(key=lambda x: (x.get('strength', 0), x.get('timestamp', datetime.now())), reverse=True)
        
        return filtered_events
    
    def _remove_duplicates(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """去除重复事件"""
        unique_events = []
        
        for event in events:
            is_duplicate = False
            event_timestamp = event.get('timestamp', datetime.now())
            
            for existing_event in unique_events:
                existing_timestamp = existing_event.get('timestamp', datetime.now())
                
                # 检查是否为相同类型且时间接近的事件
                if (event.get('type') == existing_event.get('type') and
                    abs((event_timestamp - existing_timestamp).total_seconds()) < self.filter_params['time_window']):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_events.append(event)
        
        return unique_events
    
    def _limit_events_per_type(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """限制每种类型的事件数量"""
        type_counts = {}
        limited_events = []
        
        for event in events:
            event_type = event.get('type', 'UNKNOWN')
            current_count = type_counts.get(event_type, 0)
            
            if current_count < self.filter_params['max_events_per_type']:
                limited_events.append(event)
                type_counts[event_type] = current_count + 1
        
        return limited_events


class EventManager:
    """
    事件管理器
    负责技术分析事件的检测、过滤和管理
    """
    
    def __init__(self, datastore):
        """
        初始化事件管理器
        
        Args:
            datastore: 数据存储实例
        """
        self.ds = datastore
        self.data_processor = datastore  # 直接使用 datastore，它现在包含了 processor 功能
        self.indicator_manager = create_indicator_manager(datastore)
        self.detector = EventDetector(self.data_processor, self.indicator_manager)
        self.filter = EventFilter()
        
        # 事件历史记录
        self._event_history = []
        self._detection_count = 0
        
        print("🎯 事件管理器初始化完成")
        print(f"   数据处理器: ✅")
        print(f"   指标管理器: ✅")
        print(f"   事件检测器: ✅")
        print(f"   事件过滤器: ✅")
    
    def detect_all_events(self, key: str, count: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        检测所有类型的事件
        
        Args:
            key: 数据键值
            count: 检测的数据点数量
            
        Returns:
            按类型分组的事件字典
        """
        all_events = {
            'price_events': [],
            'volume_events': [],
            'volatility_events': [],
            'technical_events': []
        }
        
        try:
            print(f"🔍 开始检测事件: {key}")
            
            # 检测各类事件
            all_events['price_events'] = self.detector.detect_price_events(key, count)
            all_events['volume_events'] = self.detector.detect_volume_events(key, count)
            all_events['volatility_events'] = self.detector.detect_volatility_events(key, count)
            all_events['technical_events'] = self.detector.detect_technical_events(key, count)
            
            # 统计检测结果
            total_events = sum(len(events) for events in all_events.values())
            self._detection_count += 1
            
            print(f"📊 事件检测完成: 共检测到 {total_events} 个事件")
            
            # 过滤事件
            for event_type in all_events:
                all_events[event_type] = self.filter.filter_events(all_events[event_type])
            
            # 记录到历史
            self._add_to_history(key, all_events)
            
        except Exception as e:
            print(f"❌ 事件检测失败: {e}")
        
        return all_events
    
    def detect_events_by_type(self, key: str, event_type: str, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        按类型检测事件
        
        Args:
            key: 数据键值
            event_type: 事件类型 ('price', 'volume', 'volatility', 'technical')
            count: 检测的数据点数量
            
        Returns:
            事件列表
        """
        events = []
        
        try:
            if event_type.lower() == 'price':
                events = self.detector.detect_price_events(key, count)
            elif event_type.lower() == 'volume':
                events = self.detector.detect_volume_events(key, count)
            elif event_type.lower() == 'volatility':
                events = self.detector.detect_volatility_events(key, count)
            elif event_type.lower() == 'technical':
                events = self.detector.detect_technical_events(key, count)
            else:
                print(f"❌ 不支持的事件类型: {event_type}")
                return events
            
            # 过滤事件
            events = self.filter.filter_events(events)
            
            print(f"📊 {event_type}事件检测完成: {len(events)} 个事件")
            
        except Exception as e:
            print(f"❌ {event_type}事件检测失败: {e}")
        
        return events
    
    def get_recent_events(self, key: Optional[str] = None, hours: int = 24) -> List[Dict[str, Any]]:
        """
        获取近期事件
        
        Args:
            key: 数据键值（可选，获取所有股票）
            hours: 时间范围（小时）
            
        Returns:
            近期事件列表
        """
        recent_events = []
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        for record in self._event_history:
            if key is None or record['key'] == key:
                timestamp = record.get('timestamp', datetime.now())
                if timestamp >= cutoff_time:
                    # 收集所有类型的事件
                    for event_type, events in record['events'].items():
                        for event in events:
                            event_copy = event.copy()
                            event_copy['stock_key'] = record['key']
                            event_copy['category'] = event_type
                            recent_events.append(event_copy)
        
        # 按时间排序
        recent_events.sort(key=lambda x: x.get('timestamp', datetime.now()), reverse=True)
        
        return recent_events
    
    def get_event_statistics(self) -> Dict[str, Any]:
        """获取事件统计信息"""
        stats = {
            'total_detections': self._detection_count,
            'total_events': len(self._event_history),
            'event_types': {},
            'recent_activity': {}
        }
        
        # 统计事件类型
        for record in self._event_history:
            for event_type, events in record['events'].items():
                if event_type not in stats['event_types']:
                    stats['event_types'][event_type] = 0
                stats['event_types'][event_type] += len(events)
        
        # 统计近期活动
        recent_events = self.get_recent_events(hours=24)
        for event in recent_events:
            category = event.get('category', 'unknown')
            if category not in stats['recent_activity']:
                stats['recent_activity'][category] = 0
            stats['recent_activity'][category] += 1
        
        return stats
    
    def _add_to_history(self, key: str, events: Dict[str, List[Dict[str, Any]]]) -> None:
        """添加到事件历史"""
        record = {
            'key': key,
            'timestamp': datetime.now(),
            'events': events
        }
        
        self._event_history.append(record)
        
        # 限制历史记录数量
        if len(self._event_history) > 1000:
            self._event_history = self._event_history[-500:]
    
    def clear_history(self) -> None:
        """清空事件历史"""
        self._event_history.clear()
        print("🧹 事件历史已清空")
    
    def export_events(self, filename: Optional[str] = None) -> str:
        """
        导出事件到文件
        
        Args:
            filename: 文件名（可选）
            
        Returns:
            导出的文件路径
        """
        if filename is None:
            filename = f"events_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        
        try:
            # 准备导出数据
            export_data = {
                'export_time': datetime.now().isoformat(),
                'total_records': len(self._event_history),
                'statistics': self.get_event_statistics(),
                'events': []
            }
            
            # 转换事件数据
            for record in self._event_history:
                record_copy = record.copy()
                record_copy['timestamp'] = record_copy['timestamp'].isoformat()
                
                # 转换事件时间戳
                for event_type, events in record_copy['events'].items():
                    for event in events:
                        if 'timestamp' in event and hasattr(event['timestamp'], 'isoformat'):
                            event['timestamp'] = event['timestamp'].isoformat()
                
                export_data['events'].append(record_copy)
            
            # 写入文件
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            print(f"📁 事件已导出到: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ 导出事件失败: {e}")
            return ""


# 便捷函数
def create_event_manager(datastore) -> EventManager:
    """创建事件管理器实例"""
    return EventManager(datastore)


if __name__ == "__main__":
    # 测试代码
    class MockDataStore:
        def __init__(self):
            self.data = {}
        
        def get_data(self, key):
            return self.data.get(key)
        
        def update_data(self, key, data):
            self.data[key] = data
    
    # 创建测试数据
    np.random.seed(42)
    test_data = pd.DataFrame({
        'open': np.random.randn(100) + 100,
        'high': np.random.randn(100) + 102,
        'low': np.random.randn(100) + 98,
        'close': np.random.randn(100) + 100,
        'volume': np.random.randint(1000000, 5000000, 100)
    })
    
    # 测试事件管理器
    mock_ds = MockDataStore()
    mock_ds.update_data('test_stock', test_data)
    
    manager = create_event_manager(mock_ds)
    
    # 测试事件检测
    all_events = manager.detect_all_events('test_stock')
    
    total_events = sum(len(events) for events in all_events.values())
    print(f"📊 检测测试: 共 {total_events} 个事件")
    
    # 测试统计信息
    stats = manager.get_event_statistics()
    print(f"📋 统计信息: {stats['total_detections']} 次检测")
    
    print("\n✅ 事件管理器测试完成")
