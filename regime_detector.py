import pandas as pd
import numpy as np
from typing import Dict, List
from .logging import get_logger

logger = get_logger(__name__)

class MarketRegimeDetector:
    """Advanced market regime detection for adaptive trading"""
    
    def __init__(self):
        self.regime_history = []
    
    def detect_regime(self, d1: pd.DataFrame, h4: pd.DataFrame, h1: pd.DataFrame) -> Dict:
        """
        Detect current market regime with confidence scoring
        Returns regime info with adaptive parameters
        """
        # Calculate regime indicators
        volatility_regime = self._detect_volatility_regime(h1)
        trend_regime = self._detect_trend_regime(d1, h4)
        momentum_regime = self._detect_momentum_regime(h1)
        
        # Combine for final regime
        final_regime = self._combine_regimes(volatility_regime, trend_regime, momentum_regime)
        
        # Get adaptive parameters
        adaptive_params = self._get_adaptive_parameters(final_regime)
        
        logger.debug(
            f"Market regime detected: {final_regime['type']}",
            extra={
                "regime": final_regime['type'],
                "confidence": final_regime['confidence'],
                "volatility": volatility_regime['type'],
                "trend": trend_regime['type']
            }
        )
        
        return {
            **final_regime,
            'adaptive_params': adaptive_params,
            'volatility_subtype': volatility_regime,
            'trend_subtype': trend_regime,
            'momentum_subtype': momentum_regime
        }
    
    def _detect_volatility_regime(self, df: pd.DataFrame) -> Dict:
        """Detect volatility regime"""
        atr = self._calculate_atr(df, 14)
        current_atr = atr.iloc[-1]
        avg_atr = atr.tail(50).mean()
        
        volatility_ratio = current_atr / avg_atr if avg_atr > 0 else 1
        
        if volatility_ratio >= 1.8:
            return {"type": "HIGH_VOLATILITY", "ratio": volatility_ratio, "confidence": 0.9}
        elif volatility_ratio >= 1.3:
            return {"type": "ELEVATED_VOLATILITY", "ratio": volatility_ratio, "confidence": 0.7}
        elif volatility_ratio <= 0.7:
            return {"type": "LOW_VOLATILITY", "ratio": volatility_ratio, "confidence": 0.8}
        elif volatility_ratio <= 0.5:
            return {"type": "EXTREME_LOW_VOLATILITY", "ratio": volatility_ratio, "confidence": 0.9}
        else:
            return {"type": "NORMAL_VOLATILITY", "ratio": volatility_ratio, "confidence": 0.6}
    
    def _detect_trend_regime(self, d1: pd.DataFrame, h4: pd.DataFrame) -> Dict:
        """Detect trend regime using multiple timeframes"""
        # D1 trend analysis
        d1_trend_strength = self._calculate_trend_strength(d1)
        h4_trend_strength = self._calculate_trend_strength(h4)
        
        # Combined trend strength
        combined_strength = (d1_trend_strength * 0.6 + h4_trend_strength * 0.4)
        
        # Determine trend direction
        d1_ema21 = d1['c'].ewm(span=21).mean()
        d1_ema50 = d1['c'].ewm(span=50).mean()
        
        is_uptrend = d1_ema21.iloc[-1] > d1_ema50.iloc[-1]
        
        if combined_strength >= 0.7:
            trend_type = "STRONG_TREND"
            confidence = 0.8
        elif combined_strength >= 0.5:
            trend_type = "MODERATE_TREND" 
            confidence = 0.6
        elif combined_strength >= 0.3:
            trend_type = "WEAK_TREND"
            confidence = 0.4
        else:
            trend_type = "RANGING"
            confidence = 0.7
        
        return {
            "type": trend_type,
            "direction": "BULLISH" if is_uptrend else "BEARISH",
            "strength": combined_strength,
            "confidence": confidence
        }
    
    def _detect_momentum_regime(self, df: pd.DataFrame) -> Dict:
        """Detect momentum regime"""
        rsi = self._calculate_rsi(df, 14)
        current_rsi = rsi.iloc[-1]
        
        # RSI-based momentum
        if current_rsi >= 70:
            momentum_type = "OVERBOUGHT"
            confidence = 0.8
        elif current_rsi >= 60:
            momentum_type = "BULLISH_MOMENTUM"
            confidence = 0.6
        elif current_rsi <= 30:
            momentum_type = "OVERSOLD"
            confidence = 0.8
        elif current_rsi <= 40:
            momentum_type = "BEARISH_MOMENTUM"
            confidence = 0.6
        else:
            momentum_type = "NEUTRAL_MOMENTUM"
            confidence = 0.5
        
        return {
            "type": momentum_type,
            "rsi": current_rsi,
            "confidence": confidence
        }
    
    def _combine_regimes(self, volatility: Dict, trend: Dict, momentum: Dict) -> Dict:
        """Combine all regime analyses"""
        # Weighted confidence calculation
        total_confidence = (
            volatility['confidence'] * 0.4 +
            trend['confidence'] * 0.4 + 
            momentum['confidence'] * 0.2
        )
        
        # Determine primary regime
        if volatility['type'] in ["HIGH_VOLATILITY", "EXTREME_LOW_VOLATILITY"]:
            primary_regime = volatility['type']
        elif trend['type'] == "STRONG_TREND":
            primary_regime = f"TRENDING_{trend['direction']}"
        elif trend['type'] == "RANGING":
            primary_regime = "RANGING_MARKET"
        else:
            primary_regime = "MIXED_REGIME"
        
        return {
            "type": primary_regime,
            "confidence": total_confidence,
            "description": self._get_regime_description(primary_regime)
        }
    
    def _get_adaptive_parameters(self, regime: Dict) -> Dict:
        """Get adaptive trading parameters for each regime"""
        base_params = {
            "score_threshold": 6,
            "risk_multiplier": 1.0,
            "position_size_multiplier": 1.0,
            "max_trades_per_day": 3
        }
        
        regime_type = regime['type']
        
        if regime_type == "HIGH_VOLATILITY":
            return {
                **base_params,
                "score_threshold": 8,
                "risk_multiplier": 0.5,
                "position_size_multiplier": 0.7,
                "max_trades_per_day": 2
            }
        elif regime_type == "EXTREME_LOW_VOLATILITY":
            return {
                **base_params,
                "score_threshold": 7,
                "risk_multiplier": 0.8,
                "position_size_multiplier": 1.2,
                "max_trades_per_day": 4
            }
        elif "TRENDING_BULLISH" in regime_type:
            return {
                **base_params,
                "score_threshold": 6,
                "risk_multiplier": 1.2,
                "position_size_multiplier": 1.1,
                "max_trades_per_day": 5
            }
        elif "TRENDING_BEARISH" in regime_type:
            return {
                **base_params, 
                "score_threshold": 6,
                "risk_multiplier": 1.1,
                "position_size_multiplier": 1.1,
                "max_trades_per_day": 5
            }
        elif regime_type == "RANGING_MARKET":
            return {
                **base_params,
                "score_threshold": 7,
                "risk_multiplier": 0.9,
                "position_size_multiplier": 0.9,
                "max_trades_per_day": 3
            }
        else:
            return base_params
    
    def _calculate_trend_strength(self, df: pd.DataFrame) -> float:
        """Calculate trend strength 0-1"""
        if len(df) < 50:
            return 0.5
            
        # EMA slope calculation
        ema20 = df['c'].ewm(span=20).mean()
        ema50 = df['c'].ewm(span=50).mean()
        
        # Slope of EMAs
        ema20_slope = (ema20.iloc[-1] - ema20.iloc[-5]) / 5
        ema50_slope = (ema50.iloc[-1] - ema50.iloc[-10]) / 10
        
        # Price making higher highs/lows or lower highs/lows
        recent_highs = df['h'].tail(20)
        recent_lows = df['l'].tail(20)
        
        higher_highs = recent_highs.iloc[-1] > recent_highs.max()
        higher_lows = recent_lows.iloc[-1] > recent_lows.max()
        lower_highs = recent_highs.iloc[-1] < recent_highs.min()
        lower_lows = recent_lows.iloc[-1] < recent_lows.min()
        
        trend_strength = 0.0
        
        if (higher_highs and higher_lows) or (lower_highs and lower_lows):
            trend_strength += 0.4
        
        # EMA alignment strength
        if (ema20_slope > 0 and ema50_slope > 0) or (ema20_slope < 0 and ema50_slope < 0):
            trend_strength += 0.3
        
        # Momentum confirmation
        price_change = abs(df['c'].iloc[-1] - df['c'].iloc[-10]) / df['c'].iloc[-10]
        trend_strength += min(0.3, price_change * 10)
        
        return min(1.0, trend_strength)
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high_low = df['h'] - df['l']
        high_close = (df['h'] - df['c'].shift()).abs()
        low_close = (df['l'] - df['c'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate RSI"""
        delta = df['c'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _get_regime_description(self, regime_type: str) -> str:
        """Get human-readable regime description"""
        descriptions = {
            "HIGH_VOLATILITY": "High volatility - Reduce position sizes, increase score threshold",
            "ELEVATED_VOLATILITY": "Elevated volatility - Trade with caution",
            "LOW_VOLATILITY": "Low volatility - Normal trading conditions",
            "EXTREME_LOW_VOLATILITY": "Extremely low volatility - Good for range trading",
            "TRENDING_BULLISH": "Strong bullish trend - Favor long setups",
            "TRENDING_BEARISH": "Strong bearish trend - Favor short setups", 
            "RANGING_MARKET": "Ranging market - Trade reversals at edges",
            "MIXED_REGIME": "Mixed signals - Wait for clearer conditions"
        }
        return descriptions.get(regime_type, "Unknown regime")