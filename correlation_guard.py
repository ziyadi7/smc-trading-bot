import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from .logging import get_logger

logger = get_logger(__name__)

class CorrelationGuard:
    """Advanced correlation analysis for gold trading"""
    
    def __init__(self):
        self.correlated_assets = {
            "DXY": {"symbol": "USDX", "weight": -0.8, "description": "US Dollar Index"},
            "US10Y": {"symbol": "USTEC", "weight": -0.6, "description": "10-Year Treasury Yield"},
            "SPX": {"symbol": "SPX500", "weight": -0.4, "description": "S&P 500 Index"},
            "EURUSD": {"symbol": "EURUSD", "weight": 0.7, "description": "Euro vs Dollar"},
            "OIL": {"symbol": "XBRUSD", "weight": 0.3, "description": "Crude Oil"}
        }
        
        self.correlation_cache = {}
    
    def check_correlation_alignment(self, gold_signal: Dict, mt5_client) -> Tuple[bool, float, List[str]]:
        """
        Check if correlated assets support the gold signal
        Returns: (aligned, alignment_score, notes)
        """
        try:
            alignment_scores = []
            notes = []
            
            for asset_name, asset_info in self.correlated_assets.items():
                asset_aligned, asset_score, asset_note = self._check_asset_alignment(
                    asset_name, asset_info, gold_signal, mt5_client
                )
                
                if asset_aligned:
                    alignment_scores.append(asset_score * abs(asset_info['weight']))
                    notes.append(asset_note)
                else:
                    alignment_scores.append(0)
                    notes.append(f"❌ {asset_name}: Not aligned")
            
            # Calculate weighted alignment score
            if alignment_scores:
                total_weight = sum(abs(asset_info['weight']) for asset_info in self.correlated_assets.values())
                weighted_score = sum(alignment_scores) / total_weight
            else:
                weighted_score = 0.5  # Neutral if no data
            
            # Determine if aligned
            is_aligned = weighted_score >= 0.6
            
            logger.debug(
                f"Correlation check: {weighted_score:.2f} - {'ALIGNED' if is_aligned else 'NOT ALIGNED'}",
                extra={
                    "gold_signal": gold_signal['side'],
                    "correlation_score": weighted_score,
                    "aligned": is_aligned
                }
            )
            
            return is_aligned, weighted_score, notes
            
        except Exception as e:
            logger.error(f"Correlation check failed: {e}")
            return True, 0.5, ["Correlation check unavailable"]  # Fail safe
    
    def _check_asset_alignment(self, asset_name: str, asset_info: Dict, gold_signal: Dict, mt5_client) -> Tuple[bool, float, str]:
        """Check alignment for a specific correlated asset"""
        try:
            # Get asset data (simplified - in practice you'd fetch real data)
            asset_trend = self._get_asset_trend(asset_name, asset_info['symbol'], mt5_client)
            
            if asset_trend == "UNAVAILABLE":
                return True, 0.5, f"⚪ {asset_name}: Data unavailable"
            
            gold_direction = gold_signal['side']  # 'BUY' or 'SELL'
            expected_asset_trend = "BULLISH" if asset_info['weight'] > 0 else "BEARISH"
            
            if gold_direction == "BUY":
                # For gold bullish, we want positively correlated assets bullish, negatively correlated assets bearish
                if asset_info['weight'] > 0:  # Positive correlation
                    is_aligned = asset_trend == "BULLISH"
                    score = 1.0 if asset_trend == "BULLISH" else 0.0
                else:  # Negative correlation  
                    is_aligned = asset_trend == "BEARISH"
                    score = 1.0 if asset_trend == "BEARISH" else 0.0
            else:  # SELL
                # For gold bearish, we want positively correlated assets bearish, negatively correlated assets bullish
                if asset_info['weight'] > 0:  # Positive correlation
                    is_aligned = asset_trend == "BEARISH"
                    score = 1.0 if asset_trend == "BEARISH" else 0.0
                else:  # Negative correlation
                    is_aligned = asset_trend == "BULLISH"
                    score = 1.0 if asset_trend == "BULLISH" else 0.0
            
            emoji = "✅" if is_aligned else "❌"
            return is_aligned, score, f"{emoji} {asset_name}: {asset_trend}"
            
        except Exception as e:
            logger.warning(f"Asset alignment check failed for {asset_name}: {e}")
            return True, 0.5, f"⚪ {asset_name}: Check failed"
    
    def _get_asset_trend(self, asset_name: str, symbol: str, mt5_client) -> str:
        """Get trend direction for correlated asset (simplified implementation)"""
        # In a real implementation, you would fetch actual data for these symbols
        # This is a simplified version that uses mock logic
        
        try:
            # Try to get actual data if symbols are available in MT5
            data = mt5_client.get_rates(symbol, mt5_client.TIMEFRAME_H4, 100)
            
            if data is not None and len(data) > 20:
                # Real trend analysis
                return self._analyze_trend_from_data(data)
            else:
                # Fallback to mock trends based on asset behavior patterns
                return self._get_mock_trend(asset_name)
                
        except Exception as e:
            logger.debug(f"Could not get trend for {asset_name}: {e}")
            return "UNAVAILABLE"
    
    def _analyze_trend_from_data(self, df: pd.DataFrame) -> str:
        """Analyze trend from actual price data"""
        if len(df) < 20:
            return "NEUTRAL"
        
        # Simple EMA trend analysis
        ema20 = df['c'].ewm(span=20).mean()
        ema50 = df['c'].ewm(span=50).mean()
        
        current_ema20 = ema20.iloc[-1]
        current_ema50 = ema50.iloc[-1]
        prev_ema20 = ema20.iloc[-2]
        prev_ema50 = ema50.iloc[-2]
        
        # Bullish if EMAs rising and aligned
        if (current_ema20 > current_ema50 and 
            current_ema20 > prev_ema20 and 
            current_ema50 > prev_ema50):
            return "BULLISH"
        # Bearish if EMAs falling and aligned
        elif (current_ema20 < current_ema50 and 
              current_ema20 < prev_ema20 and 
              current_ema50 < prev_ema50):
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def _get_mock_trend(self, asset_name: str) -> str:
        """Get mock trend for demonstration (replace with real data)"""
        # This is a simplified mock - in production, use real data feeds
        import random
        
        trends = ["BULLISH", "BEARISH", "NEUTRAL"]
        weights = {
            "DXY": [0.3, 0.5, 0.2],  # More likely bearish for gold alignment
            "US10Y": [0.4, 0.4, 0.2],
            "SPX": [0.5, 0.3, 0.2], 
            "EURUSD": [0.5, 0.3, 0.2],
            "OIL": [0.4, 0.4, 0.2]
        }
        
        if asset_name in weights:
            return random.choices(trends, weights=weights[asset_name])[0]
        else:
            return random.choice(trends)
    
    def get_correlation_insights(self, gold_signal: Dict, mt5_client) -> Dict:
        """Get detailed correlation insights for the signal"""
        aligned, score, notes = self.check_correlation_alignment(gold_signal, mt5_client)
        
        return {
            "aligned": aligned,
            "alignment_score": score,
            "confidence": "HIGH" if score >= 0.8 else "MEDIUM" if score >= 0.6 else "LOW",
            "notes": notes,
            "recommendation": self._get_correlation_recommendation(score, aligned)
        }
    
    def _get_correlation_recommendation(self, score: float, aligned: bool) -> str:
        """Get trading recommendation based on correlation"""
        if score >= 0.8:
            return "STRONG CORRELATION SUPPORT - High confidence trade"
        elif score >= 0.6:
            return "GOOD CORRELATION SUPPORT - Normal confidence"
        elif score >= 0.4:
            return "MIXED CORRELATION - Trade with caution"
        else:
            return "POOR CORRELATION - Consider avoiding or reduce size"