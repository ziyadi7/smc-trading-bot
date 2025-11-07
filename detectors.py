import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict
from .logging import get_logger

logger = get_logger(__name__)

@dataclass
class OrderBlock:
    idx: int
    bullish: bool
    body_low: float
    body_high: float
    wick_low: float
    wick_high: float
    tf: str
    displacement: float = 0.0
    volume: float = 0.0
    quality: float = 0.0

@dataclass
class FUCandle:
    idx: int
    direction: str  # 'bullish' or 'bearish'
    body_size: float
    range_size: float
    close_frac: float
    volume_ratio: float
    strength: float

@dataclass
class LiquidityZone:
    price: float
    type: str  # 'equal_high', 'equal_low', 'session_high', 'session_low'
    strength: float
    timeframe: str

class InstitutionalDetector:
    """Elite SMC detection with institutional-grade logic"""
    
    def __init__(self, config: dict):
        self.config = config
        self.min_displacement = config.get('min_displacement', 2.0)
        self.fu_body_atr = config.get('fu_body_atr', 0.8)
        self.fu_close_frac = config.get('fu_close_frac', 0.6)
        self.ob_prox_atr = config.get('ob_prox_atr', 1.0)
    
    # ===== CORE MARKET STRUCTURE =====
    
    def bos_mss_confirmed(self, df: pd.DataFrame, lookback: int = 10) -> Tuple[bool, str, float]:
        """
        Advanced Break of Structure + Market Structure Shift
        Returns: (confirmed, direction, strength)
        """
        if len(df) < lookback + 5:
            return False, "none", 0.0
            
        # Calculate recent structure
        recent_highs = df['h'].tail(lookback)
        recent_lows = df['l'].tail(lookback)
        
        current_high = df['h'].iloc[-1]
        current_low = df['l'].iloc[-1]
        current_close = df['c'].iloc[-1]
        
        # Structure levels
        resistance = recent_highs.max()
        support = recent_lows.min()
        
        # BOS: Price breaks structure with momentum
        bos_up = current_high > resistance and current_close > resistance
        bos_down = current_low < support and current_close < support
        
        # MSS: Follow-through confirmation
        if bos_up:
            # Check for consecutive higher highs/lows
            higher_highs = current_high > df['h'].iloc[-2]
            higher_lows = current_low > df['l'].iloc[-2]
            strength = 0.5 + (0.5 * (1 if higher_highs and higher_lows else 0))
            return True, "bullish", strength
            
        elif bos_down:
            # Check for consecutive lower highs/lows
            lower_highs = current_high < df['h'].iloc[-2]
            lower_lows = current_low < df['l'].iloc[-2]
            strength = 0.5 + (0.5 * (1 if lower_highs and lower_lows else 0))
            return True, "bearish", strength
            
        return False, "none", 0.0
    
    def find_liquidity_zones(self, df: pd.DataFrame, tf: str) -> List[LiquidityZone]:
        """Find all types of liquidity zones"""
        zones = []
        
        # Equal highs/lows
        equal_highs = self._find_equal_highs(df, tf)
        equal_lows = self._find_equal_lows(df, tf)
        
        # Session highs/lows
        session_highs = self._find_session_highs(df, tf)
        session_lows = self._find_session_lows(df, tf)
        
        zones.extend(equal_highs)
        zones.extend(equal_lows)
        zones.extend(session_highs)
        zones.extend(session_lows)
        
        return sorted(zones, key=lambda x: x.strength, reverse=True)
    
    def _find_equal_highs(self, df: pd.DataFrame, tf: str) -> List[LiquidityZone]:
        """Find equal highs (retail resistance)"""
        zones = []
        left_bars = 3
        right_bars = 2
        
        for i in range(left_bars, len(df) - right_bars):
            if (df['h'].iloc[i] == max(df['h'].iloc[i-left_bars:i+right_bars+1]) and
                sum(df['h'].iloc[i] == df['h'].iloc[i-left_bars:i+right_bars+1]) >= 2):
                
                # Calculate strength based on volume and rejection
                volume_strength = df['v'].iloc[i] / df['v'].rolling(20).mean().iloc[i]
                rejection = (df['h'].iloc[i] - df['c'].iloc[i]) / (df['h'].iloc[i] - df['l'].iloc[i])
                
                strength = min(1.0, (volume_strength * 0.6 + rejection * 0.4))
                
                zones.append(LiquidityZone(
                    price=df['h'].iloc[i],
                    type='equal_high',
                    strength=strength,
                    timeframe=tf
                ))
        
        return zones
    
    def _find_equal_lows(self, df: pd.DataFrame, tf: str) -> List[LiquidityZone]:
        """Find equal lows (retail support)"""
        zones = []
        left_bars = 3
        right_bars = 2
        
        for i in range(left_bars, len(df) - right_bars):
            if (df['l'].iloc[i] == min(df['l'].iloc[i-left_bars:i+right_bars+1]) and
                sum(df['l'].iloc[i] == df['l'].iloc[i-left_bars:i+right_bars+1]) >= 2):
                
                # Calculate strength based on volume and rejection
                volume_strength = df['v'].iloc[i] / df['v'].rolling(20).mean().iloc[i]
                rejection = (df['c'].iloc[i] - df['l'].iloc[i]) / (df['h'].iloc[i] - df['l'].iloc[i])
                
                strength = min(1.0, (volume_strength * 0.6 + rejection * 0.4))
                
                zones.append(LiquidityZone(
                    price=df['l'].iloc[i],
                    type='equal_low',
                    strength=strength,
                    timeframe=tf
                ))
        
        return zones
    
    def _find_session_highs(self, df: pd.DataFrame, tf: str) -> List[LiquidityZone]:
        """Find session highs (institutional interest)"""
        zones = []
        
        # Use the last 5 sessions for context
        if len(df) >= 100:
            session_high = df['h'].tail(100).max()
            zones.append(LiquidityZone(
                price=session_high,
                type='session_high',
                strength=0.8,
                timeframe=tf
            ))
        
        return zones
    
    def _find_session_lows(self, df: pd.DataFrame, tf: str) -> List[LiquidityZone]:
        """Find session lows (institutional interest)"""
        zones = []
        
        # Use the last 5 sessions for context
        if len(df) >= 100:
            session_low = df['l'].tail(100).min()
            zones.append(LiquidityZone(
                price=session_low,
                type='session_low',
                strength=0.8,
                timeframe=tf
            ))
        
        return zones
    
    def liquidity_grab_detected(self, df: pd.DataFrame) -> Optional[Dict]:
        """Advanced liquidity grab detection"""
        if len(df) < 10:
            return None
            
        current = df.iloc[-1]
        
        # Look for recent swing points
        recent_highs = df['h'].tail(20)
        recent_lows = df['l'].tail(20)
        
        swing_high = recent_highs.max()
        swing_low = recent_lows.min()
        
        swing_high_idx = recent_highs.idxmax()
        swing_low_idx = recent_lows.idxmin()
        
        atr_val = self.atr(df).iloc[-1]
        
        # Grab highs: price sweeps swing high but closes below with volume
        if (current['h'] > swing_high and 
            current['c'] < swing_high and 
            current['v'] > df['v'].iloc[swing_high_idx]):
            
            strength = min(2.0, (current['h'] - swing_high) / atr_val)
            
            return {
                'type': 'grab_highs',
                'sweep_price': swing_high,
                'strength': strength,
                'volume_ratio': current['v'] / df['v'].iloc[swing_high_idx],
                'atr_distance': (current['h'] - swing_high) / atr_val
            }
        
        # Grab lows: price sweeps swing low but closes above with volume
        if (current['l'] < swing_low and 
            current['c'] > swing_low and 
            current['v'] > df['v'].iloc[swing_low_idx]):
            
            strength = min(2.0, (swing_low - current['l']) / atr_val)
            
            return {
                'type': 'grab_lows',
                'sweep_price': swing_low,
                'strength': strength,
                'volume_ratio': current['v'] / df['v'].iloc[swing_low_idx],
                'atr_distance': (swing_low - current['l']) / atr_val
            }
            
        return None
    
    # ===== ORDER BLOCKS & INSTITUTIONAL ZONES =====
    
    def find_elite_order_blocks(self, df: pd.DataFrame, tf: str) -> List[OrderBlock]:
        """Find elite order blocks with quality scoring"""
        blocks = []
        
        for i in range(20, len(df) - 10):
            displacement = self._calculate_displacement(df, i)
            
            if displacement < self.min_displacement:
                continue
                
            # Bullish impulse (look for bearish OB before)
            if df['c'].iloc[i] > df['o'].iloc[i]:
                ob_idx, quality = self._find_elite_opposite_candle(df, i, find_bearish=True)
                if ob_idx is not None:
                    block = self._create_elite_order_block(df, ob_idx, False, tf, displacement, quality)
                    blocks.append(block)
            
            # Bearish impulse (look for bullish OB before)
            else:
                ob_idx, quality = self._find_elite_opposite_candle(df, i, find_bearish=False)
                if ob_idx is not None:
                    block = self._create_elite_order_block(df, ob_idx, True, tf, displacement, quality)
                    blocks.append(block)
        
        return sorted(blocks, key=lambda x: (x.quality, x.displacement), reverse=True)
    
    def _find_elite_opposite_candle(self, df: pd.DataFrame, start_idx: int, find_bearish: bool) -> Tuple[Optional[int], float]:
        """Find opposite candle with quality scoring"""
        best_idx = None
        best_quality = 0.0
        
        for i in range(start_idx - 1, max(0, start_idx - 15), -1):
            # Check if candle is opposite
            is_opposite = (find_bearish and df['c'].iloc[i] < df['o'].iloc[i]) or \
                         (not find_bearish and df['c'].iloc[i] > df['o'].iloc[i])
            
            if not is_opposite:
                continue
            
            # Calculate candle quality
            quality = self._calculate_candle_quality(df, i)
            
            if quality > best_quality:
                best_idx = i
                best_quality = quality
        
        return best_idx, best_quality
    
    def _calculate_candle_quality(self, df: pd.DataFrame, idx: int) -> float:
        """Calculate candle quality for OB selection"""
        # Body to range ratio
        body_size = abs(df['c'].iloc[idx] - df['o'].iloc[idx])
        range_size = df['h'].iloc[idx] - df['l'].iloc[idx]
        body_ratio = body_size / range_size if range_size > 0 else 0
        
        # Volume significance
        avg_volume = df['v'].rolling(20).mean().iloc[idx]
        volume_ratio = df['v'].iloc[idx] / avg_volume if avg_volume > 0 else 1
        
        # Proximity to key levels (simplified)
        proximity_score = 0.5  # Placeholder
        
        quality = (body_ratio * 0.4 + min(2.0, volume_ratio) * 0.3 + proximity_score * 0.3)
        return min(1.0, quality)
    
    def _create_elite_order_block(self, df: pd.DataFrame, idx: int, bullish: bool, tf: str, displacement: float, quality: float) -> OrderBlock:
        """Create elite order block with enhanced properties"""
        body_low = min(df['o'].iloc[idx], df['c'].iloc[idx])
        body_high = max(df['o'].iloc[idx], df['c'].iloc[idx])
        
        return OrderBlock(
            idx=idx,
            bullish=bullish,
            body_low=body_low,
            body_high=body_high,
            wick_low=df['l'].iloc[idx],
            wick_high=df['h'].iloc[idx],
            tf=tf,
            displacement=displacement,
            volume=df['v'].iloc[idx],
            quality=quality
        )
    
    # ===== FU CANDLES (INSTITUTIONAL FAKEOUTS) =====
    
    def detect_elite_fu_candle(self, df: pd.DataFrame) -> Optional[FUCandle]:
        """Detect elite FU candles with strength scoring"""
        if len(df) < 10:
            return None
            
        current = df.iloc[-1]
        atr_val = self.atr(df).iloc[-1]
        
        # Basic FU criteria
        body_size = abs(current['c'] - current['o'])
        range_size = current['h'] - current['l']
        
        if body_size < self.fu_body_atr * atr_val:
            return None
        
        # Volume significance
        avg_volume = df['v'].rolling(20).mean().iloc[-1]
        volume_ratio = current['v'] / avg_volume if avg_volume > 0 else 1
        
        # Liquidity sweep detection
        swept_high = current['h'] > df['h'].iloc[-10:-1].max()
        swept_low = current['l'] < df['l'].iloc[-10:-1].min()
        
        # Close position for rejection strength
        close_frac = (current['c'] - current['l']) / range_size if range_size > 0 else 0.5
        
        # Bullish FU: sweeps lows but closes high
        if swept_low and close_frac >= self.fu_close_frac:
            strength = self._calculate_fu_strength(df, 'bullish', body_size, atr_val, volume_ratio)
            return FUCandle(
                idx=len(df) - 1,
                direction='bullish',
                body_size=body_size,
                range_size=range_size,
                close_frac=close_frac,
                volume_ratio=volume_ratio,
                strength=strength
            )
        
        # Bearish FU: sweeps highs but closes low
        elif swept_high and close_frac <= (1 - self.fu_close_frac):
            strength = self._calculate_fu_strength(df, 'bearish', body_size, atr_val, volume_ratio)
            return FUCandle(
                idx=len(df) - 1,
                direction='bearish',
                body_size=body_size,
                range_size=range_size,
                close_frac=close_frac,
                volume_ratio=volume_ratio,
                strength=strength
            )
            
        return None
    
    def _calculate_fu_strength(self, df: pd.DataFrame, direction: str, body_size: float, atr_val: float, volume_ratio: float) -> float:
        """Calculate FU candle strength"""
        # Body strength (0-0.4 points)
        body_strength = min(0.4, (body_size / atr_val) * 0.4)
        
        # Volume strength (0-0.3 points)
        volume_strength = min(0.3, (min(3.0, volume_ratio) * 0.1))
        
        # Rejection strength (0-0.3 points)
        current = df.iloc[-1]
        range_size = current['h'] - current['l']
        if direction == 'bullish':
            rejection = (current['c'] - current['l']) / range_size
        else:
            rejection = (current['h'] - current['c']) / range_size
        
        rejection_strength = min(0.3, rejection * 0.3)
        
        return body_strength + volume_strength + rejection_strength
    
    # ===== IMBALANCES & FAIR VALUE GAPS =====
    
    def find_imbalances(self, df: pd.DataFrame, min_gap_size: float = 0.0002) -> List[Dict]:
        """Find fair value gaps and imbalances"""
        imbalances = []
        
        for i in range(2, len(df) - 1):
            # Bullish FVG (gap up)
            if df['l'].iloc[i] > df['h'].iloc[i-1] + min_gap_size:
                gap_size = df['l'].iloc[i] - df['h'].iloc[i-1]
                imbalances.append({
                    'type': 'bullish_fvg',
                    'low': df['h'].iloc[i-1],
                    'high': df['l'].iloc[i],
                    'gap_size': gap_size,
                    'idx': i,
                    'filled': False,
                    'strength': min(1.0, gap_size / self.atr(df).iloc[i])
                })
            
            # Bearish FVG (gap down)
            elif df['h'].iloc[i] < df['l'].iloc[i-1] - min_gap_size:
                gap_size = df['l'].iloc[i-1] - df['h'].iloc[i]
                imbalances.append({
                    'type': 'bearish_fvg',
                    'high': df['l'].iloc[i-1],
                    'low': df['h'].iloc[i],
                    'gap_size': gap_size,
                    'idx': i,
                    'filled': False,
                    'strength': min(1.0, gap_size / self.atr(df).iloc[i])
                })
        
        # Check which are filled
        current_price = df['c'].iloc[-1]
        for imb in imbalances:
            if imb['low'] <= current_price <= imb['high']:
                imb['filled'] = True
        
        return imbalances
    
    # ===== UTILITIES =====
    
    def atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Average True Range with elite handling"""
        high_low = df['h'] - df['l']
        high_close = (df['h'] - df['c'].shift()).abs()
        low_close = (df['l'] - df['c'].shift()).abs()
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    
    def _calculate_displacement(self, df: pd.DataFrame, idx: int) -> float:
        """Calculate elite displacement score"""
        atr_val = self.atr(df).iloc[idx]
        if atr_val <= 0:
            return 0
            
        body = abs(df['c'].iloc[idx] - df['o'].iloc[idx])
        rng = df['h'].iloc[idx] - df['l'].iloc[idx]
        volume_ratio = df['v'].iloc[idx] / df['v'].rolling(20).mean().iloc[idx]
        
        # Enhanced displacement with volume consideration
        base_displacement = (body / atr_val) * (rng / atr_val) * 0.5
        volume_boost = min(0.5, (volume_ratio - 1) * 0.25)  # Max 0.5 boost
        
        return base_displacement + volume_boost
    
    def get_best_order_block(self, h4_blocks: List[OrderBlock], h1_blocks: List[OrderBlock]) -> Optional[OrderBlock]:
        """Select the best order block from multiple timeframes"""
        all_blocks = h4_blocks + h1_blocks
        if not all_blocks:
            return None
        
        # Score blocks based on multiple factors
        scored_blocks = []
        for block in all_blocks:
            score = (block.quality * 0.4 + 
                    min(1.0, block.displacement / 5.0) * 0.3 +
                    (1.0 if block.tf == 'H4' else 0.5) * 0.3)
            scored_blocks.append((score, block))
        
        # Return highest scored block
        return max(scored_blocks, key=lambda x: x[0])[1]