import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from .logging import get_logger

logger = get_logger(__name__)

class InstitutionalFlowDetector:
    """Detect institutional order flow and block trades"""
    
    def __init__(self, volume_threshold: float = 2.0, atr_threshold: float = 1.0):
        self.volume_threshold = volume_threshold
        self.atr_threshold = atr_threshold
        self.recent_blocks = []
    
    def detect_block_trades(self, df: pd.DataFrame) -> List[Dict]:
        """Detect potential institutional block trades"""
        blocks = []
        
        for i in range(20, len(df)):
            block = self._analyze_candle_for_blocks(df, i)
            if block:
                blocks.append(block)
        
        # Keep only recent blocks
        self.recent_blocks = [b for b in blocks if b['idx'] >= len(df) - 10]
        
        return self.recent_blocks
    
    def _analyze_candle_for_blocks(self, df: pd.DataFrame, idx: int) -> Optional[Dict]:
        """Analyze individual candle for block trade characteristics"""
        # Volume spike check
        avg_volume = df['v'].rolling(20).mean().iloc[idx]
        volume_ratio = df['v'].iloc[idx] / avg_volume if avg_volume > 0 else 1
        
        if volume_ratio < self.volume_threshold:
            return None
        
        # Price movement significance
        atr = self._calculate_atr(df).iloc[idx]
        body_size = abs(df['c'].iloc[idx] - df['o'].iloc[idx])
        body_atr_ratio = body_size / atr if atr > 0 else 0
        
        if body_atr_ratio < self.atr_threshold:
            return None
        
        # Determine direction and strength
        direction = "BUY" if df['c'].iloc[idx] > df['o'].iloc[idx] else "SELL"
        strength = min(2.0, (volume_ratio * 0.6 + body_atr_ratio * 0.4))
        
        return {
            'idx': idx,
            'direction': direction,
            'volume_ratio': volume_ratio,
            'body_atr_ratio': body_atr_ratio,
            'strength': strength,
            'price': df['c'].iloc[idx],
            'time': df.index[idx] if hasattr(df.index, 'iloc') else idx
        }
    
    def is_flow_aligned(self, signal: Dict, blocks: List[Dict]) -> Tuple[bool, float, List[str]]:
        """Check if institutional flow supports the signal"""
        if not blocks:
            return True, 0.5, ["No recent block data"]  # Neutral if no data
        
        # Filter recent blocks (last 5 candles)
        recent_blocks = [b for b in blocks if b['idx'] >= len(signal['df']) - 5]
        
        if not recent_blocks:
            return True, 0.5, ["No very recent blocks"]
        
        # Count aligned vs non-aligned blocks
        aligned_blocks = [b for b in recent_blocks if b['direction'] == signal['side']]
        non_aligned_blocks = [b for b in recent_blocks if b['direction'] != signal['side']]
        
        total_blocks = len(recent_blocks)
        alignment_ratio = len(aligned_blocks) / total_blocks if total_blocks > 0 else 0
        
        # Calculate strength-weighted alignment
        aligned_strength = sum(b['strength'] for b in aligned_blocks)
        total_strength = sum(b['strength'] for b in recent_blocks)
        strength_alignment = aligned_strength / total_strength if total_strength > 0 else 0
        
        # Combined alignment score
        combined_score = (alignment_ratio * 0.6 + strength_alignment * 0.4)
        
        is_aligned = combined_score >= 0.6
        
        notes = []
        notes.append(f"Blocks: {len(aligned_blocks)}/{total_blocks} aligned")
        notes.append(f"Strength: {strength_alignment:.1%} aligned")
        
        if aligned_blocks:
            strongest_block = max(aligned_blocks, key=lambda x: x['strength'])
            notes.append(f"Strongest: {strongest_block['direction']} (strength: {strongest_block['strength']:.1f})")
        
        logger.debug(
            f"Institutional flow: {combined_score:.2f} - {'ALIGNED' if is_aligned else 'NOT ALIGNED'}",
            extra={
                "signal_side": signal['side'],
                "alignment_score": combined_score,
                "aligned_blocks": len(aligned_blocks),
                "total_blocks": total_blocks
            }
        )
        
        return is_aligned, combined_score, notes
    
    def get_flow_analysis(self, signal: Dict, df: pd.DataFrame) -> Dict:
        """Get comprehensive institutional flow analysis"""
        blocks = self.detect_block_trades(df)
        aligned, score, notes = self.is_flow_aligned(signal, blocks)
        
        return {
            "aligned": aligned,
            "alignment_score": score,
            "recent_blocks_count": len(blocks),
            "aligned_blocks_count": len([b for b in blocks if b['direction'] == signal['side']]),
            "total_block_strength": sum(b['strength'] for b in blocks),
            "notes": notes,
            "recommendation": self._get_flow_recommendation(score, aligned)
        }
    
    def _get_flow_recommendation(self, score: float, aligned: bool) -> str:
        """Get trading recommendation based on institutional flow"""
        if score >= 0.8:
            return "STRONG INSTITUTIONAL SUPPORT - High confidence"
        elif score >= 0.6:
            return "GOOD INSTITUTIONAL SUPPORT - Normal confidence"
        elif score >= 0.4:
            return "MIXED INSTITUTIONAL FLOW - Trade with caution"
        else:
            return "CONTRARY INSTITUTIONAL FLOW - Consider avoiding"
    
    def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range"""
        high_low = df['h'] - df['l']
        high_close = (df['h'] - df['c'].shift()).abs()
        low_close = (df['l'] - df['c'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean()