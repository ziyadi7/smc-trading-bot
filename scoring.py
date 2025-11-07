from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from .detectors import OrderBlock, FUCandle, LiquidityZone
from .logging import get_logger

logger = get_logger(__name__)

class EliteScorer:
    """Institutional-grade scoring system 1-10"""
    
    def __init__(self, config: dict):
        self.config = config
        
    def calculate_elite_score(self, d1: pd.DataFrame, h4: pd.DataFrame, 
                            h1: pd.DataFrame, ob: OrderBlock, 
                            fu_candle: FUCandle = None,
                            liquidity_zones: List[LiquidityZone] = None) -> Tuple[int, Dict, List[str]]:
        """
        Calculate elite institutional score 1-10
        Returns: (score, breakdown, notes)
        """
        score_breakdown = {}
        notes = []
        total_score = 0
        
        # 1. DAILY TIMEFRAME BIAS (Max: 1.5 points)
        daily_score, daily_notes = self._score_daily_bias(d1, ob)
        score_breakdown['daily_bias'] = daily_score
        total_score += daily_score
        notes.extend(daily_notes)
        
        # 2. ORDER BLOCK QUALITY (Max: 1.5 points)  
        ob_score, ob_notes = self._score_order_block_quality(ob, h4 if ob.tf == 'H4' else h1)
        score_breakdown['ob_quality'] = ob_score
        total_score += ob_score
        notes.extend(ob_notes)
        
        # 3. LIQUIDITY CONFIRMATION (Max: 1.5 points)
        liq_score, liq_notes = self._score_liquidity_confirmation(h1, ob, liquidity_zones)
        score_breakdown['liquidity'] = liq_score
        total_score += liq_score
        notes.extend(liq_notes)
        
        # 4. FU CANDLE STRENGTH (Max: 1.5 points)
        fu_score, fu_notes = self._score_fu_candle_strength(fu_candle, h1, ob)
        score_breakdown['fu_strength'] = fu_score
        total_score += fu_score
        notes.extend(fu_notes)
        
        # 5. MARKET STRUCTURE (Max: 1.5 points)
        ms_score, ms_notes = self._score_market_structure(h1, h4, ob)
        score_breakdown['market_structure'] = ms_score
        total_score += ms_score
        notes.extend(ms_notes)
        
        # 6. INSTITUTIONAL ZONE PROXIMITY (Max: 1.0 points)
        zone_score, zone_notes = self._score_institutional_zone(h1, ob)
        score_breakdown['institutional_zone'] = zone_score
        total_score += zone_score
        notes.extend(zone_notes)
        
        # 7. VOLUME CONFIRMATION (Max: 1.0 points)
        volume_score, volume_notes = self._score_volume_confirmation(h1, ob, fu_candle)
        score_breakdown['volume'] = volume_score
        total_score += volume_score
        notes.extend(volume_notes)
        
        # 8. IMBALANCE ANALYSIS (Max: 0.5 points)
        imb_score, imb_notes = self._score_imbalance_analysis(h1, ob)
        score_breakdown['imbalance'] = imb_score
        total_score += imb_score
        notes.extend(imb_notes)
        
        # Ensure score is between 1-10
        final_score = max(1, min(10, int(round(total_score * 2))))  # Convert to 1-10 scale
        
        # Add quality rating
        quality_rating = self._get_quality_rating(final_score)
        notes.append(f"Institutional Quality: {quality_rating}")
        
        logger.debug(
            f"Elite score calculated: {final_score}/10",
            extra={"score": final_score, "breakdown": score_breakdown}
        )
        
        return final_score, score_breakdown, notes
    
    def _score_daily_bias(self, d1: pd.DataFrame, ob: OrderBlock) -> Tuple[float, List[str]]:
        """Score daily timeframe alignment (1.5 points max)"""
        score = 0.0
        notes = []
        
        # EMA alignment for trend
        ema21 = d1['c'].ewm(span=21).mean()
        ema50 = d1['c'].ewm(span=50).mean()
        ema100 = d1['c'].ewm(span=100).mean()
        
        current_price = d1['c'].iloc[-1]
        
        # Strong trend alignment (0.75 points)
        if ((ema21.iloc[-1] > ema50.iloc[-1] > ema100.iloc[-1] and ob.bullish) or
            (ema21.iloc[-1] < ema50.iloc[-1] < ema100.iloc[-1] and not ob.bullish)):
            score += 0.75
            notes.append("D1: Strong trend alignment")
        
        # Price above/below key EMAs (0.5 points)
        elif ((current_price > ema50.iloc[-1] and ob.bullish) or
              (current_price < ema50.iloc[-1] and not ob.bullish)):
            score += 0.5
            notes.append("D1: Price aligned with 50 EMA")
        
        # Recent momentum (0.25 points)
        recent_trend = self._calculate_recent_momentum(d1, ob.bullish)
        if recent_trend > 0.6:
            score += 0.25
            notes.append("D1: Strong recent momentum")
        
        return min(1.5, score), notes
    
    def _score_order_block_quality(self, ob: OrderBlock, df: pd.DataFrame) -> Tuple[float, List[str]]:
        """Score order block quality (1.5 points max)"""
        score = 0.0
        notes = []
        
        # Displacement strength (0.5 points)
        if ob.displacement >= 3.0:
            score += 0.5
            notes.append(f"OB: Strong displacement ({ob.displacement:.1f})")
        elif ob.displacement >= 2.0:
            score += 0.3
            notes.append(f"OB: Good displacement ({ob.displacement:.1f})")
        elif ob.displacement >= 1.5:
            score += 0.1
            notes.append(f"OB: Moderate displacement ({ob.displacement:.1f})")
        
        # Timeframe significance (0.5 points)
        if ob.tf == "H4":
            score += 0.5
            notes.append("OB: H4 institutional zone")
        elif ob.tf == "H1":
            score += 0.3
            notes.append("OB: H1 institutional zone")
        
        # Volume confirmation (0.25 points)
        avg_volume = df['v'].rolling(20).mean().iloc[ob.idx]
        volume_ratio = ob.volume / avg_volume if avg_volume > 0 else 1
        if volume_ratio >= 1.5:
            score += 0.25
            notes.append(f"OB: High volume ({volume_ratio:.1f}x)")
        elif volume_ratio >= 1.2:
            score += 0.15
            notes.append(f"OB: Above average volume ({volume_ratio:.1f}x)")
        
        # Quality score from detector (0.25 points)
        if ob.quality >= 0.8:
            score += 0.25
            notes.append("OB: Elite quality formation")
        elif ob.quality >= 0.6:
            score += 0.15
            notes.append("OB: Good quality formation")
        
        return min(1.5, score), notes
    
    def _score_liquidity_confirmation(self, h1: pd.DataFrame, ob: OrderBlock, 
                                    liquidity_zones: List[LiquidityZone]) -> Tuple[float, List[str]]:
        """Score liquidity confirmation (1.5 points max)"""
        score = 0.0
        notes = []
        
        from .detectors import InstitutionalDetector
        detector = InstitutionalDetector(self.config)
        
        # Liquidity grab detection (0.75 points)
        grab = detector.liquidity_grab_detected(h1)
        if grab:
            score += 0.5
            strength = grab.get('strength', 0)
            if strength >= 1.0:
                score += 0.25  # Strong grab bonus
                notes.append(f"Liq: Strong {grab['type']} (strength: {strength:.1f})")
            else:
                notes.append(f"Liq: {grab['type']} confirmed")
        
        # Liquidity zone alignment (0.5 points)
        if liquidity_zones:
            current_price = h1['c'].iloc[-1]
            relevant_zones = []
            
            for zone in liquidity_zones:
                if ((ob.bullish and zone.type in ['equal_low', 'session_low']) or
                    (not ob.bullish and zone.type in ['equal_high', 'session_high'])):
                    relevant_zones.append(zone)
            
            if relevant_zones:
                strongest_zone = max(relevant_zones, key=lambda x: x.strength)
                price_diff = abs(current_price - strongest_zone.price)
                atr_val = detector.atr(h1).iloc[-1]
                
                if price_diff <= atr_val * 0.5:
                    score += 0.5
                    notes.append(f"Liq: Near strong {strongest_zone.type}")
                elif price_diff <= atr_val:
                    score += 0.25
                    notes.append(f"Liq: Approaching {strongest_zone.type}")
        
        # Multiple confirmations (0.25 points)
        confirmations = 0
        if grab:
            confirmations += 1
        if liquidity_zones:
            confirmations += 1
        
        if confirmations >= 2:
            score += 0.25
            notes.append("Liq: Multiple confirmations")
        
        return min(1.5, score), notes
    
    def _score_fu_candle_strength(self, fu_candle: FUCandle, h1: pd.DataFrame, ob: OrderBlock) -> Tuple[float, List[str]]:
        """Score FU candle strength (1.5 points max)"""
        if not fu_candle:
            return 0.0, []
            
        score = 0.0
        notes = []
        
        from .detectors import InstitutionalDetector
        detector = InstitutionalDetector(self.config)
        
        # FU candle body strength (0.5 points)
        atr_val = detector.atr(h1).iloc[-1]
        body_atr_ratio = fu_candle.body_size / atr_val
        
        if body_atr_ratio >= 1.2:
            score += 0.5
            notes.append(f"FU: Very strong body ({body_atr_ratio:.1f} ATR)")
        elif body_atr_ratio >= 0.8:
            score += 0.3
            notes.append(f"FU: Strong body ({body_atr_ratio:.1f} ATR)")
        elif body_atr_ratio >= 0.5:
            score += 0.1
            notes.append(f"FU: Moderate body ({body_atr_ratio:.1f} ATR)")
        
        # FU close strength (0.4 points)
        if ((fu_candle.direction == 'bullish' and fu_candle.close_frac >= 0.7) or
            (fu_candle.direction == 'bearish' and fu_candle.close_frac <= 0.3)):
            score += 0.4
            notes.append("FU: Strong close rejection")
        elif ((fu_candle.direction == 'bullish' and fu_candle.close_frac >= 0.6) or
              (fu_candle.direction == 'bearish' and fu_candle.close_frac <= 0.4)):
            score += 0.2
            notes.append("FU: Good close rejection")
        
        # FU and OB alignment (0.3 points)
        if ((fu_candle.direction == 'bullish' and not ob.bullish) or
            (fu_candle.direction == 'bearish' and ob.bullish)):
            score += 0.3
            notes.append("FU: Perfect OB alignment")
        
        # Volume confirmation (0.3 points)
        if fu_candle.volume_ratio >= 2.0:
            score += 0.3
            notes.append(f"FU: High volume ({fu_candle.volume_ratio:.1f}x)")
        elif fu_candle.volume_ratio >= 1.5:
            score += 0.15
            notes.append(f"FU: Above average volume ({fu_candle.volume_ratio:.1f}x)")
        
        return min(1.5, score), notes
    
    def _score_market_structure(self, h1: pd.DataFrame, h4: pd.DataFrame, ob: OrderBlock) -> Tuple[float, List[str]]:
        """Score market structure strength (1.5 points max)"""
        score = 0.0
        notes = []
        
        from .detectors import InstitutionalDetector
        detector = InstitutionalDetector(self.config)
        
        # BOS/MSS confirmation (0.75 points)
        bos_h1, direction_h1, strength_h1 = detector.bos_mss_confirmed(h1)
        bos_h4, direction_h4, strength_h4 = detector.bos_mss_confirmed(h4)
        
        if bos_h4 and direction_h4 == ('bullish' if ob.bullish else 'bearish'):
            score += 0.75
            notes.append("MS: H4 BOS/MSS confirmed")
        elif bos_h1 and direction_h1 == ('bullish' if ob.bullish else 'bearish'):
            score += 0.5
            notes.append("MS: H1 BOS/MSS confirmed")
        elif strength_h1 >= 0.7 or strength_h4 >= 0.7:
            score += 0.25
            notes.append("MS: Strong structure forming")
        
        # Multi-timeframe alignment (0.5 points)
        if ((ob.bullish and direction_h1 == 'bullish' and direction_h4 == 'bullish') or
            (not ob.bullish and direction_h1 == 'bearish' and direction_h4 == 'bearish')):
            score += 0.5
            notes.append("MS: Multi-TF alignment")
        
        # Recent momentum (0.25 points)
        recent_momentum = self._calculate_recent_momentum(h1, ob.bullish)
        if recent_momentum >= 0.7:
            score += 0.25
            notes.append("MS: Strong recent momentum")
        
        return min(1.5, score), notes
    
    def _score_institutional_zone(self, h1: pd.DataFrame, ob: OrderBlock) -> Tuple[float, List[str]]:
        """Score institutional zone proximity (1.0 points max)"""
        score = 0.0
        notes = []
        
        from .detectors import InstitutionalDetector
        detector = InstitutionalDetector(self.config)
        
        current_price = h1['c'].iloc[-1]
        ob_mid = (ob.body_high + ob.body_low) / 2
        atr_val = detector.atr(h1).iloc[-1]
        distance_atr = abs(current_price - ob_mid) / atr_val
        
        # Proximity to OB mid (1.0 points)
        if distance_atr <= 0.3:
            score += 1.0
            notes.append("Zone: Very close to OB mid")
        elif distance_atr <= 0.5:
            score += 0.7
            notes.append("Zone: Close to OB mid")
        elif distance_atr <= 0.8:
            score += 0.4
            notes.append("Zone: Near OB mid")
        elif distance_atr <= 1.0:
            score += 0.2
            notes.append("Zone: Approaching OB mid")
        
        return min(1.0, score), notes
    
    def _score_volume_confirmation(self, h1: pd.DataFrame, ob: OrderBlock, fu_candle: FUCandle) -> Tuple[float, List[str]]:
        """Score volume confirmation (1.0 points max)"""
        score = 0.0
        notes = []
        
        # Recent volume analysis
        recent_volume = h1['v'].tail(10)
        avg_volume = h1['v'].rolling(20).mean().iloc[-1]
        volume_ratio = recent_volume.mean() / avg_volume if avg_volume > 0 else 1
        
        # Volume trend (0.5 points)
        if volume_ratio >= 1.5:
            score += 0.5
            notes.append(f"Volume: High activity ({volume_ratio:.1f}x)")
        elif volume_ratio >= 1.2:
            score += 0.3
            notes.append(f"Volume: Above average ({volume_ratio:.1f}x)")
        
        # FU volume (0.3 points)
        if fu_candle and fu_candle.volume_ratio >= 1.5:
            score += 0.3
            notes.append(f"Volume: FU confirmation ({fu_candle.volume_ratio:.1f}x)")
        
        # OB volume (0.2 points)
        ob_volume_ratio = ob.volume / h1['v'].rolling(20).mean().iloc[ob.idx] if ob.idx < len(h1) else 1
        if ob_volume_ratio >= 1.5:
            score += 0.2
            notes.append(f"Volume: OB was significant ({ob_volume_ratio:.1f}x)")
        
        return min(1.0, score), notes
    
    def _score_imbalance_analysis(self, h1: pd.DataFrame, ob: OrderBlock) -> Tuple[float, List[str]]:
        """Score imbalance analysis (0.5 points max)"""
        score = 0.0
        notes = []
        
        from .detectors import InstitutionalDetector
        detector = InstitutionalDetector(self.config)
        
        imbalances = detector.find_imbalances(h1)
        current_price = h1['c'].iloc[-1]
        
        # Recent imbalances
        recent_imbalances = [imb for imb in imbalances if imb['idx'] >= len(h1) - 10]
        filled_imbalances = [imb for imb in recent_imbalances if imb['filled']]
        unfilled_imbalances = [imb for imb in recent_imbalances if not imb['filled']]
        
        # Filled imbalances (0.25 points)
        if filled_imbalances:
            score += 0.25
            notes.append("Imbalance: Recent fill")
        
        # Unfilled imbalances in direction (0.25 points)
        directional_imbalances = [
            imb for imb in unfilled_imbalances 
            if (ob.bullish and imb['type'] == 'bullish_fvg') or 
               (not ob.bullish and imb['type'] == 'bearish_fvg')
        ]
        
        if directional_imbalances:
            score += 0.25
            notes.append("Imbalance: Target available")
        
        return min(0.5, score), notes
    
    def _calculate_recent_momentum(self, df: pd.DataFrame, bullish: bool) -> float:
        """Calculate recent momentum strength 0-1"""
        if len(df) < 10:
            return 0.5
            
        recent_closes = df['c'].tail(10)
        if bullish:
            # Count bullish candles and strength
            bullish_candles = sum(recent_closes > recent_closes.shift(1))
            momentum = bullish_candles / 10.0
        else:
            # Count bearish candles and strength
            bearish_candles = sum(recent_closes < recent_closes.shift(1))
            momentum = bearish_candles / 10.0
        
        return momentum
    
    def _get_quality_rating(self, score: int) -> str:
        """Convert score to elite quality rating"""
        if score >= 9:
            return "EXCEPTIONAL 🏆"
        elif score >= 8:
            return "ELITE ✅"
        elif score >= 7:
            return "HIGH QUALITY 👍" 
        elif score >= 6:
            return "SOLID 💪"
        elif score >= 5:
            return "MODERATE ⚠️"
        elif score >= 4:
            return "LOW CONFIDENCE 🔶"
        else:
            return "POOR 🔴"