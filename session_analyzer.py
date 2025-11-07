import pandas as pd
import datetime as dt
from typing import Dict, List, Tuple
import pytz
from .logging import get_logger

logger = get_logger(__name__)

class SessionAnalyzer:
    """Analyze trading sessions for optimal entry timing"""
    
    def __init__(self):
        self.sessions = {
            "ASIAN": {
                "start": dt.time(0, 0),   # 00:00 UTC
                "end": dt.time(8, 0),     # 08:00 UTC
                "characteristics": ["low_volatility", "range_forming", "liquidity_building"],
                "score_boost": 0.8
            },
            "LONDON": {
                "start": dt.time(8, 0),   # 08:00 UTC  
                "end": dt.time(16, 0),    # 16:00 UTC
                "characteristics": ["high_volatility", "trend_establishing", "institutional_flow"],
                "score_boost": 1.2
            },
            "NEW_YORK": {
                "start": dt.time(13, 0),  # 13:00 UTC
                "end": dt.time(21, 0),    # 21:00 UTC
                "characteristics": ["high_volatility", "momentum", "liquidity_taking"],
                "score_boost": 1.5
            },
            "OVERLAP": {
                "start": dt.time(13, 0),  # 13:00 UTC (London-NY overlap)
                "end": dt.time(16, 0),    # 16:00 UTC
                "characteristics": ["extremely_high_volatility", "institutional_activity", "breakouts"],
                "score_boost": 1.8
            }
        }
    
    def get_current_session(self) -> Tuple[str, Dict, float]:
        """Get current trading session info"""
        now_utc = dt.datetime.now(pytz.UTC)
        current_time = now_utc.time()
        
        for session_name, session_info in self.sessions.items():
            if session_info['start'] <= current_time < session_info['end']:
                time_remaining = self._calculate_time_remaining(now_utc, session_info['end'])
                return session_name, session_info, time_remaining
        
        # If no session matches, it's after hours
        return "AFTER_HOURS", {"score_boost": 0.5, "characteristics": ["low_liquidity"]}, 0.0
    
    def analyze_session_optimality(self, signal: Dict) -> Tuple[bool, float, List[str]]:
        """
        Analyze if current session is optimal for the signal type
        Returns: (is_optimal, optimality_score, notes)
        """
        session_name, session_info, time_remaining = self.get_current_session()
        
        # Base session score
        session_score = session_info.get('score_boost', 1.0)
        
        # Adjust based on signal type and session characteristics
        signal_type = signal.get('side', 'BUY')
        characteristics = session_info.get('characteristics', [])
        
        notes = [f"Session: {session_name}"]
        
        # Session-specific optimizations
        if "OVERLAP" in session_name:
            # Overlap sessions are good for all signals
            session_score *= 1.2
            notes.append("🎯 Optimal: London-NY overlap")
        
        elif "LONDON" in session_name:
            # London session good for institutional setups
            if signal.get('score', 0) >= 7:  # High-quality signals
                session_score *= 1.1
                notes.append("✅ Good: London session for institutional setups")
            else:
                session_score *= 0.9
                notes.append("⚠️ Moderate: Wait for higher quality in London")
        
        elif "NEW_YORK" in session_name:
            # NY session good for momentum
            if "momentum" in signal.get('notes', []):
                session_score *= 1.1
                notes.append("✅ Good: NY session for momentum")
            else:
                session_score *= 1.0
        
        elif "ASIAN" in session_name:
            # Asian session generally less optimal for gold
            session_score *= 0.7
            notes.append("🔶 Suboptimal: Asian session for gold")
        
        else:  # AFTER_HOURS
            session_score *= 0.5
            notes.append("🔴 Poor: After hours trading")
        
        # Time remaining adjustment
        if time_remaining > 0:
            if time_remaining >= 2:  # More than 2 hours remaining
                time_boost = 1.1
                notes.append(f"⏰ Good: {time_remaining:.1f}h remaining")
            else:  # Less than 2 hours
                time_boost = 0.9
                notes.append(f"⚠️ Limited: {time_remaining:.1f}h remaining")
        else:
            time_boost = 1.0
        
        final_score = session_score * time_boost
        is_optimal = final_score >= 1.0
        
        logger.debug(
            f"Session analysis: {session_name} - Score: {final_score:.2f}",
            extra={
                "session": session_name,
                "optimality_score": final_score,
                "optimal": is_optimal
            }
        )
        
        return is_optimal, final_score, notes
    
    def _calculate_time_remaining(self, current_time: dt.datetime, session_end: dt.time) -> float:
        """Calculate hours remaining in current session"""
        end_datetime = dt.datetime.combine(current_time.date(), session_end)
        if end_datetime < current_time:
            end_datetime += dt.timedelta(days=1)
        
        time_diff = end_datetime - current_time
        return time_diff.total_seconds() / 3600  # Convert to hours
    
    def get_session_recommendation(self, signal: Dict) -> Dict:
        """Get session-based trading recommendation"""
        is_optimal, score, notes = self.analyze_session_optimality(signal)
        
        return {
            "optimal": is_optimal,
            "optimality_score": score,
            "current_session": self.get_current_session()[0],
            "notes": notes,
            "recommendation": self._get_session_recommendation(score, is_optimal)
        }
    
    def _get_session_recommendation(self, score: float, optimal: bool) -> str:
        """Get trading recommendation based on session analysis"""
        if score >= 1.5:
            return "OPTIMAL SESSION - High confidence entry"
        elif score >= 1.2:
            return "GOOD SESSION - Normal confidence"
        elif score >= 1.0:
            return "ACCEPTABLE SESSION - Proceed with caution"
        elif score >= 0.8:
            return "SUBOPTIMAL SESSION - Consider waiting"
        else:
            return "POOR SESSION - Avoid trading"