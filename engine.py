from typing import List, Dict, Optional
import pandas as pd
import datetime as dt
from .logging import get_logger
from .detectors import InstitutionalDetector, OrderBlock, FUCandle, LiquidityZone
from .scoring import EliteScorer
from .charting import ChartGenerator

logger = get_logger(__name__)

class EliteSignalEngine:
    """Institutional-grade signal engine with elite features"""
    
    def __init__(self, config, mt5_client, store, news_guard, telegram_bot, 
                 regime_detector, correlation_guard, flow_detector, session_analyzer):
        self.config = config
        self.mt5 = mt5_client
        self.store = store
        self.news_guard = news_guard
        self.telegram = telegram_bot
        self.regime_detector = regime_detector
        self.correlation_guard = correlation_guard
        self.flow_detector = flow_detector
        self.session_analyzer = session_analyzer
        
        # Core detection systems
        self.detector = InstitutionalDetector(config.trading)
        self.scorer = EliteScorer(config.trading)
        self.chart_generator = ChartGenerator()
        
        # Timeframe mapping
        self.tf_map = {
            "D1": self.mt5.TIMEFRAME_D1,
            "H4": self.mt5.TIMEFRAME_H4, 
            "H1": self.mt5.TIMEFRAME_H1
        }

    def run_cycle(self):
        """Run elite analysis cycle with institutional features"""
        logger.info("🏆 Starting elite analysis cycle")
        
        # Detect market regime first
        regime_info = self._detect_market_regime()
        
        for symbol in self.config.mt5.symbols:
            try:
                self._process_symbol_elite(symbol, regime_info)
                self._check_elite_outcomes(symbol)
            except Exception as e:
                logger.error(f"Symbol {symbol} processing failed: {e}", exc_info=True)
                continue

    def _detect_market_regime(self) -> Dict:
        """Detect current market regime for adaptive trading"""
        try:
            # Get multi-timeframe data for regime detection
            d1_data = self.mt5.get_rates("XAUUSD", self.tf_map["D1"], 200)
            h4_data = self.mt5.get_rates("XAUUSD", self.tf_map["H4"], 200)
            h1_data = self.mt5.get_rates("XAUUSD", self.tf_map["H1"], 200)
            
            if all(data is not None for data in [d1_data, h4_data, h1_data]):
                regime_info = self.regime_detector.detect_regime(d1_data, h4_data, h1_data)
                logger.info(f"📊 Market Regime: {regime_info['type']} (Confidence: {regime_info['confidence']:.2f})")
                return regime_info
            else:
                return {"type": "UNKNOWN", "confidence": 0.5, "adaptive_params": {}}
                
        except Exception as e:
            logger.error(f"Regime detection failed: {e}")
            return {"type": "UNKNOWN", "confidence": 0.5, "adaptive_params": {}}

    def _process_symbol_elite(self, symbol: str, regime_info: Dict):
        """Elite symbol processing with institutional features"""
        logger.debug(f"🔍 Analyzing {symbol} with elite features")
        
        # Fetch multi-timeframe data
        data = self._fetch_elite_data(symbol)
        if not data:
            return

        # Run elite detection
        signals = self._analyze_elite_setup(symbol, data, regime_info)
        
        for signal in signals:
            self._handle_elite_signal(signal, regime_info)

    def _fetch_elite_data(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Fetch elite multi-timeframe data with validation"""
        try:
            data = {}
            for tf_name, tf_code in self.tf_map.items():
                df = self.mt5.get_rates(symbol, tf_code, self.config.mt5.lookback)
                if df is None or len(df) < 100:
                    logger.warning(f"Insufficient data for {symbol} {tf_name}")
                    return {}
                data[tf_name] = df
            
            logger.debug(f"✅ Elite data fetched for {symbol}: {[f'{k}:{len(v)}' for k, v in data.items()]}")
            return data
            
        except Exception as e:
            logger.error(f"Elite data fetch failed for {symbol}: {e}")
            return {}

    def _analyze_elite_setup(self, symbol: str, data: Dict, regime_info: Dict) -> List[Dict]:
        """Analyze elite institutional setup with all features"""
        d1, h4, h1 = data['D1'], data['H4'], data['H1']
        
        try:
            # Find elite order blocks
            h4_blocks = self.detector.find_elite_order_blocks(h4, "H4")
            h1_blocks = self.detector.find_elite_order_blocks(h1, "H1")
            best_ob = self.detector.get_best_order_block(h4_blocks, h1_blocks)
            
            if not best_ob:
                return []

            # Detect FU candle
            fu_candle = self.detector.detect_elite_fu_candle(h1)
            
            # Find liquidity zones
            liquidity_zones = self.detector.find_liquidity_zones(h1, "H1")
            
            # Calculate elite institutional score (1-10)
            elite_score, score_breakdown, score_notes = self.scorer.calculate_elite_score(
                d1, h4, h1, best_ob, fu_candle, liquidity_zones
            )
            
            # Apply regime-based adaptive threshold
            adaptive_threshold = regime_info.get('adaptive_params', {}).get('score_threshold', 
                                                                          self.config.trading.min_elite_score)
            
            if elite_score < adaptive_threshold:
                logger.debug(f"Signal below adaptive threshold: {elite_score} < {adaptive_threshold}")
                return []

            # Build elite signal
            signal = self._build_elite_signal(symbol, best_ob, elite_score, score_breakdown, 
                                            score_notes, h1, fu_candle, liquidity_zones, regime_info)
            
            return [signal]
            
        except Exception as e:
            logger.error(f"Elite setup analysis failed for {symbol}: {e}")
            return []

    def _build_elite_signal(self, symbol: str, ob: OrderBlock, elite_score: int, 
                          score_breakdown: Dict, score_notes: List[str], df: pd.DataFrame,
                          fu_candle: FUCandle, liquidity_zones: List[LiquidityZone],
                          regime_info: Dict) -> Dict:
        """Build elite institutional signal"""
        
        # Calculate elite entry with precision
        entry = (ob.body_high + ob.body_low) / 2
        atr_val = self.detector.atr(df).iloc[-1]
        
        # Elite stop loss with regime adaptation
        if ob.bullish:
            sl = ob.wick_low - (0.25 * atr_val)
        else:
            sl = ob.wick_high + (0.25 * atr_val)
        
        # Adaptive risk management
        risk = abs(entry - sl)
        regime_multiplier = regime_info.get('adaptive_params', {}).get('risk_multiplier', 1.0)
        adaptive_risk_multiplier = min(2.0, max(0.5, regime_multiplier))
        
        # Elite TP targets with regime adaptation
        base_tps = []
        for multi in self.config.trading.risk_r_multiples:
            adjusted_multi = multi * adaptive_risk_multiplier
            if ob.bullish:
                tp = entry + (risk * adjusted_multi)
            else:
                tp = entry - (risk * adjusted_multi)
            base_tps.append(round(tp, 2))
        
        # Enhanced signal with institutional features
        return {
            'symbol': symbol,
            'tf': 'H1',
            'side': 'BUY' if ob.bullish else 'SELL',
            'entry': round(entry, 2),
            'sl': round(sl, 2),
            'tps': base_tps,
            'score': elite_score,
            'score_breakdown': score_breakdown,
            'notes': score_notes,
            'ob': ob,
            'df': df,
            'fu_candle': fu_candle,
            'liquidity_zones': liquidity_zones,
            'risk_usd': round(risk, 2),
            'atr_current': round(atr_val, 2),
            'regime_info': regime_info,
            'quality': self.scorer._get_quality_rating(elite_score),
            'timestamp': dt.datetime.utcnow().isoformat()
        }

    def _handle_elite_signal(self, signal: Dict, regime_info: Dict):
        """Handle elite signal with institutional validation"""
        
        # Deduplication check
        signal_key = self.store.get_signal_key(signal)
        if self.store.is_duplicate_signal(signal_key):
            logger.debug(f"Duplicate elite signal skipped: {signal_key}")
            return

        # News guard check
        if self.news_guard and self.config.news.enabled:
            currencies = self.config.symbol_currencies.get(signal['symbol'], [])
            in_blackout, event, minutes = self.news_guard.is_blackout(currencies)
            if in_blackout:
                logger.info(f"Elite signal muted by news: {signal['symbol']} for {minutes}m")
                return

        # Correlation analysis
        if self.config.trading.correlation_checks:
            correlation_insights = self.correlation_guard.get_correlation_insights(signal, self.mt5)
            signal['correlation_insights'] = correlation_insights
            
            if not correlation_insights['aligned'] and correlation_insights['alignment_score'] < 0.4:
                logger.info(f"Signal rejected due to poor correlation: {correlation_insights['alignment_score']:.2f}")
                return

        # Institutional flow analysis
        flow_analysis = self.flow_detector.get_flow_analysis(signal, signal['df'])
        signal['flow_analysis'] = flow_analysis
        
        if not flow_analysis['aligned'] and flow_analysis['alignment_score'] < 0.4:
            logger.info(f"Signal rejected due to contrary institutional flow: {flow_analysis['alignment_score']:.2f}")
            return

        # Session optimization
        if self.config.trading.session_aware:
            session_recommendation = self.session_analyzer.get_session_recommendation(signal)
            signal['session_analysis'] = session_recommendation
            
            if not session_recommendation['optimal'] and session_recommendation['optimality_score'] < 0.8:
                logger.info(f"Signal in suboptimal session: {session_recommendation['optimality_score']:.2f}")
                # Don't reject, but note for position sizing

        # Adaptive position sizing
        signal = self._apply_adaptive_sizing(signal, regime_info)

        # Persist and send elite signal
        signal_id = self.store.save_signal(signal)
        if signal_id > 0:
            signal['id'] = signal_id
            self._send_elite_notification(signal)
            logger.info(f"🏆 ELITE SIGNAL: {signal['symbol']} {signal['side']} Score: {signal['score']}/10")

    def _apply_adaptive_sizing(self, signal: Dict, regime_info: Dict) -> Dict:
        """Apply adaptive position sizing based on multiple factors"""
        base_risk = self.config.trading.risk_per_trade
        adaptive_params = regime_info.get('adaptive_params', {})
        
        # Regime-based adjustment
        regime_multiplier = adaptive_params.get('position_size_multiplier', 1.0)
        
        # Score-based adjustment (higher score = larger size)
        score_multiplier = signal['score'] / 10.0
        
        # Correlation adjustment
        correlation_score = signal.get('correlation_insights', {}).get('alignment_score', 0.5)
        correlation_multiplier = 0.5 + correlation_score  # 0.5-1.5 range
        
        # Flow adjustment
        flow_score = signal.get('flow_analysis', {}).get('alignment_score', 0.5)
        flow_multiplier = 0.5 + flow_score  # 0.5-1.5 range
        
        # Session adjustment
        session_score = signal.get('session_analysis', {}).get('optimality_score', 1.0)
        session_multiplier = min(1.2, session_score)  # Cap at 1.2
        
        # Calculate final risk
        final_risk = (base_risk * regime_multiplier * score_multiplier * 
                     correlation_multiplier * flow_multiplier * session_multiplier)
        
        # Apply caps
        final_risk = max(0.005, min(0.05, final_risk))  # 0.5% to 5% range
        
        signal['adaptive_risk_percent'] = round(final_risk * 100, 2)
        signal['position_size_multiplier'] = round(final_risk / base_risk, 2)
        
        return signal

    def _send_elite_notification(self, signal: Dict):
        """Send elite institutional notification"""
        try:
            # Generate elite chart
            chart_img = self.chart_generator.create_elite_chart(signal)
            
            # Format elite message
            message = self._format_elite_signal_message(signal)
            
            # Send to Telegram
            success = self.telegram.send_photo(chart_img, message)
            
            if success:
                logger.debug("Elite notification sent successfully")
            else:
                logger.error("Failed to send elite notification")
                
        except Exception as e:
            logger.error(f"Elite notification failed: {e}")

    def _format_elite_signal_message(self, signal: Dict) -> str:
        """Format elite institutional signal message"""
        breakdown = signal.get('score_breakdown', {})
        quality = signal.get('quality', '⚠️')
        regime = signal.get('regime_info', {}).get('type', 'UNKNOWN')
        
        # Score breakdown
        score_details = (
            f"Daily Bias: {breakdown.get('daily_bias', 0):.1f}/1.5 | "
            f"OB Quality: {breakdown.get('ob_quality', 0):.1f}/1.5\n"
            f"Liquidity: {breakdown.get('liquidity', 0):.1f}/1.5 | "
            f"FU Strength: {breakdown.get('fu_strength', 0):.1f}/1.5\n"
            f"Market Structure: {breakdown.get('market_structure', 0):.1f}/1.5 | "
            f"Institutional Zone: {breakdown.get('institutional_zone', 0):.1f}/1.0\n"
            f"Volume: {breakdown.get('volume', 0):.1f}/1.0 | "
            f"Imbalance: {breakdown.get('imbalance', 0):.1f}/0.5"
        )
        
        # Additional insights
        correlation_info = signal.get('correlation_insights', {})
        flow_info = signal.get('flow_analysis', {})
        session_info = signal.get('session_analysis', {})
        
        insights = []
        if correlation_info:
            insights.append(f"📈 Correlation: {correlation_info.get('confidence', 'N/A')}")
        if flow_info:
            insights.append(f"💰 Flow: {flow_info.get('aligned_blocks_count', 0)} blocks")
        if session_info:
            insights.append(f"⏰ Session: {session_info.get('current_session', 'N/A')}")
        
        insights_text = " | ".join(insights) if insights else "No additional insights"
        
        return f"""
{quality} #{signal.get('id', 'NEW')} {signal['symbol']} {signal['tf']} | {signal['side']} 
🏆 ELITE SCORE: {signal['score']}/10 {quality}

🎯 Entry: {signal['entry']} | 🛡️ SL: {signal['sl']} | 📊 Risk: ${signal['risk_usd']}
🎯 TP1: {signal['tps'][0]} | TP2: {signal['tps'][1]} | TP3: {signal['tps'][2]}

{score_details}

📈 Regime: {regime}
💼 Position Size: {signal.get('adaptive_risk_percent', 2.0)}% (Multiplier: {signal.get('position_size_multiplier', 1.0)}x)

{insights_text}

📝 Notes: {', '.join(signal['notes'][:3]) if signal['notes'] else 'Institutional setup confirmed'}

⚡ ATR: {signal['atr_current']} | 📊 OB TF: {signal['ob'].tf}
"""

    def _check_elite_outcomes(self, symbol: str):
        """Check outcomes for elite signals with precision"""
        try:
            open_signals = self.store.get_open_signals(symbol)
            if not open_signals:
                return

            # Get precise market data
            latest_data = self.mt5.get_rates(symbol, self.tf_map["H1"], 10)
            if latest_data is None:
                return

            latest_high = latest_data['h'].iloc[-1]
            latest_low = latest_data['l'].iloc[-1]
            latest_close = latest_data['c'].iloc[-1]

            for signal in open_signals:
                self._evaluate_elite_outcome(signal, latest_high, latest_low, latest_close)
                
        except Exception as e:
            logger.error(f"Elite outcome check failed for {symbol}: {e}")

    def _evaluate_elite_outcome(self, signal: Dict, high: float, low: float, close: float):
        """Evaluate outcomes with elite precision"""
        is_buy = signal['side'] == 'BUY'
        
        # Check SL with candle close confirmation
        sl_hit = (is_buy and low <= signal['sl'] and close <= signal['sl']) or \
                 (not is_buy and high >= signal['sl'] and close >= signal['sl'])
        
        if sl_hit:
            self.store.record_outcome(signal['id'], 'SL', signal['sl'])
            logger.info(f"🛑 SL hit for elite signal {signal['id']}")
            return

        # Check TP levels with precision
        for i, tp_level in enumerate([signal['tp1'], signal['tp2'], signal['tp3']], 1):
            tp_hit = (is_buy and high >= tp_level) or (not is_buy and low <= tp_level)
            
            if tp_hit and not self.store.is_tp_recorded(signal['id'], i):
                self.store.record_outcome(signal['id'], f'TP{i}', tp_level)
                logger.info(f"🎯 TP{i} hit for elite signal {signal['id']}")

# Backward compatibility
SignalEngine = EliteSignalEngine