#!/usr/bin/env python3
"""
SMC Institutional Trading Bot v5.0
Elite Order Flow & Liquidity-Based Strategy for XAUUSD
"""

import time
import sys
from smc.config import load_config
from smc.logging import setup_logging
from smc.io_mt5 import MT5Client
from smc.engine import SignalEngine
from smc.store import SignalStore
from smc.news import NewsGuard
from smc.telegram import TelegramBot
from smc.regime_detector import MarketRegimeDetector
from smc.correlation_guard import CorrelationGuard
from smc.flow_detector import InstitutionalFlowDetector

def main():
    """Main entry point with elite institutional features"""
    print("🏆 SMC Institutional Bot v5.0 Starting...")
    
    # Fail fast on config errors
    try:
        config = load_config("config.yaml")
    except Exception as e:
        print(f"❌ CRITICAL: Config validation failed: {e}")
        sys.exit(1)
    
    setup_logging("INFO")
    
    # Initialize elite components
    try:
        print("🔧 Initializing elite components...")
        mt5 = MT5Client()
        store = SignalStore(config.storage.sqlite_path)
        news = NewsGuard(config.news) if config.news.enabled else None
        telegram = TelegramBot(config.telegram.bot_token, config.telegram.chat_id)
        regime_detector = MarketRegimeDetector()
        correlation_guard = CorrelationGuard()
        flow_detector = InstitutionalFlowDetector()
        
        # Elite engine with all institutional features
        engine = SignalEngine(
            config=config,
            mt5_client=mt5,
            store=store,
            news_guard=news,
            telegram_bot=telegram,
            regime_detector=regime_detector,
            correlation_guard=correlation_guard,
            flow_detector=flow_detector
        )
        
        # Startup message
        startup_msg = f"""
🏆 SMC GOLD BOT v5.0 - INSTITUTIONAL MODE 🚀

✅ Elite Components Active:
• Market Regime Detection
• Correlation Analysis  
• Institutional Flow Tracking
• Multi-Timeframe Liquidity Analysis
• News Guard Protection
• Advanced Risk Management

🎯 Trading: {config.mt5.symbols}
📊 Min Score: {config.trading.min_elite_score}/10
⏰ Polling: {config.mt5.poll_seconds}s
        """
        telegram.send_message(startup_msg)
        print("✅ Elite SMC Bot Started Successfully!")
        
        # Main trading loop
        cycle_count = 0
        while True:
            try:
                engine.run_cycle()
                cycle_count += 1
                
                # Status update every 50 cycles
                if cycle_count % 50 == 0:
                    print(f"♻️  Cycle {cycle_count} completed - Bot running smoothly")
                    
            except Exception as e:
                print(f"⚠️  Cycle error: {e}")
                time.sleep(30)  # Shorter recovery for elite bot
                
            time.sleep(config.mt5.poll_seconds)
            
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
        telegram.send_message("🛑 SMC Bot Stopped by User")
    except Exception as e:
        error_msg = f"💥 CRITICAL: Bot failed: {e}"
        print(error_msg)
        telegram.send_message(error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()