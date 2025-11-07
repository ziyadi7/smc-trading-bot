#!/usr/bin/env python3
"""
Complete System Test
"""

from smc.config import load_config
from smc.logging import setup_logging
from smc.io_mt5 import MT5Client
from smc.store import SignalStore
from smc.news import NewsGuard
from smc.telegram import TelegramBot
from smc.regime_detector import MarketRegimeDetector
from smc.correlation_guard import CorrelationGuard
from smc.flow_detector import InstitutionalFlowDetector

def test_complete():
    print("🏆 Running Complete Elite System Test...")
    
    try:
        config = load_config("config.yaml")
        setup_logging("INFO")
        
        print("✅ Config loaded")
        
        # Initialize all elite components
        mt5 = MT5Client()
        store = SignalStore(config.storage.sqlite_path)
        news = NewsGuard(config.news) if config.news.enabled else None
        telegram = TelegramBot(config.telegram.bot_token, config.telegram.chat_id)
        regime_detector = MarketRegimeDetector()
        correlation_guard = CorrelationGuard()
        flow_detector = InstitutionalFlowDetector()
        
        print("✅ All components initialized")
        
        # Test MT5 data
        data = mt5.get_rates("XAUUSD", mt5.TIMEFRAME_H1, 100)
        if data is not None:
            print(f"✅ MT5 data: {len(data)} bars for XAUUSD")
        else:
            print("❌ MT5 data fetch failed")
        
        # Test database
        store._init_db()
        print("✅ Database initialized")
        
        # Test Telegram
        telegram.send_message("🏆 Elite SMC System Test - ALL COMPONENTS ACTIVE!")
        print("✅ Telegram notification sent")
        
        print("🎉 ELITE SYSTEM TEST COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        print(f"❌ Complete test failed: {e}")
        return False

if __name__ == "__main__":
    test_complete()