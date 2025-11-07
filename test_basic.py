#!/usr/bin/env python3
"""
Basic System Test
"""

from smc.config import load_config
from smc.logging import setup_logging
from smc.telegram import TelegramBot

def test_basic():
    print("🧪 Running Basic System Test...")
    
    try:
        # Load config
        config = load_config("config.yaml")
        setup_logging()
        
        print("✅ Config loaded successfully")
        
        # Test Telegram
        telegram = TelegramBot(config.telegram.bot_token, config.telegram.chat_id)
        success = telegram.send_message("🤖 SMC Bot - Basic Test Successful!")
        
        if success:
            print("✅ Telegram connection successful")
        else:
            print("❌ Telegram connection failed")
        
        print("🎉 Basic system test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Basic test failed: {e}")
        return False

if __name__ == "__main__":
    test_basic()