#!/usr/bin/env python3
"""
Test MT5 Connection
"""

import MetaTrader5 as mt5

def test_mt5_connection():
    print("🔧 Testing MT5 Connection...")
    
    try:
        if mt5.initialize():
            print("✅ MT5 Connected Successfully!")
            
            # Account info
            account_info = mt5.account_info()
            if account_info:
                print(f"   Account: {account_info.login}")
                print(f"   Balance: ${account_info.balance:.2f}")
                print(f"   Server: {account_info.server}")
            
            # Terminal info
            terminal_info = mt5.terminal_info()
            if terminal_info:
                print(f"   Version: {terminal_info.version}")
                print(f"   Build: {terminal_info.build}")
            
            # Available symbols
            symbols = mt5.symbols_get()
            print(f"   Available Symbols: {len(symbols)}")
            
            mt5.shutdown()
            return True
            
        else:
            error = mt5.last_error()
            print(f"❌ MT5 Connection Failed: {error}")
            return False
            
    except Exception as e:
        print(f"❌ MT5 Test Failed: {e}")
        return False

if __name__ == "__main__":
    test_mt5_connection()