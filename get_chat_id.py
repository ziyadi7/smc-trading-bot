#!/usr/bin/env python3
"""
Utility to get Telegram Chat ID
"""

from telegram import Bot
import asyncio
import sys

async def main():
    if len(sys.argv) != 2:
        print("Usage: python get_chat_id.py <YOUR_BOT_TOKEN>")
        sys.exit(1)
    
    bot_token = sys.argv[1]
    
    try:
        bot = Bot(token=bot_token)
        updates = await bot.get_updates()
        
        if updates:
            chat_id = updates[0].message.chat_id
            print(f"✅ Your Chat ID: {chat_id}")
            print(f"📝 Add this to your config.yaml:")
            print(f"telegram:")
            print(f"  chat_id: \"{chat_id}\"")
        else:
            print("❌ No messages found. Please send a message to your bot first, then run this again.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())