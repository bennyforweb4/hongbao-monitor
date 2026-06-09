"""
辅助工具：登录后打印你所有群组的 ID 和名称，用于填写 config.yaml
用法: python3 get_chat_id.py
"""

import asyncio
import yaml
from pathlib import Path
from telethon import TelegramClient

cfg = yaml.safe_load(open(Path(__file__).parent / "config.yaml", encoding="utf-8"))

async def main():
    client = TelegramClient("session", cfg["api_id"], cfg["api_secret"])
    await client.start()
    print(f"\n{'ID':>20}  {'类型':<8}  名称")
    print("-" * 60)
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        kind = type(entity).__name__
        print(f"{dialog.id:>20}  {kind:<8}  {dialog.name}")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
