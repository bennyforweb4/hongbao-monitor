"""
Telegram 群消息监听 + 关键词触发 Bot 提醒
依赖: pip install telethon aiohttp pyyaml
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
import yaml
from telethon import TelegramClient, events

# ── 加载配置 ──────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.yaml"
DYNAMIC_FILE = BASE_DIR / "dynamic.json"

def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return yaml.safe_load(f)

cfg = load_config()

# ── 日志 ──────────────────────────────────────────────────────────────────────

log_cfg = cfg.get("log", {})
logging.basicConfig(
    level=getattr(logging, log_cfg.get("level", "INFO")),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_cfg.get("file", "messages.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── 动态规则（持久化到 dynamic.json）────────────────────────────────────────

def load_dynamic() -> dict:
    if DYNAMIC_FILE.exists():
        with open(DYNAMIC_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for key, default in [("blocked_words", []), ("watch_groups", [])]:
            if key not in data:
                data[key] = default
        return data
    return {"blocked_words": [], "watch_groups": []}

def save_dynamic(data: dict):
    with open(DYNAMIC_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

dynamic = load_dynamic()

# ── 静态规则解析 ──────────────────────────────────────────────────────────────

def _normalize_ids(items: list):
    usernames, numeric_ids = set(), set()
    for item in items or []:
        s = str(item).strip().lstrip("@").lower()
        if s.lstrip("-").isdigit():
            numeric_ids.add(int(s))
        else:
            usernames.add(s)
    return usernames, numeric_ids

WATCH_USERNAMES, WATCH_GROUP_IDS = _normalize_ids(cfg.get("watch_groups", []))

notify_cfg  = cfg["notify_bot"]
BOT_TOKEN   = notify_cfg["token"]
TARGET_CHAT = notify_cfg["target_chat_id"]   # Bot API 格式，如 -1003940233039
TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# ── 规则匹配 ──────────────────────────────────────────────────────────────────

def all_blocked() -> list:
    return [w.lower() for w in dynamic["blocked_words"]]

def match_hongbao(text: str) -> bool:
    """同时包含「红包」和「/」符号才触发"""
    return "红包" in text and "/" in text

def match_blocked(text: str) -> Optional[str]:
    lower = text.lower()
    for w in all_blocked():
        if w in lower:
            return w
    return None

# ── 群组过滤 ──────────────────────────────────────────────────────────────────

def _chat_id_matches(chat_id: int, target: int) -> bool:
    """Telethon 返回裸正数 ID，Bot API 用 -100XXXXXXX，两种格式都兼容"""
    if chat_id == target:
        return True
    try:
        if int(f"-100{chat_id}") == target:
            return True
        if chat_id == int(str(target).lstrip("-").lstrip("100") or "0"):
            return True
    except ValueError:
        pass
    return False

def dynamic_watch_group_ids() -> set:
    ids = set()
    for item in dynamic["watch_groups"]:
        s = str(item).strip()
        if s.lstrip("-").isdigit():
            ids.add(int(s))
    return ids

def is_watched_chat(chat) -> bool:
    chat_id = getattr(chat, "id", None)
    username = getattr(chat, "username", None)
    all_group_ids = WATCH_GROUP_IDS | dynamic_watch_group_ids()
    if chat_id:
        for gid in all_group_ids:
            if _chat_id_matches(chat_id, gid):
                return True
    if username and username.lower() in WATCH_USERNAMES:
        return True
    return False

def is_notify_chat(chat) -> bool:
    chat_id = getattr(chat, "id", None)
    if chat_id:
        return _chat_id_matches(chat_id, TARGET_CHAT)
    return False

# ── 消息超链接 ────────────────────────────────────────────────────────────────

def message_link(chat, msg_id: int) -> str:
    username = getattr(chat, "username", None)
    chat_id  = getattr(chat, "id", None)
    if username:
        return f"https://t.me/{username}/{msg_id}"
    if chat_id:
        return f"https://t.me/c/{chat_id}/{msg_id}"
    return ""

# ── Bot 通知 ──────────────────────────────────────────────────────────────────

async def send_notification(session: aiohttp.ClientSession, text: str) -> None:
    payload = {
        "chat_id": TARGET_CHAT,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        async with session.post(TELEGRAM_SEND_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error("Bot 发送失败 %s: %s", resp.status, body)
            else:
                logger.info("提醒已发送: %s", text[:60])
    except Exception as e:
        logger.error("Bot 通知异常: %s", e)

async def bot_reply(session: aiohttp.ClientSession, text: str) -> None:
    """向报警群发一条纯文本确认消息"""
    payload = {"chat_id": TARGET_CHAT, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(TELEGRAM_SEND_URL, json=payload,
                                timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                logger.error("Bot 回复失败: %s", await resp.text())
    except Exception as e:
        logger.error("Bot 回复异常: %s", e)

# ── 格式化提醒 ────────────────────────────────────────────────────────────────

def format_alert(trigger: str, group_name: str, sender_name: str,
                 text: str, link: str) -> str:
    ts      = datetime.now().strftime("%H:%M:%S")
    preview = text[:200] + ("…" if len(text) > 200 else "")
    link_part = f'\n<a href="{link}">→ 跳转原消息</a>' if link else ""
    return (
        f"🔔 <b>[{trigger}]</b>\n"
        f"群组: {group_name}\n"
        f"发送人: {sender_name}\n"
        f"时间: {ts}\n"
        f"内容: {preview}"
        f"{link_part}"
    )

# ── 指令处理 ──────────────────────────────────────────────────────────────────

async def handle_command(session: aiohttp.ClientSession, text: str):
    text = text.strip()

    if text.startswith("加屏蔽词 ") or text.startswith("加屏蔽词　"):
        word = text[5:].strip()
        if not word:
            return
        if word.lower() not in [w.lower() for w in dynamic["blocked_words"]]:
            dynamic["blocked_words"].append(word)
            save_dynamic(dynamic)
            await bot_reply(session, f'🚫 已添加屏蔽词：<b>{word}</b>\n当前屏蔽词：{", ".join(dynamic["blocked_words"]) or "（无）"}')
        else:
            await bot_reply(session, f'"{word}" 已存在')

    elif text.startswith("删屏蔽词 ") or text.startswith("删屏蔽词　"):
        word = text[5:].strip()
        before = len(dynamic["blocked_words"])
        dynamic["blocked_words"] = [w for w in dynamic["blocked_words"] if w.lower() != word.lower()]
        if len(dynamic["blocked_words"]) < before:
            save_dynamic(dynamic)
            await bot_reply(session, f'🗑 已删除屏蔽词：<b>{word}</b>')
        else:
            await bot_reply(session, f'未找到屏蔽词："{word}"')

    elif text == "查屏蔽词":
        words = "、".join(dynamic["blocked_words"]) or "（无）"
        await bot_reply(session, f"🚫 <b>屏蔽词列表</b>：{words}")

    elif text.startswith("加监听群 ") or text.startswith("加监听群　"):
        gid = text[5:].strip()
        if not gid.lstrip("-").isdigit():
            await bot_reply(session, "❌ 群组ID格式错误，请输入纯数字（如 -1001234567890）")
            return
        gid_int = int(gid)
        if gid_int not in dynamic["watch_groups"]:
            dynamic["watch_groups"].append(gid_int)
            save_dynamic(dynamic)
            groups_str = "、".join(str(g) for g in dynamic["watch_groups"])
            await bot_reply(session, f'✅ 已添加监听群：<b>{gid_int}</b>\n当前动态监听群：{groups_str}')
        else:
            await bot_reply(session, f'群组 {gid_int} 已在监听列表中')

    elif text.startswith("删监听群 ") or text.startswith("删监听群　"):
        gid = text[5:].strip()
        if not gid.lstrip("-").isdigit():
            await bot_reply(session, "❌ 群组ID格式错误，请输入纯数字")
            return
        gid_int = int(gid)
        before = len(dynamic["watch_groups"])
        dynamic["watch_groups"] = [g for g in dynamic["watch_groups"] if g != gid_int]
        if len(dynamic["watch_groups"]) < before:
            save_dynamic(dynamic)
            await bot_reply(session, f'🗑 已删除监听群：<b>{gid_int}</b>')
        else:
            await bot_reply(session, f'未找到监听群：{gid_int}')

    elif text == "查监听群":
        static_groups = "、".join(str(g) for g in (list(WATCH_USERNAMES) + [str(g) for g in WATCH_GROUP_IDS])) or "（无）"
        dyn_groups = "、".join(str(g) for g in dynamic["watch_groups"]) or "（无）"
        await bot_reply(session, f'📡 <b>监听群列表</b>\n固定：{static_groups}\n动态：{dyn_groups}')

    elif text == "帮助" or text == "help":
        await bot_reply(session,
            "📖 <b>红包监控 Bot 使用说明</b>\n"
            "\n"
            "🔔 <b>报警规则</b>\n"
            "· 监听所有监听群里<b>任意用户</b>的消息（无用户限制）\n"
            "· 消息<b>同时包含「红包」和「/」</b>时才报警\n"
            "· 命中<b>屏蔽词</b>的消息会被跳过\n"
            "\n"
            "🚫 <b>屏蔽词管理</b>\n"
            "加屏蔽词 xx　　添加屏蔽词\n"
            "删屏蔽词 xx　　删除屏蔽词\n"
            "查屏蔽词　　　 查看所有屏蔽词\n"
            "\n"
            "📡 <b>监听群管理</b>\n"
            "加监听群 xx　　添加监听群（纯数字ID，如 -1001234567890）\n"
            "删监听群 xx　　移除监听群\n"
            "查监听群　　　 查看所有监听群\n"
            "\n"
            "ℹ️ 发送「帮助」或「help」可再次查看本说明"
        )

# ── 主逻辑 ────────────────────────────────────────────────────────────────────

async def main():
    client = TelegramClient("session", cfg["api_id"], cfg["api_secret"])

    async with aiohttp.ClientSession() as http_session:

        @client.on(events.NewMessage())
        async def handler(event):
            if not event.message.text:
                return
            text = event.message.text.strip()
            if not text:
                return

            chat = await event.get_chat()

            # ── 报警群指令处理 ────────────────────────────────────────────────
            if is_notify_chat(chat):
                cmd_prefixes = ("加屏蔽词", "删屏蔽词", "查屏蔽词",
                                "加监听群", "删监听群", "查监听群", "帮助", "help")
                if any(text.startswith(p) for p in cmd_prefixes):
                    await handle_command(http_session, text)
                return

            # ── 监控群消息处理 ────────────────────────────────────────────────
            if not is_watched_chat(chat):
                return

            sender = await event.get_sender()
            first  = getattr(sender, "first_name", "") or ""
            last   = getattr(sender, "last_name",  "") or ""
            sender_username = getattr(sender, "username", None)
            sender_name = (first + " " + last).strip() or sender_username or str(getattr(sender, "id", "?"))
            group_name  = getattr(chat, "title", None) or getattr(chat, "username", str(chat.id))

            logger.info("[%s] %s: %s", group_name, sender_name, text[:120])

            if not match_hongbao(text):
                return

            hit_blocked = match_blocked(text)
            if hit_blocked:
                logger.info("命中屏蔽词 [%s]，跳过提醒", hit_blocked)
                return

            link  = message_link(chat, event.message.id)
            alert = format_alert("红包 + /", group_name, sender_name, text, link)
            await send_notification(http_session, alert)

        logger.info("启动监听，按 Ctrl+C 退出")
        await client.start()
        await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("已退出")
