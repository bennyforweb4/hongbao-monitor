#!/usr/bin/env python3
"""红包监控 Web 控制台"""

import asyncio
import json
import logging
import re
import sys
import threading
import time
import webbrowser
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import secrets

import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.errors import PhoneCodeInvalidError, SessionPasswordNeededError
from telethon.tl.types import Channel, Chat

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
SESSION_FILE = str(BASE_DIR / "session")

# ── Config ─────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "api_id": "",
    "api_secret": "",
    "watch_groups": [],
    "trigger_keywords": ["红包", "/"],
    "blocked_words": ["/1"],
    "button_texts": [],
    "min_avg": {},          # e.g. {"USDT": 1.0, "CNY": 7.0}
    "click_delay": 0,       # seconds to wait before clicking button (0 = immediate)
    "notify_bot": {"token": "", "target_chat_id": ""},
    "web_username": "admin",
    "web_password": "admin123",
}


def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            if k not in data:
                data[k] = v
        return data
    # Migrate from old config.yaml
    old = BASE_DIR / "config.yaml"
    if old.exists():
        try:
            import yaml
            c = yaml.safe_load(open(old, encoding="utf-8"))
            cfg = DEFAULT_CONFIG.copy()
            cfg["api_id"] = str(c.get("api_id", ""))
            cfg["api_secret"] = str(c.get("api_secret", ""))
            cfg["watch_groups"] = [str(g) for g in c.get("watch_groups", [])]
            nb = c.get("notify_bot", {})
            cfg["notify_bot"]["token"] = nb.get("token", "")
            cfg["notify_bot"]["target_chat_id"] = str(nb.get("target_chat_id", ""))
            save_config(cfg)
            return cfg
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()


def save_config(data: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── Logging ────────────────────────────────────────────────────────────────────

LOG_BUFFER: deque = deque(maxlen=300)
log_subscribers: List[asyncio.Queue] = []

# ── Records ────────────────────────────────────────────────────────────────────

RECORDS_FILE = BASE_DIR / "records.json"
GRAB_RECORDS: deque = deque(maxlen=500)
record_subscribers: List[asyncio.Queue] = []


def _load_records() -> deque:
    try:
        if RECORDS_FILE.exists():
            data = json.loads(RECORDS_FILE.read_text(encoding="utf-8"))
            return deque(data, maxlen=500)
    except Exception:
        pass
    return deque(maxlen=500)


def _save_records():
    try:
        RECORDS_FILE.write_text(
            json.dumps(list(GRAB_RECORDS), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.error("保存记录失败: %s", e)


def _add_record(rec: dict):
    GRAB_RECORDS.appendleft(rec)
    _save_records()
    for q in list(record_subscribers):
        try:
            q.put_nowait(rec)
        except Exception:
            pass


# ── Full hongbao detail log ────────────────────────────────────────────────────

HONGBAO_LOG = BASE_DIR / "hongbao.log"
_hb_logger = logging.getLogger("hongbao")
_hb_logger.setLevel(logging.DEBUG)
_hb_logger.propagate = False
_hb_fh = logging.FileHandler(str(HONGBAO_LOG), encoding="utf-8")
_hb_fh.setFormatter(logging.Formatter("%(message)s"))
_hb_logger.addHandler(_hb_fh)


def _log_hongbao(group: str, sender: str, msg_id: int, text: str, raw_buttons):
    """Write a full structured record of every triggered red envelope."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "=" * 60,
        f"[{ts}] 群组: {group}  发送人: {sender}  msg_id: {msg_id}",
        "── 消息全文 ──",
        text,
        "── 按钮 ──",
    ]
    if raw_buttons is None:
        lines.append("（无内联按钮）")
    else:
        for r_idx, row in enumerate(raw_buttons):
            for b_idx, btn in enumerate(row):
                url  = getattr(btn, 'url', None)
                data = getattr(btn, 'data', None)
                kind = "URL" if url else ("Callback" if data else "Unknown")
                detail = url or (data.hex() if data else "")
                lines.append(f"  [{r_idx},{b_idx}] [{kind}] {btn.text!r}  {detail}")
    _hb_logger.info("\n".join(lines))


GRAB_RECORDS = _load_records()


class WsLogHandler(logging.Handler):
    def emit(self, record):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        LOG_BUFFER.append(entry)
        for q in list(log_subscribers):
            try:
                q.put_nowait(entry)
            except Exception:
                pass


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(BASE_DIR / "run.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
        WsLogHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── App state ──────────────────────────────────────────────────────────────────

app = FastAPI()
_web_sessions: set = set()   # active web UI session tokens

_WEB_AUTH_BYPASS = {"/", "/api/web/login", "/api/web/check"}

@app.middleware("http")
async def web_auth_middleware(request: Request, call_next):
    if request.url.path in _WEB_AUTH_BYPASS:
        return await call_next(request)
    token = (request.headers.get("X-Token") or
             request.query_params.get("token") or "")
    if token not in _web_sessions:
        return JSONResponse({"detail": "未授权"}, status_code=401)
    return await call_next(request)
_client: Optional[TelegramClient] = None
_monitor_task: Optional[asyncio.Task] = None
_auth = {"phone": None, "phone_code_hash": None, "awaiting_2fa": False}


# ── Telegram client ────────────────────────────────────────────────────────────

async def get_client() -> TelegramClient:
    global _client
    cfg = load_config()
    if not cfg.get("api_id") or not cfg.get("api_secret"):
        raise HTTPException(400, "请先配置 API ID 和 API Hash")
    if _client is None:
        _client = TelegramClient(SESSION_FILE, int(cfg["api_id"]), cfg["api_secret"])
    if not _client.is_connected():
        await _client.connect()
    return _client


async def is_authed() -> bool:
    try:
        c = await get_client()
        return await c.is_user_authorized()
    except (Exception, asyncio.CancelledError):
        return False


# ── Helpers ────────────────────────────────────────────────────────────────────

_ZW_RE = re.compile(r"[​-‏⁠﻿]")

def strip_zw(s: str) -> str:
    """Remove zero-width / invisible Unicode characters before text comparison."""
    return _ZW_RE.sub("", s)

# Matches: 总金额:8.88 USDT  /  总金额: 12888 CNY  etc.
_AMT_RE = re.compile(r"总金额[：:]\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)", re.IGNORECASE)
# Matches: 剩余2/2  /  剩余:2/2  /  剩余: 0/168  etc. — captures the denominator (total envelopes)
_CNT_RE = re.compile(r"剩余[：:]?\s*\d+\s*/\s*(\d+)")

# Circled/enclosed digit → ASCII digit mapping
# Covers ①-⑩ (U+2460), ❶-❿ (U+2776), ➀-➉ (U+2780), ➊-➓ (U+278A)
_CIRCLED_DIGIT_MAP: dict = {}
for _i, _c in enumerate('①②③④⑤⑥⑦⑧⑨⑩', 1):
    _CIRCLED_DIGIT_MAP[_c] = str(_i)
for _i, _c in enumerate('❶❷❸❹❺❻❼❽❾❿', 1):
    _CIRCLED_DIGIT_MAP[_c] = str(_i)
for _i, _c in enumerate('➀➁➂➃➄➅➆➇➈➉', 1):
    _CIRCLED_DIGIT_MAP[_c] = str(_i)
for _i, _c in enumerate('➊➋➌➍➎➏➐➑➒➓', 1):
    _CIRCLED_DIGIT_MAP[_c] = str(_i)
# Fullwidth digits ０-９ → 0-9
for _i, _c in enumerate('０１２３４５６７８９'):
    _CIRCLED_DIGIT_MAP[_c] = str(_i)

def _normalize_digits(text: str) -> str:
    """Replace circled/fullwidth digit chars with ASCII equivalents."""
    return ''.join(_CIRCLED_DIGIT_MAP.get(c, c) for c in text)


# Math quiz: "2 + 10 = ?" / "３×４＝？" / "❶ + ❽ = ？" etc.
_QUIZ_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*([+\-×÷*/＋－×÷])\s*(\d+(?:\.\d+)?)\s*[=＝]\s*[\?？]',
    re.UNICODE,
)
_QUIZ_OPS = {
    '+': lambda a, b: a + b, '＋': lambda a, b: a + b,
    '-': lambda a, b: a - b, '－': lambda a, b: a - b,
    '×': lambda a, b: a * b, '*': lambda a, b: a * b,
    '÷': lambda a, b: (a / b if b else None), '/': lambda a, b: (a / b if b else None),
}

def solve_quiz(text: str) -> Optional[str]:
    text = _normalize_digits(text)
    m = _QUIZ_RE.search(text)
    if not m:
        return None
    try:
        a, op_sym, b = float(m.group(1)), m.group(2), float(m.group(3))
        fn = _QUIZ_OPS.get(op_sym)
        if fn is None:
            return None
        result = fn(a, b)
        if result is None:
            return None
        return str(int(result)) if result == int(result) else f"{result:.6g}"
    except Exception:
        return None


# Letter captcha: buttons are 2-uppercase-letter combos; answer is in message text
_LETTER_BTN_RE = re.compile(r'^[A-Z]{2}$')

def find_letter_captcha(text: str, btn_texts: list) -> Optional[str]:
    """Find which 2-letter button appears in the message text."""
    candidates = [b for b in btn_texts if _LETTER_BTN_RE.match(b)]
    if not candidates:
        return None
    # Direct match: the exact combo appears somewhere in the message
    for c in candidates:
        if c in text:
            return c
    # Fallback: find any standalone 2-uppercase-letter sequence in text
    found = re.findall(r'\b([A-Z]{2})\b', text)
    for f in found:
        if f in candidates:
            return f
    return None

def parse_hongbao_avg(text: str) -> Optional[tuple]:
    """Return (avg_per_envelope, currency_upper) or None if not parseable."""
    ma = _AMT_RE.search(text)
    mc = _CNT_RE.search(text)
    if not (ma and mc):
        return None
    total = float(ma.group(1))
    currency = ma.group(2).upper()
    count = int(mc.group(1))
    if count == 0:
        return None
    return total / count, currency


# Pattern for Telegram bot deep-link buttons: https://t.me/BOT?start=PARAM
_TG_START_RE = re.compile(r'https://t\.me/(\w+)\?start=(.+)', re.IGNORECASE)


async def claim_via_url_bot(client, btn_url: str, group_name: str,
                             sender_name: str, text_preview: str) -> tuple:
    """
    Handle a URL button that deep-links into a bot for claiming.
    Flow: send /start PARAM → bot sends quiz → solve → click answer button
          (or send text answer if no buttons).
    Returns (popup_text, error_string). One of them will be None.
    """
    m = _TG_START_RE.match(btn_url)
    if not m:
        return None, f"URL格式不识别: {btn_url[:60]}"
    bot_username = m.group(1)
    start_param = m.group(2)
    logger.info("🤖 跳转私聊 @%s，start=%s", bot_username, start_param)
    try:
        async with client.conversation(bot_username, timeout=30, exclusive=False) as conv:
            await conv.send_message(f'/start {start_param}')
            resp = await conv.get_response()
            resp_text = resp.text or ""
            resp_btns = resp.buttons or []
            all_btn_texts = [b.text for row in resp_btns for b in row]
            logger.info("🤖 Bot回复: %s | 按钮: %s", resp_text[:120], all_btn_texts)

            quiz_ans = solve_quiz(resp_text)
            target_btn = None

            # Math quiz matched to a button
            if quiz_ans:
                for row in resp_btns:
                    for btn in row:
                        if _normalize_digits(strip_zw(btn.text).strip()) == quiz_ans:
                            target_btn = btn
                            break
                    if target_btn:
                        break

            # Letter captcha
            if target_btn is None:
                letter = find_letter_captcha(resp_text, all_btn_texts)
                if letter:
                    for row in resp_btns:
                        for btn in row:
                            if btn.text == letter:
                                target_btn = btn
                                break
                        if target_btn:
                            break

            if target_btn:
                logger.info("👆 点击Bot答题按钮: %s", target_btn.text)
                cb = await target_btn.click()
                popup = getattr(cb, 'message', '') or ''
                logger.info("✅ Bot答题完成: %s", popup)
                return popup, None

            if quiz_ans:
                # No matching button — send text answer directly
                logger.info("💬 发送文字答案到Bot: %s", quiz_ans)
                await conv.send_message(quiz_ans)
                try:
                    confirm = await asyncio.wait_for(conv.get_response(), timeout=15)
                    popup = confirm.text or ""
                    logger.info("✅ Bot文字答题完成: %s", popup[:80])
                    return popup, None
                except asyncio.TimeoutError:
                    return None, "等待Bot确认超时"

            logger.warning("⚠️ 无法识别Bot题目，完整消息:\n%s", resp_text)
            return None, f"无法识别题目: {resp_text[:80]}"

    except asyncio.TimeoutError:
        return None, "Bot超时无响应(30s)"
    except Exception as e:
        logger.error("claim_via_url_bot 异常: %s", e)
        return None, str(e)


# ── Chat matching ──────────────────────────────────────────────────────────────

def _chat_matches(chat_id: int, target: int) -> bool:
    if chat_id == target:
        return True
    try:
        # Telethon bare ID vs Bot API -100XXXX format
        if int(f"-100{chat_id}") == target:
            return True
        s = str(target).lstrip("-")
        if s.startswith("100"):
            s = s[3:]
        if s and chat_id == int(s):
            return True
    except (ValueError, TypeError):
        pass
    return False


def is_watched(chat, cfg: dict) -> bool:
    chat_id = getattr(chat, "id", None)
    username = getattr(chat, "username", None)
    for item in cfg.get("watch_groups", []):
        s = str(item).strip().lstrip("@")
        if s.lstrip("-").isdigit():
            if chat_id and _chat_matches(chat_id, int(s)):
                return True
        elif username and username.lower() == s.lower():
            return True
    return False


# ── Notification ───────────────────────────────────────────────────────────────

async def send_notify(cfg: dict, chat, msg_id: int, group: str, sender: str, text: str):
    nb = cfg.get("notify_bot", {})
    token = nb.get("token", "")
    target = nb.get("target_chat_id", "")
    admin_id = str(nb.get("admin_id", "") or "").strip()
    if not token:
        logger.warning("未配置通知 Bot，跳过")
        return
    # Send to admin_id if set, otherwise fall back to target_chat_id
    targets = []
    if admin_id:
        targets.append(admin_id)
    if target and target != admin_id:
        targets.append(target)
    if not targets:
        logger.warning("未配置通知目标，跳过")
        return
    ts = datetime.now().strftime("%H:%M:%S")
    preview = text[:200] + ("…" if len(text) > 200 else "")
    username = getattr(chat, "username", None)
    cid = getattr(chat, "id", None)
    link = f"https://t.me/{username}/{msg_id}" if username else (f"https://t.me/c/{cid}/{msg_id}" if cid else "")
    body = (
        f"🔔 <b>红包提醒</b>\n群组: {group}\n发送人: {sender}\n时间: {ts}\n内容: {preview}"
        + (f'\n<a href="{link}">→ 跳转</a>' if link else "")
    )
    for tgt in targets:
        try:
            await bot_send(token, tgt, body)
            logger.info("通知已发送 → %s", tgt)
        except Exception as e:
            logger.error("通知异常: %s", e)


# ── Bot command handler ────────────────────────────────────────────────────────

_bot_poll_task: Optional[asyncio.Task] = None
_bot_update_offset: int = 0


async def bot_send(token: str, chat_id, text: str, parse_mode: str = "HTML") -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode,
                "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                return (await r.json()).get("ok", False)
    except Exception as e:
        logger.error("bot_send 失败: %s", e)
        return False


def _stats_text() -> str:
    from datetime import date, timedelta
    records = list(GRAB_RECORDS)
    today = datetime.now().date()

    def count(start: date, end: date) -> int:
        n = 0
        for r in records:
            try:
                d = datetime.fromisoformat(r["time"]).date()
                if start <= d <= end:
                    n += 1
            except Exception:
                pass
        return n

    if today.month == 1:
        lm_start = date(today.year - 1, 12, 1)
        lm_end   = date(today.year, 1, 1) - timedelta(days=1)
    else:
        lm_start = date(today.year, today.month - 1, 1)
        lm_end   = date(today.year, today.month, 1) - timedelta(days=1)

    this_m_start = date(today.year, today.month, 1)

    lines = [
        "📊 <b>抢红包统计</b>",
        f"今日：<b>{count(today, today)}</b> 个",
        f"近 7 天：<b>{count(today - timedelta(days=6), today)}</b> 个",
        f"近 30 天：<b>{count(today - timedelta(days=29), today)}</b> 个",
        f"本月（{today.strftime('%Y-%m')}）：<b>{count(this_m_start, today)}</b> 个",
        f"上月（{lm_start.strftime('%Y-%m')}）：<b>{count(lm_start, lm_end)}</b> 个",
        f"累计记录：<b>{len(records)}</b> 个",
    ]
    return "\n".join(lines)


_BOT_HELP = (
    "📖 <b>可用命令</b>\n"
    "/status 或 /状态 — 监听运行状态\n"
    "/start 或 /开始 — 启动监听\n"
    "/stop 或 /停止 — 停止监听\n"
    "/stats 或 /统计 — 抢红包数据分析\n"
    "/records 或 /记录 — 最近10条抢包记录\n"
    "/groups 或 /群列表 — 查看监听群\n"
    "/add_group @xxx — 添加监听群\n"
    "/del_group @xxx — 删除监听群\n"
    "/blocks 或 /屏蔽列表 — 查看屏蔽词\n"
    "/add_block 词 — 添加屏蔽词\n"
    "/del_block 词 — 删除屏蔽词\n"
    "/delay 最少 最多 — 设置随机延迟（秒），如 /delay 1 3\n"
    "/help 或 /帮助 — 显示本帮助"
)


async def handle_bot_command(token: str, admin_id: int, text: str):
    global _monitor_task
    text = text.strip()
    cmd = text.split()[0].lower().lstrip("/")
    arg = text[len(text.split()[0]):].strip() if len(text.split()) > 1 else ""

    async def reply(msg: str):
        await bot_send(token, admin_id, msg)

    if cmd in ("status", "状态"):
        running = _monitor_task is not None and not _monitor_task.done()
        authed = await is_authed()
        cfg = load_config()
        groups = cfg.get("watch_groups", [])
        lines = [
            f"{'✅ 监听运行中' if running else '⏹ 监听未运行'}",
            f"Telegram 登录: {'✅' if authed else '❌'}",
            f"监听群数量: {len(groups)}",
            f"延迟: {cfg.get('click_delay_min', 0)}~{cfg.get('click_delay_max', 0)} 秒",
        ]
        await reply("\n".join(lines))

    elif cmd in ("start", "开始"):
        if _monitor_task and not _monitor_task.done():
            await reply("✅ 监听已在运行中")
        elif not await is_authed():
            await reply("❌ Telegram 未登录，请先在 Web 控制台完成登录")
        else:
            _monitor_task = asyncio.create_task(run_monitor())
            await reply("✅ 监听已启动")

    elif cmd in ("stop", "停止"):
        if _monitor_task and not _monitor_task.done():
            _monitor_task.cancel()
            try:
                await _monitor_task
            except asyncio.CancelledError:
                pass
            _monitor_task = None
            await reply("⏹ 监听已停止")
        else:
            await reply("监听未在运行")

    elif cmd in ("stats", "统计"):
        await reply(_stats_text())

    elif cmd in ("records", "记录"):
        recs = list(GRAB_RECORDS)[:10]
        if not recs:
            await reply("暂无抢包记录")
            return
        lines = ["📋 <b>最近抢包记录</b>"]
        for r in recs:
            t = (r.get("time") or "")[:19].replace("T", " ")
            btn = r.get("button", "?")
            res = (r.get("result") or "")[:40]
            ok = "✅" if not r.get("error") and res else "❌" if r.get("error") else "⚠️"
            lines.append(f"{ok} {t}  <code>{btn}</code>  {res}")
        await reply("\n".join(lines))

    elif cmd in ("groups", "群列表"):
        cfg = load_config()
        groups = cfg.get("watch_groups", [])
        if groups:
            await reply("📡 监听群：\n" + "\n".join(f"  • {g}" for g in groups))
        else:
            await reply("暂无监听群")

    elif cmd == "add_group" or text.startswith("/加群"):
        if not arg:
            await reply("用法: /add_group @username 或 群ID"); return
        cfg = load_config()
        if arg not in cfg.setdefault("watch_groups", []):
            cfg["watch_groups"].append(arg)
            save_config(cfg)
            await reply(f"✅ 已添加监听群: {arg}")
        else:
            await reply(f"该群已在监听列表中")

    elif cmd == "del_group" or text.startswith("/删群"):
        if not arg:
            await reply("用法: /del_group @username 或 群ID"); return
        cfg = load_config()
        groups = cfg.get("watch_groups", [])
        if arg in groups:
            groups.remove(arg)
            save_config(cfg)
            await reply(f"✅ 已删除监听群: {arg}")
        else:
            await reply(f"未找到: {arg}")

    elif cmd in ("blocks", "屏蔽列表"):
        cfg = load_config()
        words = cfg.get("blocked_words", [])
        if words:
            await reply("🚫 屏蔽词：\n" + "\n".join(f"  • {w}" for w in words))
        else:
            await reply("暂无屏蔽词")

    elif cmd == "add_block" or text.startswith("/加屏蔽"):
        if not arg:
            await reply("用法: /add_block 关键词"); return
        cfg = load_config()
        if arg not in cfg.setdefault("blocked_words", []):
            cfg["blocked_words"].append(arg)
            save_config(cfg)
            await reply(f"✅ 已添加屏蔽词: {arg}")
        else:
            await reply("该词已在屏蔽列表中")

    elif cmd == "del_block" or text.startswith("/删屏蔽"):
        if not arg:
            await reply("用法: /del_block 关键词"); return
        cfg = load_config()
        words = cfg.get("blocked_words", [])
        if arg in words:
            words.remove(arg)
            save_config(cfg)
            await reply(f"✅ 已删除屏蔽词: {arg}")
        else:
            await reply(f"未找到: {arg}")

    elif cmd == "delay":
        parts = arg.split()
        if len(parts) < 2:
            await reply("用法: /delay 最少秒 最多秒  (如 /delay 1 3)"); return
        try:
            dmin = max(0.0, float(parts[0]))
            dmax = max(dmin, float(parts[1]))
        except ValueError:
            await reply("参数格式错误，请输入数字"); return
        cfg = load_config()
        cfg["click_delay_min"] = dmin
        cfg["click_delay_max"] = dmax
        save_config(cfg)
        await reply(f"✅ 随机延迟已设为 {dmin}~{dmax} 秒")

    elif cmd in ("help", "帮助"):
        await reply(_BOT_HELP)

    else:
        await reply(f"未知命令: /{cmd}\n\n{_BOT_HELP}")


async def run_bot_poll():
    global _bot_update_offset
    logger.info("🤖 Bot 命令轮询已启动")
    while True:
        try:
            cfg = load_config()
            nb = cfg.get("notify_bot", {})
            token = nb.get("token", "")
            admin_id_str = str(nb.get("admin_id", "") or "").strip()
            if not token or not admin_id_str:
                await asyncio.sleep(15)
                continue
            admin_id = int(admin_id_str)

            url = f"https://api.telegram.org/bot{token}/getUpdates"
            params = {
                "timeout": 30,
                "offset": _bot_update_offset,
                "allowed_updates": ["message"],
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params,
                                  timeout=aiohttp.ClientTimeout(total=40)) as r:
                    data = await r.json()

            if not data.get("ok"):
                logger.warning("getUpdates 返回非 ok: %s", data)
                await asyncio.sleep(5)
                continue

            for upd in data.get("result", []):
                _bot_update_offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("edited_message")
                if not msg:
                    continue
                chat_id = msg.get("chat", {}).get("id")
                if chat_id != admin_id:
                    continue  # Ignore non-admin messages silently
                text = msg.get("text", "")
                if not text:
                    continue
                logger.info("🤖 Bot 收到管理员命令: %s", text[:80])
                try:
                    await handle_bot_command(token, admin_id, text)
                except Exception as e:
                    logger.error("Bot 命令处理异常: %s", e, exc_info=True)

        except asyncio.CancelledError:
            logger.info("🤖 Bot 轮询已停止")
            raise
        except Exception as e:
            logger.error("Bot 轮询异常: %s", e)
            await asyncio.sleep(5)


# ── Unlock polling ─────────────────────────────────────────────────────────────

def _is_grab_btn(text: str, keywords: list) -> bool:
    clean = strip_zw(text)
    return any(kw and kw in clean for kw in keywords)

async def poll_unlock(client, chat_id: int, msg_id: int,
                      group_name: str, sender_name: str, text_preview: str,
                      keywords: list):
    """
    After clicking an unlock button, poll the message until the real grab
    button appears, then click it.
    Strategy: every 1 s for the first minute, every 10 s after that,
    give up at 30 minutes.
    """
    start = asyncio.get_event_loop().time()
    attempt = 0
    logger.info("🔄 解锁轮询开始: 群[%s] msg=%d", group_name, msg_id)

    while True:
        elapsed = asyncio.get_event_loop().time() - start
        if elapsed > 1800:
            logger.info("⏰ 解锁超时放弃(30分钟): msg=%d", msg_id)
            return

        interval = 1 if elapsed < 60 else 10
        await asyncio.sleep(interval)
        attempt += 1

        try:
            msg = await client.get_messages(chat_id, ids=msg_id)
            if not msg or not msg.buttons:
                continue

            poll_btn_info = []
            for row in msg.buttons:
                for btn in row:
                    url = getattr(btn, 'url', None)
                    poll_btn_info.append(f"{btn.text}{'→'+url if url else ''}")
            if attempt == 1:
                logger.info("🔄 轮询[%d]按钮: %s", attempt, poll_btn_info)

            new_text = msg.text or ""
            quiz_answer = solve_quiz(new_text)
            target_btn = None

            # Math quiz check
            if quiz_answer:
                for row in msg.buttons:
                    for btn in row:
                        if _normalize_digits(strip_zw(btn.text).strip()) == quiz_answer:
                            target_btn = btn
                            break
                    if target_btn:
                        break

            # Keyword grab check
            if target_btn is None:
                cfg = load_config()
                kws = cfg.get("button_texts") or keywords
                for row in msg.buttons:
                    for btn in row:
                        if _is_grab_btn(btn.text, kws):
                            target_btn = btn
                            break
                    if target_btn:
                        break

            if target_btn is None:
                continue

            logger.info("🔓 按钮已变化 → 点击: %s (第%d次轮询, %.0fs后)", target_btn.text, attempt, elapsed)
            popup, click_error = "", False
            try:
                cb = await target_btn.click()
                popup = getattr(cb, 'message', '') or ''
                logger.info("✅ 解锁后点击完成: %s", popup)
            except Exception as e:
                popup = str(e)
                click_error = True
                logger.error("解锁后点击失败: %s", e)

            _add_record({
                "time": datetime.now().isoformat(),
                "group": group_name,
                "sender": sender_name,
                "button": target_btn.text,
                "quiz_answer": quiz_answer,
                "result": popup,
                "error": click_error,
                "text_preview": text_preview,
                "unlock_attempts": attempt,
                "unlock_elapsed": round(elapsed),
            })
            return

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("解锁轮询异常: %s", e)


# ── Monitor ────────────────────────────────────────────────────────────────────

async def run_monitor():
    c = await get_client()

    async def on_message(event):
        try:
            if not event.message or not event.message.text:
                return
            text = event.message.text.strip()
            if not text:
                return

            cfg = load_config()
            # get_chat() may call iter_dialogs() if entity not cached, which can
            # fail during Telegram flood waits — use cached event.chat first.
            chat = event.chat
            if chat is None:
                try:
                    chat = await event.get_chat()
                except Exception:
                    return
            if not is_watched(chat, cfg):
                return

            sender = await event.get_sender()

            # Skip messages from our own notify bot (prevents notification loop)
            nb_token = cfg.get("notify_bot", {}).get("token", "")
            if nb_token and ":" in nb_token:
                try:
                    nb_bot_id = int(nb_token.split(":")[0])
                    if getattr(sender, "id", None) == nb_bot_id:
                        return
                except (ValueError, TypeError):
                    pass

            first = getattr(sender, "first_name", "") or ""
            last = getattr(sender, "last_name", "") or ""
            sender_name = (first + " " + last).strip() or getattr(sender, "username", None) or str(getattr(sender, "id", "?"))
            group_name = getattr(chat, "title", None) or getattr(chat, "username", None) or str(getattr(chat, "id", "?"))

            logger.info("[%s] %s: %s", group_name, sender_name, text[:120])

            keywords = cfg.get("trigger_keywords", ["红包", "/"])
            if not all(kw in text for kw in keywords):
                return

            def _match_blocked(w, t):
                try:
                    return bool(re.search(w, t, re.IGNORECASE))
                except re.error:
                    return w.lower() in t.lower()
            hit = next((w for w in cfg.get("blocked_words", []) if _match_blocked(w, text)), None)
            if hit:
                logger.info("屏蔽词 [%s]，跳过", hit)
                return

            # Min average amount filter — below threshold: alert but don't click
            auto_click = True
            min_avg = cfg.get("min_avg") or {}
            if min_avg:
                parsed = parse_hongbao_avg(text)
                if parsed:
                    avg, currency = parsed
                    threshold = float(min_avg.get(currency, 0) or 0)
                    if threshold and avg < threshold:
                        logger.info("💰 均值 %.4g %s < 最低 %.4g，仅报警不点击", avg, currency, threshold)
                        auto_click = False
                    else:
                        logger.info("💰 均值 %.4g %s，通过过滤", avg, currency)
                else:
                    logger.info("💰 无法解析金额，已设最低均值，仅报警不点击")
                    auto_click = False

            logger.info("🔔 触发 [%s] %s", group_name, text[:80])

            # Auto-click: math quiz first, then keyword matching
            raw_buttons = event.message.buttons

            # Full detail log (hongbao.log)
            _log_hongbao(group_name, sender_name, event.message.id, text, raw_buttons)
            quiz_answer = solve_quiz(text)
            if not auto_click:
                logger.info("⏩ 金额不达标，跳过自动点击")
            elif raw_buttons is None:
                logger.info("📋 消息无内联按钮，跳过点击")
            else:
                all_btn_texts = [btn.text for row in raw_buttons for btn in row]
                # Log text + URL for every button so we can see URL-type buttons
                btn_details = []
                for row in raw_buttons:
                    for btn in row:
                        url = getattr(btn, 'url', None)
                        btn_details.append(f"{btn.text}{'→'+url if url else ''}")
                logger.info("📋 按钮列表: %s", btn_details)
                target_btn = None

                if quiz_answer is not None:
                    logger.info("🧮 检测到数学题，答案: %s", quiz_answer)
                    for row in raw_buttons:
                        for btn in row:
                            if _normalize_digits(strip_zw(btn.text).strip()) == quiz_answer:
                                target_btn = btn
                                break
                        if target_btn:
                            break
                    if target_btn is None:
                        logger.info("⚠️ 未找到答案按钮 [%s]，降级为关键词匹配", quiz_answer)

                # 2a. Letter captcha (2-uppercase-letter buttons)
                if target_btn is None:
                    letter_answer = find_letter_captcha(text, all_btn_texts)
                    if letter_answer is not None:
                        logger.info("🔤 字母验证码答案: %s", letter_answer)
                        for row in raw_buttons:
                            for btn in row:
                                if btn.text == letter_answer:
                                    target_btn = btn
                                    break
                            if target_btn:
                                break
                    elif any(_LETTER_BTN_RE.match(b) for b in all_btn_texts):
                        # Log full text so we can understand the pattern
                        logger.info("🔤 字母验证码未识别，完整消息:\n%s", text)

                if target_btn is None:
                    btn_kws = cfg.get("button_texts") or keywords
                    for row in raw_buttons:
                        for btn in row:
                            btn_clean = strip_zw(btn.text)
                            if any(kw and kw in btn_clean for kw in btn_kws):
                                target_btn = btn
                                break
                        if target_btn:
                            break

                # 3rd fallback: unlock button (even if keywords don't match)
                if target_btn is None:
                    for row in raw_buttons:
                        for btn in row:
                            if "解锁" in btn.text:
                                target_btn = btn
                                break
                        if target_btn:
                            break

                if target_btn is not None:
                    btn_url = getattr(target_btn, 'url', None) or ''
                    is_url_claim = bool(_TG_START_RE.match(btn_url))
                    is_unlock = "解锁" in target_btn.text
                    d_min = float(cfg.get("click_delay_min") or cfg.get("click_delay") or 0)
                    d_max = float(cfg.get("click_delay_max") or d_min)
                    if d_max < d_min:
                        d_max = d_min
                    import random as _random
                    delay = _random.uniform(d_min, d_max) if d_min > 0 or d_max > 0 else 0
                    if delay > 0 and not is_unlock and not is_url_claim:
                        logger.info("⏳ 随机延迟 %.1fs 后点击: %s", delay, target_btn.text)
                        await asyncio.sleep(delay)
                    else:
                        suffix = " → 解锁轮询" if is_unlock else (" → URL私聊Bot" if is_url_claim else "")
                        logger.info("👆 点击按钮: %s%s", target_btn.text, suffix)
                    popup, click_error = "", False
                    try:
                        if is_url_claim:
                            popup, err = await claim_via_url_bot(
                                c, btn_url, group_name, sender_name, text[:100])
                            if err:
                                click_error = True
                                popup = err
                                logger.error("❌ URL认领失败: %s", err)
                            else:
                                logger.info("✅ URL认领完成: %s", popup)
                        else:
                            cb = await target_btn.click()
                            popup = getattr(cb, 'message', '') or ''
                            logger.info("✅ 点击完成，弹窗: %s", popup)
                    except Exception as e:
                        popup = str(e)
                        click_error = True
                        logger.error("点击按钮失败: %s", e)

                    if is_unlock and not click_error:
                        # Don't record yet — record after polling finds the real button
                        chat_bare_id = getattr(chat, "id", None)
                        asyncio.create_task(poll_unlock(
                            c, chat_bare_id, event.message.id,
                            group_name, sender_name, text[:100], keywords,
                        ))
                    else:
                        _add_record({
                            "time": datetime.now().isoformat(),
                            "group": group_name,
                            "sender": sender_name,
                            "button": target_btn.text,
                            "quiz_answer": quiz_answer,
                            "result": popup,
                            "error": click_error,
                            "text_preview": text[:100],
                        })
                else:
                    btn_kws = cfg.get("button_texts") or keywords
                    logger.info("⚠️ 无按钮匹配关键词 %s", btn_kws)

            await send_notify(cfg, chat, event.message.id, group_name, sender_name, text)

        except Exception as e:
            logger.error("on_message 异常: %s", e, exc_info=True)

    c.add_event_handler(on_message, events.NewMessage())
    logger.info("✅ 监听已启动，等待消息...")
    try:
        while True:
            await asyncio.sleep(30)
    except asyncio.CancelledError:
        c.remove_event_handler(on_message)
        logger.info("⏹ 监听已停止")
        raise


# ── Startup ────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    def _open():
        time.sleep(1.2)
        webbrowser.open("http://localhost:8888")
    threading.Thread(target=_open, daemon=True).start()
    logger.info("Web 控制台: http://localhost:8888")
    asyncio.create_task(_start_bot_poll_soon())
    asyncio.create_task(_start_monitor_soon())


async def _start_bot_poll_soon():
    global _bot_poll_task
    await asyncio.sleep(2)
    if _bot_poll_task is None or _bot_poll_task.done():
        _bot_poll_task = asyncio.create_task(run_bot_poll())


async def _start_monitor_soon():
    global _monitor_task
    await asyncio.sleep(3)
    if await is_authed():
        if _monitor_task is None or _monitor_task.done():
            _monitor_task = asyncio.create_task(run_monitor())
            logger.info("🚀 监听已自动启动")
    else:
        logger.warning("⚠️ Telegram 未登录，跳过自动启动监听")


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return (BASE_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.get("/api/status")
async def api_status():
    cfg = load_config()
    has_creds = bool(cfg.get("api_id") and cfg.get("api_secret"))
    authed = await is_authed() if has_creds else False
    me = None
    if authed:
        try:
            c = await get_client()
            u = await c.get_me()
            me = {
                "name": ((u.first_name or "") + (" " + u.last_name if u.last_name else "")).strip(),
                "username": u.username,
                "phone": u.phone,
            }
        except Exception:
            pass
    return {
        "has_creds": has_creds,
        "authenticated": authed,
        "me": me,
        "monitoring": _monitor_task is not None and not _monitor_task.done(),
        "awaiting_2fa": _auth["awaiting_2fa"],
    }


# Web UI auth

class WebLoginReq(BaseModel):
    username: str
    password: str


@app.post("/api/web/login")
async def web_login(req: WebLoginReq):
    cfg = load_config()
    expected_user = cfg.get("web_username") or "admin"
    expected_pass = cfg.get("web_password") or "admin123"
    if req.username == expected_user and req.password == expected_pass:
        token = secrets.token_urlsafe(32)
        _web_sessions.add(token)
        return {"ok": True, "token": token}
    raise HTTPException(401, "用户名或密码错误")


@app.post("/api/web/logout")
async def web_logout(request: Request):
    token = (request.headers.get("X-Token") or
             request.query_params.get("token") or "")
    _web_sessions.discard(token)
    return {"ok": True}


@app.get("/api/web/check")
async def web_check(request: Request):
    token = (request.headers.get("X-Token") or
             request.query_params.get("token") or "")
    return {"ok": token in _web_sessions}


# Telegram Auth

class CredReq(BaseModel):
    api_id: str
    api_secret: str

class PhoneReq(BaseModel):
    phone: str

class CodeReq(BaseModel):
    code: str

class PassReq(BaseModel):
    password: str


@app.post("/api/auth/credentials")
async def set_credentials(req: CredReq):
    global _client
    cfg = load_config()
    cfg["api_id"] = req.api_id.strip()
    cfg["api_secret"] = req.api_secret.strip()
    save_config(cfg)
    _client = None
    return {"ok": True}


@app.post("/api/auth/send-code")
async def send_code(req: PhoneReq):
    c = await get_client()
    try:
        sent = await c.send_code_request(req.phone)
        _auth["phone"] = req.phone
        _auth["phone_code_hash"] = sent.phone_code_hash
        _auth["awaiting_2fa"] = False
        return {"ok": True}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/auth/verify-code")
async def verify_code(req: CodeReq):
    c = await get_client()
    try:
        await c.sign_in(_auth["phone"], req.code, phone_code_hash=_auth["phone_code_hash"])
        return {"ok": True, "needs_2fa": False}
    except SessionPasswordNeededError:
        _auth["awaiting_2fa"] = True
        return {"ok": True, "needs_2fa": True}
    except PhoneCodeInvalidError:
        raise HTTPException(400, "验证码错误，请重试")
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/auth/verify-2fa")
async def verify_2fa(req: PassReq):
    c = await get_client()
    try:
        await c.sign_in(password=req.password)
        _auth["awaiting_2fa"] = False
        return {"ok": True}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/auth/logout")
async def logout():
    global _client, _monitor_task
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
        _monitor_task = None
    if _client:
        try:
            await _client.log_out()
        except Exception:
            pass
        _client = None
    return {"ok": True}


# Config

@app.get("/api/config")
async def get_config():
    cfg = load_config()
    return {k: v for k, v in cfg.items() if k not in ("api_id", "api_secret", "web_password")}


class ConfigBody(BaseModel):
    watch_groups: Optional[List[str]] = None
    trigger_keywords: Optional[List[str]] = None
    blocked_words: Optional[List[str]] = None
    button_texts: Optional[List[str]] = None
    min_avg: Optional[dict] = None
    click_delay: Optional[int] = None
    click_delay_min: Optional[float] = None
    click_delay_max: Optional[float] = None
    notify_bot: Optional[dict] = None
    web_username: Optional[str] = None
    web_password: Optional[str] = None


@app.put("/api/config")
async def update_config(body: ConfigBody):
    cfg = load_config()
    cfg.update({k: v for k, v in body.dict().items() if v is not None or k == "click_delay"})
    save_config(cfg)
    return {"ok": True}


# Groups list

@app.get("/api/groups")
async def list_groups():
    if not await is_authed():
        raise HTTPException(401, "未登录")
    c = await get_client()
    groups = []
    try:
        async for dialog in c.iter_dialogs(limit=200):
            e = dialog.entity
            if isinstance(e, (Channel, Chat)):
                groups.append({
                    "id": str(dialog.id),
                    "name": dialog.name,
                    "type": type(e).__name__,
                    "username": getattr(e, "username", None),
                })
    except Exception as ex:
        raise HTTPException(500, str(ex))
    return groups


# ── Manual message watcher ─────────────────────────────────────────────────────

_watch_tasks: dict = {}   # key: (chat_id, msg_id) -> asyncio.Task


async def _watch_message_loop(chat_id: int, msg_id: int, interval_fast: int = 1,
                               interval_slow: int = 10, timeout: int = 1800):
    """Poll a specific message for button/text changes and log every change."""
    c = await get_client()
    start = asyncio.get_event_loop().time()
    prev_text = None
    prev_btns = None
    attempt   = 0
    logger.info("👁 开始监听消息变化: chat=%d msg=%d", chat_id, msg_id)

    try:
        while True:
            elapsed = asyncio.get_event_loop().time() - start
            if elapsed > timeout:
                logger.info("⏰ 消息监听超时(%.0f分钟): msg=%d", timeout / 60, msg_id)
                return

            interval = interval_fast if elapsed < 60 else interval_slow
            await asyncio.sleep(interval)
            attempt += 1

            try:
                msg = await c.get_messages(chat_id, ids=msg_id)
                if not msg:
                    logger.warning("👁 消息已消失: msg=%d", msg_id)
                    return

                cur_text = msg.text or ""
                cur_btns = []
                if msg.buttons:
                    for row in msg.buttons:
                        for btn in row:
                            url  = getattr(btn, 'url', None) or ''
                            data = getattr(btn, 'data', None)
                            kind = 'URL' if url else ('CB' if data else '?')
                            cur_btns.append(f"[{kind}]{btn.text!r}{('→'+url) if url else ''}")

                text_changed = (prev_text is not None and cur_text != prev_text)
                btns_changed = (prev_btns is not None and cur_btns != prev_btns)

                if prev_text is None:
                    logger.info("👁 [%d] 初始状态 | 按钮: %s", attempt, cur_btns)
                    logger.info("👁 初始文本:\n%s", cur_text)
                elif text_changed or btns_changed:
                    logger.info("👁 [%d] 消息变化! 按钮: %s → %s", attempt, prev_btns, cur_btns)
                    if text_changed:
                        logger.info("👁 新文本:\n%s", cur_text)

                prev_text = cur_text
                prev_btns = cur_btns

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("👁 监听轮询异常: %s", e)

    except asyncio.CancelledError:
        logger.info("👁 消息监听已停止: msg=%d", msg_id)
        raise
    finally:
        key = (chat_id, msg_id)
        _watch_tasks.pop(key, None)


async def _resolve_message(c, chat_id: int, msg_id: int):
    """Try multiple ID formats to fetch a message."""
    for cid in [chat_id, -chat_id, int(f"-100{chat_id}") if chat_id > 0 else None]:
        if cid is None:
            continue
        try:
            entity = await c.get_entity(cid)
            msg = await c.get_messages(entity, ids=msg_id)
            if msg:
                return entity, msg
        except Exception:
            pass
    return None, None


def _fmt_buttons(msg):
    btns = []
    if msg.buttons:
        for r, row in enumerate(msg.buttons):
            for b, btn in enumerate(row):
                url  = getattr(btn, 'url', None) or ''
                data = getattr(btn, 'data', None)
                kind = 'URL' if url else ('Callback' if data else 'Unknown')
                btns.append({"pos": f"{r},{b}", "kind": kind,
                             "text": btn.text, "url": url,
                             "data": data.hex() if data else ""})
    return btns


@app.get("/api/watch-message")
async def get_message_state(chat_id: int, msg_id: int):
    """Fetch current state of a message (text + buttons)."""
    if not await is_authed():
        raise HTTPException(401, "未登录")
    c = await get_client()
    entity, msg = await _resolve_message(c, chat_id, msg_id)
    if not msg:
        raise HTTPException(404, f"消息不存在 (chat_id={chat_id} msg_id={msg_id})")
    return {"text": msg.text, "buttons": _fmt_buttons(msg),
            "chat_title": getattr(entity, 'title', str(chat_id)),
            "watching": (chat_id, msg_id) in _watch_tasks}


@app.post("/api/watch-message")
async def start_watch_message(chat_id: int, msg_id: int,
                               interval_fast: int = 1, interval_slow: int = 10,
                               timeout: int = 1800):
    """Start polling a message for changes."""
    if not await is_authed():
        raise HTTPException(401, "未登录")
    # Verify we can fetch it first
    c = await get_client()
    entity, msg = await _resolve_message(c, chat_id, msg_id)
    if not msg:
        raise HTTPException(404, f"消息不存在 (chat_id={chat_id} msg_id={msg_id})")
    # Use the resolved entity id for polling
    resolved_id = entity.id
    key = (chat_id, msg_id)
    if key in _watch_tasks and not _watch_tasks[key].done():
        return {"ok": True, "message": "已在监听"}
    _watch_tasks[key] = asyncio.create_task(
        _watch_message_loop(resolved_id, msg_id, interval_fast, interval_slow, timeout)
    )
    return {"ok": True, "chat_title": getattr(entity, 'title', ''),
            "text_preview": (msg.text or '')[:100],
            "buttons": _fmt_buttons(msg)}


@app.delete("/api/watch-message")
async def stop_watch_message(chat_id: int, msg_id: int):
    key = (chat_id, msg_id)
    task = _watch_tasks.get(key)
    if task and not task.done():
        task.cancel()
    return {"ok": True}


# Bot control

@app.post("/api/bot/restart")
async def restart_bot():
    global _bot_poll_task, _bot_update_offset
    if _bot_poll_task and not _bot_poll_task.done():
        _bot_poll_task.cancel()
        try:
            await _bot_poll_task
        except asyncio.CancelledError:
            pass
    _bot_update_offset = 0
    _bot_poll_task = asyncio.create_task(run_bot_poll())
    return {"ok": True}


@app.get("/api/stats")
async def get_stats():
    return {"text": _stats_text()}


# Monitor control

@app.post("/api/monitor/start")
async def start_monitor():
    global _monitor_task
    if not await is_authed():
        raise HTTPException(401, "未登录")
    if _monitor_task and not _monitor_task.done():
        return {"ok": True, "message": "已在运行"}
    _monitor_task = asyncio.create_task(run_monitor())
    return {"ok": True}


@app.post("/api/monitor/stop")
async def stop_monitor():
    global _monitor_task
    if _monitor_task and not _monitor_task.done():
        _monitor_task.cancel()
        try:
            await _monitor_task
        except asyncio.CancelledError:
            pass
        _monitor_task = None
    return {"ok": True}


# WebSocket log stream

@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    log_subscribers.append(q)
    try:
        for entry in list(LOG_BUFFER):
            await ws.send_json(entry)
        while True:
            entry = await q.get()
            await ws.send_json(entry)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if q in log_subscribers:
            log_subscribers.remove(q)


@app.get("/api/records")
async def get_records():
    return list(GRAB_RECORDS)


@app.delete("/api/records")
async def clear_records():
    GRAB_RECORDS.clear()
    _save_records()
    return {"ok": True}


@app.websocket("/ws/records")
async def ws_records(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    record_subscribers.append(q)
    try:
        while True:
            rec = await q.get()
            await ws.send_json(rec)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if q in record_subscribers:
            record_subscribers.remove(q)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="warning")
