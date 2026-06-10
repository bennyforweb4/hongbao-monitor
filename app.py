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

import aiohttp
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
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
# Matches: 剩余:2/2  /  剩余: 0/168  etc. — captures the denominator (total envelopes)
_CNT_RE = re.compile(r"剩余[：:]\s*\d+\s*/\s*(\d+)")

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
    token, target = nb.get("token", ""), nb.get("target_chat_id", "")
    if not token or not target:
        logger.warning("未配置通知 Bot，跳过")
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
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": target, "text": body, "parse_mode": "HTML", "disable_web_page_preview": False}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    logger.error("通知发送失败 %s: %s", r.status, await r.text())
                else:
                    logger.info("通知已发送 → %s", target)
    except Exception as e:
        logger.error("通知异常: %s", e)


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
            chat = await event.get_chat()
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

            # Min average amount filter
            min_avg = cfg.get("min_avg") or {}
            if min_avg:
                parsed = parse_hongbao_avg(text)
                if parsed:
                    avg, currency = parsed
                    threshold = float(min_avg.get(currency, 0) or 0)
                    if threshold and avg < threshold:
                        logger.info("💰 均值 %.4g %s < 最低 %.4g，跳过", avg, currency, threshold)
                        return
                    logger.info("💰 均值 %.4g %s，通过过滤", avg, currency)

            logger.info("🔔 触发 [%s] %s", group_name, text[:80])

            # Auto-click: use button_texts if configured, otherwise fall back to trigger keywords
            raw_buttons = event.message.buttons
            if raw_buttons is None:
                logger.info("📋 消息无内联按钮，跳过点击")
            else:
                all_btn_texts = [btn.text for row in raw_buttons for btn in row]
                logger.info("📋 按钮列表: %s", all_btn_texts)
                btn_kws = cfg.get("button_texts") or keywords
                clicked = False
                for row in raw_buttons:
                    for btn in row:
                        btn_clean = strip_zw(btn.text)
                        if any(kw and kw in btn_clean for kw in btn_kws):
                            try:
                                delay = int(cfg.get("click_delay") or 0)
                                if delay > 0:
                                    logger.info("⏳ 延迟 %ds 后点击按钮: %s", delay, btn.text)
                                    await asyncio.sleep(delay)
                                else:
                                    logger.info("👆 点击按钮: %s", btn.text)
                                await event.message.click(text=btn.text)
                                clicked = True
                            except Exception as e:
                                logger.error("点击按钮失败: %s", e)
                            break
                    if clicked:
                        break
                if not clicked:
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


# Auth

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
    return {k: v for k, v in cfg.items() if k not in ("api_id", "api_secret")}


class ConfigBody(BaseModel):
    watch_groups: Optional[List[str]] = None
    trigger_keywords: Optional[List[str]] = None
    blocked_words: Optional[List[str]] = None
    button_texts: Optional[List[str]] = None
    min_avg: Optional[dict] = None
    click_delay: Optional[int] = None
    notify_bot: Optional[dict] = None


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="warning")
