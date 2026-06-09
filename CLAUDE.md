# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

A Telegram "red envelope" (红包) monitor. It uses a **userbot** (Telethon, logged-in personal account) to listen to specified groups, and fires an alert via a **Telegram Bot** (HTTP API) when a message simultaneously contains "红包" and "/" — the typical pattern for bot-issued red envelopes in Chinese Telegram trading groups.

## Setup and running

Install dependencies:
```bash
pip3 install -r requirements.txt
```

First-run Telethon authentication (interactive, writes `session.session`):
```bash
python3 monitor.py
```

After the session file exists, use the shell scripts:
```bash
./start.sh   # launches monitor.py as a background process, PID in monitor.pid
./stop.sh    # kills the process by PID and cleans up
```

Find group IDs to put in `config.yaml`:
```bash
python3 get_chat_id.py
```

## Configuration

Two config files are used together:

**`config.yaml`** — static, requires restart to take effect:
- `api_id` / `api_secret`: Telethon user credentials from https://my.telegram.org
- `watch_groups`: list of group usernames (`@foo`) or numeric IDs (Telethon bare format or Bot API `-100...` format — both are accepted)
- `notify_bot.token`: the Bot token that sends alerts
- `notify_bot.target_chat_id`: group/chat where alerts and commands are sent (Bot API `-100...` format)
- `log.file` / `log.level`: logging config

**`dynamic.json`** — runtime-mutable, persisted immediately on change, no restart needed:
- `blocked_words`: messages matching any of these are silently skipped
- `watch_groups`: extra groups added at runtime via bot commands

## Architecture

```
Telegram groups (watched)
        │  Telethon userbot (session.session)
        ▼
   monitor.py
        │
        ├─ is_watched_chat()  →  merge of config.yaml + dynamic.json watch_groups
        ├─ match_hongbao()    →  "红包" AND "/" both present
        ├─ match_blocked()    →  any blocked_word in text → skip
        │
        ▼
  send_notification()         →  Bot HTTP API → target_chat_id
        │
        └─ is_notify_chat()   →  messages FROM target_chat are parsed as commands
               ▼
         handle_command()     →  mutates dynamic.json in place
```

**ID normalisation**: Telethon gives bare positive chat IDs; the Bot API uses `-100XXXXXXXXX`. `_chat_id_matches()` translates between formats so the same group ID works in both systems.

**Command interface**: any message sent to `target_chat_id` that starts with a recognised prefix (`加屏蔽词`, `删屏蔽词`, `查屏蔽词`, `加监听群`, `删监听群`, `查监听群`, `帮助`, `help`) is handled as a management command rather than passed through the alert pipeline.

## Important notes

- `launchd_start.sh` still hardcodes an old path (`/Users/bennylife/Desktop/富豪群红包监听`). Update it before using it as a macOS LaunchAgent.
- `session.session` is the Telethon auth token — treat it like a password; do not commit it.
- `run.log` captures stdout/stderr from the background process; `messages.log` records every monitored group message at INFO level.
