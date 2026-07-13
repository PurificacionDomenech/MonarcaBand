#!/usr/bin/env python3
"""
scripts/notify_push_error.py
Sends a Telegram alert when a GitHub push fails.

Usage:
    python3 scripts/notify_push_error.py "Error reason here"
"""

import sys
import os
import json
import urllib.request
import urllib.error

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           ".cache", "telegram_chat_ids.json")


def load_chat_ids() -> list:
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
            return data.get("chat_ids", [])
    except Exception:
        return []


def send_telegram(chat_id: int, text: str) -> bool:
    if not TELEGRAM_TOKEN:
        print("[notify_push_error] TELEGRAM_TOKEN not set — skipping Telegram alert")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            return result.get("ok", False)
    except Exception as e:
        print(f"[notify_push_error] Telegram error for chat_id {chat_id}: {e}")
        return False


def main():
    error_reason = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Unknown error"

    chat_ids = load_chat_ids()
    if not chat_ids:
        print("[notify_push_error] No chat_ids found in cache — cannot send alert")
        return

    message = (
        "⚠️ <b>GitHub Push Failed</b>\n\n"
        "The automatic sync to GitHub did not complete successfully.\n\n"
        f"<b>Error:</b>\n<code>{error_reason[:800]}</code>\n\n"
        "<i>Check the repository and push manually if needed.</i>"
    )

    sent = 0
    for cid in chat_ids:
        if send_telegram(int(cid), message):
            sent += 1

    if sent:
        print(f"[notify_push_error] Alert sent to {sent}/{len(chat_ids)} chat(s)")
    else:
        print("[notify_push_error] Failed to send alert to any chat")


if __name__ == "__main__":
    main()
