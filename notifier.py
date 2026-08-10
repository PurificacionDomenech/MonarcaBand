"""
notifier.py — Trading Band
Cada usuario tiene sus propios tickers vigilados y canales de notificación.

Variables de entorno en Railway:
  TELEGRAM_TOKEN    → Token del bot (@BotFather)
  SUPABASE_URL      → https://xxxxx.supabase.co
  SUPABASE_KEY      → service_role key (empieza por eyJ...)
  MAIL_FROM         → tu@gmail.com  (opcional)
  MAIL_PASSWORD     → App Password de Gmail
  MAIL_SMTP         → smtp.gmail.com
  MAIL_PORT         → 587
"""

import os
import re
import html
import json
import asyncio
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx

# Cache persistente en disco para chat_ids (fallback cuando Supabase falla)
_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
os.makedirs(_CACHE_DIR, exist_ok=True)
_CHAT_IDS_CACHE_FILE = os.path.join(_CACHE_DIR, "telegram_chat_ids.json")

def _load_cached_chat_ids() -> list[int]:
    try:
        with open(_CHAT_IDS_CACHE_FILE, "r") as f:
            data = json.load(f)
            return data.get("chat_ids", [])
    except Exception:
        return []

def _save_cached_chat_ids(chat_ids: list[int]) -> None:
    try:
        with open(_CHAT_IDS_CACHE_FILE, "w") as f:
            json.dump({"chat_ids": chat_ids, "updated_at": datetime.now().isoformat()}, f)
    except Exception as e:
        print(f"[notifier] Cache save error: {e}")

logger = logging.getLogger("notifier")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
SUPABASE_URL   = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
MAIL_FROM      = os.getenv("MAIL_FROM", "")
MAIL_PASSWORD  = os.getenv("MAIL_PASSWORD", "")
MAIL_SMTP      = os.getenv("MAIL_SMTP", "smtp.gmail.com")
MAIL_PORT      = int(os.getenv("MAIL_PORT", "587"))

NIVEL_EMOJI    = {"bullish": "🟢", "bearish": "🔴", "info": "🔵"}
NIVEL_LABEL    = {"bullish": "Favorable",  "bearish": "Atención",  "info": "Interesante"}
NIVEL_LABEL_EN = {"bullish": "Bullish",    "bearish": "Bearish",   "info": "Watch"}


def _translate_en(msg: str) -> str:
    msg = re.sub(r'Precio cruza (EMA\d+) al alza',  r'Price crosses \1 upward',   msg)
    msg = re.sub(r'Precio cruza (EMA\d+) a la baja', r'Price crosses \1 downward', msg)
    msg = re.sub(r'Precio tocando (EMA\d+)',          r'Price touching \1',          msg)
    return msg

ASSET_NAMES = {
    '^DJI':'US30 · Dow Jones','^NDX':'NAS100 · Nasdaq','^GSPC':'SPX · S&P 500',
    '^RUT':'RTY · Russell 2000','GC=F':'XAUUSD · Oro','SI=F':'XAGUSD · Plata',
    'CL=F':'WTI · Petróleo','USDJPY=X':'USD/JPY','GBPJPY=X':'GBP/JPY',
    'EURUSD=X':'EUR/USD','AUDUSD=X':'AUD/USD','GBPUSD=X':'GBP/USD','AUDJPY=X':'AUD/JPY',
    '^TNX':'US10Y · Bono',
    '^TYX':'US30Y · Bono','DX=F':'DXY · Dólar','BTC-USD':'BTC · Bitcoin',
    'ETH-USD':'ETH · Ethereum','SPY':'SPY','VOO':'VOO','QQQ':'QQQ',
    'QQQM':'QQQM','GLD':'GLD','IAU':'IAU','GDX':'GDX','IWM':'IWM',
    'SMH':'SMH','XLE':'XLE','AAPL':'AAPL · Apple',
}

TRADINGVIEW_URLS = {
    '^DJI':     'https://www.tradingview.com/chart/?symbol=TVC:DJI',
    '^NDX':     'https://www.tradingview.com/chart/?symbol=NASDAQ:NDX',
    '^GSPC':    'https://www.tradingview.com/chart/?symbol=SP:SPX',
    '^RUT':     'https://www.tradingview.com/chart/?symbol=TVC:RUT',
    'GC=F':     'https://www.tradingview.com/chart/?symbol=COMEX:GC1!',
    'SI=F':     'https://www.tradingview.com/chart/?symbol=COMEX:SI1!',
    'CL=F':     'https://www.tradingview.com/chart/?symbol=NYMEX:CL1!',
    'USDJPY=X': 'https://www.tradingview.com/chart/?symbol=FX:USDJPY',
    'GBPJPY=X': 'https://www.tradingview.com/chart/?symbol=FX:GBPJPY',
    'EURUSD=X': 'https://www.tradingview.com/chart/?symbol=FX:EURUSD',
    'AUDUSD=X': 'https://www.tradingview.com/chart/?symbol=FX:AUDUSD',
    'GBPUSD=X': 'https://www.tradingview.com/chart/?symbol=FX:GBPUSD',
    'AUDJPY=X': 'https://www.tradingview.com/chart/?symbol=FX:AUDJPY',
    '^TNX':     'https://www.tradingview.com/chart/?symbol=TVC:TNX',
    '^TYX':     'https://www.tradingview.com/chart/?symbol=TVC:TYX',
    'DX=F':     'https://www.tradingview.com/chart/?symbol=TVC:DXY',
    'BTC-USD':  'https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSDT',
    'ETH-USD':  'https://www.tradingview.com/chart/?symbol=BINANCE:ETHUSDT',
    'SPY':      'https://www.tradingview.com/chart/?symbol=AMEX:SPY',
    'VOO':      'https://www.tradingview.com/chart/?symbol=AMEX:VOO',
    'QQQ':      'https://www.tradingview.com/chart/?symbol=NASDAQ:QQQ',
    'QQQM':     'https://www.tradingview.com/chart/?symbol=NASDAQ:QQQM',
    'GLD':      'https://www.tradingview.com/chart/?symbol=AMEX:GLD',
    'IAU':      'https://www.tradingview.com/chart/?symbol=AMEX:IAU',
    'GDX':      'https://www.tradingview.com/chart/?symbol=AMEX:GDX',
    'IWM':      'https://www.tradingview.com/chart/?symbol=AMEX:IWM',
    'SMH':      'https://www.tradingview.com/chart/?symbol=NASDAQ:SMH',
    'XLE':      'https://www.tradingview.com/chart/?symbol=AMEX:XLE',
    'AAPL':     'https://www.tradingview.com/chart/?symbol=NASDAQ:AAPL',
}

def _tv_link(ticker: str, lang: str = "es") -> str:
    url = TRADINGVIEW_URLS.get(ticker.upper())
    if not url:
        return ""
    label = "Ver en TradingView 📊" if lang == "es" else "View on TradingView 📊"
    return f'<a href="{url}">{label}</a>'

def _strip_ticker(msg: str) -> str:
    return re.sub(r'^\[[^\]]+\]\s*', '', msg)

_DIA_ES = {
    "Monday":    "Lunes",
    "Tuesday":   "Martes",
    "Wednesday": "Miércoles",
    "Thursday":  "Jueves",
    "Friday":    "Viernes",
    "Saturday":  "Sábado",
    "Sunday":    "Domingo",
}
_DIA_EN = {
    "Monday": "Monday", "Tuesday": "Tuesday", "Wednesday": "Wednesday",
    "Thursday": "Thursday", "Friday": "Friday", "Saturday": "Saturday", "Sunday": "Sunday",
}

def _format_hora_tz(ts_utc_iso: str, tz_str: str) -> tuple[str, str, str]:
    """
    Convierte un timestamp UTC ISO al timezone del usuario.
    Retorna (hora_local_str, dia_name_en, tz_label).
    """
    if not ts_utc_iso or not tz_str or tz_str == "UTC":
        return "", "", "UTC"
    try:
        from datetime import datetime, timezone as dt_timezone
        from zoneinfo import ZoneInfo
        # Parsear el timestamp UTC
        dt_utc = datetime.fromisoformat(ts_utc_iso)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=dt_timezone.utc)
        tz = ZoneInfo(tz_str)
        dt_local = dt_utc.astimezone(tz)
        hora_local = dt_local.strftime("%d/%m %H:%M")
        dia_name   = dt_local.strftime("%A")   # English day name
        # Usar el nombre de la ciudad como etiqueta (Europe/Madrid → Madrid)
        tz_label = tz_str.split("/")[-1].replace("_", " ")
        return hora_local, dia_name, tz_label
    except Exception as e:
        print(f"[tz] Error convirtiendo {ts_utc_iso!r} → {tz_str!r}: {e}")
        return "", "", "UTC"


def _rsi_ctx(rsi: float, lang: str) -> str:
    if lang == "en":
        if rsi > 66:   return "overbought"
        if rsi < 33:   return "oversold"
        if rsi > 66:   return "approaching overbought"
        if rsi < 33:   return "approaching oversold"
        return "neutral"
    else:
        if rsi > 66:   return "sobrecompra"
        if rsi < 33:   return "sobreventa"
        if rsi > 66:   return "zona alta"
        if rsi < 33:   return "zona baja"
        return "zona neutra"

def _signal_lines(a: dict, lang: str) -> list[str]:
    """Genera las líneas explicativas para una alerta concreta."""
    tipo  = a.get("tipo", "")
    nivel = a.get("nivel", "info")
    close = a.get("close")
    lines = []

    return lines


def _build_tg_grouped(alerts_by_ticker: dict, now_str: str, lang: str = "es") -> str:
    labels  = NIVEL_LABEL_EN if lang == "en" else NIVEL_LABEL
    dia_map = _DIA_EN if lang == "en" else _DIA_ES
    blocks  = [f"<b>⬡ Trading Band · {now_str}</b>"]

    for ticker, alertas in alerts_by_ticker.items():
        if not alertas:
            continue
        name = ASSET_NAMES.get(ticker.upper(), ticker)

        # Agrupar alertas de la misma vela (mismo 'hora')
        by_candle: dict = {}
        for a in alertas:
            h = a.get("hora", "")
            by_candle.setdefault(h, []).append(a)

        for hora, candle_alerts in by_candle.items():
            # Representante para datos comunes de la vela
            rep       = candle_alerts[0]
            nivel_rep = rep.get("nivel", "info")
            emoji_rep = NIVEL_EMOJI.get(nivel_rep, "⚪")
            label_rep = labels.get(nivel_rep, "")
            close_v   = rep.get("close")
            rsi_v     = rep.get("rsi")
            dia_name  = rep.get("dia_name", "")
            dia_num   = rep.get("dia_num", -1)
            dia_pts   = rep.get("dia_pts", 0)
            score     = max(a.get("score", 0) for a in candle_alerts)

            # ── Cabecera del bloque ──────────────────────────
            blocks.append("")
            blocks.append(f"<b>{emoji_rep} {name}  ·  {label_rep}</b>")

            # Fecha y hora de la vela
            if hora:
                dia_es_str = dia_map.get(dia_name, dia_name)
                if lang == "en":
                    blocks.append(f"🕐 Vela 1H · {dia_es_str}  {hora} UTC")
                else:
                    blocks.append(f"🕐 Vela 1H · {dia_es_str}  {hora} UTC")

            # Precio actual
            if close_v is not None:
                if lang == "en":
                    blocks.append(f"💰 Price: <code>{close_v:.5g}</code>")
                else:
                    blocks.append(f"💰 Precio: <code>{close_v:.5g}</code>")

            # ── Señales técnicas ────────────────────────────
            for a in candle_alerts:
                sig_lines = _signal_lines(a, lang)
                blocks.extend(sig_lines)

            # ── RSI ─────────────────────────────────────────
            if rsi_v is not None:
                ctx = _rsi_ctx(rsi_v, lang)
                rsi_pts = rep.get("rsi_pts", 0)
                pt_tag  = f"  <i>(+{rsi_pts} pt)</i>" if rsi_pts else ""
                if lang == "en":
                    blocks.append(f"📊 RSI: <code>{rsi_v:.1f}</code>  ·  {ctx}{pt_tag}")
                else:
                    blocks.append(f"📊 RSI: <code>{rsi_v:.1f}</code>  ·  {ctx}{pt_tag}")

            # ── Día de la semana ─────────────────────────────
            if dia_name:
                dia_es_str = dia_map.get(dia_name, dia_name)
                if dia_pts:
                    if lang == "en":
                        blocks.append(f"📅 {dia_es_str}: mid-week session — higher liquidity  <i>(+1 pt)</i>")
                    else:
                        blocks.append(f"📅 {dia_es_str}: sesión central — mayor liquidez  <i>(+1 pt)</i>")
                else:
                    if lang == "en":
                        blocks.append(f"📅 {dia_es_str}: low-liquidity session")
                    else:
                        blocks.append(f"📅 {dia_es_str}: sesión de menor liquidez")

            # ── Puntuación final ─────────────────────────────
            if lang == "en":
                blocks.append(f"⚡ Score: <b>{score}/12</b> pts")
            else:
                blocks.append(f"⚡ Puntuación: <b>{score}/12</b> pts")

            # ── Enlace TradingView ────────────────────────────
            tv = _tv_link(ticker, lang)
            if tv:
                blocks.append(tv)

    if lang == "en":
        blocks.append("<i>Automated technical analysis · Not financial advice</i>")
    else:
        blocks.append("<i>Análisis técnico automatizado · No es asesoría financiera</i>")
    return "\n".join(blocks)

def _build_html_grouped(alerts_by_ticker: dict, now_str: str, lang: str = "es") -> str:
    color_map = {"bullish": "#00cc33", "bearish": "#ff3333", "info": "#4da6ff"}
    labels = NIVEL_LABEL_EN if lang == "en" else NIVEL_LABEL
    disclaimer = "Automated technical analysis · Not financial advice" if lang == "en" else "Análisis técnico automatizado · No es asesoría financiera"
    rows = ""
    for ticker, alertas in alerts_by_ticker.items():
        if not alertas:
            continue
        name = ASSET_NAMES.get(ticker.upper(), ticker)
        rows += (f'<tr><td style="padding:8px 10px 4px;font-family:monospace;font-size:12px;'
                 f'color:#00ff41;font-weight:bold;border-top:1px solid #0a1a0a">📊 {name}</td></tr>')
        for a in alertas:
            nivel = a.get("nivel", "info")
            c = color_map.get(nivel, "#888")
            e = NIVEL_EMOJI.get(nivel, "⚪")
            lbl = labels.get(nivel, "")
            msg = _strip_ticker(a.get("msg", ""))
            if lang == "en":
                msg = _translate_en(msg)
            rows += (f'<tr><td style="padding:3px 10px 3px 20px;border-bottom:1px solid #1a2a1a;'
                     f'color:{c};font-family:monospace;font-size:13px">'
                     f'{e} {lbl} · {msg}</td></tr>')
    return f"""<html><body style="background:#000;padding:20px;">
      <div style="max-width:600px;margin:auto;background:#010801;border:1px solid #00ff4120;border-radius:8px;overflow:hidden;">
        <div style="background:#010f01;padding:14px 20px;border-bottom:1px solid #00ff4115;">
          <span style="font-family:monospace;font-size:14px;color:#00ff41;font-weight:bold;">⬡ TRADING BAND</span>
          <span style="font-family:monospace;font-size:11px;color:#666;margin-left:10px;">{now_str}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
        <div style="padding:10px 20px;font-size:10px;color:#333;font-family:monospace;border-top:1px solid #00ff4110;text-align:center;">
          {disclaimer}
        </div>
      </div></body></html>"""

print(f"[notifier] Telegram: {'OK' if TELEGRAM_TOKEN else 'FALTA TELEGRAM_TOKEN'} | "
      f"Supabase: {'OK' if SUPABASE_URL and SUPABASE_KEY else 'FALTA SUPABASE_URL/KEY'} | "
      f"Email: {'OK' if MAIL_FROM else 'no configurado'}")


def _headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }


async def _supa_get(path: str) -> list:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=_headers())
            if r.status_code == 200:
                return r.json()
            print(f"[notifier] Supabase GET error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[notifier] Supabase excepción: {e}")
    return []


async def _supa_post(path: str, payload: dict, prefer: str = "") -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    h = _headers()
    if prefer:
        h["Prefer"] = prefer
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{SUPABASE_URL}/rest/v1/{path}", headers=h, json=payload)
            if r.status_code == 409:
                print(f"[notifier] Supabase 409 en {path} — registro ya existe, se trata como éxito")
                return True
            if r.status_code not in (200, 201, 204):
                print(f"[notifier] Supabase POST {path} → {r.status_code}: {r.text[:300]}")
                return False
            return True
    except Exception as e:
        print(f"[notifier] Supabase POST excepción: {e}")
        return False


async def _supa_patch(path: str, payload: dict) -> bool:
    """PATCH (UPDATE) a existing row in Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    h = _headers()
    h["Prefer"] = "return=minimal"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.patch(f"{SUPABASE_URL}/rest/v1/{path}", headers=h, json=payload)
            if r.status_code not in (200, 201, 204):
                print(f"[notifier] Supabase PATCH {path} → {r.status_code}: {r.text[:300]}")
                return False
            return True
    except Exception as e:
        print(f"[notifier] Supabase PATCH excepción: {e}")
        return False


async def _supa_delete(path: str) -> bool:
    """DELETE rows from Supabase matching the given filter path."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    h = _headers()
    h["Prefer"] = "return=minimal"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.delete(f"{SUPABASE_URL}/rest/v1/{path}", headers=h)
            if r.status_code not in (200, 204):
                print(f"[notifier] Supabase DELETE {path} → {r.status_code}: {r.text[:300]}")
                return False
            return True
    except Exception as e:
        print(f"[notifier] Supabase DELETE excepción: {e}")
        return False


# ── Telegram subs (registro via /start) ─────────────────────

async def get_chat_ids() -> list[int]:
    rows = await _supa_get("telegram_subs?select=chat_id")
    chat_ids = [r["chat_id"] for r in rows if r.get("chat_id")]
    if chat_ids:
        _save_cached_chat_ids(chat_ids)
    if not chat_ids:
        chat_ids = _load_cached_chat_ids()
        if chat_ids:
            print(f"[notifier] Fallback: {len(chat_ids)} chat_ids desde cache local")
    return chat_ids


async def register_chat(chat_id: int, username: str = "") -> bool:
    ok = await _supa_post(
        "telegram_subs",
        {"chat_id": chat_id, "username": username or ""},
        prefer="resolution=merge-duplicates"
    )
    if ok:
        existing = _load_cached_chat_ids()
        if chat_id not in existing:
            existing.append(chat_id)
            _save_cached_chat_ids(existing)
    return ok


# ── Preferencias por usuario ─────────────────────────────────

async def get_user_prefs(user_id: str) -> dict:
    rows = await _supa_get(f"notification_prefs?user_id=eq.{user_id}&select=*")
    return rows[0] if rows else {}


async def save_user_prefs(user_id: str, prefs: dict) -> bool:
    """Guarda preferencias de notificación del usuario (INSERT o UPDATE según exista)."""
    raw_levels = prefs.get("div_levels")
    if isinstance(raw_levels, list) and raw_levels:
        div_levels = [int(x) for x in raw_levels if str(x).isdigit()]
    else:
        div_levels = [1, 2]
    payload = {
        "telegram_chat_id": prefs.get("telegram_chat_id"),
        "telegram_enabled": bool(prefs.get("telegram_enabled", False)),
        "email_address":    prefs.get("email_address", "") or "",
        "email_enabled":    bool(prefs.get("email_enabled", False)),
        "tickers":          prefs.get("tickers", []),
        "timezone":         prefs.get("timezone", "UTC") or "UTC",
        "div_levels":       div_levels,
    }
    async def _try_save(p: dict, is_existing: bool) -> bool:
        if is_existing:
            ok = await _supa_patch(f"notification_prefs?user_id=eq.{user_id}", p)
            if ok:
                print(f"[notifier] Prefs actualizadas para {user_id[:8]}…")
            return ok
        else:
            q = dict(p)
            q["user_id"] = user_id
            ok = await _supa_post("notification_prefs", q, prefer="")
            if ok:
                print(f"[notifier] Prefs creadas para {user_id[:8]}…")
            return ok

    # ¿Ya existe el registro?
    existing = await _supa_get(f"notification_prefs?user_id=eq.{user_id}&select=id")
    is_existing = bool(existing)

    ok = await _try_save(payload, is_existing)
    if not ok and "div_levels" in payload:
        # Columna div_levels puede no existir aún en Supabase — reintentar sin ella
        print(f"[notifier] Reintentando guardado sin div_levels para {user_id[:8]}…")
        fallback = {k: v for k, v in payload.items() if k != "div_levels"}
        ok = await _try_save(fallback, is_existing)
    return ok


async def get_all_user_prefs() -> list[dict]:
    return await _supa_get(
        "notification_prefs"
        "?or=(telegram_enabled.eq.true,email_enabled.eq.true)"
        "&select=*"
    )


# ── Envío Telegram ───────────────────────────────────────────

async def send_telegram_to(chat_id: int, text: str) -> bool:
    if not TELEGRAM_TOKEN:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text,
                      "parse_mode": "HTML", "disable_web_page_preview": True}
            )
            d = r.json()
            if not d.get("ok"):
                print(f"[notifier] Telegram error {chat_id}: {d.get('description')}")
            return d.get("ok", False)
    except Exception as e:
        print(f"[notifier] Telegram excepción {chat_id}: {e}")
        return False


# ── Envío Email ──────────────────────────────────────────────

def _smtp_send(to_addr: str, subject: str, body_html: str) -> bool:
    if not MAIL_FROM or not MAIL_PASSWORD:
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = MAIL_FROM
        msg["To"]      = to_addr
        msg.attach(MIMEText(body_html, "html"))
        port = int(MAIL_PORT)
        if port == 465:
            with smtplib.SMTP_SSL(MAIL_SMTP, port, timeout=10) as s:
                s.login(MAIL_FROM, MAIL_PASSWORD); s.sendmail(MAIL_FROM, to_addr, msg.as_string())
        else:
            with smtplib.SMTP(MAIL_SMTP, port, timeout=10) as s:
                s.starttls(); s.login(MAIL_FROM, MAIL_PASSWORD); s.sendmail(MAIL_FROM, to_addr, msg.as_string())
        return True
    except Exception as e:
        print(f"[notifier] Email error → {to_addr}: {e}")
        return False


def _build_html(alertas: list[dict], now_str: str) -> str:
    color_map = {"bullish": "#00cc33", "bearish": "#ff3333", "info": "#4da6ff"}
    filas = "".join(
        f'<tr><td style="padding:6px 10px;border-bottom:1px solid #1a2a1a;'
        f'color:{color_map.get(a.get("nivel","info"),"#888")};font-family:monospace;font-size:13px;">'
        f'{NIVEL_EMOJI.get(a.get("nivel","info"),"⚪")} {NIVEL_LABEL.get(a.get("nivel","info"),"")} · {a["msg"]}</td></tr>'
        for a in alertas
    )
    return f"""<html><body style="background:#000;padding:20px;">
      <div style="max-width:600px;margin:auto;background:#010801;border:1px solid #00ff4120;border-radius:8px;overflow:hidden;">
        <div style="background:#010f01;padding:14px 20px;border-bottom:1px solid #00ff4115;">
          <span style="font-family:monospace;font-size:14px;color:#00ff41;font-weight:bold;">⬡ TRADING BAND</span>
          <span style="font-family:monospace;font-size:11px;color:#666;margin-left:10px;">{now_str}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;">{filas}</table>
        <div style="padding:10px 20px;font-size:10px;color:#333;font-family:monospace;border-top:1px solid #00ff4110;text-align:center;">
          Análisis técnico automatizado · No es asesoría financiera
        </div>
      </div></body></html>"""


# ── Contexto informativo (precio vs aperturas y componentes) ─

def _ref_label(ref_val, bar_high, bar_low, is_high, lang):
    """Devuelve etiqueta si la vela toc\u00f3 o super\u00f3 la referencia."""
    if ref_val is None or bar_high is None or bar_low is None:
        return ""
    touched  = (bar_high >= ref_val * 0.9995) if is_high else (bar_low <= ref_val * 1.0005)
    exceeded = (bar_high > ref_val) if is_high else (bar_low < ref_val)
    if exceeded:
        return "  " + chr(0x1F525) + (" NEW HIGH" if lang == "en" else " NUEVO M\u00c1XIMO")
    elif touched:
        return "  " + chr(0x1F4A5) + (" TOUCHED" if lang == "en" else " TOCADO")
    return ""


def _build_day_context_lines(resultado: dict, lang: str) -> list[str]:
    """
    Muestra referencias ordenadas por precio (mayor a menor).
    Si la vela de la alerta toc\u00f3 o super\u00f3 alguna referencia, se marca con \u2757 o \ud83d\udd25.
    """
    lines = []
    day_ctx  = resultado.get("day_context")
    week_ctx = resultado.get("week_context")
    hod      = resultado.get("hod")
    lod      = resultado.get("lod")
    pdh      = resultado.get("pdh")
    pdl      = resultado.get("pdl")
    bar_high = resultado.get("bar_high")
    bar_low  = resultado.get("bar_low")

    has_any = day_ctx or week_ctx or hod is not None or lod is not None or pdh is not None or pdl is not None
    if not has_any:
        return lines

    def dir_arrow(d):
        return chr(0x1F4C8) if d == "above" else (chr(0x1F4C9) if d == "below" else chr(0x2194) + chr(0xFE0F))

    def _rl(ref_val, is_high):
        if ref_val is None or bar_high is None or bar_low is None:
            return ""
        touched  = (bar_high >= ref_val * 0.9995) if is_high else (bar_low <= ref_val * 1.0005)
        exceeded = (bar_high > ref_val) if is_high else (bar_low < ref_val)
        if exceeded:
            return "  " + chr(0x1F525) + (" NEW HIGH" if lang == "en" else " NUEVO M\u00c1XIMO")
        elif touched:
            return "  " + chr(0x1F4A5) + (" TOUCHED" if lang == "en" else " TOCADO")
        return ""

    # Coleccionar referencias como (precio, texto)
    refs = []

    if day_ctx:
        do = day_ctx["open"]
        pct = day_ctx["pct"]
        arr = dir_arrow(day_ctx["direction"])
        txt = (f"  {arr} Day open: <code>{do:.5g}</code>  <i>{pct:+.2f}%</i>"
               if lang == "en" else
               f"  {arr} Apertura d\u00eda: <code>{do:.5g}</code>  <i>{pct:+.2f}%</i>")
        refs.append((do, txt))

    if week_ctx:
        wo = week_ctx["open"]
        pct = week_ctx["pct"]
        arr = dir_arrow(week_ctx["direction"])
        txt = (f"  {arr} Week open: <code>{wo:.5g}</code>  <i>{pct:+.2f}%</i>"
               if lang == "en" else
               f"  {arr} Apertura semana: <code>{wo:.5g}</code>  <i>{pct:+.2f}%</i>")
        refs.append((wo, txt))

    if pdh is not None:
        lbl = _rl(pdh, True)
        txt = (f"  {chr(0x2B06) + chr(0xFE0F)} Prev day high (PDH): <code>{pdh:.5g}</code>{lbl}"
               if lang == "en" else
               f"  {chr(0x2B06) + chr(0xFE0F)} M\u00e1ximo d\u00eda anterior (PDH): <code>{pdh:.5g}</code>{lbl}")
        refs.append((pdh, txt))

    if hod is not None:
        lbl = _rl(hod, True)
        txt = (f"  {chr(0x1F4C8)} Day high (HOD): <code>{hod:.5g}</code>{lbl}"
               if lang == "en" else
               f"  {chr(0x1F4C8)} M\u00e1ximo d\u00eda (HOD): <code>{hod:.5g}</code>{lbl}")
        refs.append((hod, txt))

    if lod is not None:
        lbl = _rl(lod, False)
        txt = (f"  {chr(0x1F4C9)} Day low (LOD): <code>{lod:.5g}</code>{lbl}"
               if lang == "en" else
               f"  {chr(0x1F4C9)} M\u00ednimo d\u00eda (LOD): <code>{lod:.5g}</code>{lbl}")
        refs.append((lod, txt))

    if pdl is not None:
        lbl = _rl(pdl, False)
        txt = (f"  {chr(0x2B07) + chr(0xFE0F)} Prev day low (PDL): <code>{pdl:.5g}</code>{lbl}"
               if lang == "en" else
               f"  {chr(0x2B07) + chr(0xFE0F)} M\u00ednimo d\u00eda anterior (PDL): <code>{pdl:.5g}</code>{lbl}")
        refs.append((pdl, txt))

    # Ordenar por precio descendente (m\u00e1s alto arriba)
    refs.sort(key=lambda x: x[0], reverse=True)

    lines.append("")
    lines.append(chr(0x1F4CC) + (" <b>References (sorted by price):</b>"
             if lang == "en" else
             " <b>Referencias ordenadas por precio:</b>"))
    for _, txt in refs:
        lines.append(txt)

    return lines


def _build_components_context_lines(ticker: str, components_ctx: dict | None, lang: str) -> list[str]:
    """
    Muestra los tickers concretos que suben/bajan dentro del índice.
    La conclusión direccional ya está en la confluencia ⑥ de la matriz.
    Solo se muestra si hay datos (^DJI / ^NDX).
    """
    if not components_ctx:
        return []

    bulls  = components_ctx.get("bulls", [])
    bears  = components_ctx.get("bears", [])
    total  = components_ctx.get("total", 0)
    if total == 0:
        return []

    index_name = ASSET_NAMES.get(ticker.upper(), ticker)
    lines = [""]

    if lang == "en":
        lines.append(f"🏢 <b>{index_name} components (vs today's open):</b>")
        if bulls:
            lines.append(f"  🟢 Up: {', '.join(bulls[:6])}{'…' if len(bulls) > 6 else ''}")
        if bears:
            lines.append(f"  🔴 Down: {', '.join(bears[:6])}{'…' if len(bears) > 6 else ''}")
    else:
        lines.append(f"🏢 <b>Componentes {index_name} (vs apertura del día):</b>")
        if bulls:
            lines.append(f"  🟢 Subiendo: {', '.join(bulls[:6])}{'…' if len(bulls) > 6 else ''}")
        if bears:
            lines.append(f"  🔴 Bajando: {', '.join(bears[:6])}{'…' if len(bears) > 6 else ''}")

    return lines


# ── Mensaje rico por confluencias ────────────────────────────

def _build_confluencia_msg(resultado: dict, hora: str, dia_name: str, now_str: str,
                           lang: str = "es", components_ctx: dict | None = None,
                           ts_utc_iso: str = "", timezone: str = "UTC") -> str:
    """
    Construye el mensaje Telegram de la matriz de confluencias.
    Muestra la dirección real (LARGO / CORTO) y alerta si hay contradicción.
    """
    t             = resultado.get("ticker", "")
    name          = ASSET_NAMES.get(t, t)
    precio        = resultado.get("precio", 0)
    rsi           = resultado.get("rsi")
    puntos        = resultado.get("puntos", 0)
    estado        = resultado.get("estado", "NO AHORA")
    direction     = resultado.get("direction", "info")
    contradiccion = resultado.get("contradiccion", False)
    confs         = resultado.get("confluencias", [])

    hora_display = hora
    tz_label     = "UTC"
    dia_display  = dia_name
    if ts_utc_iso and timezone and timezone != "UTC":
        h_local, d_local, tz_lbl = _format_hora_tz(ts_utc_iso, timezone)
        if h_local:
            hora_display = h_local
            dia_display  = d_local
            tz_label     = tz_lbl

    if contradiccion:
        estado_emoji = "⚠️"
    elif estado == "FAVORABLE":
        estado_emoji = "🟢" if direction == "bullish" else "🔴"
    elif estado == "INTERESANTE":
        estado_emoji = "🔵"
    elif estado == "CONSIDERAR":
        estado_emoji = "🟡"
    else:
        estado_emoji = "⚪"

    dia_map_es = {"Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miércoles",
                  "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sábado","Sunday":"Domingo"}
    dia_label = dia_map_es.get(dia_display, dia_display) if lang == "es" else dia_display

    if lang == "en":
        conf_label = f"{puntos} points"
        sec_header = "<b>Active confluences:</b>"
        candle_lbl = "Range bar"
        contr_warn = ("⚠️ <b>CONFLICTING SIGNALS</b> — confluences point in opposite directions.") if contradiccion else ""
    else:
        conf_label = f"{puntos} puntos"
        sec_header = "<b>Confluencias activas:</b>"
        candle_lbl = "Vela 1H"
        contr_warn = ("⚠️ <b>SEÑALES CONTRADICTORIAS</b> — las confluencias apuntan en direcciones opuestas.") if contradiccion else ""

    is_rsi_rt = resultado.get("rsi_realtime", False)
    rt_banner = ""
    if is_rsi_rt:
        if lang == "en":
            rt_banner = "⚡ <b>RSI REAL-TIME ALERT</b> — RSI just entered the extreme zone!"
        else:
            rt_banner = "⚡ <b>ALERTA RSI EN TIEMPO REAL</b> — ¡El RSI acaba de entrar en zona extrema!"

    lines = [
        f"<b>⬡ TRADING BAND · {now_str}</b>",
    ]
    if rt_banner:
        lines.append(rt_banner)
    lines.append("")
    lines.append(f"<b>📊 {name}</b>  |  <b>{precio:,.5g}</b>")

    if hora_display:
        lines.append(f"🕐 {candle_lbl} · {dia_label} {hora_display} {tz_label}")

    rsi_str = f"RSI {rsi:.1f}" if rsi is not None else "RSI —"
    estado_line = f"{estado_emoji} <b>{estado}</b>  ·  {rsi_str}  ·  {conf_label}"
    lines.append(estado_line)

    if contr_warn:
        lines.append("")
        lines.append(contr_warn)

    lines.append("")
    lines.append(sec_header)

    for c in confs:
        en_conflicto = c.get("conflicto", False)
        activa       = c.get("ok", False)
        tipo         = c.get("tipo", "info")
        descartada   = c.get("descartada", False)

        if en_conflicto:
            icon = "❌"
        elif descartada:
            icon = "🚫"
        elif activa and tipo == "bullish":
            icon = "✅🟢"
        elif activa and tipo == "bearish":
            icon = "✅🔴"
        elif activa and tipo == "neutral":
            icon = "✅⚪"
        else:
            icon = "◻️"

        texto_escapado = html.escape(c['texto'], quote=False)
        lines.append(f"{icon} {texto_escapado}")

    lines.extend(_build_day_context_lines(resultado, lang))
    lines.extend(_build_components_context_lines(t, components_ctx, lang))

    tv = _tv_link(t, lang)
    if tv:
        lines.append("")
        lines.append(tv)

    lines.append("")
    if lang == "en":
        lines.append("<i>Automated technical analysis · Not financial advice</i>")
    else:
        lines.append("<i>Análisis técnico automatizado · No es asesoría financiera</i>")

    return "\n".join(lines)


def _build_tg_for_user(alerts_by_ticker: dict, now_str: str, lang: str = "es",
                       timezone: str = "UTC") -> str:
    """
    Wrapper inteligente: usa _build_confluencia_msg si hay 'resultado',
    y _build_tg_grouped como fallback para alertas antiguas.
    """
    has_resultado = any(
        a.get("resultado")
        for al in alerts_by_ticker.values()
        for a in al
    )
    if has_resultado:
        blocks = [f"<b>⬡ Trading Band · {now_str}</b>"]
        for ticker, alertas in alerts_by_ticker.items():
            for a in alertas:
                res = a.get("resultado")
                if res:
                    msg = _build_confluencia_msg(
                        res,
                        hora=a.get("hora", ""),
                        dia_name=a.get("dia_name", ""),
                        now_str=now_str,
                        lang=lang,
                        components_ctx=a.get("components_ctx"),
                        ts_utc_iso=a.get("ts_utc_iso", ""),
                        timezone=timezone,
                    )
                    # Omitir la primera línea (cabecera) para no duplicarla
                    body = "\n".join(msg.split("\n")[1:])
                    blocks.append(body)
        return "\n".join(blocks)
    return _build_tg_grouped(alerts_by_ticker, now_str, lang=lang)


# ── Función principal — por usuario ─────────────────────────

async def notify_users_with_alerts(alerts_by_ticker: dict) -> None:
    """
    alerts_by_ticker = {"^DJI": [alertas], "GC=F": [alertas], ...}
    Envía a cada usuario solo las alertas de sus tickers elegidos.
    Incluye también a suscriptores básicos de Telegram (vía /start) sin preferencias configuradas.
    """
    if not alerts_by_ticker:
        return

    now_str          = datetime.now(ZoneInfo('Europe/Madrid')).strftime("%d/%m/%Y %H:%M")
    all_alertas_flat = [a for al in alerts_by_ticker.values() for a in al]

    # Obtener prefs completas, subs básicas, y mapa timezone para subs básicos en paralelo
    all_prefs, basic_chat_ids, extra_prefs_raw = await asyncio.gather(
        get_all_user_prefs(),
        get_chat_ids(),
        _supa_get(
            "notification_prefs"
            "?telegram_chat_id=not.is.null"
            "&select=telegram_chat_id,timezone"
        )
    )
    # Mapa chat_id → {timezone, language} para usuarios que guardaron prefs aunque no tengan telegram_enabled
    chat_prefs_map: dict = {}
    for p in (extra_prefs_raw or []):
        cid_p = p.get("telegram_chat_id")
        if cid_p:
            chat_prefs_map[int(cid_p)] = p

    loop = asyncio.get_running_loop()
    covered_chat_ids: set = set()

    # 1 — Usuarios con preferencias configuradas (alertas personalizadas por ticker)
    if all_prefs:
        print(f"[notifier] {len(all_prefs)} usuario(s) con preferencias activas")
        for prefs in all_prefs:
            user_tickers = [t.upper() for t in (prefs.get("tickers") or [])]

            # Sin tickers configurados → recibe todos
            if not user_tickers:
                user_alertas = all_alertas_flat
            else:
                user_alertas = [a for t in user_tickers for a in alerts_by_ticker.get(t, [])]

            if not user_alertas:
                continue

            # Build grouped dict for this user's tickers
            if not user_tickers:
                user_by_ticker = alerts_by_ticker
            else:
                user_by_ticker = {t: alerts_by_ticker[t] for t in user_tickers if alerts_by_ticker.get(t)}

            if not user_by_ticker:
                continue

            lang     = "es"
            timezone = prefs.get("timezone", "UTC") or "UTC"

            if prefs.get("telegram_enabled") and prefs.get("telegram_chat_id"):
                cid = int(prefs["telegram_chat_id"])
                covered_chat_ids.add(cid)
                for tkr, tkr_alertas in user_by_ticker.items():
                    if tkr_alertas:
                        texto_tg = _build_tg_for_user({tkr: tkr_alertas}, now_str, lang=lang, timezone=timezone)
                        await send_telegram_to(cid, texto_tg)
                        await asyncio.sleep(0.3)

            if prefs.get("email_enabled") and prefs.get("email_address"):
                html = _build_html_grouped(user_by_ticker, now_str, lang=lang)
                await loop.run_in_executor(
                    None, _smtp_send, prefs["email_address"],
                    f"⬡ Trading Band · {now_str}", html
                )

    # 2 — Suscriptores básicos de Telegram (/start) sin preferencias configuradas
    if TELEGRAM_TOKEN and all_alertas_flat and basic_chat_ids:
        nuevos = 0
        for cid in basic_chat_ids:
            cid_int = int(cid)
            if cid_int not in covered_chat_ids:
                user_p   = chat_prefs_map.get(cid_int, {})
                user_tz  = user_p.get("timezone") or "UTC"
                user_lang = "es"
                if user_tz != "UTC":
                    print(f"[notifier] Suscriptor básico {cid_int}: usando timezone={user_tz}, lang={user_lang}")
                for tkr, tkr_alertas in alerts_by_ticker.items():
                    if tkr_alertas:
                        texto_base = _build_tg_for_user(
                            {tkr: tkr_alertas}, now_str,
                            lang=user_lang, timezone=user_tz
                        )
                        await send_telegram_to(cid_int, texto_base)
                        await asyncio.sleep(0.3)
                nuevos += 1
        if nuevos:
            print(f"[notifier] {nuevos} suscriptor(es) básico(s) notificados")

    if not all_prefs and not basic_chat_ids:
        print("[notifier] Sin usuarios con notificaciones activas")


# ── Alertas de divergencias RSI ──────────────────────────────

_DIV_TYPE_LABELS = {
    "bull":  ("📈 Divergencia Alcista Regular",  "📈 Regular Bullish Divergence"),
    "hbull": ("📈 Divergencia Alcista Oculta",   "📈 Hidden Bullish Divergence"),
    "bear":  ("📉 Divergencia Bajista Regular",  "📉 Regular Bearish Divergence"),
    "hbear": ("📉 Divergencia Bajista Oculta",   "📉 Hidden Bearish Divergence"),
}


def _build_div_tg_msg(ticker: str, divs: list, now_str: str, lang: str = "es") -> str:
    name = ASSET_NAMES.get(ticker.upper(), ticker)
    tv   = _tv_link(ticker, lang)

    if lang == "en":
        header = f"<b>📊 RSI Divergence · {now_str}</b>"
        price_lbl = "Price"
    else:
        header = f"<b>📊 Divergencia RSI · {now_str}</b>"
        price_lbl = "Precio"

    lines = [header, "", f"<b>{name}</b>", ""]

    for dv in divs:
        idx        = 1 if lang == "en" else 0
        type_label = _DIV_TYPE_LABELS.get(dv["type"], (dv["type"], dv["type"]))[idx]
        level_lbl  = f"N{dv['level']}"
        rsi_val    = dv["rsi"]
        in_zone    = dv.get("in_zone", False)

        if lang == "en":
            zone_tag = "  ✅ <i>in zone</i>" if in_zone else "  ⬜ <i>out of zone</i>"
        else:
            zone_tag = "  ✅ <i>en zona</i>" if in_zone else "  ⬜ <i>fuera de zona</i>"

        lines.append(f"{type_label}  ·  {level_lbl}")
        lines.append(f"📊 RSI: <code>{rsi_val:.1f}</code>{zone_tag}")
        lines.append(f"💰 {price_lbl}: <code>{dv['price']:.5g}</code>")
        lines.append("")

    if tv:
        lines.append("")
        lines.append(tv)
    lines.append("")
    if lang == "en":
        lines.append("<i>Automated technical analysis · Not financial advice</i>")
    else:
        lines.append("<i>Análisis técnico automatizado · No es asesoría financiera</i>")

    return "\n".join(lines)


def _build_div_html(divs_by_ticker: dict, now_str: str) -> str:
    color_map = {"bull": "#00cc33", "hbull": "#00cc33", "bear": "#ff3333", "hbear": "#ff3333"}
    rows = ""
    for ticker, divs in divs_by_ticker.items():
        name = ASSET_NAMES.get(ticker.upper(), ticker)
        rows += (
            f'<tr><td style="padding:8px 10px 4px;font-family:monospace;font-size:12px;'
            f'color:#00ff41;font-weight:bold;border-top:1px solid #0a1a0a">📊 {name}</td></tr>'
        )
        for dv in divs:
            c         = color_map.get(dv["type"], "#888")
            lbl_es, _ = _DIV_TYPE_LABELS.get(dv["type"], (dv["type"], dv["type"]))
            level_lbl = f"N{dv['level']}"
            in_zone   = "✅ en zona" if dv.get("in_zone") else "⬜ fuera de zona"
            rows += (
                f'<tr><td style="padding:3px 10px 3px 20px;border-bottom:1px solid #1a2a1a;'
                f'color:{c};font-family:monospace;font-size:13px">'
                f'{lbl_es} · {level_lbl} · RSI {dv["rsi"]:.1f} · {dv["price"]:.5g} · {in_zone}</td></tr>'
            )
    return f"""<html><body style="background:#000;padding:20px;">
      <div style="max-width:600px;margin:auto;background:#010801;border:1px solid #00ff4120;border-radius:8px;overflow:hidden;">
        <div style="background:#010f01;padding:14px 20px;border-bottom:1px solid #00ff4115;">
          <span style="font-family:monospace;font-size:14px;color:#00ff41;font-weight:bold;">📊 RSI Divergencias · Trading Band</span>
          <span style="font-family:monospace;font-size:11px;color:#666;margin-left:10px;">{now_str}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
        <div style="padding:10px 20px;font-size:10px;color:#333;font-family:monospace;border-top:1px solid #00ff4110;text-align:center;">
          Análisis técnico automatizado · No es asesoría financiera
        </div>
      </div></body></html>"""


async def notify_divergences(divs_by_ticker: dict) -> None:
    """
    Send RSI divergence alerts to all subscribed users.
    divs_by_ticker: {ticker_upper: [div_dict, ...]}
    Each div_dict has: type, level, bar, time, rsi, price, in_zone
    """
    if not divs_by_ticker:
        return

    now_str = datetime.now(ZoneInfo('Europe/Madrid')).strftime("%d/%m/%Y %H:%M")

    all_prefs, basic_chat_ids = await asyncio.gather(
        get_all_user_prefs(),
        get_chat_ids(),
    )

    loop = asyncio.get_running_loop()
    covered_chat_ids: set = set()

    def _filter_divs_by_levels(ticker_divs: dict, enabled_levels: list) -> dict:
        """Keep only divs whose level is in the enabled_levels list."""
        result = {}
        for tkr, divs in ticker_divs.items():
            filtered = [d for d in divs if d.get("level", 1) in enabled_levels]
            if filtered:
                result[tkr] = filtered
        return result

    if all_prefs:
        for prefs in all_prefs:
            user_tickers = [t.upper() for t in (prefs.get("tickers") or [])]
            if not user_tickers:
                base_divs = divs_by_ticker
            else:
                base_divs = {t: divs_by_ticker[t] for t in user_tickers if divs_by_ticker.get(t)}

            if not base_divs:
                continue

            raw_levels = prefs.get("div_levels")
            enabled_levels = raw_levels if isinstance(raw_levels, list) and raw_levels else [1, 2]
            user_divs = _filter_divs_by_levels(base_divs, enabled_levels)

            if not user_divs:
                continue

            lang = "es"

            if prefs.get("telegram_enabled") and prefs.get("telegram_chat_id"):
                cid = int(prefs["telegram_chat_id"])
                covered_chat_ids.add(cid)
                for tkr, divs in user_divs.items():
                    msg = _build_div_tg_msg(tkr, divs, now_str, lang)
                    await send_telegram_to(cid, msg)
                    await asyncio.sleep(0.3)

            if prefs.get("email_enabled") and prefs.get("email_address"):
                html = _build_div_html(user_divs, now_str)
                await loop.run_in_executor(
                    None, _smtp_send, prefs["email_address"],
                    f"📊 RSI Divergencia · {now_str}", html
                )

    if TELEGRAM_TOKEN and basic_chat_ids:
        default_divs = _filter_divs_by_levels(divs_by_ticker, [1, 2])
        for cid in basic_chat_ids:
            cid_int = int(cid)
            if cid_int not in covered_chat_ids:
                for tkr, divs in default_divs.items():
                    msg = _build_div_tg_msg(tkr, divs, now_str, "es")
                    await send_telegram_to(cid_int, msg)
                    await asyncio.sleep(0.3)

    print(f"[notifier] Divergencias RSI enviadas — {sum(len(v) for v in divs_by_ticker.values())} divergencia(s) en {len(divs_by_ticker)} ticker(s)")


# ── Setup Engine — ELIMINADO (código legacy) ──────────────────


def _fmt_price(p: float) -> str:
    """Formatea un precio con decimales adecuados."""
    if p is None:
        return "—"
    if p >= 1000:
        return f"{p:,.0f}"
    if p >= 10:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.5f}"


# (Setup Engine, notify_setup_alerts — ELIMINADOS, código legacy)


# ── Compatibilidad con scheduler existente ───────────────────

async def notify_alertas(alertas: list[dict], source: str = "") -> None:
    """Broadcast a todos los chat_ids registrados."""
    if not alertas:
        return
    now_str = datetime.now(ZoneInfo('Europe/Madrid')).strftime("%d/%m/%Y %H:%M")
    # Group by ticker
    by_ticker: dict = {}
    for a in alertas:
        m = re.match(r'^\[([^\]]+)\]', a.get('msg', ''))
        tk = m.group(1) if m else 'GENERAL'
        by_ticker.setdefault(tk, []).append(a)
    texto = _build_tg_grouped(by_ticker, now_str)
    if TELEGRAM_TOKEN:
        for cid in await get_chat_ids():
            await send_telegram_to(cid, texto)
            await asyncio.sleep(0.05)
