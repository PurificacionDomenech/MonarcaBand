"""
notifier.py — The Matrix Lab
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
import asyncio
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx

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

RISK_WARNING_ES = "\n❗️ <b>No arriesgar más de un 1% del balance de tu cuenta en un trade!</b>"
RISK_WARNING_EN = "\n❗️ <b>Don't risk more than 1% of the balance of your account in any trade!</b>"
RISK_WARNING = RISK_WARNING_ES  

def _translate_en(msg: str) -> str:
    msg = re.sub(r'Precio cruza (EMA\d+) al alza',  r'Price crosses \1 upward',   msg)
    msg = re.sub(r'Precio cruza (EMA\d+) a la baja', r'Price crosses \1 downward', msg)
    msg = re.sub(r'Precio tocando (EMA\d+)',          r'Price touching \1',          msg)
    msg = msg.replace('Vela toca fractal MAYOR SOPORTE',     'Candle touches MAJOR fractal SUPPORT')
    msg = msg.replace('Vela toca fractal MAYOR RESISTENCIA', 'Candle touches MAJOR fractal RESISTANCE')
    msg = msg.replace('Vela toca fractal SOPORTE',           'Candle touches fractal SUPPORT')
    msg = msg.replace('Vela toca fractal RESISTENCIA',       'Candle touches fractal RESISTANCE')
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
        if rsi > 70:   return "overbought"
        if rsi < 30:   return "oversold"
        if rsi > 60:   return "approaching overbought"
        if rsi < 40:   return "approaching oversold"
        return "neutral"
    else:
        if rsi > 70:   return "sobrecompra"
        if rsi < 30:   return "sobreventa"
        if rsi > 60:   return "zona alta"
        if rsi < 40:   return "zona baja"
        return "zona neutra"

def _signal_lines(a: dict, lang: str) -> list[str]:
    """Genera las líneas explicativas para una alerta concreta."""
    tipo  = a.get("tipo", "")
    nivel = a.get("nivel", "info")
    close = a.get("close")
    lines = []

    if tipo in ("ema_cross_up", "ema_cross_down", "ema_touch"):
        ema_n = a.get("ema_nombre", "EMA")
        ema_v = a.get("ema_val")
        is_800 = "800" in ema_n
        if lang == "en":
            if tipo == "ema_cross_up":
                desc = f"Price breaks above {ema_n}"
                role = "(key resistance broken)" if is_800 else ""
            elif tipo == "ema_cross_down":
                desc = f"Price breaks below {ema_n}"
                role = "(key support lost)" if is_800 else ""
            else:
                desc = f"Price testing {ema_n}"
                role = "(major support/resistance)" if is_800 else ""
        else:
            if tipo == "ema_cross_up":
                desc = f"Precio supera la {ema_n} al alza"
                role = "(resistencia clave superada)" if is_800 else ""
            elif tipo == "ema_cross_down":
                desc = f"Precio rompe la {ema_n} a la baja"
                role = "(soporte clave perdido)" if is_800 else ""
            else:
                desc = f"Precio testeando la {ema_n}"
                role = "(soporte/resistencia mayor)" if is_800 else ""
        ema_str = f"<code>{ema_v:.5g}</code>" if ema_v else ""
        lines.append(f"📈 {desc} {ema_str} {role}".strip())

    elif tipo in ("golden_cross", "death_cross"):
        if lang == "en":
            if tipo == "golden_cross":
                lines.append("⚡ Golden Cross — bullish EMA crossover")
            else:
                lines.append("⚡ Death Cross — bearish EMA crossover")
        else:
            if tipo == "golden_cross":
                lines.append("⚡ Golden Cross — cruce alcista de medias")
            else:
                lines.append("⚡ Death Cross — cruce bajista de medias")

    elif tipo == "fractal":
        f_tipo   = a.get("fractal_tipo", "")
        f_precio = a.get("fractal_precio")
        f_mayor  = a.get("fractal_mayor", False)
        f_str    = f"<code>{f_precio:.5g}</code>" if f_precio else ""
        mayor_tag = ("MAYOR " if f_mayor else "")
        if lang == "en":
            role = "support" if f_tipo == "soporte" else "resistance"
            lines.append(f"⬡ Candle touches {'major ' if f_mayor else ''}fractal {role.upper()} at {f_str}")
        else:
            role = "SOPORTE" if f_tipo == "soporte" else "RESISTENCIA"
            lines.append(f"⬡ Vela toca fractal {mayor_tag}{role} en {f_str}")

    return lines


def _build_tg_grouped(alerts_by_ticker: dict, now_str: str, lang: str = "es") -> str:
    labels  = NIVEL_LABEL_EN if lang == "en" else NIVEL_LABEL
    dia_map = _DIA_EN if lang == "en" else _DIA_ES
    blocks  = [f"<b>⬡ Matrix Lab · {now_str}</b>"]

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
                    blocks.append(f"🕐 Range bar · {dia_es_str}  {hora} UTC")
                else:
                    blocks.append(f"🕐 Vela de rango · {dia_es_str}  {hora} UTC")

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

    blocks.append(RISK_WARNING_EN if lang == "en" else RISK_WARNING_ES)
    blocks.append("")
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
    risk_row = (
        '<tr><td style="padding:10px;font-family:monospace;font-size:11px;'
        'color:#ffaa00;border-top:1px solid #2a2000;background:rgba(255,170,0,0.05);">'
        '❗️ No arriesgar más de un 1% del balance de tu cuenta en un trade!<br>'
        '❗️ Don\'t risk more than 1% of the balance of your account in any trade!'
        '</td></tr>'
    )
    return f"""<html><body style="background:#000;padding:20px;">
      <div style="max-width:600px;margin:auto;background:#010801;border:1px solid #00ff4120;border-radius:8px;overflow:hidden;">
        <div style="background:#010f01;padding:14px 20px;border-bottom:1px solid #00ff4115;">
          <span style="font-family:monospace;font-size:14px;color:#00ff41;font-weight:bold;">⬡ THE MATRIX LAB</span>
          <span style="font-family:monospace;font-size:11px;color:#666;margin-left:10px;">{now_str}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;">{rows}{risk_row}</table>
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


# ── Telegram subs (registro via /start) ─────────────────────

async def get_chat_ids() -> list[int]:
    rows = await _supa_get("telegram_subs?select=chat_id")
    return [r["chat_id"] for r in rows if r.get("chat_id")]


async def register_chat(chat_id: int, username: str = "") -> bool:
    return await _supa_post(
        "telegram_subs",
        {"chat_id": chat_id, "username": username or ""},
        prefer="resolution=merge-duplicates"
    )


# ── Preferencias por usuario ─────────────────────────────────

async def get_user_prefs(user_id: str) -> dict:
    rows = await _supa_get(f"notification_prefs?user_id=eq.{user_id}&select=*")
    return rows[0] if rows else {}


async def save_user_prefs(user_id: str, prefs: dict) -> bool:
    """Guarda preferencias de notificación del usuario (INSERT o UPDATE según exista)."""
    payload = {
        "telegram_chat_id": prefs.get("telegram_chat_id"),
        "telegram_enabled": bool(prefs.get("telegram_enabled", False)),
        "email_address":    prefs.get("email_address", "") or "",
        "email_enabled":    bool(prefs.get("email_enabled", False)),
        "tickers":          prefs.get("tickers", []),
        "timezone":         prefs.get("timezone", "UTC") or "UTC",
    }
    # ¿Ya existe el registro?
    existing = await _supa_get(f"notification_prefs?user_id=eq.{user_id}&select=id")
    if existing:
        # UPDATE — PATCH con filtro por user_id
        ok = await _supa_patch(f"notification_prefs?user_id=eq.{user_id}", payload)
        if ok:
            print(f"[notifier] Prefs actualizadas para {user_id[:8]}…")
        return ok
    else:
        # INSERT — POST con user_id incluido
        payload["user_id"] = user_id
        ok = await _supa_post("notification_prefs", payload, prefer="")
        if ok:
            print(f"[notifier] Prefs creadas para {user_id[:8]}…")
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
          <span style="font-family:monospace;font-size:14px;color:#00ff41;font-weight:bold;">⬡ THE MATRIX LAB</span>
          <span style="font-family:monospace;font-size:11px;color:#666;margin-left:10px;">{now_str}</span>
        </div>
        <table style="width:100%;border-collapse:collapse;">{filas}</table>
        <div style="padding:10px 20px;font-size:10px;color:#333;font-family:monospace;border-top:1px solid #00ff4110;text-align:center;">
          Análisis técnico automatizado · No es asesoría financiera
        </div>
      </div></body></html>"""


# ── Contexto informativo (precio vs aperturas y componentes) ─

def _build_day_context_lines(resultado: dict, lang: str) -> list[str]:
    """
    Muestra los valores numéricos de apertura día/semana/año como referencia.
    La conclusión direccional ya se incluye en la confluencia ④, por lo que
    aquí solo se muestran los datos crudos para que el trader los tenga a mano.
    """
    lines = []
    day_ctx  = resultado.get("day_context")
    week_ctx = resultado.get("week_context")

    if not day_ctx and not week_ctx:
        return lines

    def dir_arrow(d):
        return "📈" if d == "above" else ("📉" if d == "below" else "↔️")

    if lang == "en":
        lines.append("")
        lines.append("📌 <b>Reference opens:</b>")
        if day_ctx:
            do  = day_ctx["open"]
            pct = day_ctx["pct"]
            arr = dir_arrow(day_ctx["direction"])
            lines.append(f"  {arr} Day open: <code>{do:.5g}</code>  <i>{pct:+.2f}%</i>")
        if week_ctx:
            wo  = week_ctx["open"]
            pct = week_ctx["pct"]
            arr = dir_arrow(week_ctx["direction"])
            lines.append(f"  {arr} Week open: <code>{wo:.5g}</code>  <i>{pct:+.2f}%</i>")
    else:
        lines.append("")
        lines.append("📌 <b>Referencias de apertura:</b>")
        if day_ctx:
            do  = day_ctx["open"]
            pct = day_ctx["pct"]
            arr = dir_arrow(day_ctx["direction"])
            lines.append(f"  {arr} Apertura día: <code>{do:.5g}</code>  <i>{pct:+.2f}%</i>")
        if week_ctx:
            wo  = week_ctx["open"]
            pct = week_ctx["pct"]
            arr = dir_arrow(week_ctx["direction"])
            lines.append(f"  {arr} Apertura semana: <code>{wo:.5g}</code>  <i>{pct:+.2f}%</i>")

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
    t             = resultado["ticker"]
    name          = ASSET_NAMES.get(t, t)
    precio        = resultado["precio"]
    rsi           = resultado["rsi"]
    puntos        = resultado["puntos"]
    estado        = resultado["estado"]
    direction     = resultado.get("direction", "info")
    contradiccion = resultado.get("contradiccion", False)
    confs         = resultado["confluencias"]

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

    if not contradiccion and direction in ("bullish", "bearish"):
        if lang == "en":
            dir_label = "📈 LONG setup" if direction == "bullish" else "📉 SHORT setup"
        else:
            dir_label = "📈 Setup LARGO" if direction == "bullish" else "📉 Setup CORTO"
    else:
        dir_label = ""

    dia_map_es = {"Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miércoles",
                  "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sábado","Sunday":"Domingo"}
    dia_label = dia_map_es.get(dia_display, dia_display) if lang == "es" else dia_display

    max_confs = resultado.get("max_confs", 5)
    if lang == "en":
        conf_label = f"{puntos}/{max_confs} confluences"
        sec_header = "<b>Active confluences:</b>"
        candle_lbl = "Range bar"
        contr_warn = ("⚠️ <b>CONFLICTING SIGNALS</b> — confluences point in opposite directions. "
                      "No valid setup.") if contradiccion else ""
    else:
        conf_label = f"{puntos}/{max_confs} confluencias"
        sec_header = "<b>Confluencias activas:</b>"
        candle_lbl = "Vela de rango"
        contr_warn = ("⚠️ <b>SEÑALES CONTRADICTORIAS</b> — las confluencias apuntan en direcciones "
                      "opuestas. Setup no válido.") if contradiccion else ""

    is_rsi_rt = resultado.get("rsi_realtime", False)
    rt_banner = ""
    if is_rsi_rt:
        if lang == "en":
            rt_banner = "⚡ <b>RSI REAL-TIME ALERT</b> — RSI just entered the extreme zone!"
        else:
            rt_banner = "⚡ <b>ALERTA RSI EN TIEMPO REAL</b> — ¡El RSI acaba de entrar en zona extrema!"

    lines = [
        f"<b>⬡ MATRIX LAB · {now_str}</b>",
    ]
    if rt_banner:
        lines.append(rt_banner)
    lines.append("")
    lines.append(f"<b>📊 {name}</b>  |  <b>{precio:,.5g}</b>")

    if hora_display:
        lines.append(f"🕐 {candle_lbl} · {dia_label} {hora_display} {tz_label}")

    estado_line = f"{estado_emoji} <b>{estado}</b>  ·  RSI {rsi:.1f}  ·  {conf_label}"
    if dir_label:
        estado_line += f"  ·  {dir_label}"
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

        lines.append(f"{icon} {c['texto']}")

    # ── Bloque especial Aleta Tiburón ──
    shark_conf = next((c for c in confs if c.get("id") == 7 and c.get("ok")), None)
    if shark_conf and shark_conf.get("alert_immediate"):
        lines.append("")
        if "EXTREMA" in shark_conf.get("texto", ""):
            if lang == "en":
                lines.append("⚡🦈 <b>EXTREME SHARK FIN</b> — RSI peak exceeded prior divergence")
                lines.append("   This is a maximum exhaustion signal — immediate alert")
            else:
                lines.append("⚡🦈 <b>ALETA TIBURÓN EXTREMA</b> — pico RSI superó divergencia previa")
                lines.append("   Señal de agotamiento máximo — alerta inmediata")
        else:
            if lang == "en":
                lines.append("🦈 <b>Shark fin confirmed</b> — RSI crossed back from extreme zone")
            else:
                lines.append("🦈 <b>Aleta tiburón confirmada</b> — RSI cruzó de vuelta la zona extrema")

    lines.extend(_build_day_context_lines(resultado, lang))
    lines.extend(_build_components_context_lines(t, components_ctx, lang))

    lines.append("")
    lines.append(RISK_WARNING_EN if lang == "en" else RISK_WARNING_ES)

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
        blocks = [f"<b>⬡ Matrix Lab · {now_str}</b>"]
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
                    f"⬡ Matrix Lab · {now_str}", html
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
