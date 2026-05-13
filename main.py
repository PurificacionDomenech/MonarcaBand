import yfinance as yf
import pandas as pd
import numpy as np
import asyncio
import time
import os
import httpx

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

try:
    from trading_band_routes import (
        router as tb_router,
        build_range_bars as _build_rb,
        TB_CONFIG as _TB_CFG,
        detect_all_divergences as _detect_divs,
        calc_rsi as _calc_rsi_tv,
    )
    HAS_TB = True
except Exception as _tb_err:
    HAS_TB = False
    _build_rb = None
    _TB_CFG = {}
    _detect_divs = None
    _calc_rsi_tv = None
    print(f"[WARN] trading_band_routes no disponible: {_tb_err}")

try:
    from user_routes import router as auth_router, users_router
    HAS_AUTH = True
except Exception as _auth_err:
    HAS_AUTH = False
    print(f"[WARN] user_routes no disponible: {_auth_err}")

try:
    from contextlib import asynccontextmanager
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    print("[WARN] apscheduler no instalado")

try:
    from notifier import (
        notify_alertas,
        notify_users_with_alerts,
        register_chat,
        send_telegram_to,
        get_user_prefs,
        save_user_prefs,
    )

    HAS_NOTIFIER = True
except Exception as e:
    HAS_NOTIFIER = False
    print(f"[WARN] notifier no disponible: {e}")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
WATCH_TICKERS = [
    "^DJI",
    "GC=F",
    "^NDX",
    "USDJPY=X",
    "GBPJPY=X",
    "EURUSD=X",
    "AUDUSD=X",
    "SI=F",
    "CL=F",
    "BTC-USD",
    "GBPUSD=X",
    "AUDJPY=X",
]

_sent_cache: dict = {}
_DEDUP_SECONDS = 1 * 3600  # 1 hora máximo para alertas recientes

_row_cache: dict = {}
_ROW_TTL = 300
_yf_lock = asyncio.Lock()

_rsi_watchlist: dict = {}
_RSI_WATCH_INTERVAL_MIN = 2

ASSET_CONFIG = {
    "^DJI": {
        "key_spacing": 500,
        "major_spacing": 1000,
        "zone_size": 100,
        "ema_short": 200,
        "ema_long": 800,
    },
    "^NDX": {
        "key_spacing": 500,
        "major_spacing": 1000,
        "zone_size": 100,
        "ema_short": 200,
        "ema_long": 800,
    },
    "GC=F": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 200,
        "ema_long": 800,
    },
    "GLD": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 200,
        "ema_long": 800,
    },
    "IAU": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 200,
        "ema_long": 800,
    },
    "SI=F": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "CL=F": {
        "key_spacing": 2,
        "major_spacing": 5,
        "zone_size": 0.5,
        "ema_short": 200,
        "ema_long": 800,
    },
    "USDJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "GBPJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "EURUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 200,
        "ema_long": 800,
    },
    "AUDUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 200,
        "ema_long": 800,
    },
    "BTC-USD": {
        "key_spacing": 1000,
        "major_spacing": 5000,
        "zone_size": 500,
        "ema_short": 200,
        "ema_long": 800,
    },
    "GBPUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 200,
        "ema_long": 800,
    },
    "AUDJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "^TNX": {
        "key_spacing": 0.1,
        "major_spacing": 0.5,
        "zone_size": 0.05,
        "ema_short": 200,
        "ema_long": 800,
    },
    "^TYX": {
        "key_spacing": 0.1,
        "major_spacing": 0.5,
        "zone_size": 0.05,
        "ema_short": 200,
        "ema_long": 800,
    },
    "DX=F": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "^GSPC": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 200,
        "ema_long": 800,
    },
    "SPY": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 200,
        "ema_long": 800,
    },
    "VOO": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 200,
        "ema_long": 800,
    },
    "^RUT": {
        "key_spacing": 25,
        "major_spacing": 50,
        "zone_size": 5,
        "ema_short": 200,
        "ema_long": 800,
    },
    "IWM": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 200,
        "ema_long": 800,
    },
    "BTC-USD": {
        "key_spacing": 1000,
        "major_spacing": 5000,
        "zone_size": 250,
        "ema_short": 200,
        "ema_long": 800,
    },
    "ETH-USD": {
        "key_spacing": 50,
        "major_spacing": 200,
        "zone_size": 25,
        "ema_short": 200,
        "ema_long": 800,
    },
    "QQQ": {
        "key_spacing": 10,
        "major_spacing": 20,
        "zone_size": 2,
        "ema_short": 200,
        "ema_long": 800,
    },
    "QQQM": {
        "key_spacing": 5,
        "major_spacing": 20,
        "zone_size": 1,
        "ema_short": 200,
        "ema_long": 800,
    },
    "GDX": {
        "key_spacing": 2,
        "major_spacing": 5,
        "zone_size": 0.5,
        "ema_short": 200,
        "ema_long": 800,
    },
    "SMH": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 200,
        "ema_long": 800,
    },
    "XLE": {
        "key_spacing": 2,
        "major_spacing": 10,
        "zone_size": 0.5,
        "ema_short": 200,
        "ema_long": 800,
    },
    "AAPL": {
        "key_spacing": 5,
        "major_spacing": 20,
        "zone_size": 1,
        "ema_short": 200,
        "ema_long": 800,
    },
    "_default": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 200,
        "ema_long": 800,
    },
}

# ── Componentes clave de índices para contexto ──────────────
INDEX_COMPONENTS = {
    "^DJI": ["AAPL", "MSFT", "JPM", "V", "UNH", "GS", "HD", "MCD", "CAT", "AXP"],
    "^NDX": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO", "COST", "NFLX"],
}


async def async_download(ticker, **kwargs):
    loop = asyncio.get_running_loop()
    async with _yf_lock:
        return await loop.run_in_executor(None, lambda: yf.download(ticker, **kwargs))


def get_cfg(t):
    return ASSET_CONFIG.get(t.upper(), ASSET_CONFIG["_default"])


def clean_df(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def safe(v):
    return float(v) if pd.notna(v) else None


def ts_ms(idx):
    return [int(t.timestamp() * 1000) for t in idx]


def calc_indicators(df, es=200, el=800):
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    df[f"EMA{es}"] = close.ewm(span=es, adjust=False).mean()
    df[f"EMA{el}"] = close.ewm(span=el, adjust=False).mean()
    d = close.diff()
    losses = (-d.where(d < 0, 0)).rolling(14).mean().replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + d.where(d > 0, 0).rolling(14).mean() / losses))
    return df


def calc_trading_band(df):
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    trigger = close.rolling(12).mean()
    average = trigger.rolling(12).mean()
    df["TB_TRIGGER"] = trigger
    df["TB_AVERAGE"] = average
    cross = pd.Series(0, index=df.index)
    prev_t = trigger.shift(1)
    prev_a = average.shift(1)
    cross[(trigger > average) & (prev_t <= prev_a)] = 1
    cross[(trigger < average) & (prev_t >= prev_a)] = -1
    df["TB_CROSS"] = cross
    return df


def calc_fractales(precio, cfg, n_above=30, n_below=30):
    ks, ms, zs = cfg["key_spacing"], cfg["major_spacing"], cfg["zone_size"]
    base = round(precio / ks) * ks
    levels = []
    for i in range(-n_below, n_above + 1):
        p = base + i * ks
        levels.append(
            {
                "price": p,
                "is_major": round(p % ms) == 0,
                "zone_top": p + zs,
                "zone_bot": p - zs,
            }
        )
    return {"levels": levels, "key_spacing": ks, "major_spacing": ms, "zone_size": zs}


def detect_fractal_touch(high, low, close, fractales):
    zs, best = fractales["zone_size"], None
    for level in fractales["levels"]:
        lp = level["price"]
        crosses = high >= (lp - zs) and low <= (lp + zs)
        in_zone = abs(close - lp) <= zs * 1.5
        if crosses or in_zone:
            tipo = "soporte" if close >= lp else "resistencia"
            c = {
                "touch": True,
                "price": lp,
                "is_major": level["is_major"],
                "tipo": tipo,
                "crosses": crosses,
            }
            if best is None or (not best["is_major"] and level["is_major"]):
                best = c
    return best or {
        "touch": False,
        "price": None,
        "is_major": False,
        "tipo": None,
        "crosses": False,
    }


def calc_opens(df):
    """Calcula aperturas de año, semana y día."""
    result = {"year_open": None, "week_open": None, "day_open": None}
    if df.empty:
        return result
    now = df.index[-1]

    # Apertura anual
    ys = pd.Timestamp(year=now.year, month=1, day=1, tz=now.tz if now.tz else None)
    ydf = df[df.index >= ys]
    if not ydf.empty:
        result["year_open"] = float(ydf["Open"].iloc[0])

    # Apertura semanal
    ws = (now - pd.Timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
    wdf = df[df.index >= ws]
    if not wdf.empty:
        result["week_open"] = float(wdf["Open"].iloc[0])

    # Apertura del día (primera vela del día actual)
    ds = pd.Timestamp(year=now.year, month=now.month, day=now.day,
                      tz=now.tz if now.tz else None)
    ddf = df[df.index >= ds]
    if not ddf.empty:
        result["day_open"] = float(ddf["Open"].iloc[0])

    return result


async def get_index_components_context(ticker: str) -> dict | None:
    """
    Para ^DJI y ^NDX: obtiene el % de componentes clave alcistas vs bajistas
    respecto al precio actual vs apertura del día.
    Retorna dict con bulls, bears, neutral, porcentajes y dirección dominante.
    """
    components = INDEX_COMPONENTS.get(ticker.upper())
    if not components:
        return None

    bulls, bears, neutral = [], [], []

    async def check_component(sym):
        try:
            df = await async_download(sym, period="2d", interval="1d", progress=False)
            if df.empty:
                return
            df = clean_df(df)
            if len(df) < 1:
                return
            last_close = float(df["Close"].iloc[-1])
            last_open  = float(df["Open"].iloc[-1])
            if last_close > last_open * 1.001:
                bulls.append(sym)
            elif last_close < last_open * 0.999:
                bears.append(sym)
            else:
                neutral.append(sym)
        except Exception:
            pass

    for s in components:
        await check_component(s)

    total = len(bulls) + len(bears) + len(neutral)
    if total == 0:
        return None

    bull_pct = round(len(bulls) / total * 100)
    bear_pct = round(len(bears) / total * 100)

    if bull_pct >= 60:
        direction = "bullish"
    elif bear_pct >= 60:
        direction = "bearish"
    else:
        direction = "mixed"

    return {
        "bulls":     bulls,
        "bears":     bears,
        "neutral":   neutral,
        "bull_pct":  bull_pct,
        "bear_pct":  bear_pct,
        "direction": direction,
        "total":     total,
    }


def detect_alerts(df, ticker="", ema_short=200, ema_long=800, cfg=None):
    alertas = []
    n = len(df) - 1
    if n < 2:
        return alertas
    pn, pp = float(df["Close"].iloc[n]), float(df["Close"].iloc[n - 1])
    prefix = f"[{ticker}] " if ticker else ""
    cs, cl = f"EMA{ema_short}", f"EMA{ema_long}"

    # RSI actual
    rsi_val = None
    if "RSI" in df.columns:
        r = df["RSI"].iloc[n]
        if pd.notna(r):
            rsi_val = float(r)

    # ── EMA crosses / touches ──────────────────────────────────
    for col, nombre, pts in [(cs, f"EMA{ema_short}", 2), (cl, f"EMA{ema_long}", 4)]:
        if col not in df.columns:
            continue
        en, ep = df[col].iloc[n], df[col].iloc[n - 1]
        if not (pd.notna(en) and pd.notna(ep)):
            continue
        en_f = float(en)
        if pp < ep and pn >= en_f:
            alertas.append({
                "nivel": "bullish",
                "tipo": "ema_cross_up",
                "ema_nombre": nombre,
                "ema_val": en_f,
                "close": pn,
                "rsi": rsi_val,
                "pts": pts,
                "msg": prefix + f"Precio cruza {nombre} al alza ${pn:.5g}",
            })
        elif pp > ep and pn <= en_f:
            alertas.append({
                "nivel": "bearish",
                "tipo": "ema_cross_down",
                "ema_nombre": nombre,
                "ema_val": en_f,
                "close": pn,
                "rsi": rsi_val,
                "pts": pts,
                "msg": prefix + f"Precio cruza {nombre} a la baja ${pn:.5g}",
            })
        elif en_f > 0 and abs(pn - en_f) / en_f * 100 <= 0.4:
            alertas.append({
                "nivel": "info",
                "tipo": "ema_touch",
                "ema_nombre": nombre,
                "ema_val": en_f,
                "close": pn,
                "rsi": rsi_val,
                "pts": pts,
                "msg": prefix + f"Precio tocando {nombre} ${pn:.5g}",
            })

    # ── Golden / Death Cross ───────────────────────────────────
    esn = df[cs].iloc[n] if cs in df.columns else None
    esp = df[cs].iloc[n - 1] if cs in df.columns else None
    eln = df[cl].iloc[n] if cl in df.columns else None
    elp = df[cl].iloc[n - 1] if cl in df.columns else None
    if all(pd.notna(x) for x in [esn, esp, eln, elp] if x is not None):
        if esp < elp and esn >= eln:
            alertas.append({
                "nivel": "bullish",
                "tipo": "golden_cross",
                "close": pn,
                "rsi": rsi_val,
                "pts": 3,
                "msg": prefix + f"Golden Cross EMA{ema_short}/{ema_long}",
            })
        elif esp > elp and esn <= eln:
            alertas.append({
                "nivel": "bearish",
                "tipo": "death_cross",
                "close": pn,
                "rsi": rsi_val,
                "pts": 3,
                "msg": prefix + f"Death Cross EMA{ema_short}/{ema_long}",
            })

    # ── Fractales ─────────────────────────────────────────────
    if cfg is not None:
        fr = calc_fractales(pn, cfg)
        ft = detect_fractal_touch(
            float(df["High"].iloc[n]), float(df["Low"].iloc[n]), pn, fr
        )
        if ft["touch"]:
            alertas.append({
                "nivel": "bullish" if ft["tipo"] == "soporte" else "bearish",
                "tipo": "fractal",
                "fractal_precio": ft["price"],
                "fractal_tipo": ft["tipo"],
                "fractal_mayor": ft["is_major"],
                "close": pn,
                "rsi": rsi_val,
                "pts": 3 if ft["is_major"] else 1,
                "msg": prefix + f"⬡ Vela toca fractal {'MAYOR ' if ft['is_major'] else ''}{ft['tipo'].upper()} ${ft['price']:.5g}",
            })
    return alertas


# ─── CONFLUENCIAS ────────────────────────────────────────────


def evaluate_confluencias(df, ticker="", cfg=None, opens=None, components_ctx=None):
    """
    Evalúa las 5 confluencias de la matriz con validación DIRECCIONAL.

    Regla fundamental: para que el setup sea FAVORABLE, todas las confluencias
    activas deben apuntar en la MISMA dirección (todas bullish o todas bearish).
    Si hay conflicto de dirección → el setup es inválido (CONTRADICCIÓN).

    Direcciones de cada confluencia:
      ① RSI < 47  → bullish (comprar barato, ideal < 30)
         RSI > 53 → bearish (vender caro, ideal > 70)
      ② EMA corta > EMA larga → bullish (tendencia alcista)
         EMA corta < EMA larga → bearish (tendencia bajista)
      ③ Fractal soporte       → bullish
         Fractal resistencia   → bearish
      ④ Apertura día/semana   → bullish si precio arriba de ambas, bearish si abajo
      ⑤ Fibonacci 55.9%       → neutral (nivel de retroceso, válido para ambas)
      ⑥ Componentes índice    → bullish/bearish según mayoría (solo ^DJI/^NDX)
    """
    if len(df) < 14:
        return None

    n     = len(df) - 1
    price = float(df["Close"].iloc[n])
    es    = cfg["ema_short"] if cfg else 200
    el    = cfg["ema_long"]  if cfg else 800

    rsi_raw = df["RSI"].iloc[n] if "RSI" in df.columns else None
    rsi = float(rsi_raw) if pd.notna(rsi_raw) else 50.0

    ema_s_val = None
    if f"EMA{es}" in df.columns and pd.notna(df[f"EMA{es}"].iloc[n]):
        ema_s_val = float(df[f"EMA{es}"].iloc[n])
    ema_l_val = None
    if f"EMA{el}" in df.columns and pd.notna(df[f"EMA{el}"].iloc[n]):
        ema_l_val = float(df[f"EMA{el}"].iloc[n])

    raw = []

    # ① RSI — comprar barato (<47) o vender caro (>53)
    if rsi < 47:
        raw.append({"id": 1, "ok": True,
            "texto": f"RSI bajo ({rsi:.1f}) → favorable para largos", "tipo": "bullish"})
    elif rsi > 53:
        raw.append({"id": 1, "ok": True,
            "texto": f"RSI alto ({rsi:.1f}) → favorable para cortos", "tipo": "bearish"})
    else:
        raw.append({"id": 1, "ok": False,
            "texto": f"RSI neutro ({rsi:.1f})", "tipo": "info"})

    # ② Relación entre EMAs (tendencia)
    if ema_s_val and ema_l_val:
        if ema_s_val > ema_l_val:
            raw.append({"id": 2, "ok": True,
                "texto": f"EMA{es} ({ema_s_val:.5g}) > EMA{el} ({ema_l_val:.5g}) — tendencia alcista",
                "tipo": "bullish"})
        else:
            raw.append({"id": 2, "ok": True,
                "texto": f"EMA{es} ({ema_s_val:.5g}) < EMA{el} ({ema_l_val:.5g}) — tendencia bajista",
                "tipo": "bearish"})
    else:
        raw.append({"id": 2, "ok": False,
            "texto": "EMAs no disponibles", "tipo": "info"})

    # ③ Fractal (soporte → bullish, resistencia → bearish)
    if cfg:
        fr = calc_fractales(price, cfg)
        ft = detect_fractal_touch(
            float(df["High"].iloc[n]), float(df["Low"].iloc[n]), price, fr)
        if ft["touch"]:
            tag = "MAYOR" if ft["is_major"] else "menor"
            if ft["tipo"] == "soporte":
                raw.append({"id": 3, "ok": True,
                    "texto": f"Nivel clave {tag} como soporte en {ft['price']:.5g}",
                    "tipo": "bullish"})
            else:
                raw.append({"id": 3, "ok": True,
                    "texto": f"Nivel clave {tag} como resistencia en {ft['price']:.5g}",
                    "tipo": "bearish"})
        else:
            raw.append({"id": 3, "ok": False,
                "texto": "Sin nivel clave fractal relevante", "tipo": "info"})
    else:
        raw.append({"id": 3, "ok": False,
            "texto": "Sin nivel clave fractal relevante", "tipo": "info"})

    # ③ BIS: MonarcaBand — dirección de tendencia + cruce reciente
    tb_cross_val = None
    if "TB_TRIGGER" in df.columns and "TB_AVERAGE" in df.columns:
        t_val = df["TB_TRIGGER"].iloc[n]
        a_val = df["TB_AVERAGE"].iloc[n]
        t_prev = df["TB_TRIGGER"].iloc[n - 1] if n > 0 else None
        a_prev = df["TB_AVERAGE"].iloc[n - 1] if n > 0 else None
        if pd.notna(t_val) and pd.notna(a_val) and pd.notna(t_prev) and pd.notna(a_prev):
            is_bull  = t_val > a_val
            was_bull = t_prev > a_prev
            tb_dir   = "bullish" if is_bull else "bearish"
            if was_bull != is_bull:
                tb_text = f"⚡ MonarcaBand CAMBIO DE TENDENCIA {'ALCISTA' if is_bull else 'BAJISTA'}"
                tb_cross_val = 1 if is_bull else -1
            else:
                tb_text = f"MonarcaBand {'ALCISTA' if is_bull else 'BAJISTA'} (tendencia activa)"
            raw.append({"id": 3.5, "ok": True, "texto": tb_text, "tipo": tb_dir})
        else:
            raw.append({"id": 3.5, "ok": False,
                "texto": "MonarcaBand no disponible", "tipo": "info"})
    else:
        raw.append({"id": 3.5, "ok": False,
            "texto": "MonarcaBand no disponible", "tipo": "info"})

    # ④ Precio sobre/bajo apertura del DÍA y SEMANA → dirección real
    if opens:
        do = opens.get("day_open")
        wo = opens.get("week_open")
        tol = 0.0005
        day_dir  = None
        week_dir = None
        if do and do > 0:
            if price > do * (1 + tol):   day_dir = "above"
            elif price < do * (1 - tol): day_dir = "below"
        if wo and wo > 0:
            if price > wo * (1 + tol):   week_dir = "above"
            elif price < wo * (1 - tol): week_dir = "below"

        if day_dir == "above" and week_dir == "above":
            raw.append({"id": 4, "ok": True,
                "texto": (f"Precio sobre apertura del día ({do:.5g}) "
                          f"y semana ({wo:.5g}) → sesgo alcista"),
                "tipo": "bullish"})
        elif day_dir == "below" and week_dir == "below":
            raw.append({"id": 4, "ok": True,
                "texto": (f"Precio bajo apertura del día ({do:.5g}) "
                          f"y semana ({wo:.5g}) → sesgo bajista"),
                "tipo": "bearish"})
        elif day_dir and week_dir:
            raw.append({"id": 4, "ok": False,
                "texto": (f"Apertura día {'↑' if day_dir=='above' else '↓'} "
                          f"vs semana {'↑' if week_dir=='above' else '↓'} — sin consenso"),
                "tipo": "info"})
        else:
            raw.append({"id": 4, "ok": False,
                "texto": "Datos de apertura del día/semana incompletos", "tipo": "info"})
    else:
        raw.append({"id": 4, "ok": False,
            "texto": "Datos de apertura no disponibles", "tipo": "info"})

    # ⑤ Fibonacci 55.9% (neutral — nivel de precio, no implica dirección)
    high_p = float(df["High"].max())
    low_p  = float(df["Low"].min())
    fib559 = high_p - (high_p - low_p) * 0.559
    tol_f  = (high_p - low_p) * 0.015
    if abs(price - fib559) <= tol_f:
        raw.append({"id": 5, "ok": True,
            "texto": f"Fibonacci 55.9% en {fib559:.5g} (rango {low_p:.5g}–{high_p:.5g})",
            "tipo": "neutral"})
    else:
        pct_dist = (price - fib559) / fib559 * 100
        raw.append({"id": 5, "ok": False,
            "texto": f"Fib 55.9% en {fib559:.5g} ({pct_dist:+.1f}% de distancia)",
            "tipo": "info"})

    # ⑥ Componentes del índice (solo ^DJI y ^NDX) — dirección real
    if components_ctx and components_ctx.get("total", 0) > 0:
        bull_pct  = components_ctx.get("bull_pct", 0)
        bear_pct  = components_ctx.get("bear_pct", 0)
        comp_dir  = components_ctx.get("direction", "mixed")
        n_bulls   = len(components_ctx.get("bulls", []))
        n_bears   = len(components_ctx.get("bears", []))
        total_c   = components_ctx.get("total", 1)
        if comp_dir == "bullish":
            raw.append({"id": 6, "ok": True,
                "texto": (f"{bull_pct}% de componentes alcistas ({n_bulls}/{total_c}) "
                          f"→ sesgo alcista del índice"),
                "tipo": "bullish"})
        elif comp_dir == "bearish":
            raw.append({"id": 6, "ok": True,
                "texto": (f"{bear_pct}% de componentes bajistas ({n_bears}/{total_c}) "
                          f"→ sesgo bajista del índice"),
                "tipo": "bearish"})
        else:
            raw.append({"id": 6, "ok": False,
                "texto": f"Componentes mixtos ({bull_pct}% ↑ / {bear_pct}% ↓)",
                "tipo": "info"})

    # ── PASO 2: determinar la dirección dominante con lógica direccional ──
    activas_bullish = [c for c in raw if c["ok"] and c["tipo"] == "bullish"]
    activas_bearish = [c for c in raw if c["ok"] and c["tipo"] == "bearish"]

    FUERTES = {1, 2}
    bullish_fuertes = [c for c in activas_bullish if c["id"] in FUERTES]
    bearish_fuertes = [c for c in activas_bearish if c["id"] in FUERTES]

    contradiccion = bool(bullish_fuertes) and bool(bearish_fuertes)

    if contradiccion:
        direction = "conflicto"
        puntos    = 0
        confluencias_final = []
        for c in raw:
            entry = dict(c)
            if c["ok"] and c["tipo"] in ("bullish", "bearish") and c["id"] in FUERTES:
                entry["conflicto"] = True
                entry["ok"] = False
            confluencias_final.append(entry)
    else:
        if bullish_fuertes and not bearish_fuertes:
            direction = "bullish"
        elif bearish_fuertes and not bullish_fuertes:
            direction = "bearish"
        elif len(activas_bullish) > len(activas_bearish):
            direction = "bullish"
        elif len(activas_bearish) > len(activas_bullish):
            direction = "bearish"
        else:
            direction = "info"

        confluencias_final = []
        puntos = 0
        for c in raw:
            entry = dict(c)
            if c["ok"] and c["tipo"] not in ("neutral", "info"):
                if direction in ("bullish", "bearish") and c["tipo"] != direction:
                    entry["ok"] = False
                    entry["descartada"] = True
                else:
                    puntos += 1
            elif c["ok"] and c["tipo"] == "neutral":
                puntos += 1
            confluencias_final.append(entry)

    # ── PASO 3: calcular contexto del día/semana ──
    day_context  = None
    week_context = None
    if opens:
        do = opens.get("day_open")
        wo = opens.get("week_open")
        if do and do > 0:
            if price > do * 1.0005:
                day_context = {"direction": "above", "open": do,
                               "pct": round((price - do) / do * 100, 3)}
            elif price < do * 0.9995:
                day_context = {"direction": "below", "open": do,
                               "pct": round((price - do) / do * 100, 3)}
            else:
                day_context = {"direction": "at", "open": do, "pct": 0.0}
        if wo and wo > 0:
            if price > wo * 1.0005:
                week_context = {"direction": "above", "open": wo,
                                "pct": round((price - wo) / wo * 100, 3)}
            elif price < wo * 0.9995:
                week_context = {"direction": "below", "open": wo,
                                "pct": round((price - wo) / wo * 100, 3)}
            else:
                week_context = {"direction": "at", "open": wo, "pct": 0.0}

    # ── PASO 4: determinar estado final ──
    if contradiccion:
        estado = "CONTRADICCIÓN"
        nivel  = "info"
        alert  = False
    elif puntos >= 4:
        estado = "FAVORABLE"
        nivel  = direction
        alert  = True
    elif puntos == 3:
        estado = "INTERESANTE"
        nivel  = direction
        alert  = False
    elif puntos == 2:
        estado = "CONSIDERAR"
        nivel  = "info"
        alert  = False
    else:
        estado = "NO AHORA"
        nivel  = "info"
        alert  = False

    max_confs = max(c["id"] for c in confluencias_final) if confluencias_final else 5

    return {
        "ticker":        ticker.upper(),
        "precio":        price,
        "rsi":           rsi,
        "puntos":        puntos,
        "max_confs":     max_confs,
        "estado":        estado,
        "nivel":         nivel,
        "direction":     direction,
        "contradiccion": contradiccion,
        "confluencias":  confluencias_final,
        "alert":         alert,
        "day_context":   day_context,
        "week_context":  week_context,
        "tb_cross":      tb_cross_val,
    }


# ─── ALERTAS POR CONFLUENCIAS DE TRADINGBAND ─────────────────

async def _check_tb_confluences(tickers: list, label: str = "") -> dict:
    """
    Alertas por confluencias de TradingBand:
      1) Cruce Trigger/Average → cambio de tendencia
      2) Divergencia RSI en zona (RSI≤45 alcista / RSI≥55 bajista)
      3) Toque de nivel clave / fractal
    Solo alerta si >=2 confluencias alineadas y el cruce ocurrió
    en las últimas 2 velas 1h (≤2h de antigüedad).
    """
    if not HAS_NOTIFIER or not HAS_TB:
        return {}
    if _build_rb is None or _detect_divs is None:
        return {}

    now = time.time()
    alerts_by_ticker: dict = {}

    for t in tickers:
        try:
            cfg = _TB_CFG.get(t.upper(), _TB_CFG.get("_default"))

            # 1h para precisión temporal (≤2h de antigüedad)
            df = await async_download(t.upper(), period="3mo", interval="1h", progress=False)
            if df.empty:
                continue
            df = clean_df(df)
            df_calc = df.copy()
            df_calc.columns = [c.lower() for c in df_calc.columns]
            df_calc = df_calc.dropna(subset=["close"])
            if len(df_calc) < 40:
                continue

            close  = df_calc["close"]
            high   = df_calc["high"]
            low    = df_calc["low"]

            # TradingBand: SMA12(Trigger) + SMA12(Average)
            trigger = close.rolling(12, min_periods=12).mean()
            average = trigger.rolling(12, min_periods=12).mean()

            # RSI (mismo cálculo que el chart)
            rsi = _calc_rsi_tv(close, 14) if _calc_rsi_tv else None
            if rsi is None or rsi.notna().sum() < 40:
                continue

            n = len(df_calc)
            # Revisar últimas 2 velas
            for offset in range(2):
                i = n - 1 - offset
                if i < 1:
                    continue

                # Filtro de antigüedad: candle <= 2h
                ts = df_calc.index[i]
                try:
                    ts_parsed = pd.Timestamp(ts)
                    if ts_parsed.tzinfo is None:
                        ts_parsed = ts_parsed.tz_localize("UTC")
                    age_h = (pd.Timestamp.now(tz="UTC") - ts_parsed).total_seconds() / 3600
                    if age_h > 2:
                        continue
                except Exception:
                    continue

                t_now = trigger.iloc[i]
                a_now = average.iloc[i]
                t_prev = trigger.iloc[i - 1]
                a_prev = average.iloc[i - 1]
                if pd.isna(t_now) or pd.isna(a_now) or pd.isna(t_prev) or pd.isna(a_prev):
                    continue

                cross_up   = (t_now > a_now) and (t_prev <= a_prev)
                cross_down = (t_now < a_now) and (t_prev >= a_prev)
                if not cross_up and not cross_down:
                    continue

                direction = "bullish" if cross_up else "bearish"
                price     = float(close.iloc[i])

                # Divergencias recientes
                df_slice = df_calc.iloc[:i + 1].copy()
                rsi_slice = rsi.iloc[:i + 1].copy()
                valid_mask = rsi_slice.notna()
                divs = []
                if valid_mask.sum() >= 40:
                    df_aligned = df_slice[valid_mask].copy()
                    rsi_valid  = rsi_slice[valid_mask]
                    all_divs   = _detect_divs(df_aligned, rsi_valid)
                    divs       = [d for d in all_divs if d.get("bar", 0) >= i - 40]

                zone_divs = []
                for d in divs:
                    is_bull = d["type"] in ("bull", "hbull")
                    is_bear = d["type"] in ("bear", "hbear")
                    if direction == "bullish" and is_bull and d.get("in_zone"):
                        zone_divs.append(d)
                    elif direction == "bearish" and is_bear and d.get("in_zone"):
                        zone_divs.append(d)

                # Toque de nivel clave
                main_cfg = get_cfg(t)
                frac_touch = False
                frac_info  = None
                if main_cfg:
                    fr = calc_fractales(price, main_cfg)
                    ft = detect_fractal_touch(float(high.iloc[i]), float(low.iloc[i]), price, fr)
                    frac_touch = ft["touch"]
                    frac_info  = ft

                # Toque de banda Monarca (precio toca Trigger o Average)
                band_touch = False
                band_which = ""
                h_val = float(high.iloc[i])
                l_val = float(low.iloc[i])
                t_now_f = float(t_now)
                a_now_f = float(a_now)
                # Tolerancia: 0.3% del precio para activos normales, 0.1% para forex
                tol_pct = 0.001 if any(x in t for x in ["JPY", "USD"]) else 0.003
                band_tol = price * tol_pct
                if h_val >= t_now_f - band_tol and l_val <= t_now_f + band_tol:
                    band_touch = True
                    band_which = "Trigger"
                elif h_val >= a_now_f - band_tol and l_val <= a_now_f + band_tol:
                    band_touch = True
                    band_which = "Average"

                # Construir confluencias
                confluencias = [{
                    "id": "tb_cross",
                    "texto": f"TradingBand {direction.upper()}",
                    "ok": True, "tipo": direction,
                }]
                if zone_divs:
                    best = zone_divs[0]
                    confluencias.append({
                        "id": "rsi_div",
                        "texto": f"Divergencia RSI {best['type'].upper()} N{best['level']} (RSI {best['rsi']})",
                        "ok": True, "tipo": direction,
                    })
                if frac_touch:
                    confluencias.append({
                        "id": "fractal",
                        "texto": f"Toque nivel clave ({frac_info['tipo'].upper()})",
                        "ok": True, "tipo": direction,
                    })
                if band_touch:
                    confluencias.append({
                        "id": "tb_band",
                        "texto": f"Precio toca banda Monarca ({band_which})",
                        "ok": True, "tipo": direction,
                    })

                puntos = sum(1 for c in confluencias if c.get("ok"))
                if puntos >= 2:
                    # Dedup por combo de confluencias (así nuevas confluencias re-avisan)
                    dedup = f"TB_CONF_{t}_{direction}_{'div' if zone_divs else ''}_{'frac' if frac_touch else ''}_{'band' if band_touch else ''}"
                    if now - _sent_cache.get(dedup, 0) > _DEDUP_SECONDS:
                        try:
                            ts_utc = ts_parsed.tz_convert("UTC") if ts_parsed.tzinfo else ts_parsed.tz_localize("UTC")
                            hora = ts_utc.strftime("%d/%m %H:%M")
                            ts_iso = ts_utc.isoformat()
                        except Exception:
                            hora = ""
                            ts_iso = ""

                        resultado = {
                            "ticker": t.upper(), "precio": price,
                            "estado": "FAVORABLE" if puntos >= 3 else "INTERESANTE",
                            "nivel": direction, "direction": direction,
                            "puntos": puntos, "confluencias": confluencias,
                            "alert": True,
                        }
                        alerts_by_ticker.setdefault(t.upper(), []).append({
                            "nivel": direction,
                            "msg": f"[{t.upper()}] {resultado['estado']} — {puntos} confluencias ({hora})",
                            "hora": hora, "ts_utc_iso": ts_iso,
                            "resultado": resultado,
                        })
                        _sent_cache[dedup] = now
                        print(f"[tb-confl] {t} {direction} {puntos} confluencias @ {hora}")
                        break  # sólo 1 alerta por ticker por ejecución
        except Exception as e:
            print(f"[tb-confl] Error en {t}: {e}")

    return alerts_by_ticker


# ─── SCHEDULER ───────────────────────────────────────────────


async def _check_tickers(tickers: list, num_candles: int = 1, label: str = "",
                         max_per_ticker: int = 0) -> dict:
    """
    Revisa los tickers dados. num_candles controla cuántas velas recientes analizar.
    max_per_ticker: si > 0, limita las alertas enviadas por activo (0 = sin límite).
    Retorna alerts_by_ticker con las alertas nuevas (sin duplicados en cache).
    """
    now = time.time()
    alerts_by_ticker: dict = {}
    for t in tickers:
        try:
            cfg = get_cfg(t)
            df = await async_download(
                t.upper(), period="6mo", interval="4h", progress=False
            )
            if df.empty:
                continue
            df = clean_df(df)
            df = calc_indicators(df, cfg["ema_short"], cfg["ema_long"])
            opens_data = calc_opens(df)

            # Obtener contexto de componentes para índices (una vez por ticker)
            components_ctx = None
            if t.upper() in INDEX_COMPONENTS:
                try:
                    components_ctx = await get_index_components_context(t.upper())
                except Exception:
                    pass

            nuevas = []
            # Analizar las últimas num_candles velas
            for i in range(min(num_candles, len(df))):
                fila = len(df) - 1 - i
                df_slice = df.iloc[: fila + 1]
                ts = df.index[fila]
                try:
                    ts_parsed   = pd.Timestamp(ts)
                    ts_utc      = ts_parsed.tz_localize("UTC") if ts_parsed.tzinfo is None else ts_parsed.tz_convert("UTC")
                    ts_utc_iso  = ts_utc.isoformat()
                    hora        = ts_utc.strftime("%d/%m %H:%M")
                    dia_num     = ts_parsed.weekday()
                    dia_name    = ts_parsed.strftime("%A")
                except Exception:
                    hora       = ""
                    ts_utc_iso = ""
                    dia_num    = -1
                    dia_name   = ""

                # Filtro de frescura: ignorar velas 4h con >1h de antigüedad
                try:
                    age_h = (pd.Timestamp.now(tz="UTC") - ts_utc).total_seconds() / 3600
                    if age_h > 1:
                        continue
                except Exception:
                    pass

                resultado = evaluate_confluencias(
                    df_slice,
                    ticker=t.upper(),
                    cfg=cfg,
                    opens=opens_data,
                    components_ctx=components_ctx,
                )

                if resultado and resultado.get("alert"):
                    # Clave de deduplicación: ticker + estado + precio redondeado
                    key = f"{t}_{resultado['estado']}_{round(resultado['precio'], -1)}"
                    if now - _sent_cache.get(key, 0) > _DEDUP_SECONDS:
                        nuevas.append({
                            "nivel":          resultado["nivel"],
                            "msg":            f"[{t.upper()}] {resultado['estado']} {hora}".strip(),
                            "hora":           hora,
                            "ts_utc_iso":     ts_utc_iso,
                            "dia_num":        dia_num,
                            "dia_name":       dia_name,
                            "resultado":      resultado,
                            "components_ctx": components_ctx,
                        })
                        _sent_cache[key] = now
            if max_per_ticker > 0:
                nuevas = nuevas[:max_per_ticker]
            if nuevas:
                alerts_by_ticker[t.upper()] = nuevas
        except Exception as e:
            print(f"[{label or 'scheduler'}] Error en {t}: {e}")
    return alerts_by_ticker


async def scheduled_watch():
    """Revisión periódica — analiza la última vela + confluencias TradingBand."""
    if not HAS_NOTIFIER:
        return
    # 1) Alertas de confluencia TradingBand (cruce Trigger/Average + div + nivel)
    tb_alerts = await _check_tb_confluences(WATCH_TICKERS, label="scheduler")
    if tb_alerts:
        total = sum(len(v) for v in tb_alerts.values())
        print(f"[scheduler] {total} alertas de TradingBand en {len(tb_alerts)} ticker(s)")
        await notify_users_with_alerts(tb_alerts)

    # 2) Alertas clásicas de confluencias EMA/RSI/fractales
    alerts_by_ticker = await _check_tickers(
        WATCH_TICKERS, num_candles=1, label="scheduler"
    )
    if alerts_by_ticker:
        total = sum(len(v) for v in alerts_by_ticker.values())
        print(f"[scheduler] {total} alertas nuevas en {len(alerts_by_ticker)} ticker(s)")
        await notify_users_with_alerts(alerts_by_ticker)
    else:
        print("[scheduler] Sin alertas nuevas")

    await _update_rsi_watchlist()


async def daily_catchup():
    """Catch-up al arrancar: revisa SOLO la última vela (≤4h) — nada de más de 1h atrás."""
    if not HAS_NOTIFIER:
        return
    # 1) Confluencias TradingBand (siempre frescas ≤2h)
    tb_alerts = await _check_tb_confluences(WATCH_TICKERS, label="catchup")
    if tb_alerts:
        total = sum(len(v) for v in tb_alerts.values())
        print(f"[catchup] {total} alertas TradingBand enviadas")
        await notify_users_with_alerts(tb_alerts)

    # 2) Alertas clásicas
    print("[catchup] Revisando vela actual (máx 1h de antigüedad)…")
    alerts_by_ticker = await _check_tickers(
        WATCH_TICKERS, num_candles=1, label="catchup", max_per_ticker=1
    )
    if alerts_by_ticker:
        total = sum(len(v) for v in alerts_by_ticker.values())
        print(f"[catchup] {total} alertas del día enviadas")
        await notify_users_with_alerts(alerts_by_ticker)
    else:
        print("[catchup] Sin alertas nuevas en las últimas 24h")

    await _update_rsi_watchlist()


async def _update_rsi_watchlist():
    """Evalúa todos los tickers y añade a la watchlist RSI aquellos con ≥3 puntos
    (sin contar RSI) cuyo RSI aún no está en zona extrema."""
    global _rsi_watchlist
    new_watchlist = {}
    for t in WATCH_TICKERS:
        try:
            cfg = get_cfg(t)
            df = await async_download(t.upper(), period="6mo", interval="4h", progress=False)
            if df.empty:
                continue
            df = clean_df(df)
            df = calc_indicators(df, cfg["ema_short"], cfg["ema_long"])
            opens_data = calc_opens(df)

            components_ctx = None
            if t.upper() in INDEX_COMPONENTS:
                try:
                    components_ctx = await get_index_components_context(t.upper())
                except Exception:
                    pass

            resultado = evaluate_confluencias(df, ticker=t.upper(), cfg=cfg,
                                              opens=opens_data, components_ctx=components_ctx)
            if not resultado or resultado.get("contradiccion"):
                continue

            confs = resultado.get("confluencias", [])
            direction = resultado.get("direction", "info")
            rsi = resultado.get("rsi", 50)

            if direction not in ("bullish", "bearish"):
                continue

            puntos_sin_rsi = 0
            for c in confs:
                if c["id"] == 1:
                    continue
                if c.get("ok") and not c.get("descartada") and not c.get("conflicto"):
                    puntos_sin_rsi += 1

            rsi_ya_extremo = (direction == "bullish" and rsi <= 30) or \
                             (direction == "bearish" and rsi >= 70)

            if puntos_sin_rsi >= 3 and not rsi_ya_extremo:
                new_watchlist[t.upper()] = {
                    "direction": direction,
                    "puntos_sin_rsi": puntos_sin_rsi,
                    "rsi_actual": rsi,
                    "resultado": resultado,
                    "components_ctx": components_ctx,
                    "cfg": cfg,
                }
                print(f"[rsi-watch] 👁 {t.upper()} en vigilancia RSI "
                      f"({puntos_sin_rsi} pts sin RSI, dir={direction}, RSI={rsi:.1f})")
        except Exception as e:
            print(f"[rsi-watch] Error evaluando {t}: {e}")

    _rsi_watchlist = new_watchlist
    if _rsi_watchlist:
        print(f"[rsi-watch] Vigilando {len(_rsi_watchlist)} activo(s): "
              f"{', '.join(_rsi_watchlist.keys())}")
    else:
        print("[rsi-watch] Sin activos en vigilancia RSI")


async def _rsi_realtime_check():
    """Cada 2 min revisa SOLO el RSI de los activos en la watchlist.
    Si el RSI cruza la zona extrema (≤30 largos, ≥70 cortos) → alerta inmediata."""
    if not HAS_NOTIFIER or not _rsi_watchlist:
        return

    now = time.time()
    alertas_rsi: dict = {}

    for ticker, info in list(_rsi_watchlist.items()):
        try:
            df = await async_download(ticker, period="5d", interval="15m", progress=False)
            if df.empty:
                continue
            df = clean_df(df)

            col_rsi = "RSI"
            if col_rsi not in df.columns:
                import ta
                df["RSI"] = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()

            rsi_series = df["RSI"].dropna()
            if rsi_series.empty:
                continue
            rsi_now = float(rsi_series.iloc[-1])

            direction = info["direction"]
            triggered = (direction == "bullish" and rsi_now <= 30) or \
                        (direction == "bearish" and rsi_now >= 70)

            if not triggered:
                continue

            dedup_key = f"RSI_RT_{ticker}_{direction}"
            if now - _sent_cache.get(dedup_key, 0) < _DEDUP_SECONDS:
                continue

            cfg = info["cfg"]
            df_4h = await async_download(ticker, period="6mo", interval="4h", progress=False)
            if df_4h.empty:
                continue
            df_4h = clean_df(df_4h)
            df_4h = calc_indicators(df_4h, cfg["ema_short"], cfg["ema_long"])
            opens_data = calc_opens(df_4h)

            components_ctx = info.get("components_ctx")
            resultado = evaluate_confluencias(df_4h, ticker=ticker, cfg=cfg,
                                              opens=opens_data, components_ctx=components_ctx)

            if not resultado:
                continue

            resultado["rsi"] = rsi_now
            for c in resultado.get("confluencias", []):
                if c["id"] == 1:
                    if direction == "bullish" and rsi_now <= 30:
                        c["ok"] = True
                        c["tipo"] = "bullish"
                        c["texto"] = f"⚡ RSI en zona ({rsi_now:.1f}) → COMPRA"
                        c.pop("descartada", None)
                        c.pop("conflicto", None)
                    elif direction == "bearish" and rsi_now >= 70:
                        c["ok"] = True
                        c["tipo"] = "bearish"
                        c["texto"] = f"⚡ RSI en zona ({rsi_now:.1f}) → VENTA"
                        c.pop("descartada", None)
                        c.pop("conflicto", None)

            puntos = sum(1 for c in resultado["confluencias"]
                         if c.get("ok") and not c.get("descartada") and not c.get("conflicto"))
            resultado["puntos"] = puntos
            resultado["estado"] = "FAVORABLE" if puntos >= 4 else "INTERESANTE"
            resultado["nivel"] = direction
            resultado["alert"] = puntos >= 4
            resultado["rsi_realtime"] = True

            if resultado["alert"]:
                ts_now = pd.Timestamp.now(tz="UTC")
                alertas_rsi[ticker] = [{
                    "nivel": direction,
                    "msg": f"[{ticker}] ⚡ RSI EN ZONA — {resultado['estado']}",
                    "hora": ts_now.strftime("%d/%m %H:%M"),
                    "ts_utc_iso": ts_now.isoformat(),
                    "dia_num": ts_now.weekday(),
                    "dia_name": ts_now.strftime("%A"),
                    "resultado": resultado,
                    "components_ctx": components_ctx,
                }]
                _sent_cache[dedup_key] = now
                del _rsi_watchlist[ticker]
                print(f"[rsi-watch] ⚡ {ticker} RSI={rsi_now:.1f} cruzó zona extrema "
                      f"({direction}) → ALERTA INMEDIATA")

        except Exception as e:
            print(f"[rsi-watch] Error revisando RSI de {ticker}: {e}")

    if alertas_rsi:
        await notify_users_with_alerts(alertas_rsi)


# ─── APP ─────────────────────────────────────────────────────

if HAS_SCHEDULER:
    from contextlib import asynccontextmanager

    async def _process_tg_message(message: dict):
        if not HAS_NOTIFIER or not message:
            return
        try:
            chat_id = message.get("chat", {}).get("id")
            username = message.get("from", {}).get("username", "")
            text = message.get("text", "").strip()
            if not chat_id:
                return
            if text.startswith("/start"):
                print(f"[telegram] /start de @{username or '?'} (chat_id={chat_id})")
                ok = await register_chat(chat_id, username)
                print(f"[telegram] register_chat → {'OK' if ok else 'FALLO'}")
                sent = await send_telegram_to(
                    chat_id,
                    f"✅ <b>¡Suscrito a The Matrix Lab!</b>\n\n"
                    f"⬡ Recibirás alertas automáticas cada 4H de tus activos favoritos.\n\n"
                    f"📋 <b>Tu Chat ID es:</b> <code>{chat_id}</code>\n"
                    f"Cópialo y pégalo en el panel de Notificaciones de la app para personalizar tus alertas.\n\n"
                    f"Comandos disponibles:\n"
                    f"/status — estado del sistema\n"
                    f"/test — prueba de alertas\n"
                    f"/stop — cancelar suscripción"
                    if ok
                    else "⚠️ No se pudo registrar. Inténtalo de nuevo.",
                )
                print(f"[telegram] Respuesta enviada → {'OK' if sent else 'FALLO'}")
            elif text.startswith("/stop"):
                await send_telegram_to(
                    chat_id, "🔕 Suscripción cancelada. Envía /start para reactivar."
                )
            elif text.startswith("/status"):
                await send_telegram_to(
                    chat_id, "✅ <b>The Matrix Lab activo</b>\nRevisión cada 4 horas."
                )
            elif text.startswith("/test"):
                await send_telegram_to(
                    chat_id,
                    "🟢 <b>[TEST]</b> El sistema funciona correctamente.\n"
                    "⬡ Recibirás mensajes cuando haya señales reales.",
                )
        except Exception as e:
            print(f"[telegram] Error procesando mensaje: {e}")

    async def _tg_polling(token: str):
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                await c.post(
                    f"https://api.telegram.org/bot{token}/deleteWebhook",
                    json={"drop_pending_updates": False},
                )
            print(
                "[telegram] Polling iniciado (webhook eliminado, mensajes pendientes conservados)"
            )
        except Exception as e:
            print(f"[telegram] No se pudo eliminar webhook: {e}")
        offset = 0
        while True:
            try:
                async with httpx.AsyncClient(timeout=35) as c:
                    r = await c.get(
                        f"https://api.telegram.org/bot{token}/getUpdates",
                        params={
                            "offset": offset,
                            "timeout": 30,
                            "allowed_updates": ["message"],
                        },
                    )
                    if r.status_code == 200:
                        for upd in r.json().get("result", []):
                            offset = upd["update_id"] + 1
                            await _process_tg_message(upd.get("message", {}))
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[telegram] Polling error: {e}")
                await asyncio.sleep(5)

    async def _warm_row_cache():
        print(f"[cache] Pre-calentando datos de los {len(WATCH_TICKERS)} activos…")
        for t in WATCH_TICKERS:
            try:
                await _compute_row(t)
            except Exception as e:
                print(f"[cache] Error calentando {t}: {e}")
        print("[cache] Cache de activos lista")

    @asynccontextmanager
    async def lifespan(app):
        scheduler = None
        polling_task = None
        token = os.getenv("TELEGRAM_TOKEN", "")
        if token:
            polling_task = asyncio.create_task(_tg_polling(token))
        try:
            scheduler = AsyncIOScheduler()
            scheduler.add_job(scheduled_watch, "interval", minutes=30, id="watch_30m")
            scheduler.add_job(lambda: _check_tb_confluences(WATCH_TICKERS),
                              "interval", minutes=5, id="tb_confl_5m")
            scheduler.add_job(_rsi_realtime_check, "interval",
                              minutes=_RSI_WATCH_INTERVAL_MIN, id="rsi_rt")
            scheduler.start()
            print(f"[scheduler] Iniciado · TB confluencias cada 5 min + revisión 30 min + RSI real-time cada {_RSI_WATCH_INTERVAL_MIN} min")
            # Catch-up: enviar alertas de las últimas 24h al arrancar
            asyncio.create_task(daily_catchup())
            asyncio.create_task(_warm_row_cache())
        except Exception as e:
            print(f"[scheduler] Error al iniciar: {e}")
        yield
        if polling_task:
            polling_task.cancel()
            try:
                await polling_task
            except asyncio.CancelledError:
                pass
        if scheduler:
            try:
                scheduler.shutdown()
            except:
                pass

    app = FastAPI(lifespan=lifespan)
else:
    app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

if HAS_TB:
    app.include_router(tb_router)

if HAS_AUTH:
    app.include_router(auth_router)
    app.include_router(users_router)


# ─── WEBHOOK TELEGRAM (fallback) ─────────────────────────────


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    try:
        body = await request.json()
        message = body.get("message") or body.get("edited_message", {})
        if HAS_SCHEDULER and message:
            await _process_tg_message(message)
    except Exception as e:
        print(f"[webhook] Error: {e}")
    return JSONResponse({"ok": True})


# ─── RUTAS ───────────────────────────────────────────────────


@app.get("/")
async def splash():
    for name in ("Splash.html", "splash.html"):
        if os.path.exists(f"templates/{name}"):
            return FileResponse(f"templates/{name}")
    return FileResponse("templates/index.html")


@app.get("/app")
async def dashboard():
    return FileResponse("templates/index.html")


@app.get("/confirm")
async def confirm_email():
    return FileResponse("templates/confirm.html")


@app.get("/en")
async def splash_en():
    return FileResponse("templates/Splash_en.html")


@app.get("/en/app")
async def dashboard_en():
    return FileResponse("templates/index_en.html")


# ─── NOTIFICACIONES ──────────────────────────────────────────


@app.get("/api/notify")
async def force_notify():
    if not HAS_NOTIFIER:
        return {"ok": False, "msg": "Notifier no configurado."}
    await daily_catchup()
    return {"ok": True, "msg": "Revisión de las últimas 24h completada."}


@app.get("/api/subs")
async def list_subs():
    if not HAS_NOTIFIER:
        return {"ok": False, "subs": 0}
    from notifier import get_chat_ids

    ids = await get_chat_ids()
    return {"ok": True, "subs": len(ids), "chat_ids": ids}


@app.get("/api/mail-subs")
async def mail_subs():
    if not HAS_NOTIFIER:
        return {"ok": False, "subs": 0}
    from notifier import get_all_user_prefs
    try:
        prefs = await get_all_user_prefs()
        count = sum(1 for p in prefs if p.get("email_enabled") and p.get("email_address"))
        return {"ok": True, "subs": count}
    except Exception:
        return {"ok": False, "subs": 0}


@app.get("/api/bot-info")
async def bot_info():
    token = os.getenv("TELEGRAM_TOKEN", "")
    if not token:
        return {"ok": False, "username": None}
    try:
        import httpx

        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"https://api.telegram.org/bot{token}/getMe")
            d = r.json()
            if d.get("ok"):
                return {
                    "ok": True,
                    "username": d["result"].get("username"),
                    "name": d["result"].get("first_name"),
                }
    except:
        pass
    return {"ok": False, "username": None}


@app.get("/api/mail-status")
async def mail_status():
    mf = os.getenv("MAIL_FROM", "")
    mp = os.getenv("MAIL_PASSWORD", "")
    mt = os.getenv("MAIL_TO", "")
    ok = bool(mf and mp and mt)
    return {"configured": ok, "mail_to": mt if ok else None}


@app.get("/api/notifier-status")
async def notifier_status():
    tg = bool(
        os.getenv("TELEGRAM_TOKEN")
        and os.getenv("SUPABASE_URL")
        and os.getenv("SUPABASE_KEY")
    )
    em = bool(
        os.getenv("MAIL_FROM") and os.getenv("MAIL_PASSWORD") and os.getenv("MAIL_TO")
    )
    return {
        "ok": tg or em,
        "telegram": tg,
        "email": em,
        "scheduler": HAS_SCHEDULER,
        "notifier": HAS_NOTIFIER,
        "next_run": "~4h desde el último ciclo",
    }


# ─── PREFERENCIAS DE USUARIO ─────────────────────────────────


@app.get("/api/user/notif-prefs")
async def get_notif_prefs(request: Request):
    """Obtiene las preferencias del usuario autenticado."""
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return JSONResponse({"ok": False, "msg": "No autenticado"}, status_code=401)
    if not HAS_NOTIFIER:
        return {"ok": False, "prefs": {}}
    prefs = await get_user_prefs(user_id)
    return {"ok": True, "prefs": prefs}


@app.post("/api/user/notif-prefs")
async def save_notif_prefs(request: Request):
    """Guarda las preferencias de notificación del usuario."""
    user_id = request.headers.get("X-User-Id")
    if not user_id:
        return JSONResponse({"ok": False, "msg": "No autenticado"}, status_code=401)
    if not HAS_NOTIFIER:
        return {"ok": False, "msg": "Notifier no disponible"}
    try:
        body = await request.json()
        ok = await save_user_prefs(user_id, body)
        return {"ok": ok, "msg": "Guardado" if ok else "Error al guardar"}
    except Exception as e:
        return JSONResponse({"ok": False, "msg": str(e)}, status_code=400)


# ─── DATOS DE MERCADO ────────────────────────────────────────


@app.get("/api/chart/{ticker}")
async def get_chart(ticker: str):
    try:
        sym = ticker.upper()
        cfg = get_cfg(sym)
        es, el = cfg["ema_short"], cfg["ema_long"]
        tb_cfg = _TB_CFG.get(sym, _TB_CFG.get("_default", {}))
        range_size = tb_cfg.get("range", 0)

        use_rb = HAS_TB and _build_rb is not None and range_size > 0
        if use_rb:
            df_raw = await async_download(sym, period="60d", interval="5m", progress=False)
            df = clean_df(df_raw)
            df.columns = [c.lower() for c in df.columns]
            df_rb = _build_rb(df, range_size)
            if df_rb is not None and not df_rb.empty:
                df = df_rb.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"})
                bar_type = f"Rango {range_size}"
            else:
                df.columns = [c.capitalize() for c in df.columns]
                bar_type = "5m"
        else:
            df = await async_download(sym, period="2y", interval="4h", progress=False)
            df = clean_df(df)
            bar_type = "4h"

        if df.empty:
            return {"error": "Simbolo no encontrado: " + ticker}
        df = calc_indicators(df, es, el)
        df = calc_trading_band(df)
        ult = float(df["Close"].iloc[-1])
        ts = ts_ms(df.index)
        candles = [
            {
                "x": ts[i],
                "o": safe(df["Open"].iloc[i]),
                "h": safe(df["High"].iloc[i]),
                "l": safe(df["Low"].iloc[i]),
                "c": safe(df["Close"].iloc[i]),
            }
            for i in range(len(df))
        ]

        def col_series(col):
            if col not in df.columns:
                return []
            return [
                {"x": ts[i], "y": float(df[col].iloc[i])}
                for i in range(len(df))
                if pd.notna(df[col].iloc[i])
            ]

        ros, rob = [], []
        for i in range(len(df)):
            r = df["RSI"].iloc[i]
            if pd.notna(r):
                if r < 30:
                    ros.append({"x": ts[i], "y": float(df["Close"].iloc[i])})
                elif r > 70:
                    rob.append({"x": ts[i], "y": float(df["Close"].iloc[i])})

        crosses = []
        for i in range(len(df)):
            c = df["TB_CROSS"].iloc[i]
            if c != 0:
                crosses.append({
                    "x": ts[i],
                    "y": float(df["Close"].iloc[i]),
                    "dir": int(c),
                })

        tb_trig = col_series("TB_TRIGGER")
        tb_avg  = col_series("TB_AVERAGE")

        last_trig = float(df["TB_TRIGGER"].dropna().iloc[-1]) if df["TB_TRIGGER"].dropna().size else None
        last_avg  = float(df["TB_AVERAGE"].dropna().iloc[-1]) if df["TB_AVERAGE"].dropna().size else None
        tb_signal = "bull" if (last_trig and last_avg and last_trig > last_avg) else "bear" if (last_trig and last_avg) else "neutral"

        opens = calc_opens(df)
        rsi_s = df["RSI"].dropna()
        first = float(df["Close"].iloc[0])
        return {
            "chart": {
                "candles": candles,
                f"ema{es}": col_series(f"EMA{es}"),
                f"ema{el}": col_series(f"EMA{el}"),
                "rsi_os": ros,
                "rsi_ob": rob,
                "tb_trigger": tb_trig,
                "tb_average": tb_avg,
                "tb_crosses": crosses,
            },
            "tb_last": {
                "trigger": last_trig,
                "average": last_avg,
                "signal": tb_signal,
            },
            "opens": opens,
            "last_price": ult,
            "change": ult - first,
            "change_pct": (ult - first) / first * 100,
            "rsi_current": float(rsi_s.iloc[-1]) if not rsi_s.empty else 50,
            "alertas": detect_alerts(
                df, ticker=sym, ema_short=es, ema_long=el, cfg=cfg
            ),
            "asset_config": {"ema_short": es, "ema_long": el},
            "bar_type": bar_type,
        }
    except Exception as e:
        return {"error": str(e)}


async def _compute_row(ticker: str) -> dict:
    key = ticker.upper()
    now = time.time()
    cached = _row_cache.get(key)
    if cached and now - cached["ts"] < _ROW_TTL:
        return cached["data"]
    cfg = get_cfg(ticker)
    es, el = cfg["ema_short"], cfg["ema_long"]
    df = await async_download(key, period="1y", interval="4h", progress=False)
    if df.empty:
        return {"error": "not found"}
    df = clean_df(df)
    df = calc_indicators(df, es, el)
    df = calc_trading_band(df)
    last, first = float(df["Close"].iloc[-1]), float(df["Close"].iloc[0])
    rsi_s = df["RSI"].dropna()
    rsi = float(rsi_s.iloc[-1]) if not rsi_s.empty else None

    def lv(col):
        if col not in df.columns:
            return None
        s = df[col].dropna()
        return float(s.iloc[-1]) if not s.empty else None

    last_trig = lv("TB_TRIGGER")
    last_avg  = lv("TB_AVERAGE")
    if last_trig is not None and last_avg is not None:
        tb_signal = "bull" if last_trig > last_avg else "bear"
    else:
        tb_signal = "neutral"

    opens = calc_opens(df)
    row_components_ctx = None
    if key in INDEX_COMPONENTS:
        try:
            row_components_ctx = await get_index_components_context(key)
        except Exception:
            pass
    confl = evaluate_confluencias(df, ticker=key, cfg=cfg, opens=opens, components_ctx=row_components_ctx)

    # ¿Cruce reciente de MonarcaBand en las últimas 2 velas?
    tb_cross_recent = False
    if "TB_TRIGGER" in df.columns and "TB_AVERAGE" in df.columns and len(df) >= 3:
        for i in range(2):
            idx = len(df) - 1 - i
            if idx < 1:
                continue
            tt_now = df["TB_TRIGGER"].iloc[idx]
            ta_now = df["TB_AVERAGE"].iloc[idx]
            tt_prev = df["TB_TRIGGER"].iloc[idx - 1]
            ta_prev = df["TB_AVERAGE"].iloc[idx - 1]
            if pd.notna(tt_now) and pd.notna(ta_now) and pd.notna(tt_prev) and pd.notna(ta_prev):
                cross = (tt_now > ta_now and tt_prev <= ta_prev) or (tt_now < ta_now and tt_prev >= ta_prev)
                if cross:
                    tb_cross_recent = True
                    break

    result = {
        "ticker": key,
        "price": last,
        "change_pct": round((last - first) / first * 100, 2),
        "rsi": round(rsi, 1) if rsi else None,
        "ema_short": lv(f"EMA{es}"),
        "ema_long": lv(f"EMA{el}"),
        "ema_short_name": f"EMA{es}",
        "ema_long_name": f"EMA{el}",
        "tb_signal": tb_signal,
        "tb_trigger": last_trig,
        "tb_average": last_avg,
        "tb_cross_recent": tb_cross_recent,
        "confluencias_puntos": confl["puntos"] if confl else 0,
        "confluencias_estado": confl["estado"] if confl else "NO AHORA",
        "confluencias": confl["confluencias"] if confl else [],
        "confluencias_rsi": confl["rsi"] if confl else None,
    }
    _row_cache[key] = {"ts": now, "data": result}
    return result


@app.get("/api/row/{ticker}")
async def get_row(ticker: str):
    try:
        return await _compute_row(ticker)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/watch")
async def watch(tickers: str = ""):
    all_alertas = []
    for t in tickers.split(","):
        t = t.strip()
        if not t:
            continue
        try:
            cfg = get_cfg(t)
            df = await async_download(
                t.upper(), period="6mo", interval="4h", progress=False
            )
            if not df.empty:
                df = clean_df(df)
                df = calc_indicators(df, cfg["ema_short"], cfg["ema_long"])
                all_alertas.extend(
                    detect_alerts(
                        df,
                        ticker=t.upper(),
                        ema_short=cfg["ema_short"],
                        ema_long=cfg["ema_long"],
                        cfg=cfg,
                    )
                )
        except:
            pass
    return {"alertas": all_alertas}


@app.get("/api/sparkline/{ticker}")
async def sparkline(ticker: str):
    try:
        df = await async_download(
            ticker.upper(), period="1mo", interval="1d", progress=False
        )
        if df.empty:
            return {"closes": [], "pct": 0}
        df = clean_df(df)
        closes = df["Close"].dropna().tolist()
        pct = (closes[-1] - closes[0]) / closes[0] * 100 if len(closes) > 1 else 0
        return {"closes": [float(c) for c in closes], "pct": round(pct, 2)}
    except:
        return {"closes": [], "pct": 0}


# ─── TRADING BAND RUTAS ──────────────────────────────────────


@app.get("/trading-band")
async def trading_band_splash():
    return FileResponse("templates/trading_band_splash.html")


@app.get("/trading-band/app")
async def trading_band_app():
    return FileResponse("templates/trading_band.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
