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
        calc_rsi_divergence as _calc_rsi_divergence,
        calc_shark_fin as _calc_shark_fin,
        calc_pattern_mw as _calc_pattern_mw,
        calc_pattern_hch as _calc_pattern_hch,
        detect_fvg as _detect_fvg,
        detect_all_divergences as _detect_all_divergences,
        calc_rsi as _calc_rsi,
    )
    HAS_TB = True
except Exception as _tb_err:
    HAS_TB = False
    _build_rb = None
    _TB_CFG = {}
    _detect_divs = None
    _calc_rsi_tv = None
    _calc_rsi_divergence = None
    _calc_shark_fin = None
    _calc_pattern_mw = None
    _calc_pattern_hch = None
    _detect_fvg = None
    _detect_all_divergences = None
    _calc_rsi = None
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
        notify_divergences,
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
_DEDUP_SECONDS = 4 * 3600  # 4 horas máximo para alertas recientes

_div_cache: dict = {}
_DIV_DEDUP_SECONDS = 7 * 24 * 3600

_row_cache: dict = {}
_ROW_TTL = 60
_yf_lock = asyncio.Lock()

_rsi_watchlist: dict = {}
_RSI_WATCH_INTERVAL_MIN = 15

# Cache para evitar re-alerta de Shark Fin en la misma fase
_shark_phase_cache: dict = {}  # ticker -> {"tipo": str, "phase": str, "ts": float}

ASSET_CONFIG = {
    "^DJI": {
        "key_spacing": 500,
        "major_spacing": 1000,
        "zone_size": 100,
        "ema_short": 50,
        "ema_long": 200,
    },
    "^NDX": {
        "key_spacing": 500,
        "major_spacing": 1000,
        "zone_size": 100,
        "ema_short": 50,
        "ema_long": 200,
    },
    "GC=F": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 50,
        "ema_long": 200,
    },
    "GLD": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 50,
        "ema_long": 200,
    },
    "IAU": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 50,
        "ema_long": 200,
    },
    "SI=F": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "CL=F": {
        "key_spacing": 2,
        "major_spacing": 5,
        "zone_size": 0.5,
        "ema_short": 50,
        "ema_long": 200,
    },
    "USDJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "GBPJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "EURUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 50,
        "ema_long": 200,
    },
    "AUDUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 50,
        "ema_long": 200,
    },
    "BTC-USD": {
        "key_spacing": 1000,
        "major_spacing": 5000,
        "zone_size": 500,
        "ema_short": 50,
        "ema_long": 200,
    },
    "GBPUSD=X": {
        "key_spacing": 0.005,
        "major_spacing": 0.01,
        "zone_size": 0.001,
        "ema_short": 50,
        "ema_long": 200,
    },
    "AUDJPY=X": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "^TNX": {
        "key_spacing": 0.1,
        "major_spacing": 0.5,
        "zone_size": 0.05,
        "ema_short": 50,
        "ema_long": 200,
    },
    "^TYX": {
        "key_spacing": 0.1,
        "major_spacing": 0.5,
        "zone_size": 0.05,
        "ema_short": 50,
        "ema_long": 200,
    },
    "DX=F": {
        "key_spacing": 1,
        "major_spacing": 5,
        "zone_size": 0.25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "^GSPC": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 50,
        "ema_long": 200,
    },
    "SPY": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 50,
        "ema_long": 200,
    },
    "VOO": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 50,
        "ema_long": 200,
    },
    "^RUT": {
        "key_spacing": 25,
        "major_spacing": 50,
        "zone_size": 5,
        "ema_short": 50,
        "ema_long": 200,
    },
    "IWM": {
        "key_spacing": 5,
        "major_spacing": 10,
        "zone_size": 1,
        "ema_short": 50,
        "ema_long": 200,
    },
    "BTC-USD": {
        "key_spacing": 1000,
        "major_spacing": 5000,
        "zone_size": 250,
        "ema_short": 50,
        "ema_long": 200,
    },
    "ETH-USD": {
        "key_spacing": 50,
        "major_spacing": 200,
        "zone_size": 25,
        "ema_short": 50,
        "ema_long": 200,
    },
    "QQQ": {
        "key_spacing": 10,
        "major_spacing": 20,
        "zone_size": 2,
        "ema_short": 50,
        "ema_long": 200,
    },
    "QQQM": {
        "key_spacing": 5,
        "major_spacing": 20,
        "zone_size": 1,
        "ema_short": 50,
        "ema_long": 200,
    },
    "GDX": {
        "key_spacing": 2,
        "major_spacing": 5,
        "zone_size": 0.5,
        "ema_short": 50,
        "ema_long": 200,
    },
    "SMH": {
        "key_spacing": 10,
        "major_spacing": 50,
        "zone_size": 2,
        "ema_short": 50,
        "ema_long": 200,
    },
    "XLE": {
        "key_spacing": 2,
        "major_spacing": 10,
        "zone_size": 0.5,
        "ema_short": 50,
        "ema_long": 200,
    },
    "AAPL": {
        "key_spacing": 5,
        "major_spacing": 20,
        "zone_size": 1,
        "ema_short": 50,
        "ema_long": 200,
    },
    "_default": {
        "key_spacing": 50,
        "major_spacing": 100,
        "zone_size": 10,
        "ema_short": 50,
        "ema_long": 200,
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


async def async_download_rb(ticker: str, label: str = "") -> tuple:
    """Descarga velas de 1 hora directamente de yfinance.
    Retorna (DataFrame con columnas Open/High/Low/Close/Volume, bar_type_label)."""
    df = await async_download(ticker, period="6mo", interval="1h", progress=False)
    df = clean_df(df)
    if df.empty:
        return df, "1h"
    return df, "1h"


def get_cfg(t):
    return ASSET_CONFIG.get(t.upper(), ASSET_CONFIG["_default"])


def _get_tb_cfg(t):
    """Devuelve la config de TradingBand (range, ema_s, ema_l)."""
    return _TB_CFG.get(t.upper(), _TB_CFG.get("_default", {})) if _TB_CFG else {}


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
    # RSI con RMA (igual que TradingView) — usar _calc_rsi_tv si está disponible
    if _calc_rsi_tv is not None:
        df["RSI"] = _calc_rsi_tv(close, 14)
    else:
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
    """Alertas de vigilancia — FOCO: RSI extremo, Divergencias, FVG, Patrones.
    EMAs y fractales eliminados del sistema de alertas (quedan en chart visual)."""
    alertas = []
    n = len(df) - 1
    if n < 2:
        return alertas
    pn = float(df["Close"].iloc[n])
    prefix = f"[{ticker}] " if ticker else ""

    # RSI actual
    rsi_val = None
    if "RSI" in df.columns:
        r = df["RSI"].iloc[n]
        if pd.notna(r):
            rsi_val = float(r)

    # ── RSI extremo (≤30 o ≥70) ──────────────────────────────
    if rsi_val is not None:
        if rsi_val <= 30:
            alertas.append({
                "nivel": "bullish",
                "tipo": "rsi_extreme",
                "close": pn,
                "rsi": rsi_val,
                "pts": 2,
                "msg": prefix + f"RSI {rsi_val:.1f} ≤ 30 — zona de sobreventa",
            })
        elif rsi_val >= 70:
            alertas.append({
                "nivel": "bearish",
                "tipo": "rsi_extreme",
                "close": pn,
                "rsi": rsi_val,
                "pts": 2,
                "msg": prefix + f"RSI {rsi_val:.1f} ≥ 70 — zona de sobrecompra",
            })

    return alertas


# ─── CONFLUENCIAS ────────────────────────────────────────────


def evaluate_confluencias(df, ticker="", cfg=None, opens=None, components_ctx=None):
    """
    Evalúa las confluencias principales con validación DIRECCIONAL.

    NUEVO FOCO: RSI, Divergencias, Vacíos (FVG), Patrones, Fibonacci
    EMAs y MonarcaBand se mantienen en chart pero NO puntúan en confluencias.

    Direcciones:
      ① RSI < 47  → bullish  |  RSI > 53 → bearish
      ② Divergencia RSI activa → bullish/bearish según tipo
      ③ Vacío FVG activo cerca del precio → bullish/bearish según dirección
      ④ Patrón M/W/HCH confirmado → bearish/bullish
      ⑤ Fibonacci 55.9% → neutral
      ⑥ Apertura día/semana → bullish/bearish
      ⑦ Shark Fin → agotamiento extremo (+2 crossed / +4 exceeded)
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

    # ② Divergencias RSI (3 niveles)
    div_data = None
    if _detect_divs is not None and "RSI" in df.columns:
        rsi_series = df["RSI"]
        if isinstance(rsi_series, pd.DataFrame): rsi_series = rsi_series.iloc[:, 0]
        all_divs = _detect_divs(df, rsi_series)
        recent_divs = [d for d in all_divs if d.get("bar", 0) >= len(df) - 60]
        if recent_divs:
            best = recent_divs[0]
            div_type = best["type"]
            div_dir = "bullish" if div_type in ("bull", "hbull") else "bearish" if div_type in ("bear", "hbear") else "info"
            div_data = best
            raw.append({"id": 2, "ok": True,
                "texto": f"Divergencia RSI {div_type.upper()} N{best.get('level',1)} (RSI {best.get('rsi',0):.1f})",
                "tipo": div_dir})
    if not div_data:
        raw.append({"id": 2, "ok": False,
            "texto": "Sin divergencias recientes", "tipo": "info"})

    # ③ Vacíos FVG (Imbalances)
    fvg_data = None
    if _detect_fvg is not None:
        fvgs = _detect_fvg(df, max_zonas=10)
        for fvg in fvgs:
            # Si el precio está dentro o muy cerca del FVG (sin romperlo)
            if fvg.get("touched") and not fvg.get("frozen"):
                # FVG alcista = zona de soporte (bullish) | FVG bajista = resistencia (bearish)
                fvg_dir = "bullish" if fvg["direction"] == "bull" else "bearish"
                fvg_text = (f"Vacío FVG {'alcista' if fvg_dir=='bullish' else 'bajista'} "
                           f"activo {fvg['bottom']:.2f}–{fvg['top']:.2f}")
                fvg_data = fvg
                raw.append({"id": 3, "ok": True, "texto": fvg_text, "tipo": fvg_dir})
                break
    if not fvg_data:
        raw.append({"id": 3, "ok": False,
            "texto": "Sin vacíos FVG activos", "tipo": "info"})

    # ④ Patrones M/W/HCH
    pattern_data = None
    if _calc_pattern_mw is not None:
        pat = _calc_pattern_mw(df)
        if pat.get("M") and pat["M"].get("estado") in ("confirmado", "formando_p2"):
            raw.append({"id": 4, "ok": True,
                "texto": f"Patrón M (doble techo) {pat['M']['estado']}", "tipo": "bearish"})
            pattern_data = pat["M"]
        elif pat.get("W") and pat["W"].get("estado") in ("confirmado", "formando_v2"):
            raw.append({"id": 4, "ok": True,
                "texto": f"Patrón W (doble suelo) {pat['W']['estado']}", "tipo": "bullish"})
            pattern_data = pat["W"]
    if _calc_pattern_hch is not None:
        hch = _calc_pattern_hch(df)
        if hch.get("HCH") and hch["HCH"].get("estado") in ("confirmado", "formando_hd"):
            raw.append({"id": 4, "ok": True,
                "texto": f"HCH bajista {hch['HCH']['estado']}", "tipo": "bearish"})
            pattern_data = hch["HCH"]
        elif hch.get("HCH_inv") and hch["HCH_inv"].get("estado") in ("confirmado", "formando_hd"):
            raw.append({"id": 4, "ok": True,
                "texto": f"HCH invertido alcista {hch['HCH_inv']['estado']}", "tipo": "bullish"})
            pattern_data = hch["HCH_inv"]
    if not pattern_data:
        raw.append({"id": 4, "ok": False,
            "texto": "Sin patrones M/W/HCH activos", "tipo": "info"})

    # ⑤ Apertura día/semana
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
            raw.append({"id": 5, "ok": True,
                "texto": (f"Precio sobre apertura del día ({do:.5g}) "
                          f"y semana ({wo:.5g}) → sesgo alcista"),
                "tipo": "bullish"})
        elif day_dir == "below" and week_dir == "below":
            raw.append({"id": 5, "ok": True,
                "texto": (f"Precio bajo apertura del día ({do:.5g}) "
                          f"y semana ({wo:.5g}) → sesgo bajista"),
                "tipo": "bearish"})
        elif day_dir and week_dir:
            raw.append({"id": 5, "ok": False,
                "texto": (f"Apertura día {'↑' if day_dir=='above' else '↓'} "
                          f"vs semana {'↑' if week_dir=='above' else '↓'} — sin consenso"),
                "tipo": "info"})
        else:
            raw.append({"id": 5, "ok": False,
                "texto": "Datos de apertura del día/semana incompletos", "tipo": "info"})
    else:
        raw.append({"id": 5, "ok": False,
            "texto": "Datos de apertura no disponibles", "tipo": "info"})

    # ⑥ Fibonacci 55.9% (neutral)
    recent_df = df.tail(100) if len(df) > 100 else df
    high_p = float(recent_df["High"].max())
    low_p  = float(recent_df["Low"].min())
    fib559 = high_p - (high_p - low_p) * 0.559
    tol_f  = (high_p - low_p) * 0.015
    if abs(price - fib559) <= tol_f:
        raw.append({"id": 6, "ok": True,
            "texto": f"Fibonacci 55.9% en {fib559:.5g} (rango {low_p:.5g}–{high_p:.5g})",
            "tipo": "neutral"})
    else:
        pct_dist = (price - fib559) / fib559 * 100
        raw.append({"id": 6, "ok": False,
            "texto": f"Fib 55.9% en {fib559:.5g} ({pct_dist:+.1f}% de distancia)",
            "tipo": "info"})

    # ⑦ Shark Fin
    if _calc_shark_fin is not None:
        shark = _calc_shark_fin(df)
    else:
        shark = {
            "shark_bear": False, "shark_bull": False,
            "shark_exceeds_div": False, "shark_pts": 0,
            "shark_tipo": None, "phase": None, "alert_immediate": False,
        }

    # Shark Fin se añade a raw con su tipo nativo; el PASO 2 descartará si
    # la dirección dominante es opuesta (igual que todas las confluencias).
    if shark["shark_bear"]:
        pts_shark = shark["shark_pts"]
        if shark["phase"] == "exceeded":
            raw.append({"id": 7, "ok": True,
                "texto": (f"⚡ Aleta tiburón EXTREMA — RSI pico {shark['shark_rsi_peak']:.1f} "
                          f"supera div R1 {shark['shark_div_r1']:.1f} → agotamiento máximo"),
                "tipo": "bearish",
                "pts_extra": pts_shark,
                "alert_immediate": True})
        elif shark["phase"] == "crossed":
            raw.append({"id": 7, "ok": True,
                "texto": (f"Aleta tiburón bajista — RSI pico {shark['shark_rsi_peak']:.1f} "
                          f"cruzó <70 confirmado"),
                "tipo": "bearish",
                "pts_extra": pts_shark,
                "alert_immediate": True})
        elif shark["phase"] == "forming":
            raw.append({"id": 7, "ok": False,
                "texto": (f"Aleta tiburón formándose — RSI en zona >70 "
                          f"({shark['shark_rsi_peak']:.1f}), esperando cruce"),
                "tipo": "info",
                "pts_extra": 0,
                "alert_immediate": False})
        else:
            raw.append({"id": 7, "ok": False,
                "texto": "Sin aleta de tiburón activa", "tipo": "info",
                "pts_extra": 0, "alert_immediate": False})
    elif shark["shark_bull"]:
        pts_shark = shark["shark_pts"]
        if shark["phase"] == "exceeded":
            raw.append({"id": 7, "ok": True,
                "texto": (f"⚡ Aleta tiburón EXTREMA — RSI valle {shark['shark_rsi_peak']:.1f} "
                          f"supera div S1 {shark['shark_div_r1']:.1f} → agotamiento máximo"),
                "tipo": "bullish",
                "pts_extra": pts_shark,
                "alert_immediate": True})
        elif shark["phase"] == "crossed":
            raw.append({"id": 7, "ok": True,
                "texto": (f"Aleta tiburón alcista — RSI valle {shark['shark_rsi_peak']:.1f} "
                          f"cruzó >30 confirmado"),
                "tipo": "bullish",
                "pts_extra": pts_shark,
                "alert_immediate": True})
        elif shark["phase"] == "forming":
            raw.append({"id": 7, "ok": False,
                "texto": (f"Aleta tiburón formándose — RSI en zona <30 "
                          f"({shark['shark_rsi_peak']:.1f}), esperando cruce"),
                "tipo": "info",
                "pts_extra": 0,
                "alert_immediate": False})
        else:
            raw.append({"id": 7, "ok": False,
                "texto": "Sin aleta de tiburón activa", "tipo": "info",
                "pts_extra": 0, "alert_immediate": False})
    else:
        raw.append({"id": 7, "ok": False,
            "texto": "Sin aleta de tiburón activa", "tipo": "info",
            "pts_extra": 0, "alert_immediate": False})

    # ⑨ Componentes del índice (solo ^DJI y ^NDX) — dirección real
    if components_ctx and components_ctx.get("total", 0) > 0:
        bull_pct  = components_ctx.get("bull_pct", 0)
        bear_pct  = components_ctx.get("bear_pct", 0)
        comp_dir  = components_ctx.get("direction", "mixed")
        n_bulls   = len(components_ctx.get("bulls", []))
        n_bears   = len(components_ctx.get("bears", []))
        total_c   = components_ctx.get("total", 1)
        if comp_dir == "bullish":
            raw.append({"id": 8, "ok": True,
                "texto": (f"{bull_pct}% de componentes alcistas ({n_bulls}/{total_c}) "
                          f"→ sesgo alcista del índice"),
                "tipo": "bullish"})
        elif comp_dir == "bearish":
            raw.append({"id": 8, "ok": True,
                "texto": (f"{bear_pct}% de componentes bajistas ({n_bears}/{total_c}) "
                          f"→ sesgo bajista del índice"),
                "tipo": "bearish"})
        else:
            raw.append({"id": 8, "ok": False,
                "texto": f"Componentes mixtos ({bull_pct}% ↑ / {bear_pct}% ↓)",
                "tipo": "info"})

    # ── PASO 2: determinar la dirección dominante con lógica direccional ──
    activas_bullish = [c for c in raw if c["ok"] and c["tipo"] == "bullish"]
    activas_bearish = [c for c in raw if c["ok"] and c["tipo"] == "bearish"]

    FUERTES = {1, 2, 4}   # RSI + Divergencias + Patrones son señales fuertes
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
                    # Sumar puntos extra de aleta tiburón (el punto base ya se contó)
                    if c.get("pts_extra", 0) > 0:
                        puntos += c["pts_extra"] - 1
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
        alert  = True
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
        "tb_cross":      False,
    }


# ─── ALERTAS POR CONFLUENCIAS DE TRADINGBAND ─────────────────

async def _check_tb_confluences(tickers: list, label: str = "") -> dict:
    """
    Alertas por confluencias de TradingBand:
      1) Cruce Trigger/Average → cambio de tendencia
      2) Divergencia RSI en zona (RSI≤45 alcista / RSI≥55 bajista)
      3) Toque de nivel clave / fractal
    Solo alerta si >=2 confluencias alineadas y el cruce ocurrió
    en la última vela 1h (≤1h de antigüedad).
    """
    if not HAS_NOTIFIER or not HAS_TB:
        return {}
    if _detect_divs is None:
        return {}

    now = time.time()
    alerts_by_ticker: dict = {}

    for t in tickers:
        try:
            cfg = _TB_CFG.get(t.upper(), _TB_CFG.get("_default"))

            df, bar_type = await async_download_rb(t.upper())
            if df.empty:
                continue

            # ─── INYECTAR PRECIO ACTUAL ───
            live_price = await _get_current_price(t.upper())
            if live_price and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if abs(live_price - last_close) / last_close > 0.0005:
                    new_idx = pd.Timestamp.now(tz="UTC")
                    ghost = pd.DataFrame({
                        "Open": [live_price], "High": [max(live_price, last_close)],
                        "Low":  [min(live_price, last_close)], "Close": [live_price],
                        "Volume": [0.0],
                    }, index=[new_idx])
                    df = pd.concat([df, ghost])
                    df.sort_index(inplace=True)
            # ─── FIN INYECCIÓN ───

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

                # Filtro de antigüedad: velas 1h <= 1h de antigüedad
                ts = df_calc.index[i]
                try:
                    ts_parsed = pd.Timestamp(ts)
                    if ts_parsed.tzinfo is None:
                        ts_parsed = ts_parsed.tz_localize("UTC")
                    age_min = (pd.Timestamp.now(tz="UTC") - ts_parsed).total_seconds() / 60
                    max_age_min = 60   # 1 hora
                    if age_min > max_age_min:
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
            df, bar_type = await async_download_rb(t.upper())
            if df.empty:
                continue

            # ─── INYECTAR PRECIO ACTUAL ───
            live_price = await _get_current_price(t.upper())
            if live_price and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if abs(live_price - last_close) / last_close > 0.0005:
                    new_idx = pd.Timestamp.now(tz="UTC")
                    ghost = pd.DataFrame({
                        "Open": [live_price], "High": [max(live_price, last_close)],
                        "Low":  [min(live_price, last_close)], "Close": [live_price],
                        "Volume": [0.0],
                    }, index=[new_idx])
                    df = pd.concat([df, ghost])
                    df.sort_index(inplace=True)
            # ─── FIN INYECCIÓN ───

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

                # Filtro de frescura: fantasma <=15min permitida, velas reales <=12h
                try:
                    age_min = (pd.Timestamp.now(tz="UTC") - ts_utc).total_seconds() / 60
                    max_age_min = 60   # 1 hora
                    if age_min > max_age_min:
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


def _is_weekend_now() -> bool:
    """Retorna True si es sábado o domingo (UTC)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).weekday() >= 5


def _filter_expired_alerts(alerts_by_ticker: dict, max_age_seconds: int = 3600) -> dict:
    """
    Filtra alertas cuyo ts_utc_iso sea mayor a max_age_seconds.
    Descarta silenciosamente cualquier evento antiguo.
    """
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc)
    filtered = {}
    for ticker, alerta_list in alerts_by_ticker.items():
        valid = []
        for a in alerta_list:
            ts_iso = a.get("ts_utc_iso", "")
            try:
                if ts_iso:
                    ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    age = (now_utc - ts).total_seconds()
                    if age <= max_age_seconds:
                        valid.append(a)
                    else:
                        print(f"[alert-filter] Descartada alerta de {ticker} con {age/60:.0f} min de antigüedad")
                else:
                    valid.append(a)
            except Exception:
                valid.append(a)
        if valid:
            filtered[ticker] = valid
    return filtered


def _apply_weekend_filter(alerts_by_ticker: dict) -> dict:
    """
    En fin de semana solo permite alertas de BTC-USD.
    """
    if not _is_weekend_now():
        return alerts_by_ticker
    filtered = {t: v for t, v in alerts_by_ticker.items() if t == "BTC-USD"}
    skipped = [t for t in alerts_by_ticker if t != "BTC-USD"]
    if skipped:
        print(f"[weekend-filter] Fin de semana — ignorando alertas de: {', '.join(skipped)}")
    return filtered


async def _check_divergences() -> dict:
    """
    Checks all WATCH_TICKERS for new RSI divergences (N1 and N2 only).
    Returns divs_by_ticker: {ticker_upper: [div_dict, ...]} for fresh, unseen divergences.
    """
    if not HAS_TB or _detect_all_divergences is None or _calc_rsi is None:
        return {}

    now = time.time()
    divs_by_ticker: dict = {}

    for t in WATCH_TICKERS:
        try:
            df = await async_download(t.upper(), period="6mo", interval="4h", progress=False)
            if df.empty:
                continue
            df = clean_df(df)

            close = df["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]

            rsi = _calc_rsi(close).dropna()
            if rsi.empty:
                continue

            df_aligned = df.loc[rsi.index].copy()
            df_lower = df_aligned.rename(columns=lambda c: c.lower())

            all_divs = _detect_all_divergences(df_lower, rsi)

            n = len(rsi)
            new_divs = []
            for dv in all_divs:
                if dv["level"] > 2:
                    continue
                lb_r = 5 if dv["level"] == 1 else 10
                if dv["bar"] < n - lb_r - 1:
                    continue
                cache_key = f"{t}_{dv['type']}_{dv['level']}_{dv['time']}"
                if now - _div_cache.get(cache_key, 0) > _DIV_DEDUP_SECONDS:
                    _div_cache[cache_key] = now
                    new_divs.append(dv)

            if new_divs:
                divs_by_ticker[t.upper()] = new_divs

        except Exception as e:
            print(f"[div-check] Error en {t}: {e}")

    return divs_by_ticker


async def scheduled_watch():
    """Revisión periódica — analiza la última vela + confluencias TradingBand."""
    if not HAS_NOTIFIER:
        return

    watch_tickers = WATCH_TICKERS
    if _is_weekend_now():
        watch_tickers = [t for t in WATCH_TICKERS if t == "BTC-USD"]
        print("[scheduler] Fin de semana — revisando solo BTC-USD")

    # 1) Alertas de confluencia TradingBand (cruce Trigger/Average + div + nivel)
    tb_alerts = await _check_tb_confluences(watch_tickers, label="scheduler")
    if tb_alerts:
        tb_alerts = _apply_weekend_filter(_filter_expired_alerts(tb_alerts))
    if tb_alerts:
        total = sum(len(v) for v in tb_alerts.values())
        print(f"[scheduler] {total} alertas de TradingBand en {len(tb_alerts)} ticker(s)")
        await notify_users_with_alerts(tb_alerts)

    # 2) Alertas clásicas de confluencias EMA/RSI/fractales
    alerts_by_ticker = await _check_tickers(
        watch_tickers, num_candles=1, label="scheduler"
    )
    if alerts_by_ticker:
        alerts_by_ticker = _apply_weekend_filter(_filter_expired_alerts(alerts_by_ticker))
    if alerts_by_ticker:
        total = sum(len(v) for v in alerts_by_ticker.values())
        print(f"[scheduler] {total} alertas nuevas en {len(alerts_by_ticker)} ticker(s)")
        await notify_users_with_alerts(alerts_by_ticker)
    else:
        print("[scheduler] Sin alertas nuevas")

    divs_by_ticker = await _check_divergences()
    if divs_by_ticker:
        total_divs = sum(len(v) for v in divs_by_ticker.values())
        print(f"[div-check] {total_divs} divergencia(s) nueva(s) en {len(divs_by_ticker)} ticker(s)")
        await notify_divergences(divs_by_ticker)
    else:
        print("[div-check] Sin divergencias nuevas")

    await _update_rsi_watchlist()


async def daily_catchup():
    """Catch-up al arrancar: revisa SOLO la última vela de rango (≤12h) — nada muy antiguo."""
    if not HAS_NOTIFIER:
        return

    catchup_tickers = WATCH_TICKERS
    if _is_weekend_now():
        catchup_tickers = [t for t in WATCH_TICKERS if t == "BTC-USD"]
        print("[catchup] Fin de semana — revisando solo BTC-USD")

    # 1) Confluencias TradingBand (siempre frescas ≤12h)
    tb_alerts = await _check_tb_confluences(catchup_tickers, label="catchup")
    if tb_alerts:
        tb_alerts = _apply_weekend_filter(_filter_expired_alerts(tb_alerts))
    if tb_alerts:
        total = sum(len(v) for v in tb_alerts.values())
        print(f"[catchup] {total} alertas TradingBand enviadas")
        await notify_users_with_alerts(tb_alerts)

    # 2) Alertas clásicas
    print("[catchup] Revisando vela 1h actual (máx 1h de antigüedad)…")
    alerts_by_ticker = await _check_tickers(
        catchup_tickers, num_candles=1, label="catchup", max_per_ticker=1
    )
    if alerts_by_ticker:
        alerts_by_ticker = _apply_weekend_filter(_filter_expired_alerts(alerts_by_ticker))
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
            df, bar_type = await async_download_rb(t.upper())
            if df.empty:
                continue

            # ─── INYECTAR PRECIO ACTUAL ───
            live_price = await _get_current_price(t.upper())
            if live_price and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if abs(live_price - last_close) / last_close > 0.0005:
                    new_idx = pd.Timestamp.now(tz="UTC")
                    ghost = pd.DataFrame({
                        "Open": [live_price], "High": [max(live_price, last_close)],
                        "Low":  [min(live_price, last_close)], "Close": [live_price],
                        "Volume": [0.0],
                    }, index=[new_idx])
                    df = pd.concat([df, ghost])
                    df.sort_index(inplace=True)
            # ─── FIN INYECCIÓN ───

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
            df, bar_type = await async_download_rb(ticker)
            if df.empty:
                continue

            # ─── INYECTAR PRECIO ACTUAL ───
            live_price = await _get_current_price(ticker)
            if live_price and not df.empty:
                last_close = float(df["Close"].iloc[-1])
                if abs(live_price - last_close) / last_close > 0.0005:
                    new_idx = pd.Timestamp.now(tz="UTC")
                    ghost = pd.DataFrame({
                        "Open": [live_price], "High": [max(live_price, last_close)],
                        "Low":  [min(live_price, last_close)], "Close": [live_price],
                        "Volume": [0.0],
                    }, index=[new_idx])
                    df = pd.concat([df, ghost])
                    df.sort_index(inplace=True)
            # ─── FIN INYECCIÓN ───

            col_rsi = "RSI"
            if col_rsi not in df.columns:
                if _calc_rsi_tv is not None:
                    df["RSI"] = _calc_rsi_tv(df["Close"], 14)
                else:
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

            # ─── Detección inmediata de Aleta Tiburón (Shark Fin) ───
            shark = None
            if _calc_shark_fin is not None:
                df_shark, _ = await async_download_rb(ticker)
                if not df_shark.empty:
                    shark = _calc_shark_fin(df_shark)
                    if shark.get("alert_immediate") and shark.get("phase") in ("exceeded", "crossed"):
                        tipo  = shark["shark_tipo"]
                        phase = shark["phase"]
                        # ── Solo alertar si la fase CAMBIÓ vs la anterior ──
                        prev = _shark_phase_cache.get(ticker)
                        phase_changed = (prev is None) or (prev.get("tipo") != tipo) or (prev.get("phase") != phase)
                        if phase_changed:
                            if phase == "exceeded":
                                emoji = "⚡🦈"
                                msg_txt = (f"[{ticker}] {emoji} ALETA TIBURÓN EXTREMA\n"
                                           f"RSI pico {shark['shark_rsi_peak']:.1f} superó "
                                           f"divergencia R1={shark['shark_div_r1']:.1f}\n"
                                           f"Agotamiento máximo — señal inmediata")
                                pts_label = "+4 pts"
                            else:
                                emoji = "🦈"
                                msg_txt = (f"[{ticker}] {emoji} Aleta tiburón confirmada\n"
                                           f"RSI cruzó {'<70' if tipo == 'bearish' else '>30'} "
                                           f"tras divergencia — señal confirmada")
                                pts_label = "+2 pts"

                            ts_now = pd.Timestamp.now(tz="UTC")
                            alertas_rsi.setdefault(ticker, []).append({
                                "nivel":        tipo,
                                "msg":          msg_txt,
                                "hora":         ts_now.strftime("%d/%m %H:%M"),
                                "ts_utc_iso":   ts_now.isoformat(),
                                "dia_num":      ts_now.weekday(),
                                "dia_name":     ts_now.strftime("%A"),
                                "resultado":    None,
                                "components_ctx": None,
                                "shark_data":   shark,
                                "pts_label":    pts_label,
                            })
                            _shark_phase_cache[ticker] = {"tipo": tipo, "phase": phase, "ts": now}
                            print(f"[shark] {emoji} {ticker} — {phase} (nuevo) RSI={shark['shark_rsi_peak']:.1f}")

            df_rb, _ = await async_download_rb(ticker)
            if df_rb.empty:
                continue
            df_rb = calc_indicators(df_rb, cfg["ema_short"], cfg["ema_long"])
            opens_data = calc_opens(df_rb)

            components_ctx = info.get("components_ctx")
            resultado = evaluate_confluencias(df_rb, ticker=ticker, cfg=cfg,
                                              opens=opens_data, components_ctx=components_ctx)

            if not resultado:
                continue

            resultado["rsi"] = rsi_now
            for c in resultado.get("confluencias", []):
                if c["id"] == 1:
                    if direction == "bullish" and rsi_now <= 30:
                        c["ok"] = True
                        c["tipo"] = "bullish"
                        c["texto"] = f"⚡ RSI en zona de compra ({rsi_now:.1f}) → COMPRA"
                        c.pop("descartada", None)
                        c.pop("conflicto", None)
                    elif direction == "bearish" and rsi_now >= 70:
                        c["ok"] = True
                        c["tipo"] = "bearish"
                        c["texto"] = f"⚡ RSI en zona de venta ({rsi_now:.1f}) → VENTA"
                        c.pop("descartada", None)
                        c.pop("conflicto", None)

            puntos = sum(1 for c in resultado["confluencias"]
                         if c.get("ok") and not c.get("descartada") and not c.get("conflicto"))
            resultado["puntos"] = puntos
            resultado["estado"] = "FAVORABLE" if puntos >= 4 else "INTERESANTE"
            resultado["nivel"] = direction
            resultado["alert"] = puntos >= 3
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
        alertas_rsi = _apply_weekend_filter(_filter_expired_alerts(alertas_rsi))
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
                    f"✅ <b>¡Suscrito a Trading Band!</b>\n\n"
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
                    chat_id, "✅ <b>Trading Band activo</b>\nRevisión cada 30 min + RSI cada 2 min."
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
            from apscheduler.triggers.interval import IntervalTrigger
            async def _tb_confl_job():
                await _check_tb_confluences(WATCH_TICKERS)
            scheduler.add_job(_tb_confl_job,
                              "interval", minutes=30, id="tb_confl_30m")
            scheduler.add_job(_rsi_realtime_check, "interval",
                              minutes=_RSI_WATCH_INTERVAL_MIN, id="rsi_rt")
            scheduler.start()
            print(f"[scheduler] Iniciado · TB confluencias cada 30 min + revisión 30 min + RSI real-time cada {_RSI_WATCH_INTERVAL_MIN} min")
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
        range_size = tb_cfg.get("range_calc", tb_cfg.get("range", 0))
        range_display = tb_cfg.get("range", 1000)

        use_rb = HAS_TB and _build_rb is not None and range_size > 0
        df = await async_download(sym, period="6mo", interval="1h", progress=False)
        df = clean_df(df)
        bar_type = "1h"

        if df.empty:
            return {"error": "Simbolo no encontrado: " + ticker}
        df = calc_indicators(df, es, el)
        df = calc_trading_band(df)
        ult = float(df["Close"].iloc[-1])
        ts = ts_ms(df.index)
        max_candles = 200
        start_i = max(0, len(df) - max_candles)
        candles = [
            {
                "x": ts[i],
                "o": safe(df["Open"].iloc[i]),
                "h": safe(df["High"].iloc[i]),
                "l": safe(df["Low"].iloc[i]),
                "c": safe(df["Close"].iloc[i]),
            }
            for i in range(start_i, len(df))
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

        # ── Patrones de precio M / W / HCH ───────────────
        def _sanitize_pat(p):
            """Reemplaza NaN/Inf en un dict de patrón por None para JSON seguro."""
            if not p:
                return p
            import math
            out = {}
            for k, v in p.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    out[k] = None
                else:
                    out[k] = v
            return out

        patterns = {"M": None, "W": None, "HCH": None, "HCH_inv": None}
        try:
            if _calc_pattern_mw is not None:
                mw = _calc_pattern_mw(df)
                patterns["M"] = _sanitize_pat(mw.get("M"))
                patterns["W"] = _sanitize_pat(mw.get("W"))
            if _calc_pattern_hch is not None:
                hch = _calc_pattern_hch(df)
                patterns["HCH"]     = _sanitize_pat(hch.get("HCH"))
                patterns["HCH_inv"] = _sanitize_pat(hch.get("HCH_inv"))
        except Exception as _pe:
            print(f"[patterns] Error calculando patrones para {sym}: {_pe}")

        # Vacíos FVG para el chart
        fvg_zonas = _detect_fvg(df, max_zonas=10) if _detect_fvg else []
        fvg_chart = [
            {"top": z["top"], "bottom": z["bottom"],
             "direction": z["direction"], "touched": z.get("touched", False)}
            for z in fvg_zonas
        ]

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
                "fvg": fvg_chart,
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
            "patterns": patterns,
        }
    except Exception as e:
        return {"error": str(e)}


async def _get_current_price(ticker: str) -> float | None:
    """Obtiene precio actual de yfinance (rápido)."""
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.info
        price = info.get("currentPrice") or info.get("regularMarketPrice") or info.get("previousClose")
        if price:
            return float(price)
        # Fallback: último minuto
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        if not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return None


async def _compute_row(ticker: str) -> dict:
    key = ticker.upper()
    now = time.time()
    cached = _row_cache.get(key)
    if cached and now - cached["ts"] < _ROW_TTL:
        return cached["data"]
    cfg = get_cfg(ticker)
    es, el = cfg["ema_short"], cfg["ema_long"]
    df, bar_type = await async_download_rb(key)
    if df.empty:
        return {"error": "not found"}

    # ——— INYECTAR PRECIO ACTUAL ———
    # Obtener precio en tiempo real y agregar como última fila "fantasma"
    # para que la evaluación siempre use el precio más reciente
    live_price = await _get_current_price(key)
    if live_price and not df.empty:
        last_close = float(df["Close"].iloc[-1])
        # Solo inyectar si hay diferencia significativa (>0.05%)
        if abs(live_price - last_close) / last_close > 0.0005:
            last_idx = df.index[-1]
            new_idx = last_idx + pd.Timedelta(minutes=1)
            ghost = pd.DataFrame({
                "Open": [live_price], "High": [max(live_price, last_close)],
                "Low":  [min(live_price, last_close)], "Close": [live_price],
                "Volume": [0.0],
            }, index=[new_idx])
            df = pd.concat([df, ghost])
            df.sort_index(inplace=True)
    # ——— FIN INYECCIÓN ———

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

    # Datos de Shark Fin para la tabla — primero via confluencia (ok=True), luego directo
    shark_phase = "none"
    shark_tipo = None
    if confl:
        shark_conf = next((c for c in confl.get("confluencias", []) if c.get("id") == 7), None)
        if shark_conf and shark_conf.get("ok"):
            shark_phase = "exceeded" if "EXTREMA" in shark_conf.get("texto", "") else "crossed"
            shark_tipo = shark_conf.get("tipo")
    # Si confluencia no lo activa, intentar detección directa (formando o dirección contraria)
    if shark_phase == "none" and _calc_shark_fin is not None:
        try:
            sf_direct = _calc_shark_fin(df)
            _ph = sf_direct.get("phase")
            if _ph in ("forming", "crossed", "exceeded"):
                shark_phase = _ph
                shark_tipo = sf_direct.get("shark_tipo")
        except Exception:
            pass

    # Detección de patrones M/W/HCH para la tabla
    pat_estado = None
    pat_tipo = None  # 'M' | 'W' | 'HCH' | 'HCH_inv'
    try:
        _PRIO = {"confirmado": 5, "formando_hd": 4, "formando_p2": 3, "formando_v2": 3, "formando": 1}
        best_score = 0
        if _calc_pattern_mw is not None:
            import math as _math
            mw = _calc_pattern_mw(df)
            for _pn, _pd in (("M", mw.get("M")), ("W", mw.get("W"))):
                if _pd and _pd.get("estado"):
                    sc = _PRIO.get(_pd["estado"], 0)
                    if sc > best_score:
                        best_score = sc
                        pat_estado = _pd["estado"]
                        pat_tipo = _pn
        if _calc_pattern_hch is not None:
            hch = _calc_pattern_hch(df)
            for _pn, _pd in (("HCH", hch.get("HCH")), ("HCH_inv", hch.get("HCH_inv"))):
                if _pd and _pd.get("estado"):
                    sc = _PRIO.get(_pd["estado"], 0)
                    if sc > best_score:
                        best_score = sc
                        pat_estado = _pd["estado"]
                        pat_tipo = _pn
    except Exception as _pe:
        pass

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
        "shark_phase": shark_phase,
        "shark_tipo": shark_tipo,
        "pat_estado": pat_estado,
        "pat_tipo": pat_tipo,
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
            df, bar_type = await async_download_rb(t.upper())
            if not df.empty:
                # ─── INYECTAR PRECIO ACTUAL ───
                live_price = await _get_current_price(t.upper())
                if live_price and not df.empty:
                    last_close = float(df["Close"].iloc[-1])
                    if abs(live_price - last_close) / last_close > 0.0005:
                        new_idx = pd.Timestamp.now(tz="UTC")
                        ghost = pd.DataFrame({
                            "Open": [live_price], "High": [max(live_price, last_close)],
                            "Low":  [min(live_price, last_close)], "Close": [live_price],
                            "Volume": [0.0],
                        }, index=[new_idx])
                        df = pd.concat([df, ghost])
                        df.sort_index(inplace=True)
                # ─── FIN INYECCIÓN ───
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
