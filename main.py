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
        detect_supply_demand_zones as _detect_sd,
        detect_all_divergences as _detect_all_divergences,
        calc_rsi as _calc_rsi,
        build_rsi_div_segments as _build_rsi_div_segs,
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
    _detect_sd = None
    _detect_all_divergences = None
    _calc_rsi = None
    _build_rsi_div_segs = None
    print(f"[WARN] trading_band_routes no disponible: {_tb_err}")

try:
    from user_routes import router as auth_router, users_router
    HAS_AUTH = True
except Exception as _auth_err:
    HAS_AUTH = False
    print(f"[WARN] user_routes no disponible: {_auth_err}")

try:
    from setup_engine import evaluate_setup as _evaluate_setup
    HAS_SETUP_ENGINE = True
except Exception as _se_err:
    _evaluate_setup = None
    HAS_SETUP_ENGINE = False
    print(f"[WARN] setup_engine no disponible: {_se_err}")

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
        _supa_get,
        _supa_post,
        _supa_delete,
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

# Cache para el Setup Engine (3 criterios + scoring)
_setup_cache: dict = {}
_SETUP_DEDUP_SECONDS = 4 * 3600  # No repetir el mismo setup en 4h

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
    df = await async_download(ticker, period="3mo", interval="1h", progress=False)
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


# calc_fractales / detect_fractal_touch — ELIMINADOS (sistema legacy con TradingBand)


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
    """Alertas de vigilancia — FOCO: RSI extremo, Divergencias, Zonas Supply/Demand."""
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

    FOCO: RSI, Divergencias, Zonas Supply/Demand, Soportes/Resistencias, Shark Fin.

    Direcciones:
      ① RSI <30 o >70 → extremo (+2)  |  RSI 30-44 / 56-70 → interés (+1)  |  45-55 → neutro
      ② Divergencia RSI activa → bullish/bearish según tipo
      ③ Zona Supply/Demand activa cerca del precio → bullish/bearish según dirección
      ④ HOD/LOD/PDH/PDL → soportes y resistencias (+1 cada uno)
      ⑤ Shark Fin → agotamiento extremo (+2 crossed / +4 exceeded)
    """
    if len(df) < 14:
        return None

    n     = len(df) - 1
    price = float(df["Close"].iloc[n])
    bar_high = float(df["High"].iloc[n]) if "High" in df.columns else None
    bar_low  = float(df["Low"].iloc[n])  if "Low"  in df.columns else None
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

    # ① RSI — zona de interés para reversión
    # <30 o >70: extremo (+2 pts)  |  30-44 / 56-70: zona de interés (+1 pt)
    # 45-55: tierra de nadie (0 pts)
    if rsi < 30:
        raw.append({"id": 1, "ok": True,
            "texto": f"⚡ RSI extremo {rsi:.1f} (<30) → sobreventa máxima", "tipo": "bullish", "pts": 2})
    elif rsi < 45:
        raw.append({"id": 1, "ok": True,
            "texto": f"RSI bajo ({rsi:.1f}) → favorable para largos", "tipo": "bullish", "pts": 1})
    elif rsi > 70:
        raw.append({"id": 1, "ok": True,
            "texto": f"⚡ RSI extremo {rsi:.1f} (>70) → sobrecompra máxima", "tipo": "bearish", "pts": 2})
    elif rsi > 55:
        raw.append({"id": 1, "ok": True,
            "texto": f"RSI alto ({rsi:.1f}) → favorable para cortos", "tipo": "bearish", "pts": 1})
    else:
        raw.append({"id": 1, "ok": False,
            "texto": f"RSI neutro ({rsi:.1f})", "tipo": "info", "pts": 0})

    # ② Divergencias RSI (3 niveles)
    div_data = None
    if _detect_divs is not None and "RSI" in df.columns:
        rsi_series = df["RSI"]
        if isinstance(rsi_series, pd.DataFrame): rsi_series = rsi_series.iloc[:, 0]
        all_divs = _detect_divs(df, rsi_series)
        recent_divs = [d for d in all_divs if d.get("bar", 0) >= len(df) - 60]

        # Filtrar: solo divergencias alineadas con el RSI actual
        # RSI < 40 → solo alcistas | RSI > 60 → solo bajistas
        if recent_divs:
            if rsi < 40:
                recent_divs = [d for d in recent_divs if d["type"] in ("bull", "hbull")]
            elif rsi > 60:
                recent_divs = [d for d in recent_divs if d["type"] in ("bear", "hbear")]

        if recent_divs:
            # Puntos = nivel máximo de divergencia: N1=1, N2=2, N3=3
            best = max(recent_divs, key=lambda d: d.get("level", 1))
            div_pts = best.get("level", 1)
            div_type = best["type"]
            div_dir = "bullish" if div_type in ("bull", "hbull") else "bearish" if div_type in ("bear", "hbear") else "info"
            div_data = best
            raw.append({"id": 2, "ok": True,
                "texto": f"Divergencia RSI {div_type.upper()} N{best.get('level',1)} (RSI {best.get('rsi',0):.1f})",
                "tipo": div_dir, "pts": div_pts})
    if not div_data:
        raw.append({"id": 2, "ok": False,
            "texto": "Sin divergencias recientes", "tipo": "info"})

    # ③ ZONAS SUPPLY / DEMAND (Liquidez)
    sd_data = None
    sd_extra_pts = 0
    if _detect_sd is not None:
        sd = _detect_sd(df, lookback=10, max_zonas=30)
        zones = sd.get("zones", [])
        # Zona más cercana al precio actual que esté activa (touched)
        active_zones = [z for z in zones if z.get("touched")]
        active_zones.sort(key=lambda z: abs(price - (z["top"]+z["bottom"])/2))

        for zone in active_zones:
            zone_dir = "bullish" if zone["direction"] == "bull" else "bearish"
            zone_type = "Demand" if zone["direction"] == "bull" else "Supply"
            zone_text = (f"Zona {zone_type} activa {zone['bottom']:.2f}–{zone['top']:.2f}")
            sd_data = zone
            raw.append({"id": 3, "ok": True, "texto": zone_text, "tipo": zone_dir, "pts": 2})
            break

        # Punto extra: zona creada en vela anterior → dirección CONTRARIA
        if sd.get("created_prev") and sd.get("created_prev_dir"):
            extra_dir = "bearish" if sd["created_prev_dir"] == "bullish" else "bullish"
            extra_type = "Demand" if sd["created_prev_dir"] == "bullish" else "Supply"
            raw.append({"id": 3, "ok": True,
                "texto": f"Zona {extra_type} creada en vela anterior +1",
                "tipo": extra_dir, "pts": 1})
            sd_extra_pts += 1

        # Punto extra: zona tocada en vela actual → dirección CONTRARIA (rebote)
        if sd.get("touched_now") and sd.get("touched_now_dir"):
            extra_dir = "bearish" if sd["touched_now_dir"] == "bullish" else "bullish"
            extra_type = "Demand" if sd["touched_now_dir"] == "bullish" else "Supply"
            raw.append({"id": 3, "ok": True,
                "texto": f"Zona {extra_type} tocada ahora +1",
                "tipo": extra_dir, "pts": 1})
            sd_extra_pts += 1

    if not sd_data and sd_extra_pts == 0:
        raw.append({"id": 3, "ok": False,
            "texto": "Sin zonas Supply/Demand activas", "tipo": "info"})

    # ④ Patrones M/W/HCH — ELIMINADOS del scoring.
    # Los patrones crean contradicciones falsas con la estrategia de
    # liquidez + divergencias + Supply/Demand. Se mantienen en chart visual pero
    # NO puntúan ni determinan dirección en confluencias.
    pattern_data = None
    raw.append({"id": 4, "ok": False,
        "texto": "Patrones desactivados del scoring", "tipo": "info"})

    # ④ Soporte / Resistencia — techos y suelos: día actual (+1), día anterior (+1), semana (+1)
    try:
        idx = df.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        else:
            idx = idx.tz_convert("UTC")
        dates = idx.normalize()
        last_date = dates[-1]

        # — HOD/LOD del día actual (last 24 velas)
        day_high = float(df["High"].iloc[-min(24, len(df)):].max()) if len(df) >= 1 else None
        day_low  = float(df["Low"].iloc[-min(24, len(df)):].min())  if len(df) >= 1 else None
        vela_toca_lod = bar_low is not None and day_low is not None and bar_low <= day_low * 1.001
        vela_toca_hod = bar_high is not None and day_high is not None and bar_high >= day_high * 0.999
        if vela_toca_lod:
            raw.append({"id": 5, "ok": True,
                "texto": f"LOD {day_low:.4g} — precio tocando soporte", "tipo": "bullish", "pts": 1})
        if vela_toca_hod:
            raw.append({"id": 5, "ok": True,
                "texto": f"HOD {day_high:.4g} — precio tocando resistencia", "tipo": "bearish", "pts": 1})

        # — PDH/PDL del día anterior
        all_unique_days = sorted(dates.unique())
        prev_days = [d for d in all_unique_days if d < last_date]
        prev_date = prev_days[-1] if prev_days else None
        yest_df = df[dates == prev_date] if prev_date is not None else pd.DataFrame()
        pdh = float(yest_df["High"].max()) if not yest_df.empty else None
        pdl = float(yest_df["Low"].min())  if not yest_df.empty else None
        vela_toca_pdh = bar_high is not None and pdh is not None and bar_high >= pdh * 0.999
        vela_toca_pdl = bar_low is not None and pdl is not None and bar_low <= pdl * 1.001
        if vela_toca_pdl:
            raw.append({"id": 5, "ok": True,
                "texto": f"PDL {pdl:.4g} — precio tocando suelo ayer", "tipo": "bullish", "pts": 1})
        if vela_toca_pdh:
            raw.append({"id": 5, "ok": True,
                "texto": f"PDH {pdh:.4g} — precio tocando techo ayer", "tipo": "bearish", "pts": 1})

        # — Weekly high/low (desde inicio de semana)
        week_start = (last_date - pd.Timedelta(days=last_date.dayofweek))
        week_df = df[dates >= week_start]
        week_high = float(week_df["High"].max()) if not week_df.empty else None
        week_low  = float(week_df["Low"].min())  if not week_df.empty else None
        vela_toca_wl = bar_low is not None and week_low is not None and bar_low <= week_low * 1.001
        vela_toca_wh = bar_high is not None and week_high is not None and bar_high >= week_high * 0.999
        if vela_toca_wl:
            raw.append({"id": 5, "ok": True,
                "texto": f"WEEK LOW {week_low:.4g} — precio tocando suelo semanal", "tipo": "bullish", "pts": 1})
        if vela_toca_wh:
            raw.append({"id": 5, "ok": True,
                "texto": f"WEEK HIGH {week_high:.4g} — precio tocando techo semanal", "tipo": "bearish", "pts": 1})

        if not (vela_toca_lod or vela_toca_hod or vela_toca_pdl or vela_toca_pdh or vela_toca_wl or vela_toca_wh):
            raw.append({"id": 5, "ok": False,
                "texto": f"HOD {day_high:.4g} / LOD {day_low:.4g}", "tipo": "info"})
    except Exception:
        raw.append({"id": 5, "ok": False,
            "texto": "HOD/LOD no disponible", "tipo": "info"})

    # ⑦ Shark Fin — calcular SOLO sobre las últimas 24 velas para que la alerta
    # corresponda al momento de la vela evaluada, no a divergencias históricas.
    if _calc_shark_fin is not None:
        df_shark = df.iloc[-min(24, len(df)):].copy()
        shark = _calc_shark_fin(df_shark)
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

    # ── Dirección dominante ──
    # Señales FUERTES: Divergencias (2) + Shark Fin (7)
    # RSI (1) es de contexto (zona) — nunca determina dirección por sí solo.
    # Supply/Demand (3) y HOD/LOD (5) son confirmaciones direccionales.
    # Patrones M/W/HCH eliminados — creaban contradicciones falsas.
    FUERTES = {2, 7}
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
        # Dirección: fuertes primero, luego todas
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
                    # RSI (1) se descarta si contradice dirección fuerte — no puntúa
                    entry["ok"] = False
                    entry["descartada"] = True
                else:
                    base_pts = c.get("pts", 1)
                    puntos += base_pts
                    # Sumar puntos extra de aleta tiburón (crossed=+2, exceeded=+4)
                    if c.get("pts_extra", 0) > 0:
                        puntos += c["pts_extra"]
            elif c["ok"] and c["tipo"] == "neutral":
                puntos += c.get("pts", 1)
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
    elif puntos >= 7:
        estado = "FAVORABLE"
        nivel  = direction
        alert  = True
    elif puntos >= 5:
        estado = "INTERESANTE"
        nivel  = direction
        alert  = False
    elif puntos >= 3:
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
        "hod":           day_high,
        "lod":           day_low,
        "pdh":           pdh,
        "pdl":           pdl,
        "bar_high":      bar_high,
        "bar_low":       bar_low,
        "tb_cross":      False,
    }


# ─── (ALERTAS POR CONFLUENCIAS DE TRADINGBAND — ELIMINADO: sistema legacy con MonarcaBand/fractales) ───


# ─── SCHEDULER ───────────────────────────────────────────────


async def _check_one_ticker(t: str, num_candles: int, label: str, max_per_ticker: int) -> dict | None:
    """Revisa un solo ticker y retorna dict {ticker: [alertas]} o None."""
    now = time.time()
    try:
        cfg = get_cfg(t)
        df, bar_type = await async_download_rb(t.upper())
        if df.empty:
            return None

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

            # Filtro de frescura: vela histórica <=90min (3 velas 1h de margen)
            try:
                age_min = (pd.Timestamp.now(tz="UTC") - ts_utc).total_seconds() / 60
                max_age_min = 90
                if age_min > max_age_min:
                    continue
            except Exception:
                pass

            resultado = evaluate_confluencias(
                df_slice, ticker=t.upper(), cfg=cfg,
                opens=opens_data, components_ctx=components_ctx,
            )

            if resultado and resultado.get("alert"):
                vela_key = f"VELA_{t}_{ts_utc_iso}"
                if vela_key not in _sent_cache:
                    nuevas.append({
                        "nivel":          resultado["nivel"],
                        "msg":            f"[{t.upper()}] {resultado['estado']} {hora}".strip(),
                        "hora":           hora, "ts_utc_iso": ts_utc_iso,
                        "dia_num":        dia_num, "dia_name": dia_name,
                        "resultado":      resultado, "components_ctx": components_ctx,
                    })
                    _sent_cache[vela_key] = now
        if max_per_ticker > 0:
            nuevas = nuevas[:max_per_ticker]
        if nuevas:
            return {t.upper(): nuevas}
        return None
    except Exception as e:
        print(f"[{label or 'scheduler'}] Error en {t}: {e}")
        return None


async def _check_tickers(tickers: list, num_candles: int = 1, label: str = "",
                         max_per_ticker: int = 0) -> dict:
    """
    Revisa los tickers dados EN PARALELO (gather).
    num_candles: cuántas velas recientes analizar.
    max_per_ticker: si > 0, limita alertas enviadas por activo.
    """
    tasks = [_check_one_ticker(t, num_candles, label, max_per_ticker) for t in tickers]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    alerts_by_ticker: dict = {}
    for r in results:
        if isinstance(r, dict) and r:
            alerts_by_ticker.update(r)
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
                if dv["level"] > 3:
                    continue
                lb_r = 5 if dv["level"] == 1 else 10 if dv["level"] == 2 else 20
                if dv["bar"] < n - lb_r - 1:
                    continue
                cache_key = f"{t}_{dv['type']}_{dv['level']}_{dv['time']}"
                if now - _div_cache.get(cache_key, 0) > _DIV_DEDUP_SECONDS:
                    _div_cache[cache_key] = now
                    asyncio.create_task(
                        _persist_div_alert(cache_key, t, dv["type"], dv["level"], str(dv["time"]))
                    )
                    new_divs.append(dv)

            if new_divs:
                divs_by_ticker[t.upper()] = new_divs

        except Exception as e:
            print(f"[div-check] Error en {t}: {e}")

    return divs_by_ticker


async def _load_div_cache_from_supabase() -> None:
    """On startup: pre-populate _div_cache from the last 12 h of div_alerts_sent rows."""
    if not HAS_NOTIFIER:
        return
    try:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = await _supa_get(f"div_alerts_sent?sent_at=gte.{cutoff}&select=cache_key,sent_at")
        loaded = 0
        for row in rows:
            key = row.get("cache_key")
            sent_at_str = row.get("sent_at", "")
            if not key:
                continue
            try:
                from datetime import timezone
                ts = datetime.fromisoformat(sent_at_str.replace("Z", "+00:00")).timestamp()
            except Exception:
                ts = time.time()
            _div_cache[key] = ts
            loaded += 1
        print(f"[div-dedup] {loaded} entrada(s) cargadas de Supabase al arrancar")
    except Exception as e:
        print(f"[div-dedup] Error cargando caché de divergencias desde Supabase: {e}")


async def _persist_div_alert(cache_key: str, ticker: str, div_type: str, level: int, div_time: str) -> None:
    """Persist a sent divergence alert to Supabase so the dedup cache survives restarts."""
    if not HAS_NOTIFIER:
        return
    try:
        from datetime import datetime
        sent_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await _supa_post(
            "div_alerts_sent",
            {
                "cache_key": cache_key,
                "ticker": ticker,
                "type": div_type,
                "level": level,
                "time": div_time,
                "sent_at": sent_at,
            },
            prefer="resolution=ignore-duplicates",
        )
    except Exception as e:
        print(f"[div-dedup] Error persistiendo alerta en Supabase: {e}")


async def _cleanup_div_alerts_sent() -> None:
    """Delete div_alerts_sent rows older than 24 h to keep the table lean."""
    if not HAS_NOTIFIER:
        return
    try:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ok = await _supa_delete(f"div_alerts_sent?sent_at=lt.{cutoff}")
        if ok:
            print("[div-dedup] Limpieza de div_alerts_sent completada (filas > 24 h eliminadas)")
    except Exception as e:
        print(f"[div-dedup] Error en limpieza de div_alerts_sent: {e}")


async def _check_setups(tickers: list) -> dict:
    """
    Evalúa cada ticker con el Setup Engine (3 criterios obligatorios + scoring 15pts).
    Devuelve dict: ticker -> resultado (solo si nivel_alerta != "none").
    """
    if not HAS_SETUP_ENGINE or _evaluate_setup is None:
        return {}

    results = {}
    now_ts = time.time()

    for ticker in tickers:
        try:
            df, _ = await async_download_rb(ticker)
            if df.empty or len(df) < 50:
                continue

            cfg = get_cfg(ticker)
            df  = calc_indicators(df, cfg["ema_short"], cfg["ema_long"])

            close = df["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]

            from trading_band_routes import calc_rsi as _calc_rsi_tb
            rsi_series = _calc_rsi_tb(close)

            result = _evaluate_setup(df, rsi_series, ticker, cfg)
            if result is None:
                continue

            nivel = result.get("nivel_alerta", "none")
            if nivel == "none":
                continue

            direction = result.get("direction", "")
            estado    = result.get("estado", "")
            dedup_key = f"SETUP_{ticker.upper()}_{direction}_{estado}"

            if now_ts - _setup_cache.get(dedup_key, 0) < _SETUP_DEDUP_SECONDS:
                print(f"[setup] {ticker.upper()} — {estado} ya enviado, skip")
                continue

            _setup_cache[dedup_key] = now_ts
            results[ticker.upper()] = result
            print(f"[setup] ✅ {ticker.upper()} — {estado} {result['puntos']}pts ({nivel})")

        except Exception as e:
            print(f"[setup] Error en {ticker}: {e}")

    return results


async def scheduled_watch():
    """Revisión periódica — analiza la última vela + confluencias TradingBand."""
    if not HAS_NOTIFIER:
        return

    watch_tickers = WATCH_TICKERS
    if _is_weekend_now():
        watch_tickers = [t for t in WATCH_TICKERS if t == "BTC-USD"]
        print("[scheduler] Fin de semana — revisando solo BTC-USD")

    # Alertas del sistema de confluencias actual (RSI/Divergencias/Supply-Demand/SharkFin)
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

    await _cleanup_div_alerts_sent()

    await _update_rsi_watchlist()

    # 4) Setup Engine — DESACTIVADO (código legacy eliminado)


async def daily_catchup():
    """Catch-up al arrancar: revisa SOLO la última vela (≤12h) — nada muy antiguo."""
    if not HAS_NOTIFIER:
        return

    catchup_tickers = WATCH_TICKERS
    if _is_weekend_now():
        catchup_tickers = [t for t in WATCH_TICKERS if t == "BTC-USD"]
        print("[catchup] Fin de semana — revisando solo BTC-USD")

    # Alertas del sistema actual
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
                    puntos_sin_rsi += c.get("pts", 1)

            rsi_ya_extremo = (direction == "bullish" and rsi <= 30) or \
                             (direction == "bearish" and rsi >= 70)

            if puntos_sin_rsi >= 5 and not rsi_ya_extremo:
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

            puntos = sum(c.get("pts", 1) for c in resultado["confluencias"]
                         if c.get("ok") and not c.get("descartada") and not c.get("conflicto"))
            resultado["puntos"] = puntos
            resultado["estado"] = "FAVORABLE" if puntos >= 7 else ("INTERESANTE" if puntos >= 5 else "CONSIDERAR")
            resultado["nivel"] = direction
            resultado["alert"] = puntos >= 7
            resultado["rsi_realtime"] = True

            if resultado["alert"]:
                ts_now = pd.Timestamp.now(tz="UTC")
                alertas_rsi[ticker] = [{
                    "nivel": direction,
                    "msg": f"[{ticker}] ⚡ PUNTO CALIENTE — {resultado['estado']} ({puntos} pts)",
                    "hora": ts_now.strftime("%d/%m %H:%M"),
                    "ts_utc_iso": ts_now.isoformat(),
                    "dia_num": ts_now.weekday(),
                    "dia_name": ts_now.strftime("%A"),
                    "resultado": resultado,
                    "components_ctx": components_ctx,
                }]
                _sent_cache[dedup_key] = now
                del _rsi_watchlist[ticker]
                print(f"[rsi-watch] ⚡ {ticker} RSI={rsi_now:.1f} zona extrema + {puntos}pts → ALERTA INMEDIATA")

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
            # AsyncIOScheduler maneja coroutines nativamente
            scheduler.add_job(scheduled_watch, "interval", minutes=30, id="watch_30m")
            scheduler.add_job(_rsi_realtime_check, "interval",
                              minutes=_RSI_WATCH_INTERVAL_MIN, id="rsi_rt")
            scheduler.start()
            print(f"[scheduler] Iniciado · Revisión cada 30 min + RSI real-time cada {_RSI_WATCH_INTERVAL_MIN} min")
            # Catch-up: enviar alertas de las últimas 24h al arrancar
            asyncio.create_task(daily_catchup())
            asyncio.create_task(_warm_row_cache())
            # Pre-populate divergence dedup cache from Supabase (survives restarts)
            asyncio.create_task(_load_div_cache_from_supabase())
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
        max_candles = 700
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

        # Zonas Supply/Demand para el chart
        sd_result = _detect_sd(df, lookback=10, max_zonas=30) if _detect_sd else {"zones": []}
        sd_chart = [
            {"top": z["top"], "bottom": z["bottom"],
             "direction": z["direction"], "touched": z.get("touched", False),
             "state": z.get("state", "strong")}
            for z in sd_result.get("zones", [])
        ]

        # Divergencias RSI para el chart (segmentos x1/y1→x2/y2 en espacio RSI)
        rsi_divs = []
        try:
            if _detect_divs is not None and _calc_rsi is not None and _build_rsi_div_segs is not None:
                rsi_div_series = _calc_rsi(df["Close"])
                all_divs = _detect_divs(df, rsi_div_series)
                recent_divs = [d for d in all_divs if d.get("bar", 0) >= start_i]
                times = list(df.index)
                rsi_divs = _build_rsi_div_segs(recent_divs, rsi_div_series, times)
        except Exception as _de:
            print(f"[chart] div segments error: {_de}")

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
                "sd": sd_chart,
                "rsi_divs": rsi_divs,
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

    # ¿Cruce reciente de TradingBand en las últimas 2 velas?
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

    # Divergencia RSI para columna de tabla
    div_conf = next((c for c in (confl.get("confluencias", []) if confl else []) if c.get("id") == 2), None)
    div_estado = div_conf.get("texto") if (div_conf and div_conf.get("ok")) else None
    div_tipo   = div_conf.get("tipo")  if (div_conf and div_conf.get("ok")) else None

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
        "div_estado": div_estado,
        "div_tipo": div_tipo,
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
