# trading_band_routes.py — Rutas FastAPI para Trading Band
# Indicador: TradingBand (SMA12 Trigger + SMA12 Average) + RSI(14) con Divergencias 3 Niveles
# Alertas: cruce de tendencia (flecha), RSI OB/OS, divergencias en zonas clave RSI

from fastapi import APIRouter
import yfinance as yf
import pandas as pd
import numpy as np
import asyncio
import time
from patterns import detect_all_patterns

router = APIRouter(prefix="/api/tradingband", tags=["TradingBand"])

# ──────────────────────────────────────────────────────────
# CONFIG POR ACTIVO
# ──────────────────────────────────────────────────────────
TB_CONFIG = {
    "^DJI":    {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "US30 · Dow Jones"},
    "^NDX":    {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "NAS100 · NASDAQ"},
    "^GSPC":   {"range": 50,   "ema_s": 200, "ema_l": 800, "name": "SPX · S&P 500"},
    "GC=F":    {"range": 50,   "ema_s": 200, "ema_l": 800, "name": "XAUUSD · Oro"},
    "SI=F":    {"range": 0.5,  "ema_s": 200, "ema_l": 800, "name": "XAGUSD · Plata"},
    "CL=F":    {"range": 2,    "ema_s": 200, "ema_l": 800, "name": "WTI · Petróleo"},
    "USDJPY=X":{"range": 1,    "ema_s": 200, "ema_l": 800, "name": "USD/JPY"},
    "GBPJPY=X":{"range": 1,    "ema_s": 200, "ema_l": 800, "name": "GBP/JPY"},
    "EURUSD=X":{"range": 0.005,"ema_s": 200, "ema_l": 800, "name": "EUR/USD"},
    "AUDUSD=X":{"range": 0.005,"ema_s": 200, "ema_l": 800, "name": "AUD/USD"},
    "BTC-USD": {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "BTC · Bitcoin"},
    "GBPUSD=X":{"range": 0.005,"ema_s": 200, "ema_l": 800, "name": "GBP/USD"},
    "AUDJPY=X":{"range": 1,    "ema_s": 200, "ema_l": 800, "name": "AUD/JPY"},
    "_default":{"range": 50,   "ema_s": 200, "ema_l": 800, "name": "Activo"},
}

_yf_lock = asyncio.Lock()
_cache: dict = {}
_CACHE_TTL = 300

# ──────────────────────────────────────────────────────────
# FUNCIONES MATEMÁTICAS
# ──────────────────────────────────────────────────────────

def sma(s: pd.Series, p: int) -> pd.Series:
    return s.rolling(p, min_periods=p).mean()

def ema_calc(s: pd.Series, p: int) -> pd.Series:
    return s.ewm(span=p, adjust=False).mean()

def rma(s: pd.Series, p: int) -> pd.Series:
    result = np.full(len(s), np.nan)
    vals = s.values
    start = p - 1
    while start < len(vals) and np.isnan(vals[start]):
        start += 1
    if start >= len(vals):
        return pd.Series(result, index=s.index)
    seed_vals = vals[max(0, start - p + 1): start + 1]
    seed_vals = seed_vals[~np.isnan(seed_vals)]
    if len(seed_vals) < p:
        return pd.Series(result, index=s.index)
    result[start] = np.mean(seed_vals)
    alpha = 1.0 / p
    for i in range(start + 1, len(vals)):
        if np.isnan(vals[i]):
            result[i] = result[i - 1]
        else:
            result[i] = alpha * vals[i] + (1 - alpha) * result[i - 1]
    return pd.Series(result, index=s.index)

def calc_rsi(s: pd.Series, p: int = 14) -> pd.Series:
    change = s.diff()
    up_   = change.clip(lower=0)
    down_ = (-change).clip(lower=0)
    avg_up   = rma(up_,   p)
    avg_down = rma(down_, p)
    rs = avg_up / avg_down.replace(0, np.nan)
    rsi_out = 100 - (100 / (1 + rs))
    rsi_out[avg_down == 0] = 100.0
    rsi_out[avg_up   == 0] = 0.0
    return rsi_out

def calc_trading_band(close: pd.Series, tp: int = 12, ap: int = 12):
    trigger = sma(close, tp)
    average = sma(trigger, ap)
    return trigger, average

# ──────────────────────────────────────────────────────────
# RANGE BARS
# ──────────────────────────────────────────────────────────

def build_range_bars(df: pd.DataFrame, range_size: float) -> pd.DataFrame:
    if df.empty or range_size <= 0:
        return df
    bars = []
    o = float(df["open"].iloc[0])
    h = float(df["high"].iloc[0])
    l = float(df["low"].iloc[0])
    v = float(df["volume"].iloc[0]) if "volume" in df.columns else 0.0
    t = df.index[0]
    for i in range(1, len(df)):
        row = df.iloc[i]
        rh = float(row["high"]); rl = float(row["low"])
        rv = float(row["volume"]) if "volume" in df.columns else 0.0
        nh = max(h, rh); nl = min(l, rl)
        if nh - o >= range_size:
            c = o + range_size
            bars.append({"time": t, "open": o, "high": c, "low": l, "close": c, "volume": v})
            o, h, l, v, t = c, max(c, rh), c, rv, df.index[i]
        elif o - nl >= range_size:
            c = o - range_size
            bars.append({"time": t, "open": o, "high": h, "low": c, "close": c, "volume": v})
            o, h, l, v, t = c, c, min(c, rl), rv, df.index[i]
        else:
            h, l, v = nh, nl, v + rv
    if bars:
        rb = pd.DataFrame(bars).set_index("time")
        rb.index = pd.DatetimeIndex(rb.index)
        return rb
    return df

# ──────────────────────────────────────────────────────────
# PIVOTES
# ──────────────────────────────────────────────────────────

def pivot_low(s: pd.Series, lb_l: int, lb_r: int) -> pd.Series:
    n = len(s)
    result = pd.Series(False, index=s.index)
    for i in range(lb_l, n - lb_r):
        val = s.iloc[i]
        if pd.isna(val):
            continue
        if all(s.iloc[i - lb_l: i] >= val) and all(s.iloc[i + 1: i + lb_r + 1] >= val):
            result.iloc[i + lb_r] = True
    return result

def pivot_high(s: pd.Series, lb_l: int, lb_r: int) -> pd.Series:
    n = len(s)
    result = pd.Series(False, index=s.index)
    for i in range(lb_l, n - lb_r):
        val = s.iloc[i]
        if pd.isna(val):
            continue
        if all(s.iloc[i - lb_l: i] <= val) and all(s.iloc[i + 1: i + lb_r + 1] <= val):
            result.iloc[i + lb_r] = True
    return result

# ──────────────────────────────────────────────────────────
# DIVERGENCIAS — 3 Niveles (fiel al Pine Script)
# ──────────────────────────────────────────────────────────

def _in_range(bars_since: int, rn_min: int, rn_max: int) -> bool:
    return rn_min <= bars_since <= rn_max

def detect_divergences_level(
    price_low: pd.Series, price_high: pd.Series, rsi: pd.Series,
    lb_l: int, lb_r: int, rn_min: int, rn_max: int, level: int,
) -> list:
    divs = []
    times = rsi.index
    pl_series = pivot_low(rsi, lb_l, lb_r)
    ph_series = pivot_high(rsi, lb_l, lb_r)
    n = len(rsi)

    prev_pl = None
    for confirm_bar in range(n):
        if not pl_series.iloc[confirm_bar]:
            continue
        pivot_bar = confirm_bar - lb_r
        if pivot_bar < 0:
            continue
        rsi_cur   = float(rsi.iloc[pivot_bar])
        price_cur = float(price_low.iloc[pivot_bar])
        if prev_pl is not None:
            prev_bar, prev_rsi, prev_price = prev_pl
            bars_between = pivot_bar - prev_bar
            if _in_range(bars_between, rn_min, rn_max):
                if price_cur < prev_price and rsi_cur > prev_rsi:
                    divs.append({"type": "bull", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                        "in_zone": rsi_cur <= 45})
                elif price_cur > prev_price and rsi_cur < prev_rsi:
                    divs.append({"type": "hbull", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                        "in_zone": rsi_cur <= 45})
        prev_pl = (pivot_bar, rsi_cur, price_cur)

    prev_ph = None
    for confirm_bar in range(n):
        if not ph_series.iloc[confirm_bar]:
            continue
        pivot_bar = confirm_bar - lb_r
        if pivot_bar < 0:
            continue
        rsi_cur   = float(rsi.iloc[pivot_bar])
        price_cur = float(price_high.iloc[pivot_bar])
        if prev_ph is not None:
            prev_bar, prev_rsi, prev_price = prev_ph
            bars_between = pivot_bar - prev_bar
            if _in_range(bars_between, rn_min, rn_max):
                if price_cur > prev_price and rsi_cur < prev_rsi:
                    divs.append({"type": "bear", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                        "in_zone": rsi_cur >= 55})
                elif price_cur < prev_price and rsi_cur > prev_rsi:
                    divs.append({"type": "hbear", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                        "in_zone": rsi_cur >= 55})
        prev_ph = (pivot_bar, rsi_cur, price_cur)

    return divs


def detect_all_divergences(df: pd.DataFrame, rsi: pd.Series) -> list:
    all_divs = []
    levels = [
        (5,  5,   5,   50,   1),
        (10, 10,  10,  200,  2),
        (20, 20,  20,  1000, 3),
    ]
    for lb_l, lb_r, rn_min, rn_max, lvl in levels:
        all_divs.extend(detect_divergences_level(
            df["low"], df["high"], rsi, lb_l, lb_r, rn_min, rn_max, lvl))
    all_divs.sort(key=lambda x: x["bar"], reverse=True)
    return all_divs


def build_rsi_div_segments(divs: list, rsi: pd.Series, times: list) -> list:
    segments = []
    for dv in divs:
        bar1, bar2 = dv["bar_prev"], dv["bar"]
        if bar1 < 0 or bar2 >= len(rsi):
            continue
        # Use millisecond timestamps (same format as rsi_points x values)
        ts1 = int(times[bar1].timestamp() * 1000)
        ts2 = int(times[bar2].timestamp() * 1000)
        segments.append({
            "type": dv["type"], "level": dv["level"],
            "in_zone": dv.get("in_zone", False),
            "x1": ts1, "y1": float(rsi.iloc[bar1]),
            "x2": ts2, "y2": float(rsi.iloc[bar2]),
        })
    return segments


# ──────────────────────────────────────────────────────────
# DIVERGENCIA AGRUPADA (para calc_shark_fin)
# ──────────────────────────────────────────────────────────

def calc_rsi_divergence(df: pd.DataFrame, lookback: int = 30) -> dict:
    """
    Detecta divergencias RSI agrupadas en alcistas / bajistas.
    Devuelve líneas (rsi0, rsi1) compatibles con calc_shark_fin.
    """
    result = {
        "bear_div": False,
        "bull_div": False,
        "bear_lines": [],
        "bull_lines": [],
    }
    close = df["close"] if "close" in df.columns else df.get("Close")
    if close is None or len(close) < lookback + 10:
        return result

    rsi = calc_rsi(close, 14)
    if rsi is None or rsi.notna().sum() < lookback:
        return result

    divs = detect_all_divergences(df, rsi)
    if not divs:
        return result

    # Agrupar por tipo
    bull_divs = [d for d in divs if d["type"] in ("bull", "hbull")]
    bear_divs = [d for d in divs if d["type"] in ("bear", "hbear")]

    if bear_divs:
        result["bear_div"] = True
        for d in bear_divs:
            result["bear_lines"].append({
                "rsi0": d["rsi"],      # pivote más reciente (lower high)
                "rsi1": d["rsi_prev"], # pivote más antiguo (higher high)
            })

    if bull_divs:
        result["bull_div"] = True
        for d in bull_divs:
            result["bull_lines"].append({
                "rsi0": d["rsi"],      # pivote más reciente (higher low)
                "rsi1": d["rsi_prev"], # pivote más antiguo (lower low)
            })

    return result


# ──────────────────────────────────────────────────────────
# ALETA DE TIBURÓN (Shark Fin)
# ──────────────────────────────────────────────────────────

def calc_shark_fin(df: pd.DataFrame, lookback: int = 30) -> dict:
    """
    Detecta aleta de tiburón en RSI.
    Requiere divergencia previa confirmada.

    Casos:
      shark_bear: precio HH + RSI LH (div bajista) → RSI entra >70 y forma pico → cruza <70
      shark_bull: precio LL + RSI HL (div alcista) → RSI entra <30 y forma valle → cruza >30

    shark_exceeds_div: el pico/valle de la aleta supera el nivel del pivote R1/S1 original
    → puntuación x2 + alerta instantánea al detectar (sin esperar el cruce)
    """
    result = {
        "shark_bear":         False,
        "shark_bull":         False,
        "shark_exceeds_div":  False,
        "shark_pts":          0,
        "shark_tipo":         None,
        "shark_rsi_peak":     None,
        "shark_div_r1":       None,
        "phase":              None,   # 'forming' | 'crossed' | 'exceeded'
        "alert_immediate":    False,
    }

    if len(df) < 10:
        return result

    close = df["close"] if "close" in df.columns else df.get("Close")
    if close is None:
        return result

    rsi = calc_rsi(close, 14)
    if rsi is None or rsi.notna().sum() < 10:
        return result
    if isinstance(rsi, pd.DataFrame):
        rsi = rsi.iloc[:, 0]
    rsi = rsi.dropna()
    if len(rsi) < 10:
        return result

    # Primero necesitamos la divergencia previa
    div = calc_rsi_divergence(df, lookback=lookback)

    # ── ALETA BAJISTA ─────────────────────────────────
    if div["bear_div"] and div["bear_lines"]:
        bear_line  = div["bear_lines"][0]
        r1_rsi     = bear_line["rsi1"]   # RSI del pivote más antiguo (más alto)
        r2_rsi     = bear_line["rsi0"]   # RSI del pivote más reciente (más bajo = LH)

        # Buscar si después de R2 el RSI ha entrado en zona >70
        n = len(rsi)
        recent = rsi.iloc[-min(lookback, n):]

        # Encontrar picos locales en el RSI reciente que estén >70
        shark_peaks = []
        for i in range(1, len(recent) - 1):
            v  = float(recent.iloc[i])
            vp = float(recent.iloc[i - 1])
            vn = float(recent.iloc[i + 1])
            if v > 70 and v >= vp and v >= vn:
                shark_peaks.append((i, v))

        if shark_peaks:
            # Tomar el pico más reciente
            peak_i, peak_rsi = shark_peaks[-1]
            rsi_now = float(rsi.iloc[-1])

            result["shark_bear"]     = True
            result["shark_tipo"]     = "bearish"
            result["shark_rsi_peak"] = peak_rsi
            result["shark_div_r1"]   = r1_rsi

            # ¿La aleta supera R1 (el pico original de la divergencia)?
            exceeds = peak_rsi > r1_rsi
            result["shark_exceeds_div"] = exceeds

            if exceeds:
                # Alerta instantánea — no esperamos el cruce
                result["phase"]          = "exceeded"
                result["alert_immediate"] = True
                result["shark_pts"]       = 4
            elif rsi_now < 70 and peak_i < len(recent) - 1:
                # Ya cruzó hacia abajo — alerta al cruce
                result["phase"]          = "crossed"
                result["alert_immediate"] = True
                result["shark_pts"]       = 2
            else:
                # Aún formando — avisar pero no puntuar todavía
                result["phase"]          = "forming"
                result["alert_immediate"] = False
                result["shark_pts"]       = 1  # pre-alerta

    # ── ALETA ALCISTA ─────────────────────────────────
    elif div["bull_div"] and div["bull_lines"]:
        bull_line  = div["bull_lines"][0]
        s1_rsi     = bull_line["rsi1"]   # RSI del pivote más antiguo (más bajo)
        s2_rsi     = bull_line["rsi0"]   # RSI del pivote más reciente (más alto = HL)

        n = len(rsi)
        recent = rsi.iloc[-min(lookback, n):]

        shark_valleys = []
        for i in range(1, len(recent) - 1):
            v  = float(recent.iloc[i])
            vp = float(recent.iloc[i - 1])
            vn = float(recent.iloc[i + 1])
            if v < 30 and v <= vp and v <= vn:
                shark_valleys.append((i, v))

        if shark_valleys:
            valley_i, valley_rsi = shark_valleys[-1]
            rsi_now = float(rsi.iloc[-1])

            result["shark_bull"]     = True
            result["shark_tipo"]     = "bullish"
            result["shark_rsi_peak"] = valley_rsi
            result["shark_div_r1"]   = s1_rsi

            exceeds = valley_rsi < s1_rsi   # más bajo que S1 original
            result["shark_exceeds_div"] = exceeds

            if exceeds:
                result["phase"]          = "exceeded"
                result["alert_immediate"] = True
                result["shark_pts"]       = 4
            elif rsi_now > 30 and valley_i < len(recent) - 1:
                result["phase"]          = "crossed"
                result["alert_immediate"] = True
                result["shark_pts"]       = 2
            else:
                result["phase"]          = "forming"
                result["alert_immediate"] = False
                result["shark_pts"]       = 1

    return result

# ──────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────

async def _download(ticker: str, **kwargs) -> pd.DataFrame:
    loop = asyncio.get_running_loop()
    async with _yf_lock:
        return await loop.run_in_executor(None, lambda: yf.download(ticker, progress=False, **kwargs))

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [c.lower() for c in df.columns]
    return df.dropna(subset=["close"])

def _safe(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    return round(float(v), 6)

def _ts(idx):
    return [int(t.timestamp() * 1000) for t in idx]

# ──────────────────────────────────────────────────────────
# ENDPOINT PRINCIPAL
# ──────────────────────────────────────────────────────────

@router.get("/signal/{ticker}")
async def get_tb_signal(ticker: str, use_range_bars: bool = True):
    key = f"{ticker.upper()}_{'rb' if use_range_bars else '4h'}"
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    cfg = TB_CONFIG.get(ticker.upper(), TB_CONFIG["_default"])

    sym = ticker.upper()
    try:
        if use_range_bars:
            raw = await _download(sym, period="60d", interval="5m")
        else:
            raw = await _download(sym, period="2y", interval="4h")

        if raw is None or (hasattr(raw, "empty") and raw.empty):
            return {"error": f"Sin datos para {sym}"}

        df = _clean(raw)

        if use_range_bars:
            rb = build_range_bars(df, cfg["range"])
            df_calc = rb if (rb is not None and not rb.empty) else df
            bar_type = f"Range {cfg['range']}"
        else:
            df_calc = df
            bar_type = "4h"

        if df_calc.empty:
            return {"error": "No se pudieron construir barras"}

        close = df_calc["close"]
        trigger, average = calc_trading_band(close, 12, 12)
        rsi_s  = calc_rsi(close, 14)
        ema200 = ema_calc(close, cfg["ema_s"])
        ema800 = ema_calc(close, cfg["ema_l"])

        up_trend   = trigger > average
        cross_up   = (~up_trend.shift(1).fillna(False)) & up_trend
        cross_down = (up_trend.shift(1).fillna(True))   & (~up_trend)

        valid_mask = rsi_s.notna()
        if valid_mask.sum() >= 40:
            df_aligned = df_calc[valid_mask].copy()
            rsi_valid  = rsi_s[valid_mask]
            all_divs   = detect_all_divergences(df_aligned, rsi_valid)
            times_all  = list(rsi_valid.index)
            rsi_segs   = build_rsi_div_segments(all_divs, rsi_valid, times_all)
            patterns   = detect_all_patterns(df_aligned, rsi_valid, times_all)
        else:
            all_divs = []
            rsi_segs = []
            patterns = []

        n = len(df_calc)
        timestamps = _ts(df_calc.index)

        candles = []
        for i in range(n):
            o = _safe(df_calc["open"].iloc[i])
            h = _safe(df_calc["high"].iloc[i])
            l = _safe(df_calc["low"].iloc[i])
            c = _safe(df_calc["close"].iloc[i])
            if None in (o, h, l, c):
                continue
            candles.append({"x": timestamps[i], "o": o, "h": h, "l": l, "c": c})

        def ser(col):
            return [{"x": timestamps[i], "y": _safe(col.iloc[i])}
                    for i in range(n) if pd.notna(col.iloc[i])]

        rsi_points = []
        for i in range(n):
            v = _safe(rsi_s.iloc[i])
            if v is None:
                continue
            zone = "ob" if v >= 70 else ("os" if v <= 30 else "neutral")
            rsi_points.append({"x": timestamps[i], "y": v, "zone": zone})

        # ── Señales recientes
        recent_signals = []
        look = min(30, n)
        for i in range(n - look, n):
            t = timestamps[i]
            c_val = _safe(close.iloc[i])
            if cross_up.iloc[i]:
                recent_signals.append({"type": "tb_up",   "x": t, "price": c_val,
                    "label": "▲ TradingBand ALCISTA"})
            if cross_down.iloc[i]:
                recent_signals.append({"type": "tb_down", "x": t, "price": c_val,
                    "label": "▼ TradingBand BAJISTA"})
            rv = _safe(rsi_s.iloc[i])
            if rv and rsi_s.iloc[i] <= 30 and (i == 0 or rsi_s.iloc[i - 1] > 30):
                recent_signals.append({"type": "rsi_os", "x": t, "price": c_val, "rsi": rv,
                    "label": "● RSI Sobreventa ≤30"})
            if rv and rsi_s.iloc[i] >= 70 and (i == 0 or rsi_s.iloc[i - 1] < 70):
                recent_signals.append({"type": "rsi_ob", "x": t, "price": c_val, "rsi": rv,
                    "label": "● RSI Sobrecompra ≥70"})

        # Divergencias en zonas clave (RSI ≤30 alcistas, ≥70 bajistas)
        zone_divs = [d for d in all_divs if d.get("in_zone", False)]

        recent_signals.sort(key=lambda x: x["x"], reverse=True)

        last_rsi_val = float(rsi_s.dropna().iloc[-1]) if rsi_s.notna().any() else 50.0

        result = {
            "ticker": key,
            "name": cfg["name"],
            "bar_type": bar_type,
            "last_price": _safe(close.iloc[-1]),
            "last_rsi":   round(last_rsi_val, 2),
            "tb_up": bool(up_trend.iloc[-1]) if not up_trend.isna().all() else False,
            "trigger": _safe(trigger.iloc[-1]),
            "average": _safe(average.iloc[-1]),
            "ema_short": _safe(ema200.iloc[-1]),
            "ema_long":  _safe(ema800.iloc[-1]),
            "ema_short_name": f"EMA{cfg['ema_s']}",
            "ema_long_name":  f"EMA{cfg['ema_l']}",
            "total_bars": n,
            "chart": {
                "candles":   candles,
                "trigger":   ser(trigger),
                "average":   ser(average),
                "ema_short": ser(ema200),
                "ema_long":  ser(ema800),
                "rsi":       rsi_points,
            },
            "rsi_div_segments": rsi_segs,
            "signals":      recent_signals,
            "divergences":  all_divs[:40],
            "zone_divergences": zone_divs[:10],
            "last_divergences": all_divs[:5],
            "patterns":     patterns,
        }

        _cache[key] = {"ts": now, "data": result}
        return result

    except Exception as e:
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@router.get("/tickers")
async def get_tickers():
    return {"tickers": [
        {"id": "^DJI",     "name": "Dow Jones",  "group": "Índices"},
        {"id": "^NDX",     "name": "NASDAQ 100", "group": "Índices"},
        {"id": "^GSPC",    "name": "S&P 500",    "group": "Índices"},
        {"id": "GC=F",     "name": "Oro",        "group": "Materias Primas"},
        {"id": "SI=F",     "name": "Plata",      "group": "Materias Primas"},
        {"id": "CL=F",     "name": "Petróleo",   "group": "Materias Primas"},
        {"id": "USDJPY=X", "name": "USD/JPY",    "group": "Forex"},
        {"id": "GBPJPY=X", "name": "GBP/JPY",   "group": "Forex"},
        {"id": "EURUSD=X", "name": "EUR/USD",    "group": "Forex"},
        {"id": "AUDUSD=X", "name": "AUD/USD",    "group": "Forex"},
        {"id": "GBPUSD=X", "name": "GBP/USD",    "group": "Forex"},
        {"id": "AUDJPY=X", "name": "AUD/JPY",    "group": "Forex"},
        {"id": "BTC-USD",  "name": "Bitcoin",    "group": "Crypto"},
    ]}
