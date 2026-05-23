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
    # range_display: etiqueta visual (todos 1000). range_calc: valor real para build_range_bars.
    "^DJI":    {"range": 1000, "range_calc": 1000,    "ema_s": 200, "ema_l": 800, "name": "US30 · Dow Jones"},
    "^NDX":    {"range": 1000, "range_calc": 1000,    "ema_s": 200, "ema_l": 800, "name": "NAS100 · NASDAQ"},
    "^GSPC":   {"range": 1000, "range_calc": 80,     "ema_s": 200, "ema_l": 800, "name": "SPX · S&P 500"},
    "GC=F":    {"range": 1000, "range_calc": 15,     "ema_s": 200, "ema_l": 800, "name": "XAUUSD · Oro"},
    "SI=F":    {"range": 1000, "range_calc": 0.15,   "ema_s": 200, "ema_l": 800, "name": "XAGUSD · Plata"},
    "CL=F":    {"range": 1000, "range_calc": 0.5,    "ema_s": 200, "ema_l": 800, "name": "WTI · Petróleo"},
    "USDJPY=X":{"range": 1000, "range_calc": 0.4,    "ema_s": 200, "ema_l": 800, "name": "USD/JPY"},
    "GBPJPY=X":{"range": 1000, "range_calc": 0.6,    "ema_s": 200, "ema_l": 800, "name": "GBP/JPY"},
    "EURUSD=X":{"range": 1000, "range_calc": 0.003,  "ema_s": 200, "ema_l": 800, "name": "EUR/USD"},
    "AUDUSD=X":{"range": 1000, "range_calc": 0.003,  "ema_s": 200, "ema_l": 800, "name": "AUD/USD"},
    "BTC-USD": {"range": 1000, "range_calc": 1000,   "ema_s": 200, "ema_l": 800, "name": "BTC · Bitcoin"},
    "GBPUSD=X":{"range": 1000, "range_calc": 0.004,  "ema_s": 200, "ema_l": 800, "name": "GBP/USD"},
    "AUDJPY=X":{"range": 1000, "range_calc": 0.4,    "ema_s": 200, "ema_l": 800, "name": "AUD/JPY"},
    "_default":{"range": 1000, "range_calc": 50,     "ema_s": 200, "ema_l": 800, "name": "Activo"},
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

def calc_rsi(s: pd.Series, p: int = 14) -> pd.Series:
    """RSI con RMA (Wilder's smoothing) usando ewm(alpha=1/p). TradingView-compatible."""
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/p, min_periods=p, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/p, min_periods=p, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50.0)

def calc_trading_band(close: pd.Series, tp: int = 12, ap: int = 12):
    trigger = sma(close, tp)
    average = sma(trigger, ap)
    return trigger, average

# ──────────────────────────────────────────────────────────
# RANGE BARS
# ──────────────────────────────────────────────────────────

def build_range_bars(df: pd.DataFrame, range_size: float) -> pd.DataFrame:
    """
    Construye velas de rango a partir de datos tick (1m/5m).
    Cada barra se cierra cuando High - Low >= range_size.
    La barra siguiente abre en el cierre de la anterior.
    """
    if df.empty or range_size <= 0:
        return pd.DataFrame(columns=["open","high","low","close","volume","hl"])

    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame(columns=["open","high","low","close","volume","hl"])

    bars   = []
    o = h = l = c = ts = None

    for idx, row in df.iterrows():
        px = float(row["close"])
        if np.isnan(px):
            continue

        if o is None:
            o = px
            h = px
            l = px
            ts = idx

        # Actualizar high/low tick a tick
        if px > h:
            h = px
        if px < l:
            l = px
        c = px

        # ¿La barra alcanzó el rango?
        if (h - l) >= range_size:
            bars.append({
                "time":   ts,
                "open":   o,
                "high":   h,
                "low":    l,
                "close":  c,
                "volume": float(row.get("volume", 0)),
            })
            # La siguiente barra abre donde cerró esta
            o = c
            h = c
            l = c
            ts = idx

    if not bars:
        return pd.DataFrame(columns=["open","high","low","close","volume","hl"])

    rb = pd.DataFrame(bars).set_index("time")
    rb.index = pd.DatetimeIndex(rb.index)
    rb["close"] = rb["close"].astype(float)
    rb["open"] = rb["open"].astype(float)
    rb["high"] = rb["high"].astype(float)
    rb["low"] = rb["low"].astype(float)
    rb["volume"] = rb["volume"].astype(float)
    rb["hl"] = (rb["high"] - rb["low"]).astype(float)
    return rb

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
    low  = df["low"]  if "low"  in df.columns else df["Low"]
    high = df["high"] if "high" in df.columns else df["High"]
    for lb_l, lb_r, rn_min, rn_max, lvl in levels:
        all_divs.extend(detect_divergences_level(
            low, high, rsi, lb_l, lb_r, rn_min, rn_max, lvl))
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

        # Encontrar picos locales en el RSI reciente
        # Umbral adaptativo: pico > max(60, R1*0.95) para detectar agotamiento
        thresh_peak = max(60.0, r1_rsi * 0.95) if r1_rsi else 60.0
        shark_peaks = []
        for i in range(1, len(recent) - 1):
            v  = float(recent.iloc[i])
            vp = float(recent.iloc[i - 1])
            vn = float(recent.iloc[i + 1])
            if v >= thresh_peak and v >= vp and v >= vn:
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
            elif rsi_now < thresh_peak and peak_i < len(recent) - 1:
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
            range_calc = cfg.get("range_calc", cfg.get("range"))
            rb = build_range_bars(df, range_calc)
            df_calc = rb if (rb is not None and not rb.empty) else df
            bar_type = f"Range {cfg.get('range', 1000)}"
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


def calc_pattern_mw(df: pd.DataFrame, lookback: int = 40, tol: float = 0.018) -> dict:
    """
    Detecta M (doble techo) y W (doble suelo) en el dataframe de precios.
    Retorna coordenadas completas + estado (confirmado/formando).
    """
    result = {"M": None, "W": None}
    if len(df) < 15:
        return result

    high  = df["High"]  if "High"  in df.columns else df.get("high",  pd.Series())
    low   = df["Low"]   if "Low"   in df.columns else df.get("low",   pd.Series())
    close = df["Close"] if "Close" in df.columns else df.get("close", pd.Series())
    rsi_col = "RSI" if "RSI" in df.columns else None

    if isinstance(high,  pd.DataFrame): high  = high.iloc[:, 0]
    if isinstance(low,   pd.DataFrame): low   = low.iloc[:, 0]
    if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]

    if rsi_col:
        rsi_s = df[rsi_col]
        if isinstance(rsi_s, pd.DataFrame): rsi_s = rsi_s.iloc[:, 0]
    else:
        rsi_s = pd.Series([50.0] * len(df), index=df.index)

    window_df = df.iloc[-lookback:]
    wh = high.iloc[-lookback:]
    wl = low.iloc[-lookback:]
    wr = rsi_s.iloc[-lookback:]
    wc = close.iloc[-lookback:]
    dates = list(window_df.index)
    n = len(window_df)

    def local_highs(series, min_dist=3):
        peaks = []
        for i in range(2, n - 2):
            v = float(series.iloc[i])
            if (v >= float(series.iloc[i-1]) and v >= float(series.iloc[i-2]) and
                v >= float(series.iloc[i+1]) and v >= float(series.iloc[i+2])):
                if not peaks or (i - peaks[-1][0]) >= min_dist:
                    peaks.append((i, v))
        return peaks

    def local_lows(series, min_dist=3):
        troughs = []
        for i in range(2, n - 2):
            v = float(series.iloc[i])
            if (v <= float(series.iloc[i-1]) and v <= float(series.iloc[i-2]) and
                v <= float(series.iloc[i+1]) and v <= float(series.iloc[i+2])):
                if not troughs or (i - troughs[-1][0]) >= min_dist:
                    troughs.append((i, v))
        return troughs

    highs = local_highs(wh)
    lows  = local_lows(wl)

    # ── Patrón M (doble techo) ────────────────────────────
    if len(highs) >= 2:
        for i in range(len(highs) - 1):
            h1_i, h1_v = highs[i]
            h2_i, h2_v = highs[-1]
            if h2_i - h1_i < 5:
                continue
            if abs(h1_v - h2_v) / max(h1_v, 0.0001) <= tol:
                mid_lows = [lows[j] for j in range(len(lows)) if h1_i < lows[j][0] < h2_i]
                neckline_v = min((v for _, v in mid_lows), default=None)
                if neckline_v is None:
                    neckline_v = float(wl.iloc[h1_i:h2_i].min())
                rsi1 = float(wr.iloc[h1_i])
                rsi2 = float(wr.iloc[h2_i])
                current_price = float(wc.iloc[-1])
                if current_price < neckline_v:
                    estado = "confirmado"
                elif abs(current_price - h2_v) / max(h2_v, 0.0001) <= tol * 1.5:
                    estado = "formando_p2"
                else:
                    estado = "formando"
                result["M"] = {
                    "p1_x": int(pd.Timestamp(dates[h1_i]).timestamp() * 1000),
                    "p1_y": h1_v,
                    "p2_x": int(pd.Timestamp(dates[h2_i]).timestamp() * 1000),
                    "p2_y": h2_v,
                    "neckline": neckline_v,
                    "rsi1": rsi1, "rsi2": rsi2,
                    "rsi_div": rsi2 < rsi1 - 2,
                    "estado": estado,
                    "bearish": True,
                }
                break

    # ── Patrón W (doble suelo) ────────────────────────────
    if len(lows) >= 2:
        for i in range(len(lows) - 1):
            l1_i, l1_v = lows[i]
            l2_i, l2_v = lows[-1]
            if l2_i - l1_i < 5:
                continue
            if abs(l1_v - l2_v) / max(l1_v, 0.0001) <= tol:
                mid_highs = [highs[j] for j in range(len(highs)) if l1_i < highs[j][0] < l2_i]
                neckline_v = max((v for _, v in mid_highs), default=None)
                if neckline_v is None:
                    neckline_v = float(wh.iloc[l1_i:l2_i].max())
                rsi1 = float(wr.iloc[l1_i])
                rsi2 = float(wr.iloc[l2_i])
                current_price = float(wc.iloc[-1])
                if current_price > neckline_v:
                    estado = "confirmado"
                elif abs(current_price - l2_v) / max(l2_v, 0.0001) <= tol * 1.5:
                    estado = "formando_v2"
                else:
                    estado = "formando"
                result["W"] = {
                    "v1_x": int(pd.Timestamp(dates[l1_i]).timestamp() * 1000),
                    "v1_y": l1_v,
                    "v2_x": int(pd.Timestamp(dates[l2_i]).timestamp() * 1000),
                    "v2_y": l2_v,
                    "neckline": neckline_v,
                    "rsi1": rsi1, "rsi2": rsi2,
                    "rsi_div": rsi2 > rsi1 + 2,
                    "estado": estado,
                    "bullish": True,
                }
                break

    return result


def calc_pattern_hch(df: pd.DataFrame, lookback: int = 60, tol: float = 0.02) -> dict:
    """
    Detecta HCH bajista e HCH invertido alcista.
    Estado 'formando_hd' = hombro derecho aún no completado.
    """
    result = {"HCH": None, "HCH_inv": None}
    if len(df) < 20:
        return result

    high  = df["High"]  if "High"  in df.columns else df.get("high",  pd.Series())
    low   = df["Low"]   if "Low"   in df.columns else df.get("low",   pd.Series())
    close = df["Close"] if "Close" in df.columns else df.get("close", pd.Series())
    rsi_col = "RSI" if "RSI" in df.columns else None

    if isinstance(high,  pd.DataFrame): high  = high.iloc[:, 0]
    if isinstance(low,   pd.DataFrame): low   = low.iloc[:, 0]
    if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]

    if rsi_col:
        rsi_s = df[rsi_col]
        if isinstance(rsi_s, pd.DataFrame): rsi_s = rsi_s.iloc[:, 0]
    else:
        rsi_s = pd.Series([50.0] * len(df), index=df.index)

    wh = high.iloc[-lookback:]
    wl = low.iloc[-lookback:]
    wr = rsi_s.iloc[-lookback:]
    wc = close.iloc[-lookback:]
    dates = list(df.index[-lookback:])
    n = len(wh)

    def find_peaks(series, min_dist=4):
        peaks = []
        for i in range(3, n - 3):
            v = float(series.iloc[i])
            if all(v >= float(series.iloc[i-j]) for j in range(1, 4)) and \
               all(v >= float(series.iloc[i+j]) for j in range(1, 4)):
                if not peaks or (i - peaks[-1][0]) >= min_dist:
                    peaks.append((i, v, dates[i]))
        return peaks

    def find_troughs(series, min_dist=4):
        troughs = []
        for i in range(3, n - 3):
            v = float(series.iloc[i])
            if all(v <= float(series.iloc[i-j]) for j in range(1, 4)) and \
               all(v <= float(series.iloc[i+j]) for j in range(1, 4)):
                if not troughs or (i - troughs[-1][0]) >= min_dist:
                    troughs.append((i, v, dates[i]))
        return troughs

    def ts_ms(d):
        try: return int(pd.Timestamp(d).timestamp() * 1000)
        except: return None

    peaks   = find_peaks(wh)
    troughs = find_troughs(wl)
    current = float(wc.iloc[-1])

    # ── HCH bajista: HI < CAB > HD, con HI ≈ HD ─────────
    if len(peaks) >= 3:
        for i in range(len(peaks) - 2):
            hi_i, hi_v, hi_x = peaks[i]
            cab_i, cab_v, cab_x = peaks[i+1]
            hd_i, hd_v, hd_x = peaks[-1]
            if cab_v <= hi_v or cab_v <= hd_v:
                continue
            if abs(hi_v - hd_v) / max(hi_v, 0.0001) > tol * 2:
                continue
            if hd_i - cab_i < 5:
                continue
            v1 = [t for t in troughs if hi_i < t[0] < cab_i]
            v2 = [t for t in troughs if cab_i < t[0] < hd_i]
            if not v1 or not v2:
                continue
            nk1_i, nk1_v, nk1_x = min(v1, key=lambda x: x[1])
            nk2_i, nk2_v, nk2_x = min(v2, key=lambda x: x[1])
            if current < nk2_v:
                estado = "confirmado"
            elif abs(current - hd_v) / max(hd_v, 0.0001) <= tol:
                estado = "formando_hd"
            else:
                estado = "formando"
            result["HCH"] = {
                "hi_x":  ts_ms(hi_x),  "hi_y":  hi_v,
                "cab_x": ts_ms(cab_x), "cab_y": cab_v,
                "hd_x":  ts_ms(hd_x),  "hd_y":  hd_v,
                "nk1_x": ts_ms(nk1_x), "nk1_y": nk1_v,
                "nk2_x": ts_ms(nk2_x), "nk2_y": nk2_v,
                "rsi_hi":  float(wr.iloc[hi_i]),
                "rsi_cab": float(wr.iloc[cab_i]),
                "rsi_hd":  float(wr.iloc[hd_i]),
                "estado": estado,
                "bearish": True,
            }
            break

    # ── HCH invertido alcista ─────────────────────────────
    if len(troughs) >= 3:
        for i in range(len(troughs) - 2):
            hi_i, hi_v, hi_x = troughs[i]
            cab_i, cab_v, cab_x = troughs[i+1]
            hd_i, hd_v, hd_x = troughs[-1]
            if cab_v >= hi_v or cab_v >= hd_v:
                continue
            if abs(hi_v - hd_v) / max(abs(hi_v), 0.0001) > tol * 2:
                continue
            if hd_i - cab_i < 5:
                continue
            p1 = [t for t in peaks if hi_i < t[0] < cab_i]
            p2 = [t for t in peaks if cab_i < t[0] < hd_i]
            if not p1 or not p2:
                continue
            nk1_i, nk1_v, nk1_x = max(p1, key=lambda x: x[1])
            nk2_i, nk2_v, nk2_x = max(p2, key=lambda x: x[1])
            if current > nk2_v:
                estado = "confirmado"
            elif abs(current - hd_v) / max(abs(hd_v), 0.0001) <= tol:
                estado = "formando_hd"
            else:
                estado = "formando"
            result["HCH_inv"] = {
                "hi_x":  ts_ms(hi_x),  "hi_y":  hi_v,
                "cab_x": ts_ms(cab_x), "cab_y": cab_v,
                "hd_x":  ts_ms(hd_x),  "hd_y":  hd_v,
                "nk1_x": ts_ms(nk1_x), "nk1_y": nk1_v,
                "nk2_x": ts_ms(nk2_x), "nk2_y": nk2_v,
                "rsi_hi":  float(wr.iloc[hi_i]),
                "rsi_cab": float(wr.iloc[cab_i]),
                "rsi_hd":  float(wr.iloc[hd_i]),
                "estado": estado,
                "bullish": True,
            }
            break

    return result


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
