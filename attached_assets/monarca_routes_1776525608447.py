# monarca_routes.py — Rutas FastAPI para MonarcaBand Signal App
# © Purificación Santana
# Lógica de divergencias basada en: "RSI con Divergencias — 3 Niveles"
# Integrar en main.py:
#   from monarca_routes import router as monarca_router
#   app.include_router(monarca_router)

from fastapi import APIRouter
import yfinance as yf
import pandas as pd
import numpy as np
import asyncio
import time

router = APIRouter(prefix="/api/monarca", tags=["MonarcaSignal"])

# ──────────────────────────────────────────────────────────
# CONFIG POR ACTIVO
# ──────────────────────────────────────────────────────────
MONARCA_CONFIG = {
    "^DJI":    {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "US30"},
    "^NDX":    {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "NASDAQ"},
    "^GSPC":   {"range": 50,   "ema_s": 200, "ema_l": 800, "name": "S&P500"},
    "GC=F":    {"range": 50,   "ema_s": 200, "ema_l": 800, "name": "Gold"},
    "SI=F":    {"range": 0.5,  "ema_s": 200, "ema_l": 800, "name": "Silver"},
    "CL=F":    {"range": 2,    "ema_s": 200, "ema_l": 800, "name": "Oil"},
    "USDJPY=X":{"range": 1,    "ema_s": 200, "ema_l": 800, "name": "USD/JPY"},
    "GBPJPY=X":{"range": 1,    "ema_s": 200, "ema_l": 800, "name": "GBP/JPY"},
    "EURUSD=X":{"range": 0.005,"ema_s": 200, "ema_l": 800, "name": "EUR/USD"},
    "BTC-USD": {"range": 1000, "ema_s": 200, "ema_l": 800, "name": "Bitcoin"},
    "_default":{"range": 50,   "ema_s": 200, "ema_l": 800, "name": "Asset"},
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
    """RMA = Wilder's Smoothing (igual que ta.rma en Pine Script)"""
    result = np.full(len(s), np.nan)
    vals = s.values
    # Primer valor válido
    start = p - 1
    while start < len(vals) and np.isnan(vals[start]):
        start += 1
    if start >= len(vals):
        return pd.Series(result, index=s.index)
    # Seed: SMA de los primeros p valores
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
    """RSI usando ta.rma (Wilder), igual que Pine Script."""
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

def calc_monarca(close: pd.Series, tp: int = 12, ap: int = 12):
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
# DETECCIÓN DE PIVOTES (ta.pivotlow / ta.pivothigh de Pine)
# ──────────────────────────────────────────────────────────

def pivot_low(s: pd.Series, lb_l: int, lb_r: int) -> pd.Series:
    """
    Replica ta.pivotlow(rsi, lbL, lbR):
    Un pivote bajo en la barra i significa que s[i] es el mínimo
    en la ventana [i-lbL, i+lbR].  Devuelve True en la barra
    (i + lbR), es decir, cuando ya se confirman lbR velas a la derecha.
    """
    n = len(s)
    result = pd.Series(False, index=s.index)
    for i in range(lb_l, n - lb_r):
        val = s.iloc[i]
        if pd.isna(val):
            continue
        left_ok  = all(s.iloc[i - lb_l : i]          >= val)
        right_ok = all(s.iloc[i + 1    : i + lb_r + 1] >= val)
        if left_ok and right_ok:
            # El pivote se detecta en la barra i + lbR (cuando ya se cierra la ventana derecha)
            result.iloc[i + lb_r] = True
    return result

def pivot_high(s: pd.Series, lb_l: int, lb_r: int) -> pd.Series:
    n = len(s)
    result = pd.Series(False, index=s.index)
    for i in range(lb_l, n - lb_r):
        val = s.iloc[i]
        if pd.isna(val):
            continue
        left_ok  = all(s.iloc[i - lb_l : i]          <= val)
        right_ok = all(s.iloc[i + 1    : i + lb_r + 1] <= val)
        if left_ok and right_ok:
            result.iloc[i + lb_r] = True
    return result

# ──────────────────────────────────────────────────────────
# DIVERGENCIAS — lógica fiel al Pine Script
#
# Pine usa ta.valuewhen(cond, src, occurrence=1), que devuelve
# el valor de src en la ÚLTIMA vez que cond fue true ANTES de ahora.
# Aquí lo replicamos iterando por los pivotes en orden cronológico.
# ──────────────────────────────────────────────────────────

def _in_range(bars_since: int, rn_min: int, rn_max: int) -> bool:
    return rn_min <= bars_since <= rn_max

def detect_divergences_level(
    price_low:  pd.Series,
    price_high: pd.Series,
    rsi:        pd.Series,
    lb_l: int, lb_r: int,
    rn_min: int, rn_max: int,
    level: int,
) -> list:
    """
    Replica exacta de f_divs() del Pine Script "RSI con Divergencias — 3 Niveles".

    Retorna una lista de dicts con:
      type        : 'bull' | 'bear' | 'hbull' | 'hbear'
      level       : 1 | 2 | 3
      bar         : índice de la barra ACTUAL del pivote (0-based)
      bar_prev    : índice del pivote ANTERIOR
      time / time_prev : ISO timestamps
      rsi / rsi_prev   : valor RSI en cada pivote
      price / price_prev : precio (low/high) en cada pivote
    """
    divs = []
    times = rsi.index

    # Calculamos las series de pivotes booleanas (confirmadas con lbR de retraso)
    pl_series = pivot_low (rsi, lb_l, lb_r)
    ph_series = pivot_high(rsi, lb_l, lb_r)

    # La barra del PIVOTE REAL es i - lb_r  (porque el pivote se confirma lb_r barras después)
    # El valor RSI/precio en el pivote real está en rsi[pivot_bar]

    n = len(rsi)

    # ── Pivotes bajos (bullish divergences) ──────────────
    # Guardamos la historia de pivotes para replicar ta.valuewhen(..., 1)
    prev_pl = None   # (bar_real, rsi_val, price_val)

    for confirm_bar in range(n):
        if not pl_series.iloc[confirm_bar]:
            continue
        pivot_bar = confirm_bar - lb_r        # barra real del pivote
        if pivot_bar < 0:
            continue

        rsi_cur   = float(rsi.iloc[pivot_bar])
        price_cur = float(price_low.iloc[pivot_bar])

        if prev_pl is not None:
            prev_bar, prev_rsi, prev_price = prev_pl
            bars_between = pivot_bar - prev_bar

            if _in_range(bars_between, rn_min, rn_max):
                price_ll = price_cur < prev_price  # precio hace Lower Low
                price_hl = price_cur > prev_price  # precio hace Higher Low
                rsi_hl   = rsi_cur   > prev_rsi    # RSI hace Higher Low
                rsi_ll   = rsi_cur   < prev_rsi    # RSI hace Lower Low

                # Regular Alcista: precio LL, RSI HL
                if price_ll and rsi_hl:
                    divs.append({
                        "type": "bull", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                    })
                # Oculta Alcista: precio HL, RSI LL
                elif price_hl and rsi_ll:
                    divs.append({
                        "type": "hbull", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                    })

        prev_pl = (pivot_bar, rsi_cur, price_cur)

    # ── Pivotes altos (bearish divergences) ─────────────
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
                price_hh = price_cur > prev_price
                price_lh = price_cur < prev_price
                rsi_lh   = rsi_cur   < prev_rsi
                rsi_hh   = rsi_cur   > prev_rsi

                # Regular Bajista: precio HH, RSI LH
                if price_hh and rsi_lh:
                    divs.append({
                        "type": "bear", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                    })
                # Oculta Bajista: precio LH, RSI HH
                elif price_lh and rsi_hh:
                    divs.append({
                        "type": "hbear", "level": level,
                        "bar": pivot_bar, "bar_prev": prev_bar,
                        "time": times[pivot_bar].isoformat(),
                        "time_prev": times[prev_bar].isoformat(),
                        "rsi": round(rsi_cur, 2), "rsi_prev": round(prev_rsi, 2),
                        "price": round(price_cur, 4), "price_prev": round(prev_price, 4),
                    })

        prev_ph = (pivot_bar, rsi_cur, price_cur)

    return divs


def detect_all_divergences(df: pd.DataFrame, rsi: pd.Series) -> list:
    """Detecta divergencias en los 3 niveles, igual que el indicador Pine."""
    all_divs = []
    levels = [
        # (lb_l, lb_r, rn_min, rn_max, nivel)
        ( 5,  5,   5,   50, 1),
        (10, 10,  10,  200, 2),
        (20, 20,  20, 1000, 3),
    ]
    for lb_l, lb_r, rn_min, rn_max, lvl in levels:
        divs = detect_divergences_level(
            df["low"], df["high"], rsi,
            lb_l, lb_r, rn_min, rn_max, lvl,
        )
        all_divs.extend(divs)

    # Más reciente primero
    all_divs.sort(key=lambda x: x["bar"], reverse=True)
    return all_divs

# ──────────────────────────────────────────────────────────
# SEÑALES RSI — pares de barras para dibujar en el panel RSI
# (replica el plot() con offset del Pine Script)
# ──────────────────────────────────────────────────────────

def build_rsi_div_segments(divs: list, rsi: pd.Series, times) -> list:
    """
    Para cada divergencia, devuelve el segmento [pivote_prev → pivote_actual]
    en coordenadas RSI, listo para dibujar sobre el panel RSI.
    """
    segments = []
    for dv in divs:
        bar1 = dv["bar_prev"]
        bar2 = dv["bar"]
        if bar1 < 0 or bar2 >= len(rsi):
            continue
        # Usamos el valor RSI real en cada barra del pivote (no el confirmado)
        segments.append({
            "type":  dv["type"],
            "level": dv["level"],
            "x1": times[bar1],
            "y1": float(rsi.iloc[bar1]),
            "x2": times[bar2],
            "y2": float(rsi.iloc[bar2]),
        })
    return segments

# ──────────────────────────────────────────────────────────
# DESCARGA ASYNC
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
async def get_monarca_signal(ticker: str, use_range_bars: bool = True):
    key = ticker.upper()
    now = time.time()

    cached = _cache.get(key)
    if cached and (now - cached["ts"]) < _CACHE_TTL:
        return cached["data"]

    cfg = MONARCA_CONFIG.get(key, MONARCA_CONFIG["_default"])

    try:
        # ── Descargar ────────────────────────────────────────
        if use_range_bars:
            raw = await _download(key, period="60d", interval="5m")
        else:
            raw = await _download(key, period="2y", interval="4h")

        if raw is None or (hasattr(raw, "empty") and raw.empty):
            return {"error": f"Sin datos para {key}"}

        df = _clean(raw)

        # ── Range bars ──────────────────────────────────────
        if use_range_bars:
            rb = build_range_bars(df, cfg["range"])
            df_calc = rb if (rb is not None and not rb.empty) else df
            bar_type = f"Range {cfg['range']}"
        else:
            df_calc = df
            bar_type = "4h"

        if df_calc.empty:
            return {"error": "No se pudieron construir barras"}

        # ── Indicadores ─────────────────────────────────────
        close   = df_calc["close"]
        trigger, average = calc_monarca(close, 12, 12)
        rsi_s   = calc_rsi(close, 14)
        ema200  = ema_calc(close, cfg["ema_s"])
        ema800  = ema_calc(close, cfg["ema_l"])

        up_trend   = trigger > average
        cross_up   = (~up_trend.shift(1).fillna(False)) & up_trend
        cross_down = ( up_trend.shift(1).fillna(True))  & (~up_trend)

        # ── Divergencias ────────────────────────────────────
        valid_mask = rsi_s.notna()
        if valid_mask.sum() >= 40:
            df_aligned = df_calc[valid_mask].copy()
            rsi_valid  = rsi_s[valid_mask]
            all_divs   = detect_all_divergences(df_aligned, rsi_valid)
            # Segmentos RSI para renderizar en el panel RSI
            times_all  = list(rsi_valid.index)
            rsi_segs   = build_rsi_div_segments(all_divs, rsi_valid, times_all)
        else:
            all_divs = []
            rsi_segs = []

        # ── Serializar ──────────────────────────────────────
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

        # RSI — también incluimos si está en zona OB/OS
        rsi_points = []
        for i in range(n):
            v = _safe(rsi_s.iloc[i])
            if v is None:
                continue
            zone = "ob" if v >= 70 else ("os" if v <= 30 else "neutral")
            rsi_points.append({"x": timestamps[i], "y": v, "zone": zone})

        # ── Señales recientes (últimas 30 barras) ────────────
        recent_signals = []
        look = min(30, n)
        for i in range(n - look, n):
            t = timestamps[i]; c_val = _safe(close.iloc[i])
            if cross_up.iloc[i]:
                recent_signals.append({"type": "monarca_up",   "x": t, "price": c_val})
            if cross_down.iloc[i]:
                recent_signals.append({"type": "monarca_down", "x": t, "price": c_val})
            rv = _safe(rsi_s.iloc[i])
            if rv and rsi_s.iloc[i] <= 30 and (i == 0 or rsi_s.iloc[i - 1] > 30):
                recent_signals.append({"type": "rsi_os", "x": t, "price": c_val, "rsi": rv})
            if rv and rsi_s.iloc[i] >= 70 and (i == 0 or rsi_s.iloc[i - 1] < 70):
                recent_signals.append({"type": "rsi_ob", "x": t, "price": c_val, "rsi": rv})

        recent_signals.sort(key=lambda x: x["x"], reverse=True)

        result = {
            "ticker": key,
            "name": cfg["name"],
            "bar_type": bar_type,
            "last_price": _safe(close.iloc[-1]),
            "last_rsi":   _safe(rsi_s.dropna().iloc[-1]) if rsi_s.notna().any() else 50.0,
            "monarca_up": bool(up_trend.iloc[-1]) if not up_trend.isna().all() else False,
            "trigger":    _safe(trigger.iloc[-1]),
            "average":    _safe(average.iloc[-1]),
            "ema_short":  _safe(ema200.iloc[-1]),
            "ema_long":   _safe(ema800.iloc[-1]),
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
            "rsi_div_segments": rsi_segs,   # ← nuevos: segmentos para el panel RSI
            "signals":       recent_signals,
            "divergences":   all_divs[:40],
            "last_divergences": all_divs[:5],
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
        {"id": "BTC-USD",  "name": "Bitcoin",    "group": "Crypto"},
    ]}
