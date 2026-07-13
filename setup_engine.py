"""
setup_engine.py — Motor de detección de setups de alta probabilidad
Trading Band · 1H candles

Pipeline obligatorio (los 3 deben cumplirse):
  1. Barrido de liquidez en últimas 3 velas (HOD/LOD/PDH/PDL/semana/mes)
  2. Trampa confirmada (vela que barrió, o la siguiente, cerró al lado correcto)
  3. Divergencia RSI activa (N1 mínimo) alineada con la dirección

Scoring 0–15 pts (solo si los 3 criterios pasan):
  Div N1 +1 | N2 +1 | N3 +2             (max 4)
  Liquidez minor +1 | medium +1 | major +2 (max 4)
  FVG activo +2 | ya tocado +1           (max 3)
  Filtro DXY/VIX confirma               (max 2)
  Sesión activa (Londres/NY)             (max 1)
  Patrón M/W/HCH en formación/confirm    (max 1)

Umbrales:  ≤6→nada  7-8→guardar  9-10→FAVORABLE  11-12→MUY FAVORABLE  13-15→MÁXIMA
"""

import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, time as dtime

try:
    from trading_band_routes import (
        detect_all_divergences as _detect_all_divs,
        calc_pattern_mw        as _calc_mw,
        calc_pattern_hch       as _calc_hch,
        detect_fvg             as _detect_fvg,
    )
    HAS_TB = True
except ImportError:
    HAS_TB = False

# ── Clasificación de tickers por relación con DXY/VIX ───────────
_USD_QUOTE = {"EURUSD=X", "GBPUSD=X", "AUDUSD=X"}   # DXY sube → bearish en el par
_USD_BASE  = {"USDJPY=X"}                              # DXY sube → bullish en el par
_INDICES   = {"^DJI", "^NDX", "^GSPC"}                # usar VIX
# Commodities, BTC, cruces JPY/GBP → sin filtro DXY/VIX

# ── Ventanas de sesión (UTC) ─────────────────────────────────────
_LONDON_OPEN  = dtime(7,  0)
_LONDON_CLOSE = dtime(12, 0)
_NY_OPEN      = dtime(12, 0)
_NY_CLOSE     = dtime(17, 0)


# ─────────────────────────────────────────────────────────────────
# 1. NIVELES DE LIQUIDEZ
# ─────────────────────────────────────────────────────────────────

def detect_liquidity_levels(df: pd.DataFrame) -> dict:
    """
    Calcula HOD, LOD, PDH, PDL, WKH, WKL, MTH, MTL desde el DataFrame 1H.
    Columnas: Open/High/Low/Close (capitalizadas). Índice: DatetimeIndex UTC-aware.
    """
    if df.empty or len(df) < 4:
        return {}

    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    else:
        idx = idx.tz_convert("UTC")

    df2 = df.copy()
    df2.index = idx
    dates_utc = idx.normalize()   # fecha sin hora

    last_date = dates_utc[-1]

    # Días únicos con velas (en orden)
    unique_days = sorted(dates_utc.unique())

    # "Hoy" = la fecha de la última vela
    # "Ayer" = el día anterior que tenga velas (maneja fines de semana)
    today_mask    = dates_utc == last_date
    today_data    = df2[today_mask]

    # Buscar el día anterior hábil
    prev_days = [d for d in unique_days if d < last_date]
    yesterday_data = pd.DataFrame()
    if prev_days:
        prev_date = prev_days[-1]
        yesterday_data = df2[dates_utc == prev_date]

    # Semana: lunes de la semana de la última vela
    week_start = last_date - pd.Timedelta(days=last_date.dayofweek)
    week_mask  = (dates_utc >= week_start) & (dates_utc <= last_date)
    week_data  = df2[week_mask]

    # Mes actual
    month_mask = (idx.year == last_date.year) & (idx.month == last_date.month)
    month_data = df2[month_mask]

    def _hi(d): return float(d["High"].max())  if not d.empty else None
    def _lo(d): return float(d["Low"].min())   if not d.empty else None

    levels = {
        "hod": _hi(today_data),
        "lod": _lo(today_data),
        "pdh": _hi(yesterday_data),
        "pdl": _lo(yesterday_data),
        "wkh": _hi(week_data),
        "wkl": _lo(week_data),
        "mth": _hi(month_data),
        "mtl": _lo(month_data),
    }
    return {k: v for k, v in levels.items() if v is not None}


# ─────────────────────────────────────────────────────────────────
# 2. DETECCIÓN DE BARRIDO
# ─────────────────────────────────────────────────────────────────

_TIER = {
    "hod": "minor", "lod": "minor",
    "pdh": "medium", "pdl": "medium",
    "wkh": "major",  "wkl": "major",
    "mth": "major",  "mtl": "major",
}
_TIER_ORDER = {"minor": 0, "medium": 1, "major": 2}

# Niveles altos: se barren cuando High > nivel → trampa alcista → setup bajista
_HIGH_LEVELS = {"hod", "pdh", "wkh", "mth"}
# Niveles bajos: se barren cuando Low < nivel → trampa bajista → setup alcista
_LOW_LEVELS  = {"lod", "pdl", "wkl", "mtl"}


def detect_liquidity_sweep(df: pd.DataFrame, levels: dict) -> dict | None:
    """
    Examina las últimas 3 velas en busca de un barrido de liquidez.
    Niveles altos (HOD/PDH/WKH/MTH): barrido cuando High > nivel → dirección bearish.
    Niveles bajos (LOD/PDL/WKL/MTL): barrido cuando Low  < nivel → dirección bullish.
    Si hay múltiples barridos, devuelve el de mayor tier.
    Devuelve None si no se detecta barrido.
    """
    if not levels or len(df) < 4:
        return None

    tail = df.iloc[-3:]
    n    = len(df)

    best = None

    for i, (ts, row) in enumerate(tail.iterrows()):
        bar_idx = n - 3 + i
        hi  = float(row["High"])
        lo  = float(row["Low"])

        for lname, lprice in levels.items():
            if lprice is None:
                continue
            tier = _TIER.get(lname, "minor")

            if lname in _HIGH_LEVELS:
                # Barrido alcista del nivel alto → trampa alcista → setup corto
                if hi > lprice:
                    candidate = {
                        "level_type":  lname,
                        "level_price": lprice,
                        "level_tier":  tier,
                        "direction":   "bearish",
                        "bar_idx":     bar_idx,
                        "sweep_high":  hi,
                        "sweep_low":   lo,
                    }
                    if best is None or _TIER_ORDER[tier] > _TIER_ORDER[best["level_tier"]]:
                        best = candidate

            elif lname in _LOW_LEVELS:
                # Barrido bajista del nivel bajo → trampa bajista → setup largo
                if lo < lprice:
                    candidate = {
                        "level_type":  lname,
                        "level_price": lprice,
                        "level_tier":  tier,
                        "direction":   "bullish",
                        "bar_idx":     bar_idx,
                        "sweep_high":  hi,
                        "sweep_low":   lo,
                    }
                    if best is None or _TIER_ORDER[tier] > _TIER_ORDER[best["level_tier"]]:
                        best = candidate

    return best


# ─────────────────────────────────────────────────────────────────
# 3. TRAMPA CONFIRMADA
# ─────────────────────────────────────────────────────────────────

def detect_trap_confirmed(df: pd.DataFrame, sweep: dict) -> bool:
    """
    Verifica que la vela que barrió (o la inmediatamente siguiente)
    cerró de vuelta al lado correcto del nivel de liquidez.
    Buffer = 0.05% del precio.
    """
    if sweep is None:
        return False

    bar_idx   = sweep["bar_idx"]
    level_px  = sweep["level_price"]
    direction = sweep["direction"]
    buffer    = level_px * 0.0005

    # Vela que barrió + la siguiente (si existe)
    candidates = [bar_idx]
    if bar_idx + 1 < len(df):
        candidates.append(bar_idx + 1)

    for idx in candidates:
        close = float(df["Close"].iloc[idx])
        if direction == "bullish" and close > level_px + buffer:
            return True
        if direction == "bearish" and close < level_px - buffer:
            return True

    return False


# ─────────────────────────────────────────────────────────────────
# 4. DIVERGENCIA ACTIVA ALINEADA
# ─────────────────────────────────────────────────────────────────

def detect_active_divergence(
    df: pd.DataFrame,
    rsi_series: pd.Series,
    direction: str,
) -> dict | None:
    """
    Busca la divergencia RSI más reciente alineada con 'direction'.
    SOLO N1 y N2 satisfacen el criterio obligatorio — N3 es bonus únicamente.
    Ventana de recencia: N1 = últimas 6 velas, N2 = últimas 11.
    Devuelve None si solo hay N3 activa (no pasa el gate obligatorio).
    """
    if not HAS_TB:
        return None

    df_lower = df.rename(columns=str.lower)
    all_divs = _detect_all_divs(df_lower, rsi_series)

    n = len(rsi_series)

    # Tipos aceptados según dirección
    if direction == "bullish":
        accepted_types = {"bull", "hbull"}
    else:
        accepted_types = {"bear", "hbear"}

    # Solo N1 y N2 como gate obligatorio
    lb_mandatory = {1: 6, 2: 11}

    best = None
    for dv in all_divs:
        if dv["type"] not in accepted_types:
            continue
        lvl = dv["level"]
        if lvl not in lb_mandatory:
            continue    # N3 no satisface el gate obligatorio
        lb = lb_mandatory[lvl]
        if dv["bar"] < n - lb:
            continue    # demasiado antigua
        if best is None or dv["bar"] > best["bar"]:
            best = dv

    return best


# ─────────────────────────────────────────────────────────────────
# 5. FILTRO DXY / VIX
# ─────────────────────────────────────────────────────────────────

def _get_dxy_direction() -> str | None:
    """Devuelve 'rising' o 'falling' para el DXY, o None si falla."""
    try:
        raw = yf.download("DX-Y.NYB", period="2d", interval="1h",
                          progress=False, auto_adjust=True)
        if raw.empty or len(raw) < 2:
            return None
        close = raw["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        if len(close) < 2:
            return None
        return "rising" if float(close.iloc[-1]) > float(close.iloc[-2]) else "falling"
    except Exception:
        return None


def _get_vix_value() -> float | None:
    """Devuelve el último valor del VIX, o None si falla."""
    try:
        raw = yf.download("^VIX", period="2d", interval="1h",
                          progress=False, auto_adjust=True)
        if raw.empty:
            return None
        close = raw["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        return float(close.iloc[-1]) if not close.empty else None
    except Exception:
        return None


def _score_external_filter(ticker: str, direction: str) -> tuple[int, str]:
    """
    Devuelve (puntos, descripción) del filtro externo DXY o VIX.
    0 puntos si no aplica o no confirma. 2 puntos si confirma.
    """
    t = ticker.upper()

    if t in _USD_QUOTE:
        dxy = _get_dxy_direction()
        if dxy is None:
            return 0, ""
        confirms = (direction == "bearish" and dxy == "rising") or \
                   (direction == "bullish" and dxy == "falling")
        if confirms:
            label = f"DXY {'↑' if dxy == 'rising' else '↓'} confirma"
            return 2, label
        return 0, ""

    if t in _USD_BASE:
        dxy = _get_dxy_direction()
        if dxy is None:
            return 0, ""
        confirms = (direction == "bullish" and dxy == "rising") or \
                   (direction == "bearish" and dxy == "falling")
        if confirms:
            label = f"DXY {'↑' if dxy == 'rising' else '↓'} confirma"
            return 2, label
        return 0, ""

    if t in _INDICES:
        vix = _get_vix_value()
        if vix is None:
            return 0, ""
        confirms = (direction == "bearish" and vix > 20) or \
                   (direction == "bullish" and vix < 15)
        if confirms:
            return 2, f"VIX {vix:.1f} confirma"
        return 0, ""

    return 0, ""


# ─────────────────────────────────────────────────────────────────
# 6. SESIÓN ACTIVA
# ─────────────────────────────────────────────────────────────────

def _is_session_active(df: pd.DataFrame) -> bool:
    """
    Comprueba si la última vela cayó dentro de una sesión activa.
    Londres: 07:00–12:00 UTC | Nueva York: 12:00–17:00 UTC
    """
    ts = df.index[-1]
    if hasattr(ts, "tz") and ts.tz is None:
        ts = ts.tz_localize("UTC")
    elif hasattr(ts, "tz") and ts.tz is not None:
        ts = ts.tz_convert("UTC")

    t = ts.time()
    in_london = _LONDON_OPEN <= t < _LONDON_CLOSE
    in_ny     = _NY_OPEN     <= t < _NY_CLOSE
    return in_london or in_ny


# ─────────────────────────────────────────────────────────────────
# 7. PATRONES M/W/HCH
# ─────────────────────────────────────────────────────────────────

_FORMING_ESTADOS = {"formando_p2", "formando_v2", "formando_hd", "formando"}
_CONFIRM_ESTADOS = {"confirmado"}


def _score_patterns(df: pd.DataFrame, direction: str) -> tuple[int, dict]:
    """
    Devuelve (puntos, info_dict) de patrones M/W/HCH.
    +1 si hay patrón alineado en formación o confirmado.
    """
    if not HAS_TB:
        return 0, {}

    info = {}
    pts  = 0

    try:
        mw = _calc_mw(df)
        if direction == "bearish":
            pat = mw.get("M", {})
            if pat:
                estado = pat.get("estado", "")
                if estado in _CONFIRM_ESTADOS or estado in _FORMING_ESTADOS:
                    pts = 1
                    info["M"] = {"estado": estado}
        else:
            pat = mw.get("W", {})
            if pat:
                estado = pat.get("estado", "")
                if estado in _CONFIRM_ESTADOS or estado in _FORMING_ESTADOS:
                    pts = 1
                    info["W"] = {"estado": estado}
    except Exception:
        pass

    if pts == 0:
        try:
            hch = _calc_hch(df)
            if direction == "bearish":
                pat = hch.get("HCH", {})
                if pat:
                    estado = pat.get("estado", "")
                    if estado in _CONFIRM_ESTADOS or estado in _FORMING_ESTADOS:
                        pts = 1
                        info["HCH"] = {"estado": estado}
            else:
                pat = hch.get("HCH_inv", {})
                if pat:
                    estado = pat.get("estado", "")
                    if estado in _CONFIRM_ESTADOS or estado in _FORMING_ESTADOS:
                        pts = 1
                        info["HCH_inv"] = {"estado": estado}
        except Exception:
            pass

    return pts, info


# ─────────────────────────────────────────────────────────────────
# 8. FVG (Fair Value Gaps)
# ─────────────────────────────────────────────────────────────────

def _score_fvg(df: pd.DataFrame, direction: str) -> tuple[int, dict | None]:
    """
    Devuelve (puntos, fvg_info).
    FVG activo alineado: +2. Ya tocado: +1 extra. Max 3.
    """
    if not HAS_TB:
        return 0, None

    try:
        fvgs = _detect_fvg(df)
    except Exception:
        return 0, None

    fvg_dir = direction  # "bull" para setup bullish, "bear" para bearish
    # Pero detect_fvg usa "bull"/"bear" en minúsculas
    match_dir = "bull" if direction == "bullish" else "bear"

    for fvg in fvgs:
        if fvg.get("frozen"):
            continue
        if fvg.get("direction") != match_dir:
            continue
        # Encontrado FVG activo alineado
        pts = 2
        if fvg.get("touched"):
            pts += 1
        return pts, fvg

    return 0, None


# ─────────────────────────────────────────────────────────────────
# 9. SCORING COMPLETO
# ─────────────────────────────────────────────────────────────────

def score_setup(
    df: pd.DataFrame,
    rsi_series: pd.Series,
    sweep: dict,
    divergence: dict,
    ticker: str,
    levels: dict,
) -> dict:
    """
    Calcula la puntuación 0–15 para un setup que ya pasó los 3 criterios obligatorios.
    Devuelve el dict completo con breakdown, estado y nivel_alerta.
    """
    direction = sweep["direction"]
    pts = 0
    bd  = {}

    # ── Divergencia ──────────────────────────────────────────────
    # Detectar cuáles niveles están activos (N1, N2, N3) para puntuar cada uno
    n = len(rsi_series)
    has_n1 = has_n2 = has_n3 = False
    if HAS_TB:
        df_lower = df.rename(columns=str.lower)
        all_divs = _detect_all_divs(df_lower, rsi_series)
        accepted_types = {"bull", "hbull"} if direction == "bullish" else {"bear", "hbear"}
        for dv in all_divs:
            if dv["type"] not in accepted_types:
                continue
            lvl = dv["level"]
            if lvl == 1 and dv["bar"] >= n - 6:
                has_n1 = True
            elif lvl == 2 and dv["bar"] >= n - 11:
                has_n2 = True
            elif lvl == 3 and dv["bar"] >= n - 21:
                has_n3 = True

    div_pts = 0
    if has_n1:
        div_pts += 1
    if has_n2:
        div_pts += 1
    if has_n3:
        div_pts += 2
    # El gate obligatorio ya pasó (N1 o N2 activo), garantizamos ≥1 pt
    div_pts = max(1, min(div_pts, 4))
    bd["div_pts"] = div_pts
    pts += div_pts

    # ── Liquidez ─────────────────────────────────────────────────
    tier = sweep.get("level_tier", "minor")
    liq_pts = {"minor": 1, "medium": 2, "major": 4}.get(tier, 1)
    liq_pts = min(liq_pts, 4)
    bd["liq_pts"] = liq_pts
    pts += liq_pts

    # ── FVG ──────────────────────────────────────────────────────
    fvg_pts, fvg_info = _score_fvg(df, direction)
    bd["fvg_pts"] = fvg_pts
    pts += fvg_pts

    # ── Filtro DXY/VIX ───────────────────────────────────────────
    filter_pts, filter_label = _score_external_filter(ticker, direction)
    bd["filter_pts"]  = filter_pts
    bd["filter_label"] = filter_label
    pts += filter_pts

    # ── Sesión ───────────────────────────────────────────────────
    session_active = _is_session_active(df)
    session_pts    = 1 if session_active else 0
    bd["session_pts"] = session_pts
    pts += session_pts

    # ── Patrones ─────────────────────────────────────────────────
    pattern_pts, pattern_info = _score_patterns(df, direction)
    bd["pattern_pts"] = pattern_pts
    pts += pattern_pts

    pts = min(pts, 15)

    # ── Umbral ───────────────────────────────────────────────────
    if pts <= 6:
        estado, nivel = "NO_SIGNAL",     "none"
    elif pts <= 8:
        estado, nivel = "CONSIDERAR",    "save"
    elif pts <= 10:
        estado, nivel = "FAVORABLE",     "normal"
    elif pts <= 12:
        estado, nivel = "MUY_FAVORABLE", "high"
    else:
        estado, nivel = "MAXIMA",        "urgent"

    return {
        "puntos":         pts,
        "estado":         estado,
        "nivel_alerta":   nivel,
        "direction":      direction,
        "breakdown":      bd,
        "fvg":            fvg_info,
        "patterns":       pattern_info,
        "levels":         levels,
        "sweep":          sweep,
        "divergence":     divergence,
        "session_active": session_active,
        "ticker":         ticker.upper(),
    }


# ─────────────────────────────────────────────────────────────────
# 10. FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def evaluate_setup(
    df: pd.DataFrame,
    rsi_series: pd.Series,
    ticker: str,
    cfg: dict | None = None,
) -> dict | None:
    """
    Pipeline completo. Devuelve None si algún criterio obligatorio falla.
    Los 3 criterios obligatorios:
      1. Barrido de liquidez en últimas 3 velas
      2. Trampa confirmada (cierre al lado correcto)
      3. Divergencia RSI activa alineada (N1 mínimo)
    Si los 3 pasan → calcula scoring 0-15 y devuelve el dict.
    """
    if df.empty or len(df) < 50:
        return None

    # ── Criterio 1: niveles + barrido ───────────────────────────
    levels = detect_liquidity_levels(df)
    if not levels:
        return None

    sweep = detect_liquidity_sweep(df, levels)
    if sweep is None:
        return None

    direction = sweep["direction"]

    # ── Criterio 2: trampa confirmada ───────────────────────────
    if not detect_trap_confirmed(df, sweep):
        return None

    # ── Criterio 3: divergencia activa alineada ─────────────────
    divergence = detect_active_divergence(df, rsi_series, direction)
    if divergence is None:
        return None

    # ── Scoring ─────────────────────────────────────────────────
    return score_setup(df, rsi_series, sweep, divergence, ticker, levels)
