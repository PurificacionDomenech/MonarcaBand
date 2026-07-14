"""
setup_engine.py — Motor de detección de setups de alta probabilidad
Trading Band · 1H candles

Pipeline obligatorio (los 3 deben cumplirse):
  1. Barrido de liquidez en últimas 3 velas (HOD/LOD/PDH/PDL/semana/mes)
  2. Trampa confirmada (cierre de vuelta)
  3. Divergencia RSI activa (N1 mínimo) alineada con la dirección

Scoring 0–18 pts (solo si los 3 criterios pasan):
  Divergencia:  N1 +1 | N2 +1 | N3 +2                    (max 4)
  Liquidez:    minor +1 | medium +1 | major +2            (max 4)
  FVG:         activo +2 | ya tocado +1                   (max 3)
  Filtro DXI/VIX:  divergencia parcial +2 | confirmada +3  (max 3)
  Sesión:     cada ventana activa +1 (Londres/NY/secundaria/cierre) (max 4)
  Patrón:     M/W/HCH en formación +2 | confirmado +1    (max 3)
  Velas:       martillo/envolvente/doji 0.5–1            (max 1)  [TODO]

Umbrales Telegram:  0–6 → no enviar  |  7–8 → guardar  |  9–10 → FAVORABLE
                     11–12 → MUY FAVORABLE  |  13–15 → SEÑAL MÁXIMA
                     16–18 → CONFLUENCIA TOTAL
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

# ── Ventanas de sesión (hora Madrid → UTC) ─────────────────────
_LONDON_OPEN      = dtime(8,  0)   # 09:00 Madrid
_LONDON_CLOSE     = dtime(10, 0)  # 11:00 Madrid
_SECONDARY_OPEN   = dtime(11, 20) # 12:20 Madrid
_SECONDARY_CLOSE  = dtime(11, 40) # 12:40 Madrid
_NY_OPEN          = dtime(13, 30) # 14:30 Madrid
_NY_CLOSE         = dtime(14, 30) # 15:30 Madrid
_LONDON_CLOSE_2   = dtime(16, 0)  # 17:00 Madrid
_LONDON_CLOSE_2_E = dtime(16, 30) # 17:30 Madrid


# ─────────────────────────────────────────────────────────────────
# 1. NIVELES DE LIQUIDEZ
# ─────────────────────────────────────────────────────────────────

def detect_liquidity_levels(df: pd.DataFrame) -> dict:
    """
    Calcula HOD, LOD, PDH, PDL, WKH, WKL, MTH, MTL desde el DataFrame 1H.
    Columnas: Open/High/Low/Close (capitalizadas). Índice: DatetimeIndex UTC-aware.

    Estrategia de referencia:
    - La fecha "hoy/ayer/semana/mes" se ancla a la ÚLTIMA vela del df completo.
    - HOD/LOD/WKH/WKL/MTH/MTL se computan desde df.iloc[:-3] para que las
      últimas 3 velas puedan superar esos niveles (necesario para detectar barridos).
    - PDH/PDL se buscan también en df.iloc[:-3] pero usando la fecha de ayer
      determinada desde el df completo → correcto en lunes y semana corta.
    """
    if df.empty or len(df) < 7:
        return {}

    # ── Anclar fechas al df completo ────────────────────────────
    full_idx = df.index
    if full_idx.tz is None:
        full_idx = full_idx.tz_localize("UTC")
    else:
        full_idx = full_idx.tz_convert("UTC")
    full_dates = full_idx.normalize()
    last_date  = full_dates[-1]                         # "hoy" real

    # Todos los días únicos con velas (para encontrar "ayer" incluso en lunes)
    all_unique_days = sorted(full_dates.unique())
    prev_days   = [d for d in all_unique_days if d < last_date]
    prev_date   = prev_days[-1] if prev_days else None  # día anterior hábil

    # ── Datos de referencia: excluye últimas 3 velas ────────────
    df_ref = df.iloc[:-3].copy()
    idx_ref = df_ref.index
    if idx_ref.tz is None:
        idx_ref = idx_ref.tz_localize("UTC")
    else:
        idx_ref = idx_ref.tz_convert("UTC")
    df_ref.index = idx_ref
    dates_ref    = idx_ref.normalize()

    # ── Subconjuntos de referencia ───────────────────────────────
    today_ref     = df_ref[dates_ref == last_date]   # puede estar vacío (primeras horas)
    yest_ref      = df_ref[dates_ref == prev_date]   if prev_date is not None else pd.DataFrame()
    week_start    = last_date - pd.Timedelta(days=last_date.dayofweek)
    week_ref      = df_ref[(dates_ref >= week_start) & (dates_ref <= last_date)]
    month_ref     = df_ref[(idx_ref.year  == last_date.year) &
                            (idx_ref.month == last_date.month)]

    def _hi(d): return float(d["High"].max()) if not d.empty else None
    def _lo(d): return float(d["Low"].min())  if not d.empty else None

    # ── Sesiones: Asia (00-08 UTC), Europa (08-16 UTC) ───────────
    # El df_ref tiene índice UTC
    hrs = idx_ref.hour
    asia_ref  = df_ref[(hrs >= 0) & (hrs < 8)]
    europa_ref = df_ref[(hrs >= 8) & (hrs < 16)]

    levels = {
        "hod": _hi(today_ref),
        "lod": _lo(today_ref),
        "pdh": _hi(yest_ref),
        "pdl": _lo(yest_ref),
        "wkh": _hi(week_ref),
        "wkl": _lo(week_ref),
        "mth": _hi(month_ref),
        "mtl": _lo(month_ref),
        # máx/mín de sesiones para alertas
        "asia_high":  _hi(asia_ref),
        "asia_low":   _lo(asia_ref),
        "europe_high": _hi(europa_ref),
        "europe_low":  _lo(europa_ref),
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


def _get_all_sweeps_by_direction(
    df: pd.DataFrame,
    levels: dict,
    direction: str,
) -> list:
    """
    Devuelve TODOS los barridos en las últimas 3 velas que coincidan con 'direction'.
    Usado por score_setup para sumar los tiers activos (minor +1, medium +1, major +2).
    """
    if not levels or len(df) < 4:
        return []

    tail = df.iloc[-3:]
    n    = len(df)
    results = []
    seen_level_types = set()  # evitar duplicados del mismo nivel

    for i, (ts, row) in enumerate(tail.iterrows()):
        bar_idx = n - 3 + i
        hi  = float(row["High"])
        lo  = float(row["Low"])

        for lname, lprice in levels.items():
            if lprice is None or lname in seen_level_types:
                continue
            tier = _TIER.get(lname, "minor")

            if lname in _HIGH_LEVELS and direction == "bearish" and hi > lprice:
                results.append({"level_type": lname, "level_tier": tier,
                                 "direction": "bearish", "bar_idx": bar_idx})
                seen_level_types.add(lname)
            elif lname in _LOW_LEVELS and direction == "bullish" and lo < lprice:
                results.append({"level_type": lname, "level_tier": tier,
                                 "direction": "bullish", "bar_idx": bar_idx})
                seen_level_types.add(lname)

    return results


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


def _score_external_filter(ticker: str, direction: str) -> tuple[int, str, bool | None]:
    """
    Devuelve (puntos, descripción, confirma) del filtro externo DXY o VIX.
    0 puntos si no aplica o no confirma. 2 puntos si confirma.
    confirma = True (confirma), False (contradice), None (no aplica/fallo).
    """
    t = ticker.upper()

    if t in _USD_QUOTE:
        dxy = _get_dxy_direction()
        if dxy is None:
            return 0, "DXI: datos no disponibles", None
        # EURUSD, GBPUSD, AUDUSD: DXY sube → par baja (bearish), DXY baja → par sube (bullish)
        confirms = (direction == "bearish" and dxy == "rising") or \
                   (direction == "bullish" and dxy == "falling")
        opuesto  = (direction == "bullish" and dxy == "rising") or \
                   (direction == "bearish" and dxy == "falling")
        if confirms:
            label = f"DXI confirmado ({dxy.upper()}) — REFUERZA {direction}"
            return 2, label, True
        elif opuesto:
            label = f"DXI CONTRADICE ({dxy.upper()}) — NO ENTRAR"
            return 0, label, False
        return 0, "DXI neutral", None

    if t in _USD_BASE:
        dxy = _get_dxy_direction()
        if dxy is None:
            return 0, "DXI: datos no disponibles", None
        # USDJPY: DXY sube → par sube (bullish), DXY baja → par baja (bearish)
        confirms = (direction == "bullish" and dxy == "rising") or \
                   (direction == "bearish" and dxy == "falling")
        opuesto  = (direction == "bearish" and dxy == "rising") or \
                   (direction == "bullish" and dxy == "falling")
        if confirms:
            label = f"DXI confirmado ({dxy.upper()}) — REFUERZA {direction}"
            return 2, label, True
        elif opuesto:
            label = f"DXI CONTRADICE ({dxy.upper()}) — NO ENTRAR"
            return 0, label, False
        return 0, "DXI neutral", None

    if t in _INDICES:
        vix = _get_vix_value()
        if vix is None:
            return 0, "VIX: datos no disponibles", None
        # Índices: VIX alto → mercado baja (bearish), VIX bajo → mercado sube (bullish)
        confirms = (direction == "bearish" and vix > 20) or \
                   (direction == "bullish" and vix < 15)
        opuesto  = (direction == "bullish" and vix > 20) or \
                   (direction == "bearish" and vix < 15)
        if confirms:
            label = f"VIX {vix:.1f} confirma {direction} — REFUERZA"
            return 2, label, True
        elif opuesto:
            label = f"VIX {vix:.1f} CONTRADICE {direction} — NO ENTRAR"
            return 0, label, False
        return 0, f"VIX {vix:.1f} neutral", None

    return 0, "", None


# ─────────────────────────────────────────────────────────────────
# 6. SESIÓN ACTIVA
# ─────────────────────────────────────────────────────────────────

def _is_session_active(df: pd.DataFrame) -> tuple[int, list[str]]:
    """
    Comprueba en qué ventanas de sesión cayó la última vela.
    Devuelve (puntos, [nombres de ventanas activas]).
    Cada ventana activa suma +1 punto.
    """
    ts = df.index[-1]
    if hasattr(ts, "tz") and ts.tz is None:
        ts = ts.tz_localize("UTC")
    elif hasattr(ts, "tz") and ts.tz is not None:
        ts = ts.tz_convert("UTC")

    t = ts.time()
    active = []
    if _LONDON_OPEN <= t < _LONDON_CLOSE:
        active.append("Londres")
    if _SECONDARY_OPEN <= t < _SECONDARY_CLOSE:
        active.append("Secundaria")
    if _NY_OPEN <= t < _NY_CLOSE:
        active.append("NY")
    if _LONDON_CLOSE_2 <= t < _LONDON_CLOSE_2_E:
        active.append("Cierre Londres")
    return len(active), active


# ─────────────────────────────────────────────────────────────────
# 7. PATRONES M/W/HCH
# ─────────────────────────────────────────────────────────────────

_FORMING_ESTADOS = {"formando_p2", "formando_v2", "formando_hd"}
_CONFIRM_ESTADOS = {"confirmado"}


def _score_patterns(df: pd.DataFrame, direction: str) -> tuple[int, dict]:
    """
    Devuelve (puntos, info_dict) de patrones M/W/HCH.
    +2 si hay patrón alineado en formación.
    +1 si hay patrón confirmado.
    Máx 3 puntos total (independiente del tipo de patrón).
    """
    if not HAS_TB:
        return 0, {}

    info = {}
    pts  = 0
    best = 0  # mayor puntuación encontrada

    def _pat_pts(estado: str) -> int:
        if estado in _FORMING_ESTADOS:
            return 2
        if estado in _CONFIRM_ESTADOS:
            return 1
        return 0

    # M / W
    try:
        mw = _calc_mw(df)
        if direction == "bearish":
            pat = mw.get("M", {})
            if pat:
                estado = pat.get("estado", "")
                p = _pat_pts(estado)
                if p > best:
                    best = p
                    info = {"M": {"estado": estado}}
        else:
            pat = mw.get("W", {})
            if pat:
                estado = pat.get("estado", "")
                p = _pat_pts(estado)
                if p > best:
                    best = p
                    info = {"W": {"estado": estado}}
    except Exception:
        pass

    # HCH
    if best < 3:
        try:
            hch = _calc_hch(df)
            if direction == "bearish":
                pat = hch.get("HCH", {})
                if pat:
                    estado = pat.get("estado", "")
                    p = _pat_pts(estado)
                    if p > best:
                        best = p
                        info = {"HCH": {"estado": estado}}
            else:
                pat = hch.get("HCH_inv", {})
                if pat:
                    estado = pat.get("estado", "")
                    p = _pat_pts(estado)
                    if p > best:
                        best = p
                        info = {"HCH_inv": {"estado": estado}}
        except Exception:
            pass

    return best, info


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

    # ── Liquidez: suma de tiers activos en la misma dirección ────
    # minor +1 | medium +1 | major +2  (máx 4 si los 3 presentes)
    all_sweeps = _get_all_sweeps_by_direction(df, levels, direction)
    has_minor  = any(s["level_tier"] == "minor"  for s in all_sweeps)
    has_medium = any(s["level_tier"] == "medium" for s in all_sweeps)
    has_major  = any(s["level_tier"] == "major"  for s in all_sweeps)
    liq_pts = 0
    if has_minor:  liq_pts += 1
    if has_medium: liq_pts += 1
    if has_major:  liq_pts += 2
    liq_pts = max(1, min(liq_pts, 4))  # gate pasó → ≥1 pt garantizado
    bd["liq_pts"] = liq_pts
    pts += liq_pts

    # ── FVG ──────────────────────────────────────────────────────
    fvg_pts, fvg_info = _score_fvg(df, direction)
    bd["fvg_pts"] = fvg_pts
    pts += fvg_pts

    # ── Filtro DXY/VIX ───────────────────────────────────────────
    filter_pts, filter_label, filter_confirms = _score_external_filter(ticker, direction)
    bd["filter_pts"]     = filter_pts
    bd["filter_label"]    = filter_label
    bd["filter_confirms"] = filter_confirms
    pts += filter_pts

    # ── Sesión ───────────────────────────────────────────────────
    session_pts, session_names = _is_session_active(df)
    bd["session_pts"] = session_pts
    pts += session_pts

    # ── Patrones ─────────────────────────────────────────────────
    pattern_pts, pattern_info = _score_patterns(df, direction)
    bd["pattern_pts"] = pattern_pts
    pts += pattern_pts

    pts = min(pts, 18)

    # ── Umbral ───────────────────────────────────────────────────
    if pts <= 6:
        estado, nivel = "NO_SIGNAL",     "none"
    elif pts <= 8:
        estado, nivel = "CONSIDERAR",    "save"
    elif pts <= 10:
        estado, nivel = "FAVORABLE",     "normal"
    elif pts <= 12:
        estado, nivel = "MUY_FAVORABLE", "high"
    elif pts <= 15:
        estado, nivel = "SEÑAL_MÁXIMA", "urgent"
    else:
        estado, nivel = "CONFLUENCIA_TOTAL", "max"

    # ── Cálculo de SL/TP básico ────────────────────────────
    price_now = float(df["Close"].iloc[-1])
    atr       = _calc_atr(df, 14)
    sl, tp    = _calc_sl_tp(price_now, direction, atr, sweep, levels)

    return {
        "puntos":          pts,
        "estado":          estado,
        "nivel_alerta":    nivel,
        "direction":       direction,
        "breakdown":       bd,
        "fvg":             fvg_info,
        "patterns":        pattern_info,
        "levels":          levels,
        "sweep":           sweep,
        "divergence":      divergence,
        "session_pts":     session_pts,
        "session_names":   session_names,
        "ticker":          ticker.upper(),
        "sl":              sl,
        "tp":              tp,
        "price":           price_now,
        "filter_confirms": filter_confirms,
        "hod":             levels.get("hod"),
        "lod":             levels.get("lod"),
        "pdh":             levels.get("pdh"),
        "pdl":             levels.get("pdl"),
        "asia_high":       levels.get("asia_high"),
        "asia_low":        levels.get("asia_low"),
        "europe_high":     levels.get("europe_high"),
        "europe_low":      levels.get("europe_low"),
    }


# ─────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────
# 9b. SL / TP BÁSICO
# ─────────────────────────────────────────────────────────────────

def _calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    """Calcula ATR simple."""
    try:
        high_low = df["High"] - df["Low"]
        high_close = (df["High"] - df["Close"].shift()).abs()
        low_close = (df["Low"] - df["Close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return float(tr.tail(period).mean())
    except Exception:
        return 0.0


def _calc_sl_tp(
    price: float,
    direction: str,
    atr: float,
    sweep: dict,
    levels: dict,
) -> tuple[float | None, float | None]:
    """SL bajo el mínimo del estirón; TP al HOD/LOD opuesto o 2x ATR."""
    if not price or price <= 0:
        return None, None

    sl = None
    tp = None

    # SL: debajo del mínimo del estirón (para largos) o encima del máximo (para cortos)
    sweep_low  = sweep.get("sweep_low")
    sweep_high = sweep.get("sweep_high")

    if direction == "bullish":
        if sweep_low:
            sl = sweep_low * 0.998  # 0.2% debajo del mínimo del estirón
        elif atr > 0:
            sl = price - atr
        # TP: HOD del día o 2x ATR
        hod = levels.get("hod")
        if hod and hod > price:
            tp = hod
        elif atr > 0:
            tp = price + 2 * atr
    else:
        if sweep_high:
            sl = sweep_high * 1.002  # 0.2% encima del máximo del estirón
        elif atr > 0:
            sl = price + atr
        # TP: LOD del día o 2x ATR
        lod = levels.get("lod")
        if lod and lod < price:
            tp = lod
        elif atr > 0:
            tp = price - 2 * atr

    return sl, tp


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
