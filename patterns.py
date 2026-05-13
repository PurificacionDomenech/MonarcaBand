# patterns.py — Detección de patrones de precio: M, W, HCH, HCH Invertido
# Cada patrón incluye puntos clave, neckline, info RSI y estado (formando/confirmado)

import pandas as pd
import numpy as np


def _pivot_high_bars(s: pd.Series, lb: int) -> list:
    n = len(s)
    bars = []
    for i in range(lb, n - lb):
        val = float(s.iloc[i])
        if all(float(s.iloc[k]) <= val for k in range(i - lb, i)) and \
           all(float(s.iloc[k]) <= val for k in range(i + 1, i + lb + 1)):
            bars.append(i)
    return bars


def _pivot_low_bars(s: pd.Series, lb: int) -> list:
    n = len(s)
    bars = []
    for i in range(lb, n - lb):
        val = float(s.iloc[i])
        if all(float(s.iloc[k]) >= val for k in range(i - lb, i)) and \
           all(float(s.iloc[k]) >= val for k in range(i + 1, i + lb + 1)):
            bars.append(i)
    return bars


def _ts(times: list, bar: int) -> int:
    return int(times[bar].timestamp() * 1000)


def _rsi_at(rsi: pd.Series, bar: int):
    if bar < 0 or bar >= len(rsi):
        return None
    v = rsi.iloc[bar]
    return None if pd.isna(v) else round(float(v), 2)


# ── PATRÓN M (Doble Techo) ─────────────────────────────────────────────────────

def detect_double_top(df: pd.DataFrame, rsi: pd.Series, times: list,
                      lb: int = 5, min_gap: int = 8, max_gap: int = 100,
                      price_tol: float = 0.015, rsi_min_diff: float = 1.5) -> list:
    highs = df["high"]
    lows  = df["low"]
    ph_bars = _pivot_high_bars(highs, lb)
    patterns = []

    for i in range(len(ph_bars) - 1):
        b1 = ph_bars[i]
        for j in range(i + 1, len(ph_bars)):
            b2 = ph_bars[j]
            gap = b2 - b1
            if gap < min_gap: continue
            if gap > max_gap: break

            p1 = float(highs.iloc[b1])
            p2 = float(highs.iloc[b2])
            if abs(p2 - p1) / p1 > price_tol:
                continue

            r1 = _rsi_at(rsi, b1)
            r2 = _rsi_at(rsi, b2)
            if r1 is None or r2 is None:
                continue

            # Buscar el valle entre los dos picos
            valley_slice = lows.iloc[b1:b2 + 1]
            valley_rel   = int(np.argmin(valley_slice.values))
            valley_bar   = b1 + valley_rel
            neckline_y   = float(valley_slice.min())

            # El valle debe ser real (al menos 0.5% bajo el mínimo de los picos)
            if neckline_y > min(p1, p2) * 0.995:
                continue

            has_rsi_div = r2 < r1 - rsi_min_diff
            in_zone     = r2 >= 55

            # Confirmación: precio rompe neckline después de P2
            confirmed = False
            if b2 + 1 < len(lows):
                if float(lows.iloc[b2 + 1:].min()) < neckline_y:
                    confirmed = True

            rsi_diff   = max(0.0, r1 - r2)
            price_sim  = max(0.0, 1.0 - abs(p2 - p1) / p1 / price_tol)
            confidence = round(min(1.0,
                rsi_diff / 12.0 * 0.35 +
                price_sim * 0.30 +
                (0.20 if confirmed else 0.0) +
                (0.10 if in_zone else 0.0) +
                (0.05 if has_rsi_div else 0.0)
            ), 2)

            notes = f"P2 igual precio · RSI {'más bajo' if has_rsi_div else 'sin div'} · {'confirmado' if confirmed else 'formando'}"

            patterns.append({
                "type":       "M",
                "direction":  "bearish",
                "status":     "confirmed" if confirmed else "forming",
                "confidence": confidence,
                "rsi_div":    has_rsi_div,
                "in_zone":    in_zone,
                "notes":      notes,
                "points": [
                    {"label": "P1", "x": _ts(times, b1),       "y": round(p1, 6),         "rsi": r1},
                    {"label": "P2", "x": _ts(times, b2),       "y": round(p2, 6),         "rsi": r2},
                ],
                "mid": {"x": _ts(times, valley_bar), "y": round(neckline_y, 6)},
                "neckline": {
                    "x1": _ts(times, b1), "x2": _ts(times, b2),
                    "y1": round(neckline_y, 6), "y2": round(neckline_y, 6),
                },
                "bar1": b1, "bar2": b2,
            })

    patterns.sort(key=lambda x: x["bar2"], reverse=True)
    return patterns[:3]


# ── PATRÓN W (Doble Suelo) ─────────────────────────────────────────────────────

def detect_double_bottom(df: pd.DataFrame, rsi: pd.Series, times: list,
                         lb: int = 5, min_gap: int = 8, max_gap: int = 100,
                         price_tol: float = 0.015, rsi_min_diff: float = 1.5) -> list:
    lows  = df["low"]
    highs = df["high"]
    pl_bars = _pivot_low_bars(lows, lb)
    patterns = []

    for i in range(len(pl_bars) - 1):
        b1 = pl_bars[i]
        for j in range(i + 1, len(pl_bars)):
            b2 = pl_bars[j]
            gap = b2 - b1
            if gap < min_gap: continue
            if gap > max_gap: break

            v1 = float(lows.iloc[b1])
            v2 = float(lows.iloc[b2])
            if abs(v2 - v1) / v1 > price_tol:
                continue

            r1 = _rsi_at(rsi, b1)
            r2 = _rsi_at(rsi, b2)
            if r1 is None or r2 is None:
                continue

            peak_slice  = highs.iloc[b1:b2 + 1]
            peak_rel    = int(np.argmax(peak_slice.values))
            peak_bar    = b1 + peak_rel
            neckline_y  = float(peak_slice.max())

            if neckline_y < max(v1, v2) * 1.005:
                continue

            has_rsi_div = r2 > r1 + rsi_min_diff
            in_zone     = r2 <= 45

            confirmed = False
            if b2 + 1 < len(highs):
                if float(highs.iloc[b2 + 1:].max()) > neckline_y:
                    confirmed = True

            rsi_diff   = max(0.0, r2 - r1)
            price_sim  = max(0.0, 1.0 - abs(v2 - v1) / v1 / price_tol)
            confidence = round(min(1.0,
                rsi_diff / 12.0 * 0.35 +
                price_sim * 0.30 +
                (0.20 if confirmed else 0.0) +
                (0.10 if in_zone else 0.0) +
                (0.05 if has_rsi_div else 0.0)
            ), 2)

            notes = f"V2 igual precio · RSI {'más alto' if has_rsi_div else 'sin div'} · {'confirmado' if confirmed else 'formando'}"

            patterns.append({
                "type":       "W",
                "direction":  "bullish",
                "status":     "confirmed" if confirmed else "forming",
                "confidence": confidence,
                "rsi_div":    has_rsi_div,
                "in_zone":    in_zone,
                "notes":      notes,
                "points": [
                    {"label": "V1", "x": _ts(times, b1),      "y": round(v1, 6),        "rsi": r1},
                    {"label": "V2", "x": _ts(times, b2),      "y": round(v2, 6),        "rsi": r2},
                ],
                "mid": {"x": _ts(times, peak_bar), "y": round(neckline_y, 6)},
                "neckline": {
                    "x1": _ts(times, b1), "x2": _ts(times, b2),
                    "y1": round(neckline_y, 6), "y2": round(neckline_y, 6),
                },
                "bar1": b1, "bar2": b2,
            })

    patterns.sort(key=lambda x: x["bar2"], reverse=True)
    return patterns[:3]


# ── PATRÓN HCH (Hombro-Cabeza-Hombro Bajista) ─────────────────────────────────

def detect_hch(df: pd.DataFrame, rsi: pd.Series, times: list,
               lb: int = 5, min_gap: int = 8, max_gap: int = 90,
               shoulder_tol: float = 0.04) -> list:
    highs = df["high"]
    lows  = df["low"]
    ph_bars = _pivot_high_bars(highs, lb)
    patterns = []

    for i in range(len(ph_bars) - 2):
        bLS   = ph_bars[i]
        bHead = ph_bars[i + 1]
        bRS   = ph_bars[i + 2]

        if bHead - bLS < min_gap or bRS - bHead < min_gap: continue
        if bHead - bLS > max_gap or bRS - bHead > max_gap: continue

        pLS   = float(highs.iloc[bLS])
        pHead = float(highs.iloc[bHead])
        pRS   = float(highs.iloc[bRS])

        if pHead <= pLS or pHead <= pRS:
            continue
        if abs(pRS - pLS) / pLS > shoulder_tol:
            continue

        v1_slice = lows.iloc[bLS:bHead + 1]
        v2_slice = lows.iloc[bHead:bRS + 1]
        bV1 = bLS   + int(np.argmin(v1_slice.values))
        bV2 = bHead + int(np.argmin(v2_slice.values))
        nk_y1 = float(v1_slice.min())
        nk_y2 = float(v2_slice.min())

        rLS   = _rsi_at(rsi, bLS)
        rHead = _rsi_at(rsi, bHead)
        rRS   = _rsi_at(rsi, bRS)
        if rLS is None or rRS is None:
            continue

        has_rsi_div = rRS < rLS - 1.5 if rRS and rLS else False
        in_zone     = (rRS >= 55) if rRS else False

        neckline_avg = (nk_y1 + nk_y2) / 2
        confirmed = False
        if bRS + 1 < len(lows):
            if float(lows.iloc[bRS + 1:].min()) < neckline_avg:
                confirmed = True

        shoulder_sym = 1.0 - abs(pRS - pLS) / pLS / shoulder_tol
        confidence = round(min(1.0,
            (0.30 if has_rsi_div else 0.0) +
            (0.25 if confirmed else 0.0) +
            (0.20 if in_zone else 0.0) +
            shoulder_sym * 0.25
        ), 2)

        notes = f"HD {'menor' if pRS < pLS else 'similar'} a HI · RSI {'div bajista' if has_rsi_div else 'sin div'} · {'confirmado' if confirmed else 'formando'}"

        patterns.append({
            "type":       "HCH",
            "direction":  "bearish",
            "status":     "confirmed" if confirmed else "forming",
            "confidence": confidence,
            "rsi_div":    has_rsi_div,
            "in_zone":    in_zone,
            "notes":      notes,
            "points": [
                {"label": "HI",  "x": _ts(times, bLS),   "y": round(pLS, 6),   "rsi": rLS},
                {"label": "CAB", "x": _ts(times, bHead),  "y": round(pHead, 6), "rsi": rHead},
                {"label": "HD",  "x": _ts(times, bRS),    "y": round(pRS, 6),   "rsi": rRS},
            ],
            "mid": None,
            "neckline": {
                "x1": _ts(times, bV1), "x2": _ts(times, bV2),
                "y1": round(nk_y1, 6), "y2": round(nk_y2, 6),
            },
            "bar1": bLS, "bar2": bRS,
        })

    patterns.sort(key=lambda x: x["bar2"], reverse=True)
    return patterns[:2]


# ── PATRÓN HCH INVERTIDO (Alcista) ────────────────────────────────────────────

def detect_hch_inv(df: pd.DataFrame, rsi: pd.Series, times: list,
                   lb: int = 5, min_gap: int = 8, max_gap: int = 90,
                   shoulder_tol: float = 0.04) -> list:
    highs = df["high"]
    lows  = df["low"]
    pl_bars = _pivot_low_bars(lows, lb)
    patterns = []

    for i in range(len(pl_bars) - 2):
        bLS   = pl_bars[i]
        bHead = pl_bars[i + 1]
        bRS   = pl_bars[i + 2]

        if bHead - bLS < min_gap or bRS - bHead < min_gap: continue
        if bHead - bLS > max_gap or bRS - bHead > max_gap: continue

        pLS   = float(lows.iloc[bLS])
        pHead = float(lows.iloc[bHead])
        pRS   = float(lows.iloc[bRS])

        if pHead >= pLS or pHead >= pRS:
            continue
        if abs(pRS - pLS) / pLS > shoulder_tol:
            continue

        pk1_slice = highs.iloc[bLS:bHead + 1]
        pk2_slice = highs.iloc[bHead:bRS + 1]
        bP1 = bLS   + int(np.argmax(pk1_slice.values))
        bP2 = bHead + int(np.argmax(pk2_slice.values))
        nk_y1 = float(pk1_slice.max())
        nk_y2 = float(pk2_slice.max())

        rLS   = _rsi_at(rsi, bLS)
        rHead = _rsi_at(rsi, bHead)
        rRS   = _rsi_at(rsi, bRS)
        if rLS is None or rRS is None:
            continue

        has_rsi_div = rRS > rLS + 1.5 if rRS and rLS else False
        in_zone     = (rRS <= 45) if rRS else False

        neckline_avg = (nk_y1 + nk_y2) / 2
        confirmed = False
        if bRS + 1 < len(highs):
            if float(highs.iloc[bRS + 1:].max()) > neckline_avg:
                confirmed = True

        shoulder_sym = 1.0 - abs(pRS - pLS) / pLS / shoulder_tol
        confidence = round(min(1.0,
            (0.30 if has_rsi_div else 0.0) +
            (0.25 if confirmed else 0.0) +
            (0.20 if in_zone else 0.0) +
            shoulder_sym * 0.25
        ), 2)

        notes = f"HD {'mayor' if pRS > pLS else 'similar'} a HI · RSI {'div alcista' if has_rsi_div else 'sin div'} · {'confirmado' if confirmed else 'formando'}"

        patterns.append({
            "type":       "HCH_INV",
            "direction":  "bullish",
            "status":     "confirmed" if confirmed else "forming",
            "confidence": confidence,
            "rsi_div":    has_rsi_div,
            "in_zone":    in_zone,
            "notes":      notes,
            "points": [
                {"label": "HI",  "x": _ts(times, bLS),   "y": round(pLS, 6),   "rsi": rLS},
                {"label": "CAB", "x": _ts(times, bHead),  "y": round(pHead, 6), "rsi": rHead},
                {"label": "HD",  "x": _ts(times, bRS),    "y": round(pRS, 6),   "rsi": rRS},
            ],
            "mid": None,
            "neckline": {
                "x1": _ts(times, bP1), "x2": _ts(times, bP2),
                "y1": round(nk_y1, 6), "y2": round(nk_y2, 6),
            },
            "bar1": bLS, "bar2": bRS,
        })

    patterns.sort(key=lambda x: x["bar2"], reverse=True)
    return patterns[:2]


# ── ENTRADA PRINCIPAL ──────────────────────────────────────────────────────────

def detect_all_patterns(df: pd.DataFrame, rsi: pd.Series, times: list) -> list:
    all_pats = []
    try: all_pats.extend(detect_double_top(df, rsi, times))
    except Exception: pass
    try: all_pats.extend(detect_double_bottom(df, rsi, times))
    except Exception: pass
    try: all_pats.extend(detect_hch(df, rsi, times))
    except Exception: pass
    try: all_pats.extend(detect_hch_inv(df, rsi, times))
    except Exception: pass
    all_pats.sort(key=lambda x: x["bar2"], reverse=True)
    return all_pats
