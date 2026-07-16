---
name: Alert accuracy strictness
description: Rules for confluence detectors to avoid false positives
---

**Why:** AUD/USD alert showed 8 pts FAVORABLE when price was mid-range (0.7005), RSI 56.7 (neutral), divergence at RSI 49.6 (not extreme), FVG not touched, LOD not touched by that candle. All detectors were too permissive.

**Rules (each detector must enforce):**
1. **Divergencias**: Only count if RSI at recent pivot is <35 (bullish) or >65 (bearish). HBULL/HBEAR require the same zone check. "Formándose" divergences are ignored.
2. **FVG**: Price must be within ±0.5% of the FVG range to count. "touched=True, frozen=False" is necessary but not sufficient — proximity check required.
3. **HOD/LOD**: Only the candle being evaluated (bar_high / bar_low) must actually touch the day high/low. Using "price <= day_low * 1.003" on the close is wrong — a candle can close near the low without having touched it.
4. **Patrones M/W/HCH**: Sólo "confirmado" counts. "formando_p2", "formando_v2", "formando_hd" do NOT score. Only confirmed neckline break = real pattern.
5. **RSI context**: RSI <47 / >53 is zone context (1 pt), never determines direction. Direction comes from: Divergencias (2) + Patrones (4) + Shark Fin (7).

**How to apply:** When modifying any detector, apply these strictness checks before calling raw.append() with ok=True.
