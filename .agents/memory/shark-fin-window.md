---
name: Shark Fin 24-candle window
description: Limit Shark Fin calculation to prevent mixing historical signals
---

**Why:** Shark Fin was calculated on the full DataFrame, finding divergence peaks from 8+ hours ago when evaluating a late afternoon candle. This caused alerts with mixed data: RSI 40.9 (current) + Shark Fin peak 70.3 (morning) + HOD from morning.

**Fix:** `df_shark = df.iloc[-min(24, len(df)):].copy()` — only last 24 candles (24h on 1H charts).

**How to apply:** Always pass a sliced DataFrame to `_calc_shark_fin()` in `evaluate_confluencias`. Never the full historical DataFrame.
