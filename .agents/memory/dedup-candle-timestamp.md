---
name: Deduplication by exact candle timestamp
description: Prevent re-alerting on same candle with different rounding
---

**Why:** Old dedup used `f"{t}_{estado}_{round(precio, -1)}"` which allowed the same candle to alert multiple times with slightly different prices. Also 60-minute freshness filter was too short — scheduler every 30 min could miss candles.

**Fix:**
- Deduplication key: `f"VELA_{t}_{ts_utc_iso}"` — exact candle timestamp
- Freshness filter: 90 minutes (covers 3 x 1H candles between scheduler runs)
- `_sent_cache` stores timestamp, checked with `if vela_key not in _sent_cache`

**How to apply:** In `_check_tickers`, always use the exact candle's `ts_utc_iso` as dedup key, never price-based keys.
