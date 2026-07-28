---
name: Deduplication by exact candle timestamp
description: Prevent re-alerting on same candle with different rounding
---

**Why:** Old dedup used `f"{t}_{estado}_{round(precio, -1)}"` which allowed the same candle to alert multiple times with slightly different prices. Also 60-minute freshness filter was too short — scheduler every 30 min could miss candles.

**Fix:**
- Deduplication key: `f"VELA_{t}_{ts_utc_iso}"` — exact candle timestamp
- Freshness filter: 90 minutes (covers 3 x 1H candles between scheduler runs)
- `_sent_cache` stores timestamp, checked with `if vela_key not in _sent_cache`

**How to apply:** Every alert producer (normal confluences, RSI real-time, and divergence alerts) must claim the shared `ticker + exact UTC candle timestamp` key before sending; never use price, status, direction, or current wall-clock time as the primary dedup key.
