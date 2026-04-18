# Trading Band

Financial market dashboard with real-time technical analysis built with FastAPI.

## Project: Trading Band

The app has been renamed from "The Matrix Lab" to **Trading Band**. Same structure, indicator changed:

### What changed
- **Indicator**: Fractales replaced by **MonarcaBand** (SMA12 Trigger + SMA12 Average of Trigger)
- **Name**: "The Matrix Lab" → "Trading Band" (Splash.html + index.html)
- **Logo**: Butterfly image (`/static/trading_band_logo.png`)
- **Chart**: Shows Trigger SMA12 (amber line) + Average SMA12 (dotted amber) + cross arrows
- **Sidebar**: MONARCABAND panel with Trigger/Average values and ALCISTA/BAJISTA signal
- **Table**: "◈ BANDA" column instead of "⬡ FRACTAL" with bull/bear signal per asset
- **Buttons**: "◈ Banda" + "↕ Cruces" instead of Fractales/Zonas/Toques

### What stayed the same
- All assets (USDJPY, GBPJPY, EURUSD, AUDUSD, GC=F, SI=F, CL=F, ^DJI, ^NDX + hidden extras)
- EMA 200/800 on chart
- RSI 14 panel
- Telegram + Email notification system
- Scheduler (every 30 min)
- Supabase authentication
- Watchlist (★ vigilancia)
- All route structure: `/` → Splash login, `/app` → dashboard

## Overview

"Trading Band" is a web application that provides:
- Real-time market data for stocks, indices, forex, and commodities via yfinance
- Technical analysis: EMA 200/800, RSI, fractal levels
- Automatic alerts via Telegram and email
- A scheduler that runs market checks every 4 hours
- A Telegram bot integration for subscribing to alerts

## Stack

- **Backend**: FastAPI (Python 3.12), uvicorn
- **Data**: yfinance, pandas, numpy
- **Charts**: Plotly.js (client-side)
- **Notifications**: Telegram Bot API, SMTP email
- **Database**: Supabase (for telegram subs and user notification prefs)
- **Scheduler**: APScheduler (AsyncIOScheduler)

## Project Structure

```
main.py              # FastAPI app, API routes, scheduler
notifier.py          # Telegram and email notification logic
app.py               # Legacy tkinter GUI (not used in web mode)
graficos/chart_tv.py # Chart utilities
indicadores/etf.py   # ETF indicator utilities
templates/
  Splash.html        # Landing page
  index.html         # Main dashboard
static/              # Static files (logo, etc.)
```

## Running the App

The app runs via uvicorn on port 5000:
```
uvicorn main:app --host 0.0.0.0 --port 5000
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TELEGRAM_TOKEN` | Optional | Telegram bot token for alerts |
| `SUPABASE_URL` | Optional | Supabase project URL |
| `SUPABASE_KEY` | Optional | Supabase service role key |
| `MAIL_FROM` | Optional | Gmail address for email alerts |
| `MAIL_PASSWORD` | Optional | Gmail App Password |
| `MAIL_TO` | Optional | Default email recipient |
| `MAIL_SMTP` | Optional | SMTP host (default: smtp.gmail.com) |
| `MAIL_PORT` | Optional | SMTP port (default: 587) |

The app runs without any environment variables set — notifications are simply disabled if credentials are missing.

## Confluence Matrix (evaluate_confluencias)

The system evaluates up to 6 confluences with **directional validation**:

| # | Confluence | Direction |
|---|---|---|
| ① | RSI < 47 / > 53 | bullish (comprar barato) / bearish (vender caro) |
| ② | EMA200 vs EMA800 | bullish (EMA200 > EMA800) / bearish |
| ③ | Fractal touch | soporte → bullish / resistencia → bearish |
| ④ | Day + Week open | both above → bullish / both below → bearish |
| ⑤ | Fibonacci 55.9% | neutral (valid for both directions) |
| ⑥ | Index components (^DJI/^NDX only) | ≥60% bullish/bearish |

**Directional rules:**
- Strong signals (①②) determine direction; if they conflict → CONTRADICCIÓN
- Weak/secondary signals (③④⑥) in opposite direction are **descartada** (discarded), not counted
- Neutral signals (⑤) count regardless of direction
- FAVORABLE: ≥4 points aligned | INTERESANTE: 3 | CONSIDERAR: 2 | NO AHORA: ≤1

**Schema `notification_prefs`**: `user_id, telegram_chat_id, telegram_enabled, email_address, email_enabled, tickers, timezone, created_at, id` — NO `language` column.

## Deployment

Configured for autoscale deployment using gunicorn with UvicornWorker on port 5000.
