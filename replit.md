# The Matrix Lab + Trading Band

A multi-project financial market dashboard with real-time technical analysis built with FastAPI.

## Projects

### The Matrix Lab (/)
Original confluence analysis engine with EMA 200/800, RSI, fractal levels, and automated alerts.

### Trading Band (/trading-band)
New project using MonarcaBand (SMA12 Trigger + SMA12 Average) + RSI with 3-level divergences.
- **Dashboard**: `/trading-band/app`
- **API**: `/api/tradingband/signal/{ticker}`
- **Alerts**: trend-change arrows, RSI overbought/oversold, RSI divergences in key zones
- **Logo**: butterfly image (static/trading_band_logo.png)
- **Assets**: US30, NASDAQ, S&P500, Gold, Silver, Oil, USD/JPY, GBP/JPY, EUR/USD, Bitcoin
- **Files**: `trading_band_routes.py`, `templates/trading_band.html`, `templates/trading_band_splash.html`

## Overview

"The Matrix Lab" is a web application that provides:
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
