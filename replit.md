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

### Nuevas funcionalidades (May 2026)
- **RSI 14 con divergencias**: Detección de divergencias alcistas/bajistas en 3 fases (formándose/cruzada/excedida) con precisión tipo TradingView (RMA smoothing)
- **Shark Fin (Aleta de Tiburón)**: Indicador de agotamiento extremo post-divergencia — detecta picos/valles en zonas RSI >70/<30 y compara con pivotes originales de la divergencia
- **Columna "🦈 SHARK" en tabla**: Muestra estado del Shark Fin por activo (EXTREMA / CONFIRMADA / FORMÁNDOSE)
- **Bitcoin visible por defecto**: BTC-USD ahora aparece en el grid principal (no hidden)
- **Confluencia ⑦**: Shark Fin integrado en la matriz de confluencias con validación direccional (+2 cruzada, +4 excedida)
- **Alertas inmediatas**: El sistema alerta vía Telegram cuando Shark Fin cruza o excede
- **Deduplicación**: Claves `SHARK_{ticker}_{tipo}_{phase}` con caché 1h para evitar spam

### What stayed the same
- All assets (USDJPY, GBPJPY, EURUSD, AUDUSD, GC=F, SI=F, CL=F, ^DJI, ^NDX + **BTC-USD** + hidden extras)
- EMA 50/200 on chart
- Telegram + Email notification system
- Scheduler (every 30 min + TB confluences cada 5 min + RSI real-time cada 2 min)
- Supabase authentication (login opcional, dashboard funciona en modo anónimo)
- Watchlist (★ vigilancia)
- Route structure: `/` → Splash, `/app` → dashboard (sin redirect forzado)

## Overview

"Trading Band" is a web application that provides:
- Real-time market data for stocks, indices, forex, commodities, and crypto via yfinance
- Technical analysis: EMA 50/200, RSI 14 with divergences, MonarcaBand, Shark Fin exhaustion
- Automatic alerts via Telegram and email
- A scheduler that runs market checks every 30 min + TB confluences every 5 min + RSI real-time every 2 min
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

| # | Confluence | Direction | Points |
|---|---|---|---|
| ① | RSI <30 / >70 (extremo) | bullish (sobreventa máxima) / bearish (sobrecompra máxima) | +2 |
| ① | RSI 30-44 / 56-70 (interés) | bullish (zona de interés) / bearish (zona de interés) | +1 |
| ① | RSI 45-55 (neutro) | tierra de nadie | 0 |
| ② | Divergencia RSI | bullish / bearish según tipo (N1 +1, N2 +2, N3 +3) | +1 a +3 |
| ③ | Vacío FVG activo cerca del precio | bullish / bearish según dirección | +1 |
| ④ | Soportes (HOD/LOD/PDH/PDL/Weekly) | bullish / bearish según dirección | +1 por toque |
| ⑤ | **Shark Fin** | bullish (agotamiento bajista) / bearish (agotamiento alcista) | +2 crossed / +4 exceeded |

**Directional rules:**
- Strong signals (② Divergencias + ⑤ Shark Fin) determine direction; if they conflict → CONTRADICCIÓN
- RSI (①) es de contexto (zona) — nunca determina dirección por sí solo
- FVG (③) y Soportes (④) son confirmaciones direccionales
- Patrones M/W/HCH **eliminados** — creaban contradicciones falsas
- Shark Fin (⑤) solo cuenta cuando aligned; +2 crossed, +4 exceeded
- FAVORABLE: ≥7 puntos (alerta) | INTERESANTE: 5-6 | CONSIDERAR: 3-4 | NO AHORA: ≤2
- Alertas indican si la vela tocó máximo/mínimo de la sesión asiática (00-08 UTC) o europea (08-16 UTC)

**Schema `notification_prefs`**: `user_id, telegram_chat_id, telegram_enabled, email_address, email_enabled, tickers, timezone, created_at, id` — NO `language` column.

## Deployment

Configured for autoscale deployment using gunicorn with UvicornWorker on port 5000.
