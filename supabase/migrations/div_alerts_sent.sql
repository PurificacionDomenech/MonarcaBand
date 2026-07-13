-- Table: div_alerts_sent
-- Purpose: Persist divergence alert dedup keys so the in-memory _div_cache
--          survives server restarts (deployed or crashed).
--
-- Run this once in your Supabase SQL editor.

create table if not exists public.div_alerts_sent (
    cache_key  text        primary key,   -- "{ticker}_{type}_{level}_{time}"
    ticker     text        not null,
    type       text        not null,      -- "bullish" | "bearish"
    level      integer     not null,      -- 1 | 2 | 3
    "time"     text        not null,      -- candle timestamp as string
    sent_at    timestamptz not null default now()
);

-- Index for the time-ranged queries used on startup and cleanup
create index if not exists div_alerts_sent_sent_at_idx
    on public.div_alerts_sent (sent_at);

-- Allow the service-role key used by the notifier to read/write/delete
alter table public.div_alerts_sent enable row level security;

create policy "service role full access"
    on public.div_alerts_sent
    for all
    using (true)
    with check (true);
