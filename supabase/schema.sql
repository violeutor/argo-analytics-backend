-- ============================================================
-- Argo Analytics – Supabase Schema
-- Version: 0.1.0
-- Standort: EU (Frankfurt / Ireland)
-- RLS: off (Phase 1) — wird in Phase 2 mit Auth aktiviert
-- "automatically expose new tables": off
-- ============================================================


-- ── Extensions ───────────────────────────────────────────────────────────────

create extension if not exists "pgcrypto";   -- für gen_random_uuid()
create extension if not exists "pg_trgm";    -- für fuzzy search auf company names


-- ── Enums ────────────────────────────────────────────────────────────────────

create type target_stage as enum (
  'seed',
  'series_a',
  'series_b',
  'series_c',
  'series_d_plus',
  'public'
);

create type srr_category as enum (
  'Low Strategic',
  'High Strategic',
  'Transformational',
  'Transformational++'
);

create type mfr_signal as enum (
  'Feasible',
  'Watch',
  'Overstretch'
);

create type cap_segment as enum (
  'low',
  'mid',
  'high'
);

create type deal_rating as enum (
  'A · No-Brainer',
  'B · Solide',
  'C · Abwägen',
  'D · Uninteressant'
);

create type deal_quadrant as enum (
  'HighPotential_LowRisk',
  'HighPotential_HighRisk',
  'LowPotential_LowRisk',
  'LowPotential_HighRisk'
);

create type investment_path as enum (
  'IPO-direkt',
  'Käufer-Proxy',
  'ETF-Proxy',
  'Enabler',
  'Beobachten',
  'Archiv'
);

create type company_source as enum (
  'bestand',
  'woche1',
  'woche2',
  'manual'
);


-- ── 1. companies ─────────────────────────────────────────────────────────────
-- Master-Tabelle aller Targets (aus Climate-Tech-Matrix + manuell)

create table companies (
  id              uuid primary key default gen_random_uuid(),
  name            text not null,
  website         text,
  category        text,                        -- z.B. "Low-Carbon Concrete"
  core_technology text,                        -- Kurzbeschreibung
  tags            text[]  default '{}',        -- für Mapping-Engine
  summary         text,
  source          company_source default 'manual',

  -- Potenzial & Risiko (aus Matrix)
  potential       text,                        -- "Hoch" / "Mittel-hoch" / "Mittel"
  risk            text,                        -- "Hoch" / "Mittel"
  ipo_potential   text,

  -- Funding
  funding_total_usd_mn  numeric,
  funding_stage         target_stage,
  funding_last_round    text,                  -- Freitext, z.B. "$80 Mio. Series F (Jul 2023)"
  funding_year          smallint,

  -- Investitionspfad
  investment_path       investment_path,
  proxy_ticker          text,                  -- z.B. "CRH · NYSE"
  strategic_fit_notes   text,
  last_signal           text,                  -- "Letzte Entwicklung" aus Morning Briefing
  last_signal_date      date,

  created_at      timestamptz default now(),
  updated_at      timestamptz default now()
);

create index idx_companies_name_trgm on companies using gin (name gin_trgm_ops);
create index idx_companies_source    on companies (source);
create index idx_companies_path      on companies (investment_path);


-- ── 2. buyers ────────────────────────────────────────────────────────────────
-- Börsennotierte Käufer / Proxies

create table buyers (
  id                    uuid primary key default gen_random_uuid(),
  name                  text not null,
  ticker                text,                  -- z.B. "CRH"
  exchange              text,                  -- z.B. "NYSE"
  market_cap_usd_bn     numeric,
  cash_usd_bn           numeric,
  debt_ebitda           numeric,
  sector                text,
  notes                 text,

  created_at            timestamptz default now(),
  updated_at            timestamptz default now()
);

create index idx_buyers_ticker on buyers (ticker);


-- ── 3. deals ─────────────────────────────────────────────────────────────────
-- Eine Analyse = ein Deal (Target + Buyer + Inputs)

create table deals (
  id                      uuid primary key default gen_random_uuid(),
  company_id              uuid references companies (id) on delete cascade,
  buyer_id                uuid references buyers (id) on delete set null,

  -- Inputs (Snapshot zum Zeitpunkt der Analyse)
  tam_usd_bn              numeric not null,
  buyer_market_cap_usd_bn numeric not null,
  buyer_cash_usd_bn       numeric default 0,
  buyer_debt_ebitda       numeric default 0,
  target_funding_usd_mn   numeric default 0,
  target_stage            target_stage default 'series_b',
  url                     text,

  -- TechReadiness Inputs (7 Faktoren, 0–1)
  tr_tech_stack_fit            numeric check (tr_tech_stack_fit between 0 and 1),
  tr_gtm_fit                   numeric check (tr_gtm_fit between 0 and 1),
  tr_integration_capacity      numeric check (tr_integration_capacity between 0 and 1),
  tr_rd_intensity              numeric check (tr_rd_intensity between 0 and 1),
  tr_capital_deployment_velocity numeric check (tr_capital_deployment_velocity between 0 and 1),
  tr_regulatory_readiness      numeric check (tr_regulatory_readiness between 0 and 1),
  tr_strategic_coherence       numeric check (tr_strategic_coherence between 0 and 1),

  created_at              timestamptz default now()
);

create index idx_deals_company on deals (company_id);
create index idx_deals_buyer   on deals (buyer_id);
create index idx_deals_created on deals (created_at desc);


-- ── 4. scores ────────────────────────────────────────────────────────────────
-- Scoring-Ergebnis je Deal (1:1 zu deals)

create table scores (
  id                  uuid primary key default gen_random_uuid(),
  deal_id             uuid not null unique references deals (id) on delete cascade,

  -- SRR
  srr_value           numeric not null,
  srr_category        srr_category not null,
  srr_cap_segment     cap_segment not null,
  srr_execution_warning boolean default false,

  -- MFR
  mfr_value           numeric not null,
  mfr_signal          mfr_signal not null,

  -- TechReadiness
  tr_value            numeric not null,        -- 0–1 gewichteter Score
  tr_inputs_provided  boolean default false,   -- false = neutral fallback

  -- DealSuccessScore
  deal_success_score  numeric not null,        -- 0–1

  -- Rating & Quadrant
  rating              deal_rating not null,
  quadrant            deal_quadrant not null,

  -- Vollständige Executive Summary
  executive_summary   text,
  warnings            text[],                  -- Array von Warning-Strings

  scored_at           timestamptz default now()
);

create index idx_scores_deal_id on scores (deal_id);
create index idx_scores_rating  on scores (rating);


-- ── 5. proxies ───────────────────────────────────────────────────────────────
-- Market Exposure: welche börsennotierten Titel profitieren von einem Target

create table proxies (
  id              uuid primary key default gen_random_uuid(),
  company_id      uuid references companies (id) on delete cascade,
  buyer_id        uuid references buyers (id) on delete cascade,

  proxy_type      text not null,               -- "equity" | "etf" | "fund"
  ticker          text,
  name            text,
  exchange        text,
  relevance_score numeric check (relevance_score between 0 and 1),
  exposure_score  numeric check (exposure_score between 0 and 1),
  impact_score    numeric check (impact_score between 0 and 1),
  justification   text,

  created_at      timestamptz default now()
);

create index idx_proxies_company on proxies (company_id);


-- ── 6. signals ───────────────────────────────────────────────────────────────
-- Morning Briefing Signale (Delta-Blöcke aus Modul 02)

create table signals (
  id              uuid primary key default gen_random_uuid(),
  company_id      uuid references companies (id) on delete cascade,

  signal_text     text not null,
  signal_date     date not null,
  source          text,                        -- z.B. "BNEF Pioneer 2026", "Morning Briefing"
  signal_type     text default 'routine',      -- "ipo_change" | "ma_change" | "routine"

  created_at      timestamptz default now()
);

create index idx_signals_company on signals (company_id);
create index idx_signals_date    on signals (signal_date desc);


-- ── 7. watchlist ─────────────────────────────────────────────────────────────
-- User-spezifische Watchlist (vorbereitet für Auth in Phase 2)

create table watchlist (
  id              uuid primary key default gen_random_uuid(),
  user_id         uuid,                        -- später: references auth.users(id)
  company_id      uuid references companies (id) on delete cascade,
  notes           text,
  added_at        timestamptz default now(),
  unique (user_id, company_id)
);

create index idx_watchlist_user    on watchlist (user_id);
create index idx_watchlist_company on watchlist (company_id);


-- ── Triggers: updated_at ─────────────────────────────────────────────────────

create or replace function update_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create trigger trg_companies_updated_at
  before update on companies
  for each row execute function update_updated_at();

create trigger trg_buyers_updated_at
  before update on buyers
  for each row execute function update_updated_at();
