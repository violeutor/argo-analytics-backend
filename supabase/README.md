# Argo Analytics – Infra / Supabase Setup

## Reihenfolge beim ersten Setup

```
1. schema.sql  ausführen  → Tabellen, Enums, Indizes, Trigger
2. seed.sql    ausführen  → 43 Companies + 8 Buyers aus Climate-Tech-Matrix
3. policies.sql           → noch nicht ausführen (Phase 2 / Auth)
```

## In Supabase ausführen

Supabase Dashboard → SQL Editor → New Query → Inhalt einfügen → Run

## Einstellungen (bereits gesetzt)

| Setting | Wert |
|---|---|
| Region | EU (Frankfurt / Ireland) |
| Data API | aktiv |
| Automatically expose new tables | off |
| RLS | off (Phase 1) |

## Schema-Übersicht

| Tabelle | Beschreibung |
|---|---|
| `companies` | 43 Climate-Tech-Targets (Matrix-Daten) |
| `buyers` | Börsennotierte Käufer / Proxies |
| `deals` | Analyse-Requests (Target + Buyer + Inputs) |
| `scores` | SRR × MFR × TechReadiness Ergebnisse (1:1 zu deals) |
| `proxies` | Market Exposure: Ticker / ETF / Fonds je Target |
| `signals` | Morning Briefing Delta-Signale |
| `watchlist` | User-Watchlist (vorbereitet für Auth Phase 2) |

## Phase 2: RLS aktivieren

Wenn Supabase Auth integriert ist:
1. `policies.sql` einkommentieren und ausführen
2. `deals`-Tabelle um `user_id uuid references auth.users(id)` erweitern
3. CORS in FastAPI auf produktive Domain einschränken
