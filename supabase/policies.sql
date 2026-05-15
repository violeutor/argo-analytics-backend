-- ============================================================
-- Argo Analytics – RLS Policies
-- Status: VORBEREITET, noch nicht aktiv (Phase 1: RLS off)
-- Aktivierung: wenn Supabase Auth integriert ist (Phase 2)
-- ============================================================

-- Um RLS zu aktivieren, folgende Schritte ausführen:
-- 1. alter table companies  enable row level security;
-- 2. alter table buyers     enable row level security;
-- 3. alter table deals      enable row level security;
-- 4. alter table scores     enable row level security;
-- 5. alter table proxies    enable row level security;
-- 6. alter table signals    enable row level security;
-- 7. alter table watchlist  enable row level security;
-- 8. Policies unten einkommentieren und ausführen


-- ── Read Policies (authenticated users) ──────────────────────────────────────

/*

-- Alle authentifizierten User können Companies lesen
create policy "companies_read_authenticated"
  on companies for select
  to authenticated
  using (true);

-- Alle authentifizierten User können Buyers lesen
create policy "buyers_read_authenticated"
  on buyers for select
  to authenticated
  using (true);

-- User sehen nur eigene Deals
create policy "deals_read_own"
  on deals for select
  to authenticated
  using (auth.uid() = user_id);     -- user_id Spalte in deals ergänzen wenn Auth aktiv

-- User sehen nur Scores ihrer eigenen Deals
create policy "scores_read_own"
  on scores for select
  to authenticated
  using (
    deal_id in (
      select id from deals where user_id = auth.uid()
    )
  );

-- Watchlist: nur eigene Einträge
create policy "watchlist_read_own"
  on watchlist for select
  to authenticated
  using (user_id = auth.uid());

create policy "watchlist_insert_own"
  on watchlist for insert
  to authenticated
  with check (user_id = auth.uid());

create policy "watchlist_delete_own"
  on watchlist for delete
  to authenticated
  using (user_id = auth.uid());

*/


-- ── Write Policies (service role only) ───────────────────────────────────────
-- Companies, Buyers, Signals: nur über Backend (service role key) schreibbar
-- Deals und Scores: über Backend im Namen des Users

/*

create policy "companies_write_service"
  on companies for all
  to service_role
  using (true) with check (true);

create policy "buyers_write_service"
  on buyers for all
  to service_role
  using (true) with check (true);

create policy "signals_write_service"
  on signals for all
  to service_role
  using (true) with check (true);

create policy "deals_insert_authenticated"
  on deals for insert
  to authenticated
  with check (true);

create policy "scores_insert_service"
  on scores for all
  to service_role
  using (true) with check (true);

*/
