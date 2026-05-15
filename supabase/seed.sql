-- ============================================================
-- Argo Analytics – Seed Data
-- Quelle: PROJEKT-MASTER_ClimateTech.md v0.7
-- 43 Companies + 8 Buyers (Shortlist A · No-Brainer + wichtige Proxies)
-- ============================================================


-- ── Buyers ───────────────────────────────────────────────────────────────────

insert into buyers (name, ticker, exchange, market_cap_usd_bn, sector) values
  ('CRH',             'CRH',   'NYSE',      76.0,  'Building Materials'),
  ('Amrize',          'AMRZ',  'NYSE',      18.0,  'Building Materials'),
  ('NextEra Energy',  'NEE',   'NYSE',      196.0, 'Utilities / Renewables'),
  ('Nutrien',         'NTR',   'NYSE',      28.0,  'Agriculture'),
  ('Corteva',         'CTVA',  'NYSE',      32.0,  'Agri-Science'),
  ('Honeywell',       'HON',   'Nasdaq',    140.0, 'Industrials'),
  ('EDF',             'EDF',   'Euronext',  45.0,  'Utilities'),
  ('GE Vernova',      'GEV',   'NYSE',      280.0, 'Energy Technology');


-- ── Companies: Bestand (30) ───────────────────────────────────────────────────

insert into companies (
  name, category, core_technology, potential, risk, ipo_potential,
  funding_total_usd_mn, funding_last_round, funding_stage,
  investment_path, proxy_ticker, source
) values

('Climeworks',
  'Carbon Removal (DAC)', 'Direct Air Capture',
  'Hoch', 'Mittel', 'Mittel',
  1000, '$162 Mio. Series G (Jul 2025)', 'series_d_plus',
  'Beobachten', null, 'bestand'),

('Charm Industrial',
  'Biomass CDR', 'Biomasse-Pyrolyse',
  'Hoch', 'Mittel', 'Hoch',
  108, '$100 Mio. Series B (Jun 2023)', 'series_b',
  'Beobachten', null, 'bestand'),

('Heirloom',
  'Mineralization', 'Mineralbasierte CO₂-Bindung',
  'Mittel-hoch', 'Mittel', 'Mittel',
  203, '$150 Mio. Series B (Dez 2024)', 'series_b',
  'Beobachten', null, 'bestand'),

('Twelve',
  'CO₂-to-Chemicals', 'CO₂ → Chemikalien',
  'Hoch', 'Mittel', 'Mittel',
  843, '$645 Mio. inkl. Series C (Sep 2024)', 'series_c',
  'Beobachten', null, 'bestand'),

('LanzaTech',
  'CO₂-to-Fuels', 'CO₂ → Kraftstoffe',
  'Hoch', 'Mittel', 'IPO erfolgt',
  null, 'Börsennotiert (Nasdaq: LNZA)', 'public',
  'IPO-direkt', 'LNZA · Nasdaq', 'bestand'),

('CarbonCure',
  'Low-Carbon Concrete', 'CO₂-Bindung im Beton',
  'Hoch', 'Mittel', 'Mittel',
  169, '$80 Mio. Series F (Jul 2023)', 'series_d_plus',
  'Käufer-Proxy', 'CRH · NYSE', 'bestand'),

('Running Tide',
  'Ocean CDR', 'Ozeanbasierte CO₂-Entnahme',
  'Mittel', 'Hoch', 'Niedrig',
  12, '~$12 Mio. (Series A, 2022)', 'series_a',
  'Beobachten', null, 'bestand'),

('Living Carbon',
  'Bioengineering', 'CO₂-speichernde Pflanzen',
  'Mittel', 'Mittel', 'Niedrig',
  15, '~$15 Mio. (Series A, 2022)', 'series_a',
  'Beobachten', null, 'bestand'),

('Verdox',
  'Electrochemical Capture', 'Elektrochemische CO₂-Abscheidung',
  'Hoch', 'Mittel', 'Mittel',
  100, '$80 Mio. Series B (2022)', 'series_b',
  'Beobachten', null, 'bestand'),

('Noya',
  'Modular Capture', 'Containerisierte CO₂-Abscheidung',
  'Mittel', 'Mittel', 'Niedrig',
  null, 'n.v. (Frühphase)', 'seed',
  'Beobachten', null, 'bestand'),

('Remora',
  'Mobile Capture', 'CO₂-Abscheidung für LKWs',
  'Mittel', 'Mittel', 'Niedrig',
  12, '~$12 Mio. (Seed/Series A, 2022–23)', 'series_a',
  'Beobachten', null, 'bestand'),

('Carbon Clean',
  'Industrial Capture', 'Industrielle CO₂-Abscheidung',
  'Hoch', 'Mittel', 'Mittel',
  170, '$150 Mio. Series C (Mai 2022)', 'series_c',
  'Beobachten', null, 'bestand'),

('Brimstone',
  'Low-Carbon Cement', 'CO₂-freier Zement',
  'Hoch', 'Mittel', 'Hoch',
  60, '~$60 Mio. gesamt (Venture)', 'series_b',
  'Käufer-Proxy', 'CRH · NYSE', 'bestand'),

('Sublime Systems',
  'Electrified Cement', 'Elektrifizierte Zementproduktion',
  'Hoch', 'Mittel', 'Mittel-hoch',
  121, '~$121 Mio. gesamt; Series B Sep 2024', 'series_b',
  'Käufer-Proxy', 'CRH · NYSE / AMRZ · NYSE', 'bestand'),

('Solugen',
  'Bio-based Chemicals', 'Fossilfreie Chemikalien',
  'Hoch', 'Mittel', 'Hoch',
  850, '$200 Mio. Series D (Nov 2022)', 'series_d_plus',
  'Beobachten', null, 'bestand'),

('CropX',
  'Agritech', 'Präzisionsbewässerung (SaaS + Hardware)',
  'Hoch', 'Mittel', 'Mittel',
  91, '$30 Mio. Series C (Apr 2023)', 'series_c',
  'Beobachten', null, 'bestand'),

('AppHarvest',
  'Vertical Farming', 'Indoor-Farming',
  'Mittel', 'Hoch', 'Niedrig',
  null, 'Insolvent (2023); Assets übernommen von Mastronardi', 'public',
  'Archiv', null, 'bestand'),

('Brightmark',
  'Waste-to-Energy', 'Biogas aus Abfall',
  'Hoch', 'Mittel', 'Niedrig',
  260, '~$260 Mio. gesamt (inkl. Debt)', 'series_c',
  'Beobachten', null, 'bestand'),

('Indigo Ag',
  'Carbon Credits', 'Carbon-Marktplatz',
  'Hoch', 'Mittel', 'Hoch',
  1200, '$250 Mio. (Sep 2023)', 'series_d_plus',
  'Käufer-Proxy', 'NTR · NYSE', 'bestand'),

('Pairwise',
  'Bioengineering', 'CRISPR-Saatgut',
  'Hoch', 'Mittel', 'Mittel',
  115, '~$115 Mio. gesamt (Series B 2021)', 'series_b',
  'Käufer-Proxy', 'CTVA · NYSE', 'bestand'),

('Agmatix',
  'Agritech SaaS', 'Agrardaten-Plattform',
  'Hoch', 'Mittel', 'Niedrig',
  null, 'n.v. (nicht öffentlich)', 'series_b',
  'Beobachten', null, 'bestand'),

('Micropep',
  'Biotech', 'Biologischer Pflanzenschutz',
  'Hoch', 'Mittel', 'Niedrig',
  40, '€27 Mio. Series B (Jul 2024)', 'series_b',
  'Beobachten', null, 'bestand'),

('Amini',
  'Climate-Risk / Satelliten', 'Kreditvergabe via Satellitendaten',
  'Hoch', 'Mittel', 'Niedrig',
  6, '$6 Mio. gesamt (Seed 2023–24)', 'seed',
  'Beobachten', null, 'bestand'),

('Loam Bio',
  'Soil Carbon', 'Mikrobielle CO₂-Speicherung',
  'Hoch', 'Mittel', 'Mittel',
  105, '$73 Mio. Series B (Feb 2023)', 'series_b',
  'Käufer-Proxy', 'NTR · NYSE', 'bestand'),

('12Tree',
  'Agroforestry', 'CO₂-Bindung durch Agroforst',
  'Hoch', 'Mittel', 'Niedrig',
  null, 'n.v. (Impact/Projektfinanzierung)', 'seed',
  'Beobachten', null, 'bestand'),

('ClimateAi',
  'Climate-Risk SaaS', 'Klima-Risikoanalyse',
  'Hoch', 'Mittel', 'Mittel',
  38, '$22 Mio. Series B (Apr 2023)', 'series_b',
  'Beobachten', null, 'bestand'),

('Netafim',
  'Irrigation', 'Bewässerungstechnologie',
  'Hoch', 'Hoch', 'Niedrig',
  null, 'Tochter von Orbia (börsennotiert, Mexiko)', 'public',
  'Enabler', 'ORBIA · BMV', 'bestand'),

('Enapter',
  'Hydrogen', 'AEM-Elektrolyseure',
  'Hoch', 'Mittel', 'Mittel',
  null, 'Börsennotiert (Frankfurt: H2O)', 'public',
  'IPO-direkt', 'H2O · Frankfurt', 'bestand'),

('Notpla',
  'Sustainable Materials', 'Algenverpackungen',
  'Mittel', 'Mittel', 'Niedrig',
  47, '£20 Mio. Series A+ (Sep 2024)', 'series_a',
  'Beobachten', null, 'bestand'),

('SunCulture',
  'Solar Irrigation', 'Solar-Tröpfchenbewässerung',
  'Hoch', 'Mittel', 'Niedrig',
  102, '$27,5 Mio. Series B (Apr 2024)', 'series_b',
  'Beobachten', null, 'bestand');


-- ── Companies: Woche 1 (9) ────────────────────────────────────────────────────

insert into companies (
  name, category, core_technology, potential, risk, ipo_potential,
  funding_total_usd_mn, funding_last_round, funding_stage,
  investment_path, proxy_ticker, last_signal, last_signal_date, source
) values

('Emerald AI',
  'AI × Grid Software', 'KI-Software: Rechenzentren als Grid-Assets',
  'Hoch', 'Mittel', 'Mittel-hoch',
  50, '$24,5 Mio. Seed (Jul 2025) + $25 Mio. Strategic Round (Mär 2026)', 'series_a',
  'Käufer-Proxy', 'GEV · NYSE',
  'BNEF Pioneer 2026', '2026-05-01', 'woche1'),

('GRST',
  'Circular Battery Materials', 'PFAS-freie, wasserlösliche Binder für Li-Ion-Batterien',
  'Hoch', 'Mittel', 'Mittel',
  null, 'n.v. (nicht öffentlich)', 'seed',
  'Beobachten', null,
  'BNEF Pioneer 2026', '2026-05-01', 'woche1'),

('VoltaGrid',
  'Distributed Power Infrastructure', 'Flexible Mikrogrid-Lösungen; 7,5 GW Auftragsbestand bis 2030',
  'Hoch', 'Mittel', 'Hoch',
  6000, '$5 Mrd. Debt (Nov 2025) + $1 Mrd. Equity Blackstone/Halliburton (Mai 2026)', 'series_d_plus',
  'Käufer-Proxy', 'NEE · NYSE',
  '$5 Mrd. Raise; Blackstone/Halliburton', '2026-05-11', 'woche1'),

('Base Power',
  'Distributed Battery / Grid', 'Dezentrale Haushaltsbatterien als koordiniertes Energienetz',
  'Hoch', 'Mittel', 'Mittel-hoch',
  1200, '$200 Mio. Series B (Apr 2025) + $1 Mrd. Series C (Okt 2025)', 'series_c',
  'Käufer-Proxy', 'NEE · NYSE',
  'BNEF Pioneer 2026', '2026-05-03', 'woche1'),

('HT Materials Science',
  'Datacenter Cooling / HVAC', 'Drop-in-Wärmeträgerflüssigkeit für geschlossene HVAC-Systeme',
  'Mittel-hoch', 'Mittel', 'Mittel',
  null, 'n.v. (nicht öffentlich)', 'series_a',
  'Beobachten', null,
  'BNEF Pioneer 2026', '2026-05-03', 'woche1'),

('Beehive',
  'Climate Adaptation / AI', 'KI-Plattform für Katastrophenvorbereitung und Climate-Risk-Reporting',
  'Hoch', 'Mittel', 'Mittel',
  null, 'n.v. (Frühphase)', 'seed',
  'Beobachten', null,
  'Trellis Watch 2026', '2026-05-08', 'woche1'),

('Relectrify',
  'Battery Innovation', 'Halbleiter-Schaltkreise auf Zellebene; AC-Direktproduktion',
  'Hoch', 'Mittel', 'Mittel-hoch',
  null, 'AU$25 Mio. ARENA-Grant (Nov 2025)', 'series_a',
  'Beobachten', null,
  '100 MWh Skalierung 2026', '2026-05-08', 'woche1'),

('WAVR Technologies',
  'AI × Water / Cooling', 'Trinkwasser aus Atmosphäre via Datacenter-Abwärme',
  'Mittel-hoch', 'Hoch', 'Niedrig',
  null, 'n.v. (Frühphase)', 'seed',
  'Beobachten', null,
  'Frühphase — Beobachten', '2026-05-09', 'woche1'),

('Factorial Energy',
  'Solid-State Battery', 'Festkörperbatterien; klare Kosten- und Performancevorteile',
  'Hoch', 'Mittel', 'Hoch',
  240, '~$240 Mio. gesamt; SPAC-Valuation ~$1,1 Mrd.', 'series_d_plus',
  'IPO-direkt', 'CGCT → FAC · Nasdaq',
  'De-SPAC geplant 2026', '2026-05-10', 'woche1');


-- ── Companies: Woche 2 (4) ────────────────────────────────────────────────────

insert into companies (
  name, category, core_technology, potential, risk, ipo_potential,
  funding_total_usd_mn, funding_last_round, funding_stage,
  investment_path, proxy_ticker, last_signal, last_signal_date, source
) values

('Syzygy Plasmonics',
  'CO₂-to-Fuels / SAF', 'Photokatalytische Biogas-zu-SAF-Umwandlung (Rigel™)',
  'Hoch', 'Mittel', 'Mittel',
  211, '$24,5 Mio. Bridge (Mär 2025); Gesamt: ~$211 Mio.', 'series_c',
  'Käufer-Proxy', 'HON · Nasdaq',
  'Hormuz-Anfragespitze; Brasilien-MOU', '2026-05-11', 'woche2'),

('Ore Energy',
  'Long-Duration Storage', 'Eisen-Luft-Batterie; 24–100h Speicher',
  'Hoch', 'Mittel', 'Niedrig',
  20, '~€10 Mio. Seed (Mai 2024) + Grants', 'series_a',
  'Käufer-Proxy', 'EDF · Euronext',
  'EDF-Pilot Feb. 2026; NL-Grid aktiv', '2026-05-12', 'woche2'),

('Fervo Energy',
  'Geothermal / EGS', 'Horizontalbohrung + hydraulisches Fracturing für 24/7-Geothermie',
  'Hoch', 'Mittel', 'Hoch',
  null, 'Valuation ~$7,4 Mrd. (IPO-Einreichung 2026)', 'series_d_plus',
  'IPO-direkt', 'FRVO · Nasdaq',
  'IPO $1,82 Mrd. / $7,4 Mrd. Valuation', '2026-05-12', 'woche2'),

('Moment Energy',
  'Circular Battery / Second-Life BESS', 'Zweitleben-Batteriespeicher aus EV-Packs',
  'Mittel-hoch', 'Mittel', 'Mittel',
  100, '$40 Mio. Series B (Mai 2026)', 'series_b',
  'Beobachten', null,
  'Series B $40 Mio. geschlossen', '2026-05-05', 'woche2');


-- ── Ratings (scores-Tabelle: nur für Einträge mit abgeschlossener Analyse) ───
-- Werden über die API befüllt — hier nur Kommentar als Hinweis.
-- A · No-Brainer: CarbonCure, Brimstone, Sublime Systems, Indigo Ag,
--                 Pairwise, VoltaGrid
-- B · Solide:     Loam Bio, Base Power, Syzygy Plasmonics, Ore Energy
-- D · Uninteressant: Emerald AI
