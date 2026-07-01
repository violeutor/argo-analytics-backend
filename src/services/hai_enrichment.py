"""
HAI-01 · handelsregister.ai Enrichment für DE Private Companies
================================================================
Pfad: argo-analytics-backend/src/services/hai_enrichment.py

One-Click Cold Path für deutsche Privatunternehmen:
  GET /api/v1/fetch-organization?q={name}&feature=financial_kpi
                                          &feature=related_persons
                                          &feature=shareholders
                                          &ai_search=on-default

Kosten: 5 (Basis) + 1 (financial_kpi) + 2 (related_persons) + 5 (shareholders)
      = 13 Credits pro Call

Schreibt in:
  - companies          → Basisdaten (HQ, Website, Rechtsform, Gründungsjahr)
  - kpi_timeseries     → Revenue, Net Income historisch (Quelle: hai)
  - headcount_snapshots → Employees historisch (akkumulierend)
  - ownership_entries  → Gesellschafter (type: shareholder) + GF (type: management)

Wird aufgerufen als BackgroundTask in company_detail.py:
  - Nur für private Companies (is_listed=False)
  - Nur wenn Region DE erkannt wurde (_is_likely_german)
  - Nur beim Cold Path (neue Company, Identität noch nicht vollständig)
  - Nie im Rolling Refresh (zu teuer)

Ticketnummer: HAI-01
"""
import logging
import os
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

HAI_API_KEY  = os.environ.get("HAI_API_KEY", "")
HAI_BASE_URL = "https://handelsregister.ai/api/v1/fetch-organization"
HAI_TIMEOUT  = 20.0  # Sekunden

# Feature-Set Basis-Call (13 Credits)
HAI_FEATURES_BASE = [
    "financial_kpi",    # +1 Credit: Revenue, Net Income, Employees historisch
    "related_persons",  # +2 Credits: GF, Prokuristen
    "shareholders",     # +5 Credits: Gesellschafterliste mit Anteilen
]


# ── Haupt-Fetch ───────────────────────────────────────────────────────────────

async def fetch_hai_organization(company_name: str) -> dict | None:
    """
    Ruft fetch-organization für eine DE Company ab.
    ai_search=on-default → Entity Resolution auch bei Namensabweichungen.
    Gibt Raw-Response-Dict zurück oder None bei Fehler/kein Treffer.
    """
    if not HAI_API_KEY:
        logger.warning("HAI-01: HAI_API_KEY nicht gesetzt — Skip für %s", company_name)
        return None

    params: dict = {"q": company_name, "ai_search": "on-default"}
    for feature in HAI_FEATURES_BASE:
        params.setdefault("feature", [])
        if isinstance(params["feature"], list):
            params["feature"].append(feature)
        else:
            params["feature"] = [params["feature"], feature]

    # httpx akzeptiert feature als Liste wenn als list übergeben
    param_list = [("q", company_name), ("ai_search", "on-default")]
    for f in HAI_FEATURES_BASE:
        param_list.append(("feature", f))

    try:
        async with httpx.AsyncClient(timeout=HAI_TIMEOUT) as client:
            resp = await client.get(
                HAI_BASE_URL,
                params=param_list,
                headers={"x-api-key": HAI_API_KEY},
            )
            if resp.status_code == 402:
                logger.warning("HAI-01: Insufficient Credits für %s", company_name)
                return None
            if resp.status_code == 404:
                logger.info("HAI-01: Kein Treffer für %s", company_name)
                return None
            if not resp.is_success:
                logger.warning(
                    "HAI-01: HTTP %d für %s — %s",
                    resp.status_code, company_name, resp.text[:200],
                )
                return None
            data = resp.json()
            logger.info(
                "HAI-01: Treffer für %s → entity_id=%s name=%s",
                company_name, data.get("entity_id"), data.get("name"),
            )
            return data
    except Exception as e:
        logger.warning("HAI-01: fetch_hai_organization failed für %s: %s", company_name, e)
        return None


# ── Parser ────────────────────────────────────────────────────────────────────

def parse_hai_base(data: dict) -> dict:
    """
    Extrahiert Basisdaten aus dem HAI-Response für companies-Upsert.
    Gibt nur Felder zurück die einen Wert haben (kein None-Overwrite).
    """
    out: dict = {}

    # Adresse → headquarters
    addr = data.get("address") or {}
    city    = addr.get("city")
    country = "Deutschland"
    if city:
        out["headquarters"] = f"{city}, {country}"

    # Website
    contact = data.get("contact_data") or {}
    if contact.get("website"):
        out["website"] = contact["website"]

    # Gründungsdatum
    reg_date = data.get("registration_date")
    if reg_date:
        try:
            year = datetime.fromisoformat(reg_date[:10]).year
            out["founding_year"] = year
        except Exception:
            pass

    # Rechtsform — in companies als eigenes Feld falls vorhanden, sonst in description
    legal_form = data.get("legal_form")
    if legal_form:
        out["legal_form"] = legal_form  # FundamentalsData.ba_legal_form analog

    # HAI entity_id als externe Referenz
    entity_id = data.get("entity_id")
    if entity_id:
        out["hai_entity_id"] = entity_id

    return out


def parse_hai_kpi(data: dict, company_id: str) -> list[dict]:
    """
    Extrahiert financial_kpi → kpi_timeseries Rows.
    Schema: company_id, metric, fiscal_year, value, source='hai', currency='EUR'
    """
    rows: list[dict] = []
    kpis = data.get("financial_kpi") or []

    for entry in kpis:
        year = entry.get("year")
        if not year:
            continue

        base = {
            "company_id":  company_id,
            "fiscal_year": int(year),
            "source":      "hai",
            "currency":    "EUR",
            "confidence":  "high",
        }

        revenue = entry.get("revenue")
        if revenue is not None:
            rows.append({**base, "metric": "revenue_mn", "value": round(float(revenue) / 1_000_000, 3)})

        net_income = entry.get("net_income")
        if net_income is not None:
            rows.append({**base, "metric": "net_income_mn", "value": round(float(net_income) / 1_000_000, 3)})

        # employees → headcount_snapshots (separater Pfad, nicht kpi_timeseries)
        # Wird in enrich_hai_de_private separat verarbeitet

    return rows


def parse_hai_headcount_snapshots(data: dict, company_id: str) -> list[dict]:
    """
    Extrahiert employees-Historik aus financial_kpi für headcount_snapshots.
    Gibt Liste von {company_id, headcount, source, snapshot_date} zurück.
    snapshot_date: 31.12. des jeweiligen Jahres (Jahresabschluss-Logik).
    """
    snapshots: list[dict] = []
    kpis = data.get("financial_kpi") or []

    for entry in kpis:
        year = entry.get("year")
        employees = entry.get("employees")
        if not year or employees is None:
            continue
        try:
            hc = int(employees)
            if hc <= 0:
                continue
            # Jahresabschluss-Stichtag: 31.12. des Jahres, max heute
            snap_date = min(date(int(year), 12, 31), date.today())
            snapshots.append({
                "company_id":    company_id,
                "headcount":     hc,
                "source":        "hai",
                "snapshot_date": snap_date.isoformat(),
            })
        except Exception:
            continue

    return snapshots


def parse_hai_ownership(data: dict, company_id: str) -> list[dict]:
    """
    Extrahiert Gesellschafter + Management aus HAI-Response.
    → ownership_entries kompatibles Schema:
      name, type, role, share_pct, source, as_of_date
    """
    entries: list[dict] = []
    today = date.today().isoformat()

    # Gesellschafter (shareholders)
    shareholders_data = data.get("shareholders") or {}
    shareholder_list  = (
        shareholders_data.get("entries") or
        shareholders_data.get("shareholders") or
        (shareholders_data if isinstance(shareholders_data, list) else [])
    )
    for sh in shareholder_list:
        name = (
            sh.get("display_name") or
            sh.get("name") or
            sh.get("shareholder_name") or ""
        ).strip()
        if not name:
            continue
        pct = sh.get("percentage") or sh.get("share_pct")
        entries.append({
            "name":       name,
            "type":       "Shareholder",
            "role":       "Gesellschafter",
            "share_pct":  float(pct) if pct is not None else None,
            "source":     "handelsregister",
            "as_of_date": today,
        })

    # Management (related_persons → current)
    related = data.get("related_persons") or {}
    current_persons = (
        related.get("current") or
        (related if isinstance(related, list) else [])
    )
    for person in current_persons:
        name = (person.get("name") or "").strip()
        if not name:
            continue
        role_block = person.get("role") or {}
        role_de    = (
            (role_block.get("de") or {}).get("long") or
            (role_block.get("en") or {}).get("long") or
            "Management"
        )
        start_date = person.get("start_date")
        entries.append({
            "name":       name,
            "type":       "Management",
            "role":       role_de,
            "share_pct":  None,
            "source":     "handelsregister",
            "as_of_date": start_date[:10] if start_date else today,
        })

    return entries


# ── Haupt-Orchestrator ────────────────────────────────────────────────────────

async def enrich_hai_de_private(
    company_name: str,
    company_id:   str,
) -> bool:
    """
    Vollständige HAI-Anreicherung für eine DE Private Company.
    Wird als BackgroundTask aus company_detail.py aufgerufen.

    Schreibt:
      1. companies      → Basisdaten (HQ, Website, Gründungsjahr)
      2. kpi_timeseries → Revenue + Net Income historisch
      3. headcount_snapshots → Employees historisch
      4. ownership_entries   → Gesellschafter + Management

    Returns True wenn erfolgreich, False bei Fehler/kein Treffer.
    """
    from src.integrations.supabase import (
        get_supabase,
        upsert_company_enrichment,
        upsert_ownership_entries,
        write_headcount_snapshot,
    )

    data = await fetch_hai_organization(company_name)
    if not data:
        # HAI-GATE-01: nur stempeln wenn wirklich ein Versuch stattfand (API-Key vorhanden).
        # Fehlt der Key, ist das ein Config-Problem, kein "Company hat keine HR-Daten" —
        # sonst bleibt der Skip nach Key-Fix dauerhaft blockiert.
        if HAI_API_KEY:
            upsert_company_enrichment(company_id, {"hai_enriched_at": datetime.utcnow().isoformat()})
            logger.info("HAI-01: kein Treffer/Fehler für %s — hai_enriched_at gestempelt (kein Retry-Loop)", company_name)
        return False

    db = get_supabase()

    # 1. Basisdaten → companies
    base = parse_hai_base(data)
    # Felder die companies kennt (hai_entity_id + legal_form ggf. nicht in Schema).
    # HAI-GATE-01: hai_enriched_at IMMER stempeln bei erfolgtem Versuch — unabhängig
    # davon, ob base überhaupt Felder liefert (Gate darf nicht an Datenqualität hängen).
    companies_payload = {
        k: v for k, v in base.items()
        if k in {"headquarters", "website", "founding_year"}
    }
    companies_payload["hai_enriched_at"] = datetime.utcnow().isoformat()
    upsert_company_enrichment(company_id, companies_payload)
    logger.info("HAI-01: Basisdaten geschrieben für %s: %s", company_name, list(companies_payload.keys()))

    # 2. KPI-Zeitreihen → kpi_timeseries
    kpi_rows = parse_hai_kpi(data, company_id)
    written_kpi = 0
    for row in kpi_rows:
        try:
            result = (
                db.table("kpi_timeseries")
                .upsert(row, on_conflict="company_id,metric,fiscal_year,source", ignore_duplicates=True)
                .execute()
            )
            if result.data:
                written_kpi += 1
        except Exception as e:
            logger.warning("HAI-01: kpi_timeseries upsert failed für %s FY%s %s: %s",
                           company_name, row.get("fiscal_year"), row.get("metric"), e)
    logger.info("HAI-01: %d KPI-Rows geschrieben für %s", written_kpi, company_name)

    # 3. Headcount Snapshots (historisch aus financial_kpi)
    hc_snapshots = parse_hai_headcount_snapshots(data, company_id)
    written_hc = 0
    for snap in hc_snapshots:
        try:
            # Direkt upsert — historische Daten können vom täglichen Cron abweichen
            db.table("headcount_snapshots").upsert(
                snap,
                on_conflict="company_id,snapshot_date",
                ignore_duplicates=False,
            ).execute()
            written_hc += 1
        except Exception as e:
            logger.warning("HAI-01: headcount_snapshot upsert failed für %s %s: %s",
                           company_name, snap.get("snapshot_date"), e)
    logger.info("HAI-01: %d Headcount-Snapshots geschrieben für %s", written_hc, company_name)

    # 4. Ownership (Gesellschafter + Management) → ownership_entries
    ownership_entries = parse_hai_ownership(data, company_id)
    if ownership_entries:
        written_ow = upsert_ownership_entries(
            company_id=company_id,
            entries=ownership_entries,
            company_name=company_name,
        )
        logger.info("HAI-01: %d Ownership-Einträge geschrieben für %s", written_ow, company_name)

    logger.info(
        "HAI-01: Anreicherung abgeschlossen für %s — KPIs: %d, HC: %d, Ownership: %d",
        company_name, written_kpi, written_hc, len(ownership_entries),
    )
    return True
