"""
SE-18 · BaFin Directors' Dealings Signal Engine
================================================
Quelle:  BaFin Insider-Trades JSON-API (Art. 19 MAR / §15a WpHG)
         https://portal.mvp.bafin.de/Database/InsiderTrades/InsiderTrades.json
Kosten:  Kostenlos — kein API-Key, kein Rate-Limit
Scope:   Listed DE Companies (haben ISIN via ticker + exchange=xetra/frankfurt/fse)

Architektur-Entscheidung (Session 40):
  InsiderSignalEvent ist eine eigenständige Dataclass — kein Subclass von SignalEvent.
  Insider-Logik (Buy/Sell-Pattern, Cluster-Detection) bleibt sauber getrennt.
  Konvertierung → SignalEvent via to_signal_event() für DB-Persistenz.

Pattern-Erkennung:
  Einzelkauf   → positive / insider_buying   / medium
  Einzelverkauf → negative / insider_selling / medium
  Cluster-Buy  → positive / insider_buying   / high   (≥3 Personen, ≤30d)
  Cluster-Sell → negative / insider_selling  / high   (≥3 Personen, ≤30d)

Felder aus BaFin-API:
  issuer, isin, person, position, transactionType, transactionDate,
  publicationDate, price, volume, totalAmount, notificationType, link
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from src.services.signal_engine import SignalEvent

logger = logging.getLogger(__name__)

# ── Konstanten ────────────────────────────────────────────────────────────────

_BAFIN_URL = "https://portal.mvp.bafin.de/Database/InsiderTrades/InsiderTrades.json"

# Lookback für Cluster-Detection (Tage)
_CLUSTER_WINDOW_DAYS = 30
# Mindest-Personen für Cluster-Signal
_CLUSTER_MIN_PERSONS = 3
# Lookback für neue Meldungen (verhindert tägliche Doppel-Signale)
_SIGNAL_LOOKBACK_DAYS = 14

# Buy-Keywords in transactionType (BaFin-API nicht immer konsistent)
_BUY_KEYWORDS  = {"kauf", "erwerb", "purchase", "acquisition", "buy", "subscription", "exercise"}
_SELL_KEYWORDS = {"verkauf", "veräußerung", "sale", "disposal", "sell", "transfer"}


# ── Dataclass ─────────────────────────────────────────────────────────────────

@dataclass
class InsiderTransaction:
    """Einzelne Insider-Transaktion aus BaFin-API."""
    issuer:           str
    isin:             str
    person:           str
    position:         str          # z.B. "Vorstandsmitglied", "Aufsichtsratsmitglied"
    transaction_type: str          # Rohwert aus BaFin ("Kauf", "Verkauf", ...)
    transaction_date: date
    publication_date: date
    price:            float | None
    volume:           int | None   # Anzahl Anteile
    total_amount:     float | None # EUR-Gesamtbetrag
    notification_type: str | None
    link:             str | None   # PDF-Meldung auf BaFin-Portal

    @property
    def is_buy(self) -> bool:
        t = self.transaction_type.lower()
        return any(k in t for k in _BUY_KEYWORDS)

    @property
    def is_sell(self) -> bool:
        t = self.transaction_type.lower()
        return any(k in t for k in _SELL_KEYWORDS)


@dataclass
class InsiderSignalEvent:
    """
    Aggregiertes Insider-Signal für eine Company.
    Eigenständige Klasse — nicht von SignalEvent abgeleitet.
    """
    company_id:       str
    company_name:     str
    isin:             str
    signal_date:      date
    is_cluster:       bool          # True = Cluster-Buy/Sell (≥3 Personen)
    is_buy:           bool          # True = Buy, False = Sell
    transactions:     list[InsiderTransaction] = field(default_factory=list)
    person_count:     int = 0
    total_volume_eur: float = 0.0

    @property
    def direction(self) -> str:
        return "positive" if self.is_buy else "negative"

    @property
    def severity(self) -> str:
        return "high" if self.is_cluster else "medium"

    @property
    def signal_category(self) -> str:
        return "insider_buying" if self.is_buy else "insider_selling"

    def build_summary(self) -> str:
        action  = "kauften" if self.is_buy else "verkauften"
        persons = ", ".join(
            f"{t.person} ({t.position})" for t in self.transactions[:3]
        )
        suffix = f" (+{len(self.transactions) - 3} weitere)" if len(self.transactions) > 3 else ""
        volume = f" — Gesamtvolumen: {self.total_volume_eur:,.0f} EUR" if self.total_volume_eur else ""
        prefix = "Insider-Cluster: " if self.is_cluster else "Insider: "
        return f"{prefix}{persons}{suffix} {action} Anteile an {self.company_name}{volume}."

    def to_signal_event(self) -> "SignalEvent":
        """Konvertiert InsiderSignalEvent → SignalEvent für DB-Persistenz."""
        from src.services.signal_engine import SignalEvent

        # Quell-Link: erstes verfügbares PDF oder BaFin-Portal
        source_url = next(
            (t.link for t in self.transactions if t.link), None
        ) or f"https://portal.mvp.bafin.de/Database/InsiderTrades/InsiderTrades.json?isin={self.isin}"

        return SignalEvent(
            company_id=self.company_id,
            company_name=self.company_name,
            event_type="ownership_change",
            event_date=self.signal_date,
            summary=self.build_summary(),
            source="bafin_insider",
            source_url=source_url,
            severity=self.severity,
            raw_title=f"BaFin Directors' Dealings — {self.company_name} ({'Kauf' if self.is_buy else 'Verkauf'})",
            direction=self.direction,
            signal_category=self.signal_category,
            source_count=len(self.transactions),
            source_names=["bafin_insider"],
        )


# ── API-Client ────────────────────────────────────────────────────────────────

def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            from datetime import datetime
            return datetime.strptime(raw[:10], fmt[:8]).date()
        except ValueError:
            continue
    return None


def _parse_float(raw) -> float | None:
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", ".").replace(" ", ""))
    except (ValueError, TypeError):
        return None


def _parse_int(raw) -> int | None:
    if raw is None:
        return None
    try:
        return int(float(str(raw).replace(",", ".").replace(" ", "")))
    except (ValueError, TypeError):
        return None


async def fetch_insider_transactions(
    isin: str,
    client: httpx.AsyncClient,
    lookback_days: int = _SIGNAL_LOOKBACK_DAYS,
    issuer_name: str | None = None,
) -> list[InsiderTransaction]:
    """
    Fetcht Insider-Transaktionen für eine ISIN aus BaFin-API.
    Fallback: issuer_name-Query wenn keine ISIN vorhanden.
    Filtert auf Lookback-Fenster — verhindert tägliche Doppel-Signale.
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    transactions: list[InsiderTransaction] = []

    # Params: ISIN bevorzugt, Issuer-Name als Fallback
    if isin:
        params = {"isin": isin}
    elif issuer_name:
        params = {"issuer": issuer_name}
    else:
        logger.debug("SE-18: Kein ISIN und kein issuer_name — übersprungen")
        return []

    try:
        resp = await client.get(
            _BAFIN_URL,
            params=params,
            timeout=10.0,
        )
        if resp.status_code != 200:
            logger.warning("SE-18: BaFin HTTP %d für params=%s", resp.status_code, params)
            return []

        data = resp.json()
        # BaFin liefert entweder Liste direkt oder {'data': [...]}
        records: list[dict] = data if isinstance(data, list) else data.get("data", [])

        for r in records:
            tx_date = _parse_date(r.get("transactionDate"))
            if tx_date is None or tx_date < cutoff:
                continue

            pub_date = _parse_date(r.get("publicationDate")) or tx_date

            transactions.append(InsiderTransaction(
                issuer=r.get("issuer") or "",
                isin=isin or r.get("isin") or "",
                person=r.get("person") or "Unbekannt",
                position=r.get("position") or "",
                transaction_type=r.get("transactionType") or "",
                transaction_date=tx_date,
                publication_date=pub_date,
                price=_parse_float(r.get("price")),
                volume=_parse_int(r.get("volume")),
                total_amount=_parse_float(r.get("totalAmount")),
                notification_type=r.get("notificationType"),
                link=r.get("link"),
            ))

    except Exception as e:
        logger.warning("SE-18: BaFin fetch fehlgeschlagen für params=%s — %s", params, e)

    logger.info("SE-18: params=%s → %d Transaktionen (lookback=%dd)", params, len(transactions), lookback_days)
    return transactions


# ── Signal-Aggregation ────────────────────────────────────────────────────────

def _detect_cluster(
    transactions: list[InsiderTransaction],
    is_buy: bool,
) -> list[InsiderTransaction]:
    """
    Cluster-Detection: ≥ _CLUSTER_MIN_PERSONS verschiedene Personen
    innerhalb _CLUSTER_WINDOW_DAYS mit gleicher Richtung (buy/sell).
    """
    filtered = [t for t in transactions if (t.is_buy if is_buy else t.is_sell)]
    if len(filtered) < _CLUSTER_MIN_PERSONS:
        return []

    # Sortiert nach Datum — Sliding Window
    filtered.sort(key=lambda t: t.transaction_date)
    window_start = filtered[0].transaction_date
    window_end   = window_start + timedelta(days=_CLUSTER_WINDOW_DAYS)

    cluster = [t for t in filtered if t.transaction_date <= window_end]
    persons  = {t.person for t in cluster}

    if len(persons) >= _CLUSTER_MIN_PERSONS:
        return cluster
    return []


def build_insider_signals(
    company_id: str,
    company_name: str,
    isin: str,
    transactions: list[InsiderTransaction],
) -> list[InsiderSignalEvent]:
    """
    Aggregiert rohe Transaktionen → InsiderSignalEvents.

    Logik:
    1. Cluster-Buy  (≥3 Personen) → high severity, positiv
    2. Cluster-Sell (≥3 Personen) → high severity, negativ
    3. Einzel-Buys  (grouped)     → medium, positiv
    4. Einzel-Sells (grouped)     → medium, negativ

    Cluster hat Vorrang: wenn Cluster erkannt → keine Einzel-Events dafür.
    """
    if not transactions:
        return []

    signals: list[InsiderSignalEvent] = []

    # 1 + 2: Cluster-Detection
    cluster_buys  = _detect_cluster(transactions, is_buy=True)
    cluster_sells = _detect_cluster(transactions, is_buy=False)

    if cluster_buys:
        signals.append(InsiderSignalEvent(
            company_id=company_id,
            company_name=company_name,
            isin=isin,
            signal_date=max(t.transaction_date for t in cluster_buys),
            is_cluster=True,
            is_buy=True,
            transactions=cluster_buys,
            person_count=len({t.person for t in cluster_buys}),
            total_volume_eur=sum(t.total_amount or 0 for t in cluster_buys),
        ))

    if cluster_sells:
        signals.append(InsiderSignalEvent(
            company_id=company_id,
            company_name=company_name,
            isin=isin,
            signal_date=max(t.transaction_date for t in cluster_sells),
            is_cluster=True,
            is_buy=False,
            transactions=cluster_sells,
            person_count=len({t.person for t in cluster_sells}),
            total_volume_eur=sum(t.total_amount or 0 for t in cluster_sells),
        ))

    # 3 + 4: Einzel-Events (nur wenn kein Cluster für diese Richtung)
    if not cluster_buys:
        buys = [t for t in transactions if t.is_buy]
        if buys:
            signals.append(InsiderSignalEvent(
                company_id=company_id,
                company_name=company_name,
                isin=isin,
                signal_date=max(t.transaction_date for t in buys),
                is_cluster=False,
                is_buy=True,
                transactions=buys,
                person_count=len({t.person for t in buys}),
                total_volume_eur=sum(t.total_amount or 0 for t in buys),
            ))

    if not cluster_sells:
        sells = [t for t in transactions if t.is_sell]
        if sells:
            signals.append(InsiderSignalEvent(
                company_id=company_id,
                company_name=company_name,
                isin=isin,
                signal_date=max(t.transaction_date for t in sells),
                is_cluster=False,
                is_buy=False,
                transactions=sells,
                person_count=len({t.person for t in sells}),
                total_volume_eur=sum(t.total_amount or 0 for t in sells),
            ))

    return signals


# ── Haupt-Einstiegspunkt für signal_engine.py ─────────────────────────────────

async def parse_bafin_insider(
    company_id: str,
    company_name: str,
    isin: str,
    client: httpx.AsyncClient,
    issuer_name: str | None = None,
) -> list["SignalEvent"]:
    """
    SE-18: Haupt-Parser für BaFin Directors' Dealings.
    Aufgerufen von signal_engine.run_signal_engine() für listed DE Companies.

    Gibt Liste von SignalEvents zurück (via InsiderSignalEvent.to_signal_event()).
    Gibt [] zurück wenn keine neue Meldungen im Lookback-Fenster.
    """
    if not isin and not issuer_name:
        logger.debug("SE-18: %s — keine ISIN und kein issuer_name, übersprungen", company_name)
        return []

    transactions = await fetch_insider_transactions(isin, client, issuer_name=issuer_name)
    if not transactions:
        return []

    insider_signals = build_insider_signals(company_id, company_name, isin, transactions)
    signal_events   = [s.to_signal_event() for s in insider_signals]

    logger.info(
        "SE-18: %s (ISIN=%s) → %d Transaktionen → %d Signale (%d Cluster, %d Einzel)",
        company_name, isin or "—",
        len(transactions), len(signal_events),
        sum(1 for s in insider_signals if s.is_cluster),
        sum(1 for s in insider_signals if not s.is_cluster),
    )

    return signal_events
