"""
tests/test_region_classification.py
REGION-CLASS-01: Unit-Tests für _derive_region_from_exchange().

Warum dieser Test existiert:
  Region steuert das Fundamentals-Gate (DE→Bundesanzeiger/HAI, US→EDGAR). Eine
  falsche Region feuert aussichtslose Calls: ein DE-Unternehmen, das fälschlich
  als US klassifiziert wird, löst EDGAR SC 13G/13D → CIK-404 aus (der Original-Bug,
  den REGION-CLASS-01 schließt). Der Test fixiert die drei funktionalen Buckets
  (DE / US / EU) und — kritisch — dass eine LEERE Exchange None liefert (→ HQ-
  Fallback greift), damit eine noch nicht aufgelöste Exchange nicht als US durchläuft.

  Mapping-SSOT ist _EXCHANGE_SUFFIX (geteilt mit _looks_us_listed): alles darin ist
  ein bekanntes Nicht-US-Venue. DE-Subset → "DE", Rest davon → "EU" (Catch-all
  "weder US noch DE"; APAC/CA fallen hier bewusst rein — funktional irrelevant,
  da weder EDGAR noch BA). Präsenter Code OHNE Suffix-Eintrag → "US".
"""

import pytest

from src.routes.company_detail import _derive_region_from_exchange


@pytest.mark.parametrize("exchange", [
    "GY", "gy", "gf", "xetra", "frankfurt", "fse",
    "Frankfurt · GY",   # Display-Name + exchCode kombiniert (Resolver-Format)
])
def test_de_exchanges_map_to_de(exchange):
    assert _derive_region_from_exchange(exchange) == "DE"


@pytest.mark.parametrize("exchange", [
    "ln", "LN", "fp", "sw", "im", "sm", "av", "ss", "na",
    "london", "lse", "euronext", "six", "milan",
    "London · LN",
    # APAC/CA: bewusst "EU" (Catch-all für non-US/non-DE; kein eigener Bucket nötig)
    "tokyo", "tsx", "asx", "hkex", "bmv",
])
def test_non_de_known_venues_map_to_eu(exchange):
    assert _derive_region_from_exchange(exchange) == "EU"


@pytest.mark.parametrize("exchange", [
    "un", "uq", "ua",            # Bloomberg US-exchCodes (NYSE/Nasdaq/AMEX)
    "nyse", "nasdaq", "nms", "nyq",  # Display-Namen, nicht in _EXCHANGE_SUFFIX
])
def test_present_but_unknown_venue_maps_to_us(exchange):
    assert _derive_region_from_exchange(exchange) == "US"


@pytest.mark.parametrize("exchange", [None, "", "   "])
def test_empty_exchange_returns_none_for_hq_fallback(exchange):
    # KRITISCH: leer darf NICHT "US" werden — sonst Bayer-ohne-Exchange → EDGAR-404.
    # None signalisiert dem Insert, auf _derive_region_from_hq() zurückzufallen.
    assert _derive_region_from_exchange(exchange) is None


def test_case_insensitivity():
    assert _derive_region_from_exchange("Gy") == "DE"
    assert _derive_region_from_exchange("Ln") == "EU"


def test_separator_extraction_takes_last_segment():
    # Resolver-Format "Display · exchCode" → der exchCode (letztes Segment) zählt.
    assert _derive_region_from_exchange("Xetra · GY") == "DE"
    assert _derive_region_from_exchange("Euronext Paris · FP") == "EU"
