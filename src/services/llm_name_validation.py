"""
PEER-GEN-NAME-VALIDATION-01 · llm_name_validation.py
Pfad-Vorschlag: argo-analytics-backend/src/services/llm_name_validation.py

Shared Name-Validator für Haiku-generierte Peer-/Buyer-Namensvorschläge
(peers.py::_claude_generate_peers, buyer_enrichment.py::_claude_generate_adjacent).

Fund (S86, Duplikat-Scan für PEERS-BUYERS-PERSISTENCE-01): Haiku liefert bei
Unsicherheit gelegentlich zwei Kandidaten in einem String statt eines Namens
("Vivint Solar / Sungevity", "Sunwoda / BYD Energy Storage", "Holaluz /
EnerTIC") oder hängt eine Umbenennungs-Notiz an ("Calyxt (nun Benson Hill)").
Downstream-Code hat das bisher nie validiert/gesplittet — der kombinierte
String landete 1:1 als ein Company-Name in der DB.

Single-Chokepoint-Prinzip: eine Funktion, von beiden Generierungspfaden
importiert, statt zweimal dieselbe Regex-Logik zu pflegen.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# "X (nun Y)" / "X (now Y)" / "X (formerly Y)" / "X (vormals Y)" / "X (ehemals Y)"
# / "X (renamed to Y)" / "X (umbenannt in Y)" → Parenthese komplett abschneiden,
# nur "X" behalten. Die bestehende Lifecycle-Resolution (Wikidata P7888/
# consolidated_into_name, DISAMBIG-03-LIFECYCLE) löst Umbenennungen/Übernahmen
# bereits auf — kein eigener Extraktions-Versuch nötig, welcher Name "aktuell"
# ist.
_RENAME_NOTE = re.compile(
    r"\s*\((?:nun|now|formerly|vormals|ehemals|renamed(?:\s+to)?|umbenannt(?:\s+in)?)\s+[^)]*\)",
    re.IGNORECASE,
)


def split_llm_company_name(raw: str | None) -> list[str]:
    """
    Nimmt einen rohen Haiku-Namensvorschlag entgegen, gibt 0, 1 oder 2 saubere
    Kandidatennamen zurück.

    - Sauberer Einzelname            → [name] unverändert.
    - "X (nun Y)"-Muster             → [X] (Rename-Notiz abgeschnitten).
    - "X / Y"-Muster                 → [X, Y] (beide als eigenständige
      Kandidaten — jeder durchläuft nachgelagert ohnehin die volle Wikidata-
      Identitätsprüfung, kein Rateverlust wie bei "nur die erste Hälfte
      nehmen").
    - Alles andere, das nach der Bereinigung nicht in genau 1 oder 2 saubere
      Teile zerfällt (leer, 3+ Teile, unbekanntes Trennzeichen) → []
      (verworfen, sichtbar geloggt statt geraten).

    Bewusst NICHT auf "&"/" und "/" and " gesplittet — "Johnson & Johnson",
    "AT&T", "Procter & Gamble" sind legitime Einzelnamen. Gleiche Falle wie
    DUPLICATE-DETECTION-SHORTNAME-GAP-01, nur umgekehrte Richtung (False
    Split statt False Merge) — beide Richtungen sind gleich teuer.
    """
    if not raw:
        return []

    name = _RENAME_NOTE.sub("", raw.strip()).strip()
    if not name:
        return []

    if " / " in name:
        parts = [p.strip() for p in name.split(" / ") if p.strip()]
        if len(parts) == 2:
            return parts
        logger.warning(
            "PEER-GEN-NAME-VALIDATION-01: '%s' nicht sauber parsbar "
            "(%d Teile nach ' / '-Split) — verworfen",
            raw, len(parts),
        )
        return []

    return [name]
