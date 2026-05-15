"""
Integration-Test: Controller-Logik mit vollständig gemockter Supabase-Schicht.

Die Supabase-Library wird komplett auf Modul-Ebene ersetzt — kein echter Client,
kein Import-Konflikt. Diese Tests validieren die Controller-Logik (graceful
degradation, deal_id propagation), nicht den Netzwerk-Layer.

Zum Testen gegen echte Supabase: pytest tests/test_supabase_integration.py --live
(setzt SUPABASE_URL + SUPABASE_SERVICE_KEY in .env voraus)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest.mock import patch, MagicMock

# ── Supabase komplett mocken bevor irgendein src-Import passiert ──────────────
_supabase_mock = MagicMock()
_supabase_mock.create_client.return_value = MagicMock()

_config_mock = MagicMock()
_config_mock.settings.supabase_url = "https://mock.supabase.co"
_config_mock.settings.supabase_service_key = "mock-service-key"

with patch.dict("sys.modules", {
    "supabase":       _supabase_mock,
    "src.config":     _config_mock,
}):
    # Jetzt können wir Controller und Schemas sauber importieren
    from src.models.schemas import AnalyzeRequest, TechReadinessInputs

    # Supabase-Integrationsfunktionen direkt mocken
    _db_mock = MagicMock()
    with patch.dict("sys.modules", {"src.integrations.supabase": _db_mock}):
        import importlib
        import src.controllers.analyze as analyze_module
        importlib.reload(analyze_module)
        run_analyze = analyze_module.run_analyze


# ── Fixtures ──────────────────────────────────────────────────────────────────

BASE_REQUEST = AnalyzeRequest(
    company_name="CarbonCure",
    buyer_name="CRH",
    tam_usd_bn=100,
    buyer_market_cap_usd_bn=76,
    buyer_cash_usd_bn=5,
    buyer_debt_ebitda=1.5,
    target_funding_usd_mn=169,
    target_stage="series_d_plus",
    tech_readiness_inputs=TechReadinessInputs(
        tech_stack_fit=0.9,
        gtm_fit=0.85,
        integration_capacity=0.8,
        rd_intensity=0.7,
        capital_deployment_velocity=0.75,
        regulatory_readiness=0.85,
        strategic_coherence=0.95,
    ),
)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_controller_graceful_degradation_on_db_error():
    """Score wird immer zurückgegeben — auch wenn Supabase wirft."""
    with patch.object(analyze_module, "fetch_company_by_name", side_effect=Exception("DB down")):
        result = run_analyze(BASE_REQUEST)

    assert result.scores.rating == "A · No-Brainer"
    assert result.deal_id is None
    assert any("DB persistence skipped" in w for w in result.warnings)


def test_controller_persists_deal_on_success():
    """deal_id aus Supabase wird in Response propagiert."""
    with patch.object(analyze_module, "fetch_company_by_name", return_value={"id": "uuid-co-123"}), \
         patch.object(analyze_module, "fetch_buyer_by_name",   return_value={"id": "uuid-bu-456"}), \
         patch.object(analyze_module, "insert_deal",           return_value="uuid-deal-789"), \
         patch.object(analyze_module, "insert_score",          return_value="uuid-score-999"):

        result = run_analyze(BASE_REQUEST)

    assert result.deal_id == "uuid-deal-789"
    assert result.scores.rating == "A · No-Brainer"
    assert not any("DB persistence skipped" in w for w in result.warnings)


def test_controller_handles_unknown_company_and_buyer():
    """Unbekannte Company/Buyer: Deal wird trotzdem ohne FKs angelegt."""
    with patch.object(analyze_module, "fetch_company_by_name", return_value=None), \
         patch.object(analyze_module, "fetch_buyer_by_name",   return_value=None), \
         patch.object(analyze_module, "insert_deal",           return_value="uuid-deal-000"), \
         patch.object(analyze_module, "insert_score",          return_value="uuid-score-000"):

        result = run_analyze(BASE_REQUEST)

    assert result.deal_id == "uuid-deal-000"
    assert result.scores.srr.category == "Transformational++"


def test_scoring_independent_of_db():
    """Scoring-Engine läuft vollständig ohne DB-Zugriff."""
    from src.pipelines.scoring import compute_scores
    scores = compute_scores(BASE_REQUEST)
    assert scores.deal_success_score > 0
    assert scores.rating == "A · No-Brainer"
