"""Curated provider boards added from the coverage audit.

The coverage audit found openings that Baluffo lacked on boards serving live roles.
These rows stage those boards as curated discovery candidates. Companion Group is
deliberately absent: the audit listed it, but the live registry already carries
``recruitee:api_url:https://companiongroupltd.recruitee.com/api/offers/`` as active,
so registering it again would have been a no-op. The local snapshot used to plan
this work was a 2026-09-17 generation and did not show the row.
"""

import json

import pytest

from src.source_discovery.reporting_candidates import stage_curated_seed_candidates

EXPECTED_IDS = {
    "greenhouse:slug:2kczech",
    "greenhouse:slug:hangar13",
    "smartrecruiters:company_id:yggdrasilsandbox",
}


def _staged() -> dict:
    return {row["id"]: row for row in stage_curated_seed_candidates()}


@pytest.mark.parametrize("source_id", sorted(EXPECTED_IDS))
def test_curated_board_is_staged_as_a_candidate(source_id: str) -> None:
    assert source_id in _staged()


def test_yggdrasil_uses_the_sandbox_org_not_the_brand_name() -> None:
    """The org is YggdrasilSandbox; 'Yggdrasil' 404s on the SmartRecruiters API."""
    row = _staged()["smartrecruiters:company_id:yggdrasilsandbox"]
    assert row["company_id"] == "YggdrasilSandbox"
    assert row["api_url"].endswith("/companies/YggdrasilSandbox/postings")


def test_greenhouse_rows_carry_a_slug_and_no_api_url() -> None:
    for slug in ("2kczech", "hangar13"):
        row = _staged()[f"greenhouse:slug:{slug}"]
        assert row["adapter"] == "greenhouse"
        assert row["slug"] == slug
        assert not row.get("api_url")


def test_all_three_declare_an_adapter_the_runtime_supports() -> None:
    from src.source_discovery.config import SUPPORTED_PROVIDERS

    for source_id in EXPECTED_IDS:
        assert source_id.split(":", 1)[0] in SUPPORTED_PROVIDERS


def test_curated_rows_are_staged_with_curated_seed_evidence() -> None:
    for source_id in EXPECTED_IDS:
        row = _staged()[source_id]
        assert row["discoveryStage"] == "curated_seed"
        assert row["evidenceTypes"] == ["seed_curated"]
        assert int(row["evidenceScore"]) >= 45


def test_no_curated_row_reintroduces_a_studio_already_on_its_ats() -> None:
    """Companion Group was already active, so it must not be staged again."""
    staged = json.dumps(stage_curated_seed_candidates()).lower()
    assert "companiongroupltd" not in staged
