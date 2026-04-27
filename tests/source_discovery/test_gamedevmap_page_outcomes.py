from __future__ import annotations

from src.source_discovery import gamedevmap_active_dry_run as dry_run

from .gamedevmap_test_helpers import INDEX_URL


def _row() -> dict[str, str]:
    return {
        "studio": "Recover Studio",
        "url": "https://recover.example.com",
        "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/profile/recover-studio",
    }


def test_gamedevmap_provider_text_extraction_uses_shared_inference_and_dedupes() -> None:
    html = (
        '<script>window.jobs = "https://boards.greenhouse.io/recoverstudio";</script>'
        '<a href="https://boards.greenhouse.io/recoverstudio">Jobs</a>'
    )

    rows = dry_run._provider_candidates_from_html_text(
        row=_row(),
        page_url="https://recover.example.com",
        html=html,
        index_url=INDEX_URL,
    )

    assert len(rows) == 1
    assert rows[0]["adapter"] == "greenhouse"
    assert rows[0]["slug"] == "recoverstudio"
    assert rows[0]["careersUrl"] == "https://recover.example.com"
    assert rows[0]["sourceDirectoryEntryUrl"] == _row()["sourceDirectoryEntryUrl"]
    assert rows[0]["gamedevmapRecovery"] is True
    assert "gamedevmap_recovery_provider_url" in rows[0]["evidenceTypes"]


def test_gamedevmap_page_outcome_prefers_provider_over_static(monkeypatch) -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [{"adapter": "greenhouse", "slug": "recoverstudio"}],
            "explicit_careers_url": "https://recover.example.com/careers",
            "generic_static_candidate": {
                "adapter": "static",
                "listing_url": "https://recover.example.com/jobs",
            },
        }

    monkeypatch.setattr(dry_run, "analyze_fetched_page", analyze_page)
    provider_rows: list[dict[str, object]] = []
    static_rows: list[dict[str, object]] = []

    found = dry_run._append_analyzed_candidates(
        page_url="https://recover.example.com",
        html="<html></html>",
        row=_row(),
        index_url=INDEX_URL,
        recovery_source="same_party_recovery_url",
        provider_candidates=provider_rows,
        static_candidates=static_rows,
    )

    assert found is True
    assert len(provider_rows) == 1
    assert static_rows == []
    assert provider_rows[0]["gamedevmapRecoverySource"] == "same_party_recovery_url"


def test_gamedevmap_page_outcome_explicit_careers_beats_generic_static(monkeypatch) -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "https://recover.example.com/careers",
            "generic_static_candidate": {
                "adapter": "static",
                "listing_url": "https://recover.example.com/jobs",
            },
        }

    monkeypatch.setattr(dry_run, "analyze_fetched_page", analyze_page)
    provider_rows: list[dict[str, object]] = []
    static_rows: list[dict[str, object]] = []

    found = dry_run._append_analyzed_candidates(
        page_url="https://recover.example.com",
        html="<html></html>",
        row=_row(),
        index_url=INDEX_URL,
        recovery_source="same_party_recovery_url",
        provider_candidates=provider_rows,
        static_candidates=static_rows,
    )

    assert found is True
    assert provider_rows == []
    assert len(static_rows) == 1
    assert static_rows[0]["listing_url"] == "https://recover.example.com/careers"
    assert static_rows[0]["gamedevmapRecovery"] is True


def test_gamedevmap_page_outcome_no_candidate_returns_false(monkeypatch) -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "",
            "generic_static_candidate": None,
        }

    monkeypatch.setattr(dry_run, "analyze_fetched_page", analyze_page)
    provider_rows: list[dict[str, object]] = []
    static_rows: list[dict[str, object]] = []

    found = dry_run._append_analyzed_candidates(
        page_url="https://recover.example.com",
        html="<html></html>",
        row=_row(),
        index_url=INDEX_URL,
        recovery_source="same_party_recovery_url",
        provider_candidates=provider_rows,
        static_candidates=static_rows,
    )

    assert found is False
    assert provider_rows == []
    assert static_rows == []
