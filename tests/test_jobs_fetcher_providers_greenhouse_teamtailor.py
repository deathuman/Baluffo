"""Tests for jobs fetcher providers Greenhouse and Teamtailor runtime behavior."""

from unittest import mock

from src import jobs_fetcher as jf
from src.jobs import registry as jobs_registry
from tests.helpers.job_fixtures import _fixture


def test_normalize_source_report_row_preserves_structured_details_and_site_changed_urls() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "lever_sources",
            "status": "ok",
            "browserEscalationEligible": True,
            "browserEscalationEligibilityReason": "js_required",
            "browserEscalationEnabled": True,
            "details": [
                {
                    "adapter": "lever",
                    "studio": "Jagex",
                    "name": "Jagex (Lever)",
                    "status": "ok",
                    "fetchedCount": 3,
                    "keptCount": 2,
                    "durationMs": 123,
                    "fetchMs": 100,
                    "parseMs": 23,
                    "error": "",
                    "slug": "jagex",
                    "providerUrl": "https://jobs.lever.co/jagex",
                    "browserEscalationEligible": False,
                    "browserEscalationEnabled": True,
                }
            ],
        }
    )
    assert row["browserEscalationEligible"] is True
    assert row["browserEscalationEligibilityReason"] == "js_required"
    assert row["browserEscalationEnabled"] is True
    details = row.get("details")
    assert isinstance(details, list)
    assert isinstance(details[0], dict)
    assert details[0]["name"] == "Jagex (Lever)"
    assert int(details[0]["keptCount"]) == 2
    assert int(details[0]["durationMs"]) == 123
    assert int(details[0]["fetchMs"]) == 100
    assert int(details[0]["parseMs"]) == 23
    assert details[0]["slug"] == "jagex"
    assert details[0]["providerUrl"] == "https://jobs.lever.co/jagex"
    assert details[0]["browserEscalationEligible"] is False
    assert details[0]["browserEscalationEnabled"] is True

    site_changed = jf.normalize_source_report_row(
        {
            "name": "greenhouse_boards",
            "status": "ok",
            "adapter": "greenhouse",
            "failureBucket": "site_changed",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    )
    assert (
        str(site_changed.get("providerUrl") or "")
        == "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true"
    )

    non_site_changed = jf.normalize_source_report_row(
        {
            "name": "greenhouse_boards",
            "status": "ok",
            "adapter": "greenhouse",
            "failureBucket": "needs_review",
            "providerUrl": "https://boards-api.greenhouse.io/v1/boards/guerrillagames/jobs?content=true",
        }
    )
    assert "providerUrl" not in non_site_changed


def test_run_greenhouse_boards_source_with_fixture() -> None:
    payload = _fixture("greenhouse_guerrilla_jobs.json")
    previous = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Guerrilla Games",
            "studio": "Guerrilla Games",
            "adapter": "greenhouse",
            "slug": "guerrilla-games",
            "enabledByDefault": True,
        }
    ]

    try:
        with mock.patch.object(
            jobs_registry, "STUDIO_SOURCE_REGISTRY", list(jf.STUDIO_SOURCE_REGISTRY)
        ):

            def fake_fetch(url: str, _: int) -> str:
                assert url.split("/", 3)[2] == "boards-api.greenhouse.io"
                assert "guerrilla-games" in url
                return payload

            rows = jf.run_greenhouse_boards_source(
                fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
            )
            assert len(rows) == 2
            assert any("guerrilla-games/jobs/" in row["jobLink"] for row in rows)
    finally:
        jf.STUDIO_SOURCE_REGISTRY = previous


def test_run_teamtailor_source_with_fixture() -> None:
    listing = _fixture("teamtailor_listing.html")
    detail = _fixture("teamtailor_job.html")

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://career.paradoxplaza.com/jobs":
            return listing
        if "/jobs/" in url:
            return detail
        raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_teamtailor_sources_source(
            fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0
        )
        assert len(rows) >= 1
        assert any("career.paradoxplaza.com/jobs/" in row["jobLink"] for row in rows)
