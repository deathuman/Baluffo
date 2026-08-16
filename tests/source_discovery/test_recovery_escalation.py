from unittest import mock

from src.source_discovery import recovery_escalation as re

from ._helpers import (
    sd,
    workspace_tmpdir,
)
from .gamedevmap_test_helpers import (
    CSV_URL,
)
from .gamedevmap_test_helpers import (
    gamedevmap_config as _config,
)
from .gamedevmap_test_helpers import (
    gamedevmap_csv_row as _csv_row,
)
from .gamedevmap_test_helpers import (
    gamedevmap_fetcher as _fetcher,
)


def test_provider_pattern_escalation_candidates_for_mythwright() -> None:
    row = {
        "studio": "Mythwright",
        "url": "https://mythwright.com",
        "reason": "no_careers_evidence",
        "reasonDetail": "recovery_fetch_failed",
    }
    candidates = re.provider_pattern_escalation_candidates(row, limit=4)

    assert candidates
    adapters = {c.get("adapter") for c in candidates}
    assert "workable" in adapters
    assert all(c.get("studio") == "Mythwright" for c in candidates)
    assert all(c.get("adapter") for c in candidates)


def test_provider_pattern_escalation_candidates_empty_without_studio() -> None:
    assert re.provider_pattern_escalation_candidates({}, limit=4) == []
    assert re.provider_pattern_escalation_candidates({"studio": "  "}, limit=4) == []
    assert re.provider_pattern_escalation_candidates({"studio": "X"}, limit=0) == []


def test_escalate_rejected_rows_splits_no_careers_rejections() -> None:
    no_careers = {"studio": "Mythwright", "reason": "no_careers_evidence"}
    other = {"studio": "Other", "reason": "homepage_fetch_failed"}

    escalated, remaining = re.escalate_rejected_rows([no_careers, other])

    assert escalated
    assert all(c.get("evidenceSource") == "recovery_escalation" for c in escalated)
    assert remaining == [other]


def test_escalate_rejected_rows_respects_max_rows() -> None:
    rows = [{"studio": f"Studio {i}", "reason": "no_careers_evidence"} for i in range(5)]

    escalated, remaining = re.escalate_rejected_rows(rows, max_rows=2, pattern_limit=4)

    assert len(escalated) > 0
    assert len(escalated) <= 2 * 4
    assert len(remaining) >= 3


def test_escalate_rejected_rows_disabled_returns_all_rejections() -> None:
    rows = [{"studio": "Studio 1", "reason": "no_careers_evidence"}]
    with mock.patch.object(re, "escalation_enabled", return_value=False):
        escalated, remaining = re.escalate_rejected_rows(rows)
    assert escalated == []
    assert remaining == rows


def test_enqueue_rejected_for_web_search_appends_bounded_rows(tmp_path) -> None:
    re.set_recheck_queue_path(tmp_path / "discovery-feed-recheck-queue.json")
    try:
        rows = [{"studio": f"Studio {i}", "reason": "no_careers_evidence"} for i in range(3)] + [
            {"studio": "Other", "reason": "homepage_fetch_failed"}
        ]

        count = re.enqueue_rejected_for_web_search(rows, max_rows=2)

        assert count == 2
        payload = (tmp_path / "discovery-feed-recheck-queue.json").read_text(encoding="utf-8")
        import json

        queued = json.loads(payload)
        assert [row["studio"] for row in queued] == ["Studio 0", "Studio 1"]

        # next bounded call appends the remaining eligible studio, dedupes queued ones
        count2 = re.enqueue_rejected_for_web_search(rows, max_rows=2)
        assert count2 == 1
        queued2 = json.loads(
            (tmp_path / "discovery-feed-recheck-queue.json").read_text(encoding="utf-8")
        )
        assert [row["studio"] for row in queued2] == ["Studio 0", "Studio 1", "Studio 2"]
    finally:
        re.set_recheck_queue_path(None)


def test_enqueue_rejected_for_web_search_tolerates_missing_parent() -> None:
    re.set_recheck_queue_path("")
    try:
        assert (
            re.enqueue_rejected_for_web_search([{"studio": "X", "reason": "no_careers_evidence"}])
            == 0
        )
    finally:
        re.set_recheck_queue_path(None)


def test_gamedevmap_dry_run_escalates_no_careers_studio_to_provider_patterns() -> None:
    payloads = {
        CSV_URL: _csv_row("Mythwright", "https://mythwright.com"),
        "https://mythwright.com": "<html><main>Games publisher</main></html>",
    }
    for path in (
        "/careers",
        "/jobs",
        "/join-us",
        "/work-with-us",
        "/company/careers",
        "/about/careers",
    ):
        payloads[f"https://mythwright.com{path}"] = "<html>No openings here</html>"

    with workspace_tmpdir("gamedevmap-active-dry-run-escalation") as root:
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(
                allowed_categories=["Developer", "Publisher"],
                activeAuditRecoveryEscalationEnabled=True,
            ),
            fetcher=_fetcher(payloads),
            output_path=root / "dry-run.json",
            batch_size=10,
            reset=True,
        )

    escalated = [
        row
        for row in output["allCandidates"]
        if str(row.get("evidenceSource") or "") == "recovery_escalation"
    ]
    assert escalated
    assert all(str(row.get("studio") or "") == "Mythwright" for row in escalated)
    assert any(row.get("adapter") == "workable" for row in escalated)
    assert all(
        str(row.get("reason") or "") != "no_careers_evidence"
        for row in output["rejectedForActivation"]
    )
