"""Tests for the homepage-analysis worker transport, and for the audit that uses it.

The transport runs a batch's analysis in a spawned worker and returns the same result the
in-process call would have produced; these tests pin the three shapes that matter and the audit's
own wiring into it:

* a batch handed over really runs out of process (the parent's call counter stays empty) and the
  receipt says so, with the worker's own wall and hand-off bytes;
* with the transport off, the batch is analyzed here and the receipt says why;
* a worker that cannot report falls back to in-process analysis for that batch and retires itself,
  so the audit cannot lose a batch to the boundary;
* a whole audit run with the transport on produces the same candidates, rejections and counts as
  the same run with it off, and publishes the receipt that says where the analysis ran.

Owner: `src/source_discovery/homepage_analysis_child.py` and its wiring in
`gamedevmap_active_dry_run`.
Verify: `python -m pytest tests/source_discovery/test_homepage_analysis_child.py -q`
"""

from __future__ import annotations

import json
import sys
from typing import Any, cast

from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery.gamedevmap_artifact_store import (
    ANALYSIS_WORKER_RECEIPT_FILENAME,
    homepage_analysis_child_receipt_path,
)
from src.source_discovery.homepage_analysis_child import (
    HOMEPAGE_ANALYSIS_CHILD_ENV,
    HomepageAnalysisChild,
    analysis_child_requested,
    analysis_child_supported,
)

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config as _config,
)
from .gamedevmap_test_helpers import (
    gamedevmap_fetcher as _fetcher,
)
from .gamedevmap_test_helpers import (
    gamedevmap_payloads as _payloads,
)

# Appended by the analysis below, in whichever process runs it. A spawned worker has its own copy,
# so an empty list in the parent is the proof that a batch was analyzed elsewhere.
PARENT_ANALYSIS_CALLS: list[str] = []


def _fake_homepage_analysis(
    results: list[dict[str, Any]],
    *,
    index_url: str = "",
) -> Any:
    """A stub for the real analysis: a mapping stands in for `ActiveHomepageBatchResult`."""
    """A stand-in analysis, module-level so the spawn context can import it by name."""
    PARENT_ANALYSIS_CALLS.append(str(index_url))
    first = results[0] if results else {}
    return {
        "pages": len(results),
        "firstUrl": str(first.get("url") or ""),
        "indexUrl": str(index_url),
    }


def _unserializable_homepage_analysis(
    results: list[dict[str, Any]],
    *,
    index_url: str = "",
) -> Any:
    """Returns something the worker cannot send back, which is the transport's failure path."""
    return {"pages": len(results), "callback": lambda: None, "indexUrl": index_url}


def _batch(count: int = 1) -> list[dict[str, Any]]:
    return [
        {"url": f"https://page{index}.example", "ok": True, "text": "<html></html>"}
        for index in range(1, count + 1)
    ]


def test_analysis_child_runs_the_batch_out_of_process_and_reports_it() -> None:
    PARENT_ANALYSIS_CALLS.clear()
    child = HomepageAnalysisChild(
        _fake_homepage_analysis,
        analysis_kwargs={"index_url": "https://index.example"},
        enabled=True,
    )
    try:
        handle = child.start(_batch(2))
        assert handle.offloaded is True

        assert child.collect(handle) == {
            "pages": 2,
            "firstUrl": "https://page1.example",
            "indexUrl": "https://index.example",
        }
        assert PARENT_ANALYSIS_CALLS == [], "the batch was analyzed in the parent, not the worker"

        receipt = child.receipt()
        assert receipt["mode"] == "child"
        assert receipt["disabledReason"] == ""
        assert receipt["summary"] == {
            "calls": 1,
            "offloaded": 1,
            "inlined": 0,
            "fallbacks": 0,
            "childWallS": receipt["summary"]["childWallS"],
            "childCpuS": receipt["summary"]["childCpuS"],
            "parentWaitS": receipt["summary"]["parentWaitS"],
            "parentHandoffS": receipt["summary"]["parentHandoffS"],
            "parentInlineS": 0.0,
            "handoffBytes": receipt["summary"]["handoffBytes"],
        }
        row = receipt["batches"][0]
        assert row["transport"] == "child"
        assert row["fellBack"] is False
        # The worker's cost is measured inside the worker (a two-page batch rounds to 0 ms) and the
        # hand-off really carried the batch: the parent's counter is empty above.
        assert row["childWallMs"] >= 0
        assert row["handoffBytes"] > 0
    finally:
        child.close()


def test_analysis_child_disabled_analyzes_in_this_process_and_says_why() -> None:
    PARENT_ANALYSIS_CALLS.clear()
    child = HomepageAnalysisChild(_fake_homepage_analysis, enabled=False)
    try:
        handle = child.start(_batch(1))
        assert handle.offloaded is False

        assert child.collect(handle) == {
            "pages": 1,
            "firstUrl": "https://page1.example",
            "indexUrl": "",
        }
        assert len(PARENT_ANALYSIS_CALLS) == 1

        receipt = child.receipt()
        assert receipt["mode"] == "inline"
        assert receipt["enabled"] is False
        assert receipt["disabledReason"] == "disabled"
        assert receipt["summary"]["inlined"] == 1
        assert receipt["summary"]["offloaded"] == 0
        assert receipt["batches"][0]["transport"] == "inline"
        assert receipt["batches"][0]["childWallMs"] == 0
    finally:
        child.close()


def test_analysis_child_falls_back_in_process_and_retires_on_a_worker_that_cannot_report() -> None:
    child = HomepageAnalysisChild(_unserializable_homepage_analysis, enabled=True)
    try:
        handle = child.start(_batch(1))
        assert handle.offloaded is True

        result = cast(dict[str, Any], child.collect(handle))
        assert result["pages"] == 1
        assert callable(result["callback"]), "the fallback must return the in-process object"

        receipt = child.receipt()
        assert receipt["batches"][0]["transport"] == "fallback"
        assert receipt["batches"][0]["fellBack"] is True
        assert receipt["summary"]["fallbacks"] == 1
        assert receipt["mode"] == "inline"
        assert receipt["disabledReason"] != ""

        # Retired for the rest of the run: the next batch never leaves this process.
        assert child.start(_batch(1)).offloaded is False
    finally:
        child.close()


def test_analysis_child_env_knob_and_support_decision(monkeypatch) -> None:
    monkeypatch.setenv(HOMEPAGE_ANALYSIS_CHILD_ENV, "0")
    assert analysis_child_requested() == (False, "disabled-by-env")

    monkeypatch.setenv(HOMEPAGE_ANALYSIS_CHILD_ENV, "1")
    supported, reason = analysis_child_supported()
    assert analysis_child_requested() == (supported, reason if not supported else "")

    monkeypatch.setattr(sys, "frozen", True, raising=False)
    assert analysis_child_supported() == (False, "frozen-runtime")
    assert analysis_child_requested()[0] is False


def test_audit_with_the_worker_matches_the_in_process_run_and_publishes_the_receipt(
    monkeypatch,
) -> None:
    """The overlap's whole point: same audit, analyzed elsewhere, and the artifact says where."""
    payloads = _payloads()
    monkeypatch.setenv(HOMEPAGE_ANALYSIS_CHILD_ENV, "0")
    with workspace_tmpdir("gamedevmap-analysis-overlap") as root:
        baseline_path = root / "in-process.json"
        baseline = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(payloads),
            output_path=baseline_path,
            batch_size=2,
            max_batches=0,
            reset=True,
        )
        baseline_receipt = json.loads(
            homepage_analysis_child_receipt_path(baseline_path).read_text(encoding="utf-8")
        )

        monkeypatch.setenv(HOMEPAGE_ANALYSIS_CHILD_ENV, "1")
        worker_path = root / "worker.json"
        worker = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=_fetcher(payloads),
            output_path=worker_path,
            batch_size=2,
            max_batches=0,
            reset=True,
        )
        worker_receipt = json.loads(
            (root / ANALYSIS_WORKER_RECEIPT_FILENAME).read_text(encoding="utf-8")
        )

    baseline_summary = baseline["summary"]
    worker_summary = worker["summary"]
    for key in (
        "eligibleRows",
        "completedUrls",
        "homepagesFetched",
        "homepageFetchAttempts",
        "recoveryPagesFetched",
        "providerCandidates",
        "staticCandidates",
        "activeCandidates",
        "zeroJobCandidates",
        "rejectedForActivation",
        "failures",
    ):
        assert worker_summary[key] == baseline_summary[key], key
    assert _candidate_keys(worker["allCandidates"]) == _candidate_keys(baseline["allCandidates"])
    assert _rejection_keys(worker["rejectedForActivation"]) == _rejection_keys(
        baseline["rejectedForActivation"]
    )

    # Both transports ran, and each receipt says which: an absent file cannot be told from a
    # transport that silently stayed in-process.
    assert baseline_receipt["homepageAnalysisChild"]["mode"] == "inline"
    assert baseline_receipt["homepageAnalysisChild"]["disabledReason"] == "disabled-by-env"
    child_receipt = worker_receipt["homepageAnalysisChild"]
    assert child_receipt["mode"] == "child"
    assert child_receipt["disabledReason"] == ""
    # One worker, several batches: the transport outlives the batch it was started for.
    assert child_receipt["summary"]["offloaded"] == child_receipt["summary"]["calls"] >= 2
    assert all(row["transport"] == "child" for row in child_receipt["batches"])
    assert child_receipt["summary"]["childWallS"] >= 0
    assert child_receipt["summary"]["handoffBytes"] > 0

    # The phase's own receipt still exists for every batch, and the batch timings say the analysis
    # ran: the parent's share is small, which is the overlap, not a missing phase.
    phases = worker_summary["phaseRuntime"]["phases"]
    assert any(row["phase"] == "homepage_analysis" for row in phases)
    assert worker["timings"]["batches"][0]["homepageAnalysisMs"] > 0


def _candidate_keys(rows: list[dict[str, Any]]) -> list[str]:
    return sorted(
        f"{row.get('adapter')}|{row.get('id') or row.get('careersUrl') or row.get('listing_url')}"
        for row in rows
    )


def _rejection_keys(rows: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for row in rows:
        raw_candidate = row.get("candidate")
        candidate: dict[str, Any] = raw_candidate if isinstance(raw_candidate, dict) else {}
        keys.append(
            f"{row.get('reason')}|{row.get('reasonDetail')}|"
            f"{candidate.get('id') or row.get('sourceId') or row.get('url')}"
        )
    return sorted(keys)


def test_homepage_analysis_child_receipt_path_sits_beside_the_artifact(tmp_path) -> None:
    artifact = tmp_path / "gamedevmap-active-dry-run.json"
    assert homepage_analysis_child_receipt_path(artifact).parent == tmp_path
    assert homepage_analysis_child_receipt_path(artifact).name.endswith(".json")
    assert dry_run.gamedevmap_active_dry_run_path().name != ANALYSIS_WORKER_RECEIPT_FILENAME
