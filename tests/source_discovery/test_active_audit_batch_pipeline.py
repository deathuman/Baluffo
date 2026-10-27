"""The pipelined active-audit loop: one batch's deferrable analysis, overlapped head to tail.

The batch runtime splits a batch into a head (prepare, homepage fetch, analysis hand-off) and a tail
(everything the analysis feeds). This file pins the loop's use of that split when the batch strategy
can defer its analysis: the head of batch N+1 runs before the tail of batch N, a strategy without a
transport keeps the sequential loop it always had, and the receipts and identities stay per batch.

Owner: `src/source_discovery/active_audit_loop_runtime.py` and the batch head/tail in
`active_audit_batch_runtime`.
Verify: `python -m pytest tests/source_discovery/test_active_audit_batch_pipeline.py -q`
"""

from __future__ import annotations

from dataclasses import replace

from src.source_discovery import active_audit_runtime
from src.source_discovery.active_audit_runtime import (
    ActiveAuditBatchStrategy,
    ActiveAuditCandidateMergeResult,
    ActiveAuditPreparedRows,
    ActiveAuditRecoveryApplicationResult,
    ActiveAuditRecoveryFetchResult,
    ActiveHomepageBatchResult,
    finish_active_audit_batch,
    run_active_audit_loop,
    start_active_audit_batch,
)


def _deferred_strategy(events: list[str]) -> ActiveAuditBatchStrategy:
    """A batch strategy that defers its analysis, recording every phase it is asked to run."""

    def _prepare(rows):
        events.append("prepare")
        return ActiveAuditPreparedRows(homepage_rows=rows)

    def _fetch_homepages(rows):
        events.append("fetch")
        return [
            {"ok": True, "url": row["url"], "payload": row, "text": "<html></html>"} for row in rows
        ]

    def _analysis_start(results):
        events.append("analysis_start")
        return {"pages": len(results)}

    def _analysis_collect(handle):
        events.append("analysis_collect")
        return ActiveHomepageBatchResult(homepages_fetched=int(handle["pages"]))

    def _fetch_recovery(jobs, label):
        events.append("fetch_wave2" if "wave 2" in label else "fetch_wave1")
        return ActiveAuditRecoveryFetchResult(unique_jobs=len(jobs), network_jobs=len(jobs))

    def _apply_recovery(results, grouped, finalize):
        events.append("apply_wave2" if finalize else "apply_wave1")
        return ActiveAuditRecoveryApplicationResult(
            grouped_state=grouped or {},
            recovered_homepages=set(),
        )

    def _merge(direct, provider, static, recovery_provider, recovery_static):
        events.append("merge")
        return ActiveAuditCandidateMergeResult(
            candidates=[*direct, *provider, *static, *recovery_provider, *recovery_static]
        )

    def _probe(candidates):
        events.append("probe")
        return []

    return ActiveAuditBatchStrategy(
        prepare_rows=_prepare,
        fetch_homepages=_fetch_homepages,
        analyze_homepages=lambda _results: ActiveHomepageBatchResult(),
        analysis_start=_analysis_start,
        analysis_collect=_analysis_collect,
        fetch_recovery=_fetch_recovery,
        apply_recovery=_apply_recovery,
        recovery_homepage_key=lambda job: str(job.get("payload", {}).get("homepageUrl") or ""),
        merge_candidates=_merge,
        merge_artifact_updates=lambda *_args: None,
        update_summary=lambda _counts: None,
        probe_candidates=_probe,
        apply_probe_results=lambda _results: None,
        row_identity=lambda row: str(row.get("url") or ""),
        append_timing=lambda _timing: None,
    )


def test_active_audit_batch_head_defers_the_analysis_and_the_tail_collects_it() -> None:
    events: list[str] = []
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    completed: set[str] = set()
    batch_rows = [{"url": "https://one.example"}, {"url": "https://two.example"}]
    strategy = _deferred_strategy(events)

    head = start_active_audit_batch(
        artifact=artifact,
        batch_rows=batch_rows,
        cursor=2,
        batch_number=1,
        strategy=strategy,
        defer_analysis=True,
    )

    assert head.deferred_analysis is True
    assert events == ["prepare", "fetch", "analysis_start"]
    assert "homepageAnalysisMs" not in dict(head.batch_timing)
    assert completed == set()

    result = finish_active_audit_batch(
        head,
        artifact=artifact,
        strategy=strategy,
        completed_identities=completed,
    )

    assert events[:4] == ["prepare", "fetch", "analysis_start", "analysis_collect"]
    assert "homepageAnalysisMs" in result.timing
    assert completed == {"https://one.example", "https://two.example"}


def test_active_audit_batch_analyzes_in_the_tail_when_the_caller_does_not_defer() -> None:
    events: list[str] = []
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    strategy = replace(
        _deferred_strategy(events),
        analysis_start=None,
        analysis_collect=None,
    )

    head = start_active_audit_batch(
        artifact=artifact,
        batch_rows=[{"url": "https://one.example"}],
        cursor=1,
        batch_number=1,
        strategy=strategy,
        defer_analysis=True,
    )
    assert head.deferred_analysis is False
    assert events == ["prepare", "fetch"]

    finish_active_audit_batch(
        head,
        artifact=artifact,
        strategy=strategy,
        completed_identities=set(),
    )
    assert "analysis_start" not in events


def test_active_audit_loop_runs_the_next_head_before_the_previous_tail() -> None:
    """The overlap itself: batch 2's fetch has started before batch 1's analysis is collected."""
    events: list[str] = []
    artifact: dict[str, object] = {"progress": {}, "timings": {}}
    completed: set[str] = set()
    writes: list[bool] = []
    rows = [
        {"url": "https://a.example"},
        {"url": "https://b.example"},
        {"url": "https://c.example"},
        {"url": "https://d.example"},
    ]
    loop_strategy = active_audit_runtime.build_active_audit_loop_strategy(
        artifact=artifact,
        row_identity=lambda row: str(row.get("url") or ""),
        batch_strategy=_deferred_strategy(events),
        completed_identities=completed,
        emit_batch_log=lambda *_args: None,
        before_write=lambda: events.append("before_write"),
        write_artifact=lambda complete: writes.append(bool(complete)),
    )

    result = run_active_audit_loop(
        artifact=artifact,
        source_rows=rows,
        completed_identities=completed,
        batch_size=2,
        max_batches=0,
        strategy=loop_strategy,
    )

    assert result.batches_run == 2
    assert result.complete is True
    assert completed == {row["url"] for row in rows}
    assert writes == [False, True, True]
    assert [event for event in events if event in {"analysis_start", "analysis_collect"}] == [
        "analysis_start",
        "analysis_start",
        "analysis_collect",
        "analysis_collect",
    ]
    starts = [index for index, event in enumerate(events) if event == "analysis_start"]
    collects = [index for index, event in enumerate(events) if event == "analysis_collect"]
    fetches = [index for index, event in enumerate(events) if event == "fetch"]
    # Batch 2's whole head -- its fetch included -- is what batch 1's analysis hides behind,
    # and batch 1's tail still finishes (and is written) before batch 2's.
    assert fetches[1] < starts[1] < collects[0] < collects[1]


def test_active_audit_loop_pauses_at_max_batches_without_starting_another_head() -> None:
    events: list[str] = []
    artifact: dict[str, object] = {"progress": {}, "timings": {}}
    completed: set[str] = set()
    writes: list[bool] = []
    loop_strategy = active_audit_runtime.build_active_audit_loop_strategy(
        artifact=artifact,
        row_identity=lambda row: str(row.get("url") or ""),
        batch_strategy=_deferred_strategy(events),
        completed_identities=completed,
        emit_batch_log=lambda *_args: None,
        before_write=lambda: events.append("before_write"),
        write_artifact=lambda complete: writes.append(bool(complete)),
    )

    result = run_active_audit_loop(
        artifact=artifact,
        source_rows=[{"url": "https://a.example"}, {"url": "https://b.example"}],
        completed_identities=completed,
        batch_size=1,
        max_batches=1,
        strategy=loop_strategy,
    )

    assert result.batches_run == 1
    assert result.complete is False
    assert events.count("prepare") == 1
    assert completed == {"https://a.example"}
    assert writes == [False]
