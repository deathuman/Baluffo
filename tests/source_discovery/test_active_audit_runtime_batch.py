from __future__ import annotations

from src.source_discovery import active_audit_runtime
from src.source_discovery.active_audit_runtime import (
    ActiveAuditBatchStrategy,
    ActiveAuditCandidateMergeResult,
    ActiveAuditLoopStrategy,
    ActiveAuditPreparedRows,
    ActiveAuditRecoveryApplicationResult,
    ActiveAuditRecoveryFetchResult,
    ActiveHomepageBatchResult,
    run_active_audit_batch,
    run_active_audit_loop,
)
from tests.helpers.mutation import append_and_return


def test_active_audit_batch_sequences_recovery_probe_and_progress() -> None:
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    completed: set[str] = set()
    fetch_labels: list[str] = []
    summary_updates: list[dict[str, object]] = []
    probed_candidates: list[list[dict[str, object]]] = []
    applied_probe_results: list[object] = []
    appended_timing: list[dict[str, object]] = []
    merged_updates: list[dict[str, object]] = []

    result = run_active_audit_batch(
        artifact=artifact,
        batch_rows=[{"url": "https://one.example"}, {"url": "https://two.example"}],
        cursor=2,
        batch_number=1,
        strategy=ActiveAuditBatchStrategy(
            prepare_rows=lambda rows: ActiveAuditPreparedRows(
                direct_provider_candidates=[{"adapter": "greenhouse", "url": "direct"}],
                homepage_rows=rows,
                rejected_rows=[{"reason": "missing"}],
            ),
            fetch_homepages=lambda rows: [
                {"ok": True, "url": row["url"], "payload": row, "text": "<html></html>"}
                for row in rows
            ],
            analyze_homepages=lambda results: ActiveHomepageBatchResult(
                provider_candidates=[{"adapter": "lever", "url": "homepage-provider"}],
                static_candidates=[{"adapter": "static", "url": "homepage-static"}],
                primary_recovery_jobs=[{"url": "https://one.example/careers"}],
                secondary_recovery_jobs=[
                    {"url": "https://one.example/jobs", "payload": {"homepageUrl": "h1"}},
                    {"url": "https://two.example/jobs", "payload": {"homepageUrl": "h2"}},
                ],
                browser_recovery_candidates=[{"url": "https://shell.example"}],
                homepages_fetched=2,
            ),
            fetch_recovery=lambda jobs, label: append_and_return(
                fetch_labels,
                label,
                ActiveAuditRecoveryFetchResult(
                    results=[
                        {"url": job["url"], "payload": job.get("payload", {})} for job in jobs
                    ],
                    unique_jobs=len(jobs),
                    network_jobs=len(jobs),
                ),
            ),
            apply_recovery=lambda results, grouped, finalize: ActiveAuditRecoveryApplicationResult(
                provider_candidates=[] if finalize else [{"adapter": "ashby", "url": "wave1"}],
                static_candidates=[{"adapter": "static", "url": "wave2"}] if finalize else [],
                rejected_rows=[{"reason": "recovery_miss"}] if finalize else [],
                failures=[{"stage": "recovery_fetch"}] if finalize else [],
                pages_fetched=len(results),
                grouped_state={"wave1": True},
                recovered_homepages={"h1"} if not finalize else {"h2"},
            ),
            recovery_homepage_key=lambda job: str(job.get("payload", {}).get("homepageUrl") or ""),
            merge_candidates=lambda direct, provider, static, recovery_provider, recovery_static: (
                ActiveAuditCandidateMergeResult(
                    candidates=[
                        *direct,
                        *provider,
                        *static,
                        *recovery_provider,
                        *recovery_static,
                    ],
                    rejected_rows=[{"reason": "bad_provider"}],
                )
            ),
            merge_artifact_updates=lambda candidates, browser, homepage_failures, recovery_failures, rejected: (
                merged_updates.append(
                    {
                        "candidates": candidates,
                        "browser": browser,
                        "homepageFailures": homepage_failures,
                        "recoveryFailures": recovery_failures,
                        "rejected": rejected,
                    }
                )
            ),
            update_summary=summary_updates.append,
            probe_candidates=lambda candidates: append_and_return(
                probed_candidates, candidates, [{"candidate": candidates[0]}]
            ),
            apply_probe_results=applied_probe_results.append,
            row_identity=lambda row: str(row.get("url") or ""),
            append_timing=appended_timing.append,
        ),
        completed_identities=completed,
    )

    assert fetch_labels == [
        "GameDevMap active dry run careers recovery fetch wave 1",
        "GameDevMap active dry run careers recovery fetch wave 2",
    ]
    assert result.recovered_homepages == {"h1", "h2"}
    assert result.timing["recoverySkippedByWave1"] == 1
    assert result.timing["recoverySkippedByGeneratedNotFound"] == 0
    assert result.timing["primaryRecoveryJobs"] == 1
    assert result.timing["secondaryRecoveryJobs"] == 1
    assert summary_updates == [
        {
            "homepageFetchAttempts": 2,
            "homepagesFetched": 2,
            "recoveryFetchAttempts": 2,
            "recoveryUniqueFetchAttempts": 2,
            "recoveryNetworkFetchAttempts": 2,
            "recoveryPagesFetched": 2,
        }
    ]
    assert completed == {"https://one.example", "https://two.example"}
    assert artifact["progress"] == {"batchesCompleted": 1}
    assert probed_candidates == [result.candidates]
    assert applied_probe_results == [[{"candidate": result.candidates[0]}]]
    assert appended_timing == [result.timing]
    assert merged_updates[0]["rejected"] == [
        {"reason": "missing"},
        {"reason": "recovery_miss"},
        {"reason": "bad_provider"},
    ]


def test_active_audit_batch_prunes_secondary_generated_paths_after_primary_not_found() -> None:
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    completed: set[str] = set()
    recovery_fetches: list[list[str]] = []

    def fetch_recovery(jobs, _label):
        recovery_fetches.append([str(job.get("url") or "") for job in jobs])
        if len(recovery_fetches) == 1:
            return ActiveAuditRecoveryFetchResult(
                results=[
                    {
                        "url": job["url"],
                        "payload": job.get("payload", {}),
                        "ok": False,
                        "error": "Client error '404 Not Found'",
                    }
                    for job in jobs
                ],
                unique_jobs=len(jobs),
                network_jobs=len(jobs),
            )
        return ActiveAuditRecoveryFetchResult(
            results=[
                {
                    "url": job["url"],
                    "payload": job.get("payload", {}),
                    "ok": True,
                    "text": "<html></html>",
                }
                for job in jobs
            ],
            unique_jobs=len(jobs),
            network_jobs=len(jobs),
        )

    result = run_active_audit_batch(
        artifact=artifact,
        batch_rows=[{"url": "https://one.example"}],
        cursor=1,
        batch_number=1,
        strategy=ActiveAuditBatchStrategy(
            prepare_rows=lambda rows: ActiveAuditPreparedRows(homepage_rows=rows),
            fetch_homepages=lambda rows: [
                {"ok": True, "url": row["url"], "payload": row, "text": "<html></html>"}
                for row in rows
            ],
            analyze_homepages=lambda _results: ActiveHomepageBatchResult(
                primary_recovery_jobs=[
                    {
                        "url": "https://one.example/careers",
                        "payload": {
                            "homepageUrl": "https://one.example",
                            "recoveryUrlSource": "generated_common_path",
                        },
                        "recoveryUrlSource": "generated_common_path",
                    }
                ],
                secondary_recovery_jobs=[
                    {
                        "url": "https://one.example/join-us",
                        "payload": {
                            "homepageUrl": "https://one.example",
                            "recoveryUrlSource": "generated_common_path",
                        },
                        "recoveryUrlSource": "generated_common_path",
                    },
                    {
                        "url": "https://one.example/jobs",
                        "payload": {
                            "homepageUrl": "https://one.example",
                            "recoveryUrlSource": "html_jobish_link",
                        },
                        "recoveryUrlSource": "html_jobish_link",
                    },
                ],
            ),
            fetch_recovery=fetch_recovery,
            apply_recovery=lambda results, grouped, finalize: ActiveAuditRecoveryApplicationResult(
                pages_fetched=sum(1 for result in results if result.get("ok")),
                failures=[
                    {"stage": "gamedevmap_recovery_fetch"}
                    for result in results
                    if not result.get("ok")
                ],
                grouped_state=grouped or {},
                recovered_homepages=set(),
            ),
            recovery_homepage_key=lambda job: str(job.get("payload", {}).get("homepageUrl") or ""),
            merge_candidates=lambda direct, provider, static, recovery_provider, recovery_static: (
                ActiveAuditCandidateMergeResult(candidates=[])
            ),
            merge_artifact_updates=lambda *_args: None,
            update_summary=lambda _summary: None,
            probe_candidates=lambda candidates: [],
            apply_probe_results=lambda _results: None,
            row_identity=lambda row: str(row.get("url") or ""),
            append_timing=lambda _timing: None,
        ),
        completed_identities=completed,
    )

    assert recovery_fetches == [
        ["https://one.example/careers"],
        ["https://one.example/jobs"],
    ]
    assert result.timing["recoverySkippedByGeneratedNotFound"] == 1
    assert result.timing["secondaryRecoveryJobs"] == 1


def test_active_audit_batch_keeps_secondary_paths_only_where_wave1_left_the_question_open() -> None:
    """A generated guess that answered settles the host; a guess that was not read does not.

    The third case of one question, beside `recoverySkippedByGeneratedNotFound`: measured on the
    audit's own population this drops 928 of 1,304 second-wave requests (232 of 326 hosts), and the
    wave's one candidate in either of two full runs came from a host of the kept kind.
    """
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}, "timings": {}}
    recovery_fetches: list[list[str]] = []

    def job(url: str, homepage: str, source: str) -> dict[str, object]:
        return {
            "url": url,
            "payload": {"homepageUrl": homepage, "recoveryUrlSource": source},
            "recoveryUrlSource": source,
        }

    def result(
        url: str, homepage: str, source: str, ok: bool, error: str = ""
    ) -> dict[str, object]:
        return {
            "url": url,
            "payload": {"homepageUrl": homepage, "recoveryUrlSource": source},
            "ok": ok,
            "text": "<html></html>" if ok else "",
            "error": error,
        }

    def fetch_recovery(jobs, _label):
        recovery_fetches.append([str(job_.get("url") or "") for job_ in jobs])
        if len(recovery_fetches) == 1:
            return ActiveAuditRecoveryFetchResult(
                results=[
                    # Its own generated guess answered: the site's careers page has been read.
                    result(
                        "https://guessed.example/careers",
                        "https://guessed.example",
                        "generated_common_path",
                        ok=True,
                    ),
                    # Every generated guess 404'd, but the homepage linked a job-ish page that
                    # answered: wave 2 still has somewhere to look.
                    result(
                        "https://linked.example/careers",
                        "https://linked.example",
                        "generated_common_path",
                        ok=False,
                        error="Client error '404 Not Found'",
                    ),
                    result(
                        "https://linked.example/joinus",
                        "https://linked.example",
                        "html_jobish_link",
                        ok=True,
                    ),
                    # Nothing about this host could be read at all.
                    result(
                        "https://silent.example/careers",
                        "https://silent.example",
                        "generated_common_path",
                        ok=False,
                        error="Client error '403 Forbidden'",
                    ),
                ],
                unique_jobs=len(jobs),
                network_jobs=len(jobs),
            )
        return ActiveAuditRecoveryFetchResult(results=[], unique_jobs=0, network_jobs=0)

    result_row = run_active_audit_batch(
        artifact=artifact,
        batch_rows=[
            {"url": "https://guessed.example"},
            {"url": "https://linked.example"},
            {"url": "https://silent.example"},
        ],
        cursor=3,
        batch_number=1,
        strategy=ActiveAuditBatchStrategy(
            prepare_rows=lambda rows: ActiveAuditPreparedRows(homepage_rows=rows),
            fetch_homepages=lambda rows: [
                {"ok": True, "url": row["url"], "payload": row, "text": "<html></html>"}
                for row in rows
            ],
            analyze_homepages=lambda _results: ActiveHomepageBatchResult(
                primary_recovery_jobs=[
                    job(
                        "https://guessed.example/careers",
                        "https://guessed.example",
                        "generated_common_path",
                    ),
                    job(
                        "https://linked.example/careers",
                        "https://linked.example",
                        "generated_common_path",
                    ),
                    job(
                        "https://silent.example/careers",
                        "https://silent.example",
                        "generated_common_path",
                    ),
                ],
                secondary_recovery_jobs=[
                    job(
                        "https://guessed.example/about/careers",
                        "https://guessed.example",
                        "generated_common_path",
                    ),
                    job(
                        "https://guessed.example/join-us",
                        "https://guessed.example",
                        "generated_common_path",
                    ),
                    job(
                        "https://linked.example/about/careers",
                        "https://linked.example",
                        "generated_common_path",
                    ),
                    job(
                        "https://silent.example/about/careers",
                        "https://silent.example",
                        "generated_common_path",
                    ),
                ],
            ),
            fetch_recovery=fetch_recovery,
            apply_recovery=lambda results, grouped, finalize: ActiveAuditRecoveryApplicationResult(
                pages_fetched=sum(1 for result in results if result.get("ok")),
                grouped_state=grouped or {},
                recovered_homepages=set(),
            ),
            recovery_homepage_key=lambda job: str(job.get("payload", {}).get("homepageUrl") or ""),
            merge_candidates=lambda *_args: ActiveAuditCandidateMergeResult(candidates=[]),
            merge_artifact_updates=lambda *_args: None,
            update_summary=lambda _summary: None,
            probe_candidates=lambda _candidates: [],
            apply_probe_results=lambda _results: None,
            row_identity=lambda row: str(row.get("url") or ""),
            append_timing=lambda _timing: None,
        ),
        completed_identities=set(),
    )

    assert recovery_fetches == [
        [
            "https://guessed.example/careers",
            "https://linked.example/careers",
            "https://silent.example/careers",
        ],
        [
            "https://linked.example/about/careers",
            "https://silent.example/about/careers",
        ],
    ]
    assert result_row.timing["recoverySkippedByGeneratedAnswered"] == 2
    assert result_row.timing["recoverySkippedByGeneratedNotFound"] == 0
    assert result_row.timing["secondaryRecoveryJobs"] == 2


def test_active_audit_loop_writes_complete_when_no_rows_remain() -> None:
    artifact: dict[str, object] = {"progress": {}}
    order: list[str] = []

    result = run_active_audit_loop(
        artifact=artifact,
        source_rows=[{"url": "https://one.example"}],
        completed_identities={"https://one.example"},
        batch_size=10,
        max_batches=0,
        strategy=ActiveAuditLoopStrategy(
            row_identity=lambda row: str(row.get("url") or ""),
            emit_batch_log=lambda batch, rows, cursor: order.append("emit"),
            run_batch=lambda rows, cursor, batch: order.append("batch"),
            before_write=lambda: order.append("before_write"),
            write_artifact=lambda complete: order.append(f"write:{complete}"),
        ),
    )

    assert result.complete is True
    assert result.batches_run == 0
    assert artifact["progress"] == {"cursorPosition": 1}
    assert order == ["before_write", "write:True"]


def test_active_audit_loop_stops_after_max_batches_and_writes_incomplete() -> None:
    artifact: dict[str, object] = {"progress": {}}
    completed: set[str] = set()
    order: list[str] = []
    batch_calls: list[tuple[int, list[str], int]] = []

    def _run_batch(rows: list[dict[str, object]], cursor: int, batch_number: int) -> None:
        batch_calls.append((batch_number, [str(row["url"]) for row in rows], cursor))
        completed.update(str(row["url"]) for row in rows)
        order.append("batch")

    result = run_active_audit_loop(
        artifact=artifact,
        source_rows=[
            {"url": "https://one.example"},
            {"url": "https://two.example"},
            {"url": "https://three.example"},
        ],
        completed_identities=completed,
        batch_size=2,
        max_batches=1,
        strategy=ActiveAuditLoopStrategy(
            row_identity=lambda row: str(row.get("url") or ""),
            emit_batch_log=lambda batch, rows, cursor: order.append(
                f"emit:{batch}:{rows}:{cursor}"
            ),
            run_batch=_run_batch,
            before_write=lambda: order.append("before_write"),
            write_artifact=lambda complete: order.append(f"write:{complete}"),
        ),
    )

    assert result.complete is False
    assert result.batches_run == 1
    assert result.completed_identities == {"https://one.example", "https://two.example"}
    assert artifact["progress"] == {"cursorPosition": 0}
    assert batch_calls == [
        (1, ["https://one.example", "https://two.example"], 0),
    ]
    assert order == ["emit:1:2:0", "batch", "before_write", "write:False"]


def test_active_audit_batch_strategy_builder_wires_callbacks() -> None:
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 0}}
    completed: set[str] = set()
    events: list[str] = []

    strategy = active_audit_runtime.build_active_audit_batch_strategy(
        prepare_rows=lambda rows: append_and_return(
            events, "prepare", ActiveAuditPreparedRows(homepage_rows=rows)
        ),
        fetch_homepages=lambda rows: append_and_return(
            events,
            "fetch",
            [{"ok": True, "url": row["url"], "payload": row, "text": ""} for row in rows],
        ),
        analyze_homepages=lambda _results: append_and_return(
            events, "analyze", ActiveHomepageBatchResult(homepages_fetched=1)
        ),
        fetch_recovery=lambda _jobs, label: append_and_return(
            events, f"recover:{label[-6:]}", ActiveAuditRecoveryFetchResult()
        ),
        apply_recovery=lambda _results, _grouped, finalize: append_and_return(
            events, f"apply:{finalize}", ActiveAuditRecoveryApplicationResult()
        ),
        recovery_homepage_key=lambda _job: "",
        merge_candidates=lambda direct, provider, static, recovery_provider, recovery_static: (
            append_and_return(
                events,
                "merge",
                ActiveAuditCandidateMergeResult(
                    candidates=[
                        *direct,
                        *provider,
                        *static,
                        *recovery_provider,
                        *recovery_static,
                    ]
                ),
            )
        ),
        merge_artifact_updates=lambda *_args: events.append("artifact"),
        update_summary=lambda _counts: events.append("summary"),
        probe_candidates=lambda _candidates: append_and_return(events, "probe", []),
        apply_probe_results=lambda _results: events.append("probe_apply"),
        row_identity=lambda row: str(row.get("url") or ""),
        append_timing=lambda _timing: events.append("timing"),
    )

    run_active_audit_batch(
        artifact=artifact,
        batch_rows=[{"url": "https://one.example"}],
        cursor=0,
        batch_number=1,
        strategy=strategy,
        completed_identities=completed,
    )

    assert events == [
        "prepare",
        "fetch",
        "analyze",
        "recover:wave 1",
        "apply:False",
        "recover:wave 2",
        "apply:True",
        "merge",
        "artifact",
        "summary",
        "probe",
        "probe_apply",
        "timing",
    ]
    assert completed == {"https://one.example"}


def test_active_audit_loop_strategy_builder_runs_batches_from_artifact_progress(
    monkeypatch,
) -> None:
    artifact: dict[str, object] = {"progress": {"batchesCompleted": 4}}
    completed: set[str] = set()
    batch_numbers: list[int] = []
    order: list[str] = []

    def fake_run_active_audit_batch(**kwargs) -> None:
        batch_numbers.append(kwargs["batch_number"])
        kwargs["completed_identities"].update(str(row["url"]) for row in kwargs["batch_rows"])
        progress = dict(kwargs["artifact"].get("progress") or {})
        progress["batchesCompleted"] = kwargs["batch_number"]
        kwargs["artifact"]["progress"] = progress

    monkeypatch.setattr(
        active_audit_runtime,
        "run_active_audit_batch",
        fake_run_active_audit_batch,
    )

    loop_strategy = active_audit_runtime.build_active_audit_loop_strategy(
        artifact=artifact,
        row_identity=lambda row: str(row.get("url") or ""),
        batch_strategy=ActiveAuditBatchStrategy(
            prepare_rows=lambda rows: ActiveAuditPreparedRows(homepage_rows=rows),
            fetch_homepages=lambda _rows: [],
            analyze_homepages=lambda _results: ActiveHomepageBatchResult(),
            fetch_recovery=lambda _jobs, _label: ActiveAuditRecoveryFetchResult(),
            apply_recovery=lambda _results, _grouped, _finalize: (
                ActiveAuditRecoveryApplicationResult()
            ),
            recovery_homepage_key=lambda _job: "",
            merge_candidates=lambda *_args: ActiveAuditCandidateMergeResult(),
            merge_artifact_updates=lambda *_args: None,
            update_summary=lambda _counts: None,
            probe_candidates=lambda _candidates: [],
            apply_probe_results=lambda _results: None,
            row_identity=lambda row: str(row.get("url") or ""),
            append_timing=lambda _timing: None,
        ),
        completed_identities=completed,
        emit_batch_log=lambda batch, rows, cursor: order.append(f"emit:{batch}:{rows}:{cursor}"),
        before_write=lambda: order.append("before_write"),
        write_artifact=lambda complete: order.append(f"write:{complete}"),
    )

    result = run_active_audit_loop(
        artifact=artifact,
        source_rows=[{"url": "https://one.example"}],
        completed_identities=completed,
        batch_size=1,
        max_batches=1,
        strategy=loop_strategy,
    )

    assert batch_numbers == [5]
    assert result.complete is True
    assert order == ["emit:1:1:0", "before_write", "write:True"]
