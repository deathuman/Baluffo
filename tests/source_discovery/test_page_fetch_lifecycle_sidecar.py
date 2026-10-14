from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

from src.shared.page_fetch_lifecycle import PageFetchLifecycle, PageFetchLifecycleContext
from src.source_discovery import directory_audit
from src.source_discovery import page_fetch_lifecycle_sidecar as sidecar
from src.source_discovery import web_search_candidates as web_candidates

from ._helpers import workspace_tmpdir


def _clean_snapshot(
    *,
    batch_id: str,
    source: str = "web_search",
    run_id: str = "run-1",
    phase: str = "page_fetch",
    submitted: int = 2,
    finished: int = 2,
) -> dict[str, Any]:
    """A settled receipt shaped exactly like PageFetchLifecycle.snapshot()."""
    return {
        "schemaVersion": 1,
        "receiptType": "pageFetchLifecycle",
        "batchId": batch_id,
        "source": source,
        "runId": run_id,
        "phase": phase,
        "transport": "sync",
        "deadlineKind": "total_batch",
        "deadlineSeconds": 30,
        "deadlineExit": "completed",
        "ownedClientState": "not_applicable",
        "submitted": submitted,
        "started": submitted,
        "finished": finished,
        "failed": 0,
        "cancelledBeforeStart": 0,
        "queuedNow": submitted - submitted,
        "outstandingNow": submitted - finished,
        "asyncCancellationRequested": 0,
        "asyncCleanupFinished": 0,
    }


def _real_snapshot(**kwargs: Any) -> dict[str, Any]:
    """A snapshot produced by the real lifecycle, proving the sidecar reads the shipped shape."""
    context = PageFetchLifecycleContext(
        run_id=kwargs.get("run_id", "run-real"), source=kwargs.get("source", "gameprog")
    )
    lifecycle: PageFetchLifecycle = context.new_receipt(kwargs.get("phase", "directory_page_fetch"))
    lifecycle.start_batch(transport="sync", deadline_seconds=30)
    tickets = [lifecycle.submit() for _ in range(kwargs.get("workers", 2))]
    for ticket in tickets:
        ticket.start()
        ticket.finish()
    lifecycle.mark_batch_exit("completed")
    return dict(lifecycle.snapshot())


def _artifact(*snapshots: dict[str, Any]) -> dict[str, Any]:
    return {"schemaVersion": 1, "adapter": "test_adapter", "pageFetchLifecycle": list(snapshots)}


def test_sidecar_path_is_a_fixed_sibling() -> None:
    assert sidecar.sidecar_path_for_artifact(Path("data/gameprog-discovery-audit.json")) == Path(
        "data/page-fetch-lifecycle.v1.json"
    )
    # Three fixed audits sharing data/ must resolve to the same envelope path.
    assert sidecar.sidecar_path_for_artifact(
        Path("data/gamesmap-discovery-audit.json")
    ) == sidecar.sidecar_path_for_artifact(Path("data/web-search-discovery-audit.json"))


def test_build_sidecar_round_trips_real_lifecycle_snapshots() -> None:
    snapshot = _real_snapshot(run_id="run-real", source="gameprog", workers=3)
    envelope = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(snapshot), Path("data/gameprog-discovery-audit.json")
    )

    assert envelope is not None
    assert envelope["schemaVersion"] == 1
    assert envelope["receiptType"] == "pageFetchLifecycleSidecar"
    assert envelope["runId"] == "run-real"
    assert envelope["artifactName"] == "gameprog-discovery-audit.json"
    assert envelope["coverage"] == "full"
    assert envelope["truncated"] is False
    assert envelope["omittedCount"] == 0

    entry = envelope["sources"]["gameprog"]
    assert entry["source"] == "gameprog"
    assert entry["runId"] == "run-real"
    assert entry["artifactName"] == "gameprog-discovery-audit.json"
    assert entry["state"] == "clean"
    assert entry["coverage"] == "full"
    assert entry["truncated"] is False
    assert entry["receiptCount"] == 1
    assert entry["receipts"] == [snapshot]
    assert entry["rollup"]["batchCount"] == 1
    assert entry["rollup"]["submitted"] == 3
    assert entry["rollup"]["finished"] == 3
    assert entry["rollup"]["state"] == "clean"


def test_build_sidecar_dedupes_batches_first_wins() -> None:
    first = _clean_snapshot(batch_id="1", finished=1)
    duplicate = _clean_snapshot(batch_id="1", finished=99)
    second = _clean_snapshot(batch_id="2")

    envelope = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(first, duplicate, second), Path("data/web-search-discovery-audit.json")
    )

    assert envelope is not None
    entry = envelope["sources"]["web_search"]
    assert entry["receiptCount"] == 2
    assert [r["batchId"] for r in entry["receipts"]] == ["1", "2"]
    # First-wins: the duplicate's finished=99 never reaches the rollup.
    assert entry["rollup"]["finished"] == 1 + 2


def test_build_sidecar_groups_phases_of_one_source_together() -> None:
    envelope = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(
            _clean_snapshot(batch_id="1", phase="directory_page_fetch"),
            _clean_snapshot(batch_id="1", phase="recovery_page_fetch", finished=1),
        ),
        Path("data/gameprog-discovery-audit.json"),
    )

    assert envelope is not None
    entry = envelope["sources"]["web_search"]
    assert entry["receiptCount"] == 2
    assert entry["rollup"]["batchCount"] == 2


def test_build_sidecar_returns_none_without_receipts() -> None:
    path = Path("data/web-search-discovery-audit.json")
    assert sidecar.build_page_fetch_lifecycle_sidecar({}, path) is None
    assert sidecar.build_page_fetch_lifecycle_sidecar({"pageFetchLifecycle": []}, path) is None
    assert (
        sidecar.build_page_fetch_lifecycle_sidecar(
            {"pageFetchLifecycle": [{"not": "a receipt"}]}, path
        )
        is None
    )
    assert (
        sidecar.build_page_fetch_lifecycle_sidecar({"pageFetchLifecycle": ["not-a-dict"]}, path)
        is None
    )


def test_build_sidecar_returns_none_when_receipts_disagree_on_run_id() -> None:
    envelope = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(
            _clean_snapshot(batch_id="1", run_id="run-a"),
            _clean_snapshot(batch_id="2", run_id="run-b"),
        ),
        Path("data/web-search-discovery-audit.json"),
    )
    assert envelope is None


def test_publish_sidecar_writes_readable_envelope_and_never_touches_the_artifact() -> None:
    with workspace_tmpdir("sidecar-publish") as root:
        output_path = root / "web-search-discovery-audit.json"
        artifact = _artifact(_clean_snapshot(batch_id="1", run_id="run-1"))
        output_path.write_text(json.dumps(artifact), encoding="utf-8")
        before = output_path.read_bytes()

        published = sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=artifact, output_path=output_path
        )

        assert published is not None
        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)
        assert sidecar_path.exists()
        assert output_path.read_bytes() == before
        assert json.loads(sidecar_path.read_text(encoding="utf-8")) == published
        reloaded = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert reloaded is not None
        assert reloaded["runId"] == "run-1"
        assert reloaded["sources"]["web_search"]["receiptCount"] == 1


def test_publish_sidecar_writes_nothing_without_receipts() -> None:
    with workspace_tmpdir("sidecar-no-receipts") as root:
        output_path = root / "web-search-discovery-audit.json"
        assert (
            sidecar.publish_page_fetch_lifecycle_sidecar(
                artifact={"schemaVersion": 1}, output_path=output_path
            )
            is None
        )
        assert not sidecar.sidecar_path_for_artifact(output_path).exists()


def test_publish_sidecar_same_run_merges_sources_and_preserves_other_sources() -> None:
    with workspace_tmpdir("sidecar-same-run-merge") as root:
        output_path = root / "web-search-discovery-audit.json"
        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)

        sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(_clean_snapshot(batch_id="1", source="gameprog", run_id="run-1")),
            output_path=output_path,
        )
        sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(_clean_snapshot(batch_id="2", source="gamesmap", run_id="run-1")),
            output_path=output_path,
        )

        merged = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert merged is not None
        assert merged["runId"] == "run-1"
        assert set(merged["sources"]) == {"gameprog", "gamesmap"}
        assert merged["sources"]["gameprog"]["receipts"][0]["batchId"] == "1"
        assert merged["sources"]["gamesmap"]["receipts"][0]["batchId"] == "2"


def test_publish_sidecar_newer_run_replaces_older_envelope() -> None:
    with workspace_tmpdir("sidecar-newer-run") as root:
        output_path = root / "web-search-discovery-audit.json"
        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)

        sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(_clean_snapshot(batch_id="1", source="gameprog", run_id="run-old")),
            output_path=output_path,
        )
        sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(_clean_snapshot(batch_id="9", source="gamesmap", run_id="run-new")),
            output_path=output_path,
        )

        replaced = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert replaced is not None
        assert replaced["runId"] == "run-new"
        assert set(replaced["sources"]) == {"gamesmap"}
        assert replaced["sources"]["gamesmap"]["receipts"][0]["batchId"] == "9"


def test_publish_sidecar_bounds_receipt_count_and_marks_partial() -> None:
    with workspace_tmpdir("sidecar-count-bound") as root:
        output_path = root / "web-search-discovery-audit.json"
        snapshots = [
            _clean_snapshot(batch_id=str(index))
            for index in range(1, sidecar.MAX_SIDECAR_RECEIPT_RECORDS + 3)
        ]

        published = sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(*snapshots), output_path=output_path
        )

        assert published is not None
        entry = published["sources"]["web_search"]
        assert entry["receiptCount"] == sidecar.MAX_SIDECAR_RECEIPT_RECORDS
        assert entry["truncated"] is True
        assert entry["coverage"] == "partial"
        assert entry["omittedCount"] == 2
        # Truncation can never be read as cleanliness.
        assert entry["state"] != "clean"
        assert published["truncated"] is True
        assert published["coverage"] == "partial"
        assert published["omittedCount"] == 2
        # The rollup keeps the counters of the receipts that survived.
        assert entry["rollup"]["batchCount"] == sidecar.MAX_SIDECAR_RECEIPT_RECORDS


def test_publish_sidecar_bounds_bytes_and_reader_still_accepts_the_file() -> None:
    with workspace_tmpdir("sidecar-byte-bound") as root:
        output_path = root / "web-search-discovery-audit.json"
        fat = "x" * 4_000
        snapshots = [_clean_snapshot(batch_id=str(index), phase=fat) for index in range(1, 200)]

        published = sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(*snapshots), output_path=output_path
        )

        assert published is not None
        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)
        assert sidecar_path.stat().st_size <= sidecar.MAX_SIDECAR_BYTES
        # The writer uses `write_text(..., newline=None)`, so the trailing newline becomes CRLF on
        # Windows (+1 byte) and stays LF elsewhere. Assert the platform-neutral relation rather
        # than pinning the Windows-only value, so this stays a real assertion on the Linux CI lane.
        on_disk = sidecar_path.stat().st_size
        model = sidecar._serialized_envelope_bytes(published)
        assert on_disk in (model, model + sidecar._WRITE_MARGIN_BYTES)
        entry = published["sources"]["web_search"]
        assert entry["truncated"] is True
        assert entry["omittedCount"] > 0
        assert entry["state"] != "clean"
        # The reader's stat check must never reject a file this module itself wrote.
        assert sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path) is not None


def test_publish_sidecar_byte_bound_is_exact_for_many_merged_sources() -> None:
    """The cheap trim estimate must match the real serializer across a merged multi-source file."""
    with workspace_tmpdir("sidecar-byte-bound-merged") as root:
        output_path = root / "web-search-discovery-audit.json"
        fat = "y" * 3_000
        for source in ("gameprog", "gamesmap"):
            snapshots = [
                _clean_snapshot(batch_id=str(index), source=source, phase=fat, run_id="run-many")
                for index in range(1, 120)
            ]
            sidecar.publish_page_fetch_lifecycle_sidecar(
                artifact=_artifact(*snapshots), output_path=output_path
            )

        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)
        stored = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert stored is not None
        assert sidecar_path.stat().st_size <= sidecar.MAX_SIDECAR_BYTES
        on_disk = sidecar_path.stat().st_size
        model = sidecar._serialized_envelope_bytes(stored)
        assert on_disk in (model, model + sidecar._WRITE_MARGIN_BYTES)
        assert set(stored["sources"]) == {"gameprog", "gamesmap"}
        for entry in stored["sources"].values():
            assert entry["omittedCount"] > 0
            assert entry["truncated"] is True
            assert entry["state"] != "clean"


def test_publish_sidecar_survives_writer_oserror() -> None:
    with workspace_tmpdir("sidecar-oserror") as root:
        output_path = root / "web-search-discovery-audit.json"
        logged: list[str] = []

        with mock.patch.object(
            sidecar.source_registry_module,
            "save_json_atomic",
            side_effect=OSError("disk full"),
        ):
            result = sidecar.publish_page_fetch_lifecycle_sidecar(
                artifact=_artifact(_clean_snapshot(batch_id="1")),
                output_path=output_path,
                emit_log=logged.append,
            )

        assert result is None
        assert logged and "sidecar publish skipped" in logged[0]
        assert not sidecar.sidecar_path_for_artifact(output_path).exists()


def test_read_sidecar_treats_absent_oversized_and_malformed_as_absent() -> None:
    with workspace_tmpdir("sidecar-read") as root:
        missing = root / sidecar.PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME
        assert sidecar.read_page_fetch_lifecycle_sidecar(missing) is None

        malformed = root / "malformed.json"
        malformed.write_text("{not json", encoding="utf-8")
        assert sidecar.read_page_fetch_lifecycle_sidecar(malformed) is None

        not_an_object = root / "list.json"
        not_an_object.write_text("[1, 2, 3]", encoding="utf-8")
        assert sidecar.read_page_fetch_lifecycle_sidecar(not_an_object) is None

        oversized = root / "oversized.json"
        oversized.write_text("x" * (sidecar.MAX_SIDECAR_BYTES + 1), encoding="utf-8")
        assert sidecar.read_page_fetch_lifecycle_sidecar(oversized) is None

        empty = root / "empty.json"
        empty.write_text("", encoding="utf-8")
        assert sidecar.read_page_fetch_lifecycle_sidecar(empty) is None

        directory = root / "a-directory"
        directory.mkdir()
        assert sidecar.read_page_fetch_lifecycle_sidecar(directory) is None


def test_directory_audit_publishes_sidecar_on_write_and_cache_hit_leaves_it_alone() -> None:
    with workspace_tmpdir("directory-audit-sidecar") as root:
        output_path = root / "gameprog-discovery-audit.json"
        snapshot = _real_snapshot(run_id="run-directory", source="gameprog")
        scan_calls: list[int] = []

        def scan(_timeout_s: int) -> dict[str, Any]:
            scan_calls.append(1)
            return {
                "providerCandidates": [],
                "staticCandidates": [],
                "failures": [],
                "summary": {"eligibleRows": 0},
                "batchTiming": {"fetchMs": 1},
                "progress": {"complete": True, "cursor": 0},
                "pageFetchLifecycle": [snapshot],
            }

        artifact, cache_hit = directory_audit.run_directory_audit(
            adapter="gameprog",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=scan,
        )

        assert cache_hit is False
        assert artifact["pageFetchLifecycle"] == [snapshot]
        sidecar_path = sidecar.sidecar_path_for_artifact(output_path)
        assert sidecar_path.exists()
        published = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert published is not None
        assert published["runId"] == "run-directory"
        assert published["sources"]["gameprog"]["receiptCount"] == 1
        bytes_after_write = sidecar_path.read_bytes()

        directory_audit.clear_directory_audit_summaries()
        cached, cache_hit_again = directory_audit.run_directory_audit(
            adapter="gameprog",
            schema_version=1,
            output_path=output_path,
            ttl_minutes=60,
            signature={"config": "same"},
            timeout_s=5,
            scan=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh artifact should bypass scan")
            ),
        )

        assert cache_hit_again is True
        assert cached == artifact
        assert len(scan_calls) == 1
        # A cache hit must not republish the sidecar.
        assert sidecar_path.read_bytes() == bytes_after_write


def test_web_search_browser_recovery_publishes_sidecar_for_its_own_run() -> None:
    with workspace_tmpdir("browser-recovery-sidecar") as root:
        audit_path = root / "web-audit.json"
        snapshot = _clean_snapshot(batch_id="1", run_id="run-web")
        audit_path.write_text(
            json.dumps(
                {
                    "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
                    "adapter": "web_search",
                    "summary": {"browserRecoveryCandidates": 1},
                    "providerCandidates": [],
                    "staticCandidates": [],
                    "browserRecoveryCandidates": [
                        {
                            "name": "Sidecar Studio",
                            "studio": "Sidecar Studio",
                            "url": "https://sidecar.example/jobs",
                            "discoveryMethod": "web_search",
                        }
                    ],
                    "browserRecovery": {},
                    "pageFetchLifecycle": [snapshot],
                }
            ),
            encoding="utf-8",
        )

        recovered = web_candidates.run_web_search_browser_recovery(
            5,
            config={
                "webSearch": {
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            output_path=audit_path,
            browser_fetcher=lambda _url, _timeout_s: ("<html>No jobs</html>", ""),
            fetcher=lambda *_args: "",
        )

        assert recovered["pageFetchLifecycle"] == [snapshot]
        sidecar_path = sidecar.sidecar_path_for_artifact(audit_path)
        published = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert published is not None
        assert published["runId"] == "run-web"
        assert published["sources"]["web_search"]["receiptCount"] == 1


def test_web_search_browser_recovery_never_clobbers_a_newer_run_sidecar() -> None:
    with workspace_tmpdir("browser-recovery-stale") as root:
        audit_path = root / "web-audit.json"
        sidecar_path = sidecar.sidecar_path_for_artifact(audit_path)

        # A newer run already published its envelope for this data dir.
        sidecar.publish_page_fetch_lifecycle_sidecar(
            artifact=_artifact(_clean_snapshot(batch_id="7", run_id="run-newer")),
            output_path=audit_path,
        )
        newer_bytes = sidecar_path.read_bytes()

        # The browser-recovery artifact is stale: its receipts belong to an older run.
        stale = _clean_snapshot(batch_id="1", run_id="run-older")
        audit_path.write_text(
            json.dumps(
                {
                    "schemaVersion": web_candidates.WEB_SEARCH_AUDIT_SCHEMA_VERSION,
                    "adapter": "web_search",
                    "summary": {"browserRecoveryCandidates": 0},
                    "providerCandidates": [],
                    "staticCandidates": [],
                    "browserRecoveryCandidates": [],
                    "browserRecovery": {},
                    "pageFetchLifecycle": [stale],
                }
            ),
            encoding="utf-8",
        )

        web_candidates.run_web_search_browser_recovery(
            5,
            config={
                "webSearch": {
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            output_path=audit_path,
            browser_fetcher=lambda _url, _timeout_s: ("<html>No jobs</html>", ""),
            fetcher=lambda *_args: "",
        )

        assert sidecar_path.read_bytes() == newer_bytes
        unchanged = sidecar.read_page_fetch_lifecycle_sidecar(sidecar_path)
        assert unchanged is not None
        assert unchanged["runId"] == "run-newer"


def test_bounds_work_is_not_quadratic_in_the_drop_count() -> None:
    """Constrain the COST of bound enforcement, not just its result.

    The earlier implementation recomputed each trimmed entry's rollup inside the drop loop, which
    re-iterates the whole remaining receipt list per drop. That is invisible to any assertion about
    the output, so nothing failed while the function was quadratic. This measures work directly by
    counting how many receipts the rollup is asked to aggregate relative to the number dropped: a
    per-drop recomputation scales with (drops x remaining), a single post-loop pass scales with the
    receipts that survive.
    """
    dropped = 900
    receipts = [
        _clean_snapshot(batch_id=str(index), phase="x" * 40) for index in range(1, dropped + 1)
    ]
    envelope = {
        "schemaVersion": 1,
        "receiptType": "pageFetchLifecycleSidecar",
        "generatedAt": "2026-01-01T00:00:00+00:00",
        "runId": "run-cost",
        "artifactName": "web-search-discovery-audit.json",
        "coverage": "full",
        "truncated": False,
        "omittedCount": 0,
        "sources": {
            "web_search": sidecar._source_entry(
                source="web_search",
                run_id="run-cost",
                artifact_name="web-search-discovery-audit.json",
                receipts=receipts,
            )
        },
    }

    aggregated = 0
    real_rollup = sidecar._rollup_payload

    def counting_rollup(batch):
        nonlocal aggregated
        counted = list(batch)
        aggregated += len(counted)
        return real_rollup(counted)

    with mock.patch.object(sidecar, "_rollup_payload", side_effect=counting_rollup):
        bounded = sidecar._enforce_bounds(envelope)

    assert bounded is not None
    surviving = bounded["sources"]["web_search"]["receiptCount"]
    assert surviving == sidecar.MAX_SIDECAR_RECEIPT_RECORDS
    assert bounded["sources"]["web_search"]["omittedCount"] == dropped - surviving
    # A single post-loop aggregate touches the surviving receipts once. The quadratic shape would
    # touch roughly the surviving receipts for EVERY drop, i.e. hundreds of times this bound.
    assert aggregated <= surviving * 3
    # This bound alone would also be satisfied by an implementation that skips rollup recomputation
    # entirely, which would be a correctness regression, not a fix. So the VALUE is asserted here
    # too, inside the same test: the rollup must describe exactly the receipts that survived.
    # Keeping both in one test means neither can be weakened without the other failing.
    entry = bounded["sources"]["web_search"]
    assert entry["rollup"]["batchCount"] == surviving
    # _clean_snapshot submits and finishes two fetches per receipt, so the surviving total is
    # exactly twice the surviving count: the rollup describes the survivors, not the input.
    assert entry["rollup"]["submitted"] == surviving * 2
    assert entry["rollup"]["finished"] == surviving * 2
    assert entry["rollup"]["state"] != "clean"


def test_sidecar_truncated_entry_rollup_never_reports_clean() -> None:
    """A truncated entry's nested rollup must not claim 'clean' about a knowingly incomplete view.

    The counters describe the receipts that survived, which is honest, but a reader that trusts
    `rollup.state` must not be told an incomplete dataset is fully settled. The complementary case
    matters just as much: a complete, settled view must still be able to read as clean.
    """
    truncated = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(
            *[
                _clean_snapshot(batch_id=str(index))
                for index in range(1, sidecar.MAX_SIDECAR_RECEIPT_RECORDS + 50)
            ]
        ),
        Path("data/web-search-discovery-audit.json"),
    )
    assert truncated is not None
    entry = truncated["sources"]["web_search"]
    assert entry["truncated"] is True
    assert entry["state"] != "clean"
    assert entry["rollup"]["state"] != "clean"

    settled = sidecar.build_page_fetch_lifecycle_sidecar(
        _artifact(_clean_snapshot(batch_id="1")),
        Path("data/web-search-discovery-audit.json"),
    )
    assert settled is not None
    settled_entry = settled["sources"]["web_search"]
    assert settled_entry["truncated"] is False
    assert settled_entry["state"] == "clean"
    assert settled_entry["rollup"]["state"] == "clean"


def test_sidecar_publishes_nothing_for_receipts_without_a_run_identity() -> None:
    """An absent runId is not the identity "": two unknown runs must not merge into one envelope."""
    with workspace_tmpdir("sidecar-no-runid") as root:
        output_path = root / "web-search-discovery-audit.json"

        unknown = _clean_snapshot(batch_id="1")
        unknown.pop("runId")
        assert sidecar.build_page_fetch_lifecycle_sidecar(_artifact(unknown), output_path) is None
        assert (
            sidecar.publish_page_fetch_lifecycle_sidecar(
                artifact=_artifact(unknown), output_path=output_path
            )
            is None
        )
        assert not sidecar.sidecar_path_for_artifact(output_path).exists()

        empty = _clean_snapshot(batch_id="2", run_id="")
        assert sidecar.build_page_fetch_lifecycle_sidecar(_artifact(empty), output_path) is None


def test_sidecar_refuses_to_publish_an_envelope_it_cannot_bound() -> None:
    """Overhead that cannot be trimmed must publish nothing, not a file the reader would reject."""
    output_path = Path("data/web-search-discovery-audit.json")
    envelope = {
        "schemaVersion": 1,
        "receiptType": "pageFetchLifecycleSidecar",
        "generatedAt": "2026-01-01T00:00:00+00:00",
        "runId": "run-huge",
        "artifactName": "web-search-discovery-audit.json",
        "coverage": "full",
        "truncated": False,
        "omittedCount": 0,
        # Many empty source entries: the envelopes' own keys exceed the cap with no receipt to drop.
        "sources": {
            f"source-{index}": sidecar._source_entry(
                source=f"source-{index}",
                run_id="run-huge",
                artifact_name="web-search-discovery-audit.json",
                receipts=[],
            )
            for index in range(1_200)
        },
    }

    assert sidecar._serialized_envelope_bytes(envelope) > sidecar.MAX_SIDECAR_BYTES
    assert sidecar._enforce_bounds(envelope) is None


def test_sidecar_run_identity_ignores_batchless_receipts() -> None:
    """A runId-bearing receipt without a batchId still counts toward run disagreement."""
    agreeing = sidecar.receipt_run_id_or_none([_clean_snapshot(batch_id="1", run_id="run-a")])
    assert agreeing == "run-a"

    batchless_conflict = _clean_snapshot(batch_id="", run_id="run-b")
    assert (
        sidecar.receipt_run_id_or_none(
            [_clean_snapshot(batch_id="1", run_id="run-a"), batchless_conflict]
        )
        is None
    )
    assert sidecar.receipt_run_id_or_none([_clean_snapshot(batch_id="1", run_id="")]) is None
    assert sidecar.receipt_run_id_or_none([]) is None
