"""Tests for the desktop startup success gate's summary-sidecar read."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.bridge.fetch_report_summary import compact_fetch_report_summary_payload
from src.shared import fetch_report_identity as identity_module
from src.shared.fetch_report_identity import FETCH_REPORT_IDENTITY_PREFIX_BYTES
from src.ship import jobs_first_run_state as feed_state

SUCCESS_SUMMARY = {"outputCount": 3, "sourceCount": 1}
RUN_ID = "run-1"
FINISHED_AT = "2026-01-01T00:00:00Z"


def _report(
    *,
    run_id: str = RUN_ID,
    status: str = "",
    finished_at: str = FINISHED_AT,
    summary: dict[str, Any] | None = None,
    filler: str | None = None,
) -> dict[str, Any]:
    report = {
        "runId": run_id,
        "status": status,
        "finishedAt": finished_at,
        "summary": dict(SUCCESS_SUMMARY if summary is None else summary),
    }
    if filler is not None:
        # Push the identity keys past the gate's bounded report prefix.
        return {"filler": filler, **report}
    return report


def _sidecar(
    *,
    run_id: str = RUN_ID,
    finished: str = FINISHED_AT,
    status: str = "",
    **summary: Any,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "status": status,
        "finishedAt": finished,
        "summary": {"outputCount": 3, "sourceCount": 1, **summary},
        "taskProgress": {"active": not bool(finished)},
    }


def _write_gate_dir(
    tmp_path: Path,
    *,
    report: dict[str, Any] | None = None,
    sidecar: dict[str, Any] | None = None,
    feed: bool = True,
) -> Path:
    data_path = tmp_path / "data"
    data_path.mkdir()
    (data_path / feed_state.FETCH_REPORT_NAME).write_text(
        json.dumps(report if report is not None else _report()), encoding="utf-8"
    )
    if sidecar is not None:
        (data_path / feed_state.FETCH_REPORT_SUMMARY_NAME).write_text(
            json.dumps(sidecar), encoding="utf-8"
        )
    if feed:
        for name in feed_state.RUNTIME_FEED_ARTIFACTS:
            (data_path / name).write_text(json.dumps([{"id": 1}]), encoding="utf-8")
    return data_path


def _record_reads(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    reads: list[Path] = []
    real_read_json = feed_state.read_json

    def recording_read_json(path: Any, default: Any) -> Any:
        reads.append(Path(path))
        return real_read_json(path, default)

    monkeypatch.setattr(feed_state, "read_json", recording_read_json)
    return reads


def _report_path(data_path: Path) -> Path:
    return data_path / feed_state.FETCH_REPORT_NAME


def _sidecar_path(data_path: Path) -> Path:
    return data_path / feed_state.FETCH_REPORT_SUMMARY_NAME


class TestSuccessfulRuntimeJobsReport:
    def test_finished_sidecar_settles_the_gate_without_parsing_the_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar())
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) not in reads

    def test_identity_read_is_bounded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar())
        requests: list[int] = []
        real_prefix = identity_module.read_json_prefix

        def recording_prefix(path: Any, *, max_bytes: int = 0) -> str:
            requests.append(max_bytes)
            return real_prefix(path, max_bytes=max_bytes)

        monkeypatch.setattr(identity_module, "read_json_prefix", recording_prefix)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert requests == [FETCH_REPORT_IDENTITY_PREFIX_BYTES]

    def test_sidecar_identity_is_trusted_even_when_it_is_older_than_the_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar())
        report_path = _report_path(data_path)
        sidecar_path = _sidecar_path(data_path)
        report_path.write_text(json.dumps(_report()), encoding="utf-8")
        stat = sidecar_path.stat()
        os.utime(sidecar_path, ns=(stat.st_atime_ns, stat.st_mtime_ns - 1_000_000_000))
        assert sidecar_path.stat().st_mtime_ns < report_path.stat().st_mtime_ns
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == sidecar_path
        assert _report_path(data_path) not in reads

    def test_static_serving_gate_uses_the_sidecar_too(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar())
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report_for_static_serving(data_path) is True
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) not in reads

    @pytest.mark.parametrize(
        "sidecar",
        [
            _sidecar(run_id="run-0"),
            _sidecar(finished="2026-01-01T00:01:00Z"),
        ],
    )
    def test_sidecar_that_describes_another_run_falls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sidecar: dict[str, Any]
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=sidecar)
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) in reads

    @pytest.mark.parametrize(
        "sidecar",
        [
            _sidecar(finished=""),
            {**_sidecar(), "taskProgress": {"active": True}},
            {"runId": RUN_ID, "finishedAt": FINISHED_AT, "summary": {"sourceCount": 1}},
        ],
    )
    def test_sidecar_that_cannot_settle_the_gate_falls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sidecar: dict[str, Any]
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=sidecar)
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) in reads

    def test_missing_sidecar_falls_back_to_the_full_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path)
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == _report_path(data_path)

    def test_report_identity_past_the_prefix_falls_back_to_the_full_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(
            tmp_path,
            report=_report(filler="x" * (4 * FETCH_REPORT_IDENTITY_PREFIX_BYTES)),
            sidecar=_sidecar(),
        )
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is True
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) in reads

    def test_sidecar_summary_error_status_fails_the_gate_without_the_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar(status="error"))
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is False
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) not in reads

    def test_sidecar_success_still_requires_feed_artifacts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        data_path = _write_gate_dir(tmp_path, sidecar=_sidecar(), feed=False)
        reads = _record_reads(monkeypatch)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is False
        assert reads[0] == _sidecar_path(data_path)
        assert _report_path(data_path) not in reads

    def test_report_success_still_requires_feed_artifacts(self, tmp_path: Path) -> None:
        data_path = _write_gate_dir(tmp_path, feed=False)

        assert feed_state.has_successful_runtime_jobs_report(data_path) is False


def test_evaluate_runtime_jobs_report_names_the_reader(tmp_path: Path) -> None:
    data_path = _write_gate_dir(tmp_path, sidecar=_sidecar())

    success, diagnostics = feed_state.evaluate_runtime_jobs_report(data_path)

    assert success is True
    assert diagnostics["reader"] == feed_state.FETCH_REPORT_READER_SIDECAR
    assert diagnostics["hasReport"] is True
    assert diagnostics["reportRunId"] == RUN_ID
    assert diagnostics["reportFinishedAt"] == FINISHED_AT
    assert diagnostics["reportMetadataOk"] is True
    assert diagnostics["feedArtifactsOk"] is True
    assert diagnostics["success"] is True
    assert diagnostics["durationMs"] >= 0
    metadata, reader = feed_state.load_jobs_fetch_report_metadata(data_path)
    assert reader == feed_state.FETCH_REPORT_READER_SIDECAR
    assert metadata["finishedAt"] == FINISHED_AT


def test_compact_summary_preserves_the_status_the_gate_reads() -> None:
    compact = compact_fetch_report_summary_payload(
        {"summary": {"status": "error", "outputCount": 4, "failedSources": 1}},
        detail_level="summary",
    )

    assert compact["summary"]["status"] == "error"
    assert compact["summary"]["outputCount"] == 4
