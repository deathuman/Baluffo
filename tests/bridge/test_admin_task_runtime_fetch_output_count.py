"""Tests for the memoized pipeline baseline output-count read."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.bridge import admin_task_runtime
from src.bridge.fetch_report_summary import FETCH_REPORT_SUMMARY_FILE_NAME


def _report(
    *,
    output: int | None = None,
    unique: int | None = None,
    finished: str = "2026-01-01T00:00:00Z",
    status: str = "ok",
) -> dict[str, Any]:
    summary: dict[str, Any] = {"failedSources": 0, "sourceCount": 2}
    if output is not None:
        summary["outputCount"] = output
    if unique is not None:
        summary["uniqueOutputCount"] = unique
    return {
        "runId": "run-1",
        "status": status,
        "finishedAt": finished,
        "summary": summary,
        "sources": [{"name": "source-a", "status": "ok", "durationMs": 12}],
    }


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _shift_mtime(path: Path, offset_ns: int) -> None:
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + offset_ns))


def _forbid_normalization(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("the baseline count must not normalize the fetch report")


def _root(report_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        JOBS_FETCH_REPORT_PATH=report_path,
        normalize_fetch_report_contract=_forbid_normalization,
        summarize_fetch_report=_forbid_normalization,
    )


@pytest.fixture(autouse=True)
def _clear_cache() -> Any:
    admin_task_runtime.clear_fetch_output_count_cache()
    yield
    admin_task_runtime.clear_fetch_output_count_cache()


class TestFetchOutputCount:
    def test_counts_report_summary_without_normalizing(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(output=41))

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 41

    def test_falls_back_to_unique_output_count(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(unique=17))

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 17

    def test_missing_report_is_zero(self, tmp_path: Path) -> None:
        assert (
            admin_task_runtime.current_fetch_output_count(
                root_mod=_root(tmp_path / "jobs-fetch-report.json")
            )
            == 0
        )

    def test_finished_sidecar_supplies_count_without_reading_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(report_path, _report(output=5))
        _write(sidecar_path, _report(output=41))
        _shift_mtime(sidecar_path, 1_000_000_000)
        read_paths: list[Path] = []
        real_loader = admin_task_runtime.load_runtime_evidence

        def recording_loader(path: Any, default: Any = None) -> Any:
            read_paths.append(Path(path))
            return real_loader(path, default)

        monkeypatch.setattr(admin_task_runtime, "load_runtime_evidence", recording_loader)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 41
        assert read_paths == [sidecar_path]

    @pytest.mark.parametrize("offset_ns", [0, -1_000_000_000])
    def test_sidecar_is_trusted_when_its_mtime_is_not_newer(
        self, tmp_path: Path, offset_ns: int
    ) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(sidecar_path, _report(output=41))
        _write(report_path, _report(output=5))
        _shift_mtime(sidecar_path, offset_ns)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 41

    def test_sidecar_for_another_run_falls_back_to_the_report(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        sidecar = _report(output=99)
        sidecar["runId"] = "run-0"
        _write(report_path, _report(output=5))
        _write(sidecar_path, sidecar)
        _shift_mtime(sidecar_path, 1_000_000_000)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 5

    def test_sidecar_with_another_finished_at_falls_back_to_the_report(
        self, tmp_path: Path
    ) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(report_path, _report(output=5))
        _write(sidecar_path, _report(output=99, finished="2025-12-31T23:59:59Z"))
        _shift_mtime(sidecar_path, 1_000_000_000)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 5

    @pytest.mark.parametrize(
        "sidecar",
        [
            _report(output=99, finished="", status="running"),
            {**_report(output=99, finished="", status="ok"), "taskProgress": {"active": True}},
        ],
    )
    def test_live_sidecar_is_ignored(self, tmp_path: Path, sidecar: dict[str, Any]) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(output=5))
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(sidecar_path, sidecar)
        _shift_mtime(sidecar_path, 1_000_000_000)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 5

    def test_sidecar_without_count_falls_back_to_report(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(output=5))
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(
            sidecar_path,
            {"status": "ok", "finishedAt": "2026-01-01T00:00:00Z", "summary": {"sourceCount": 3}},
        )
        _shift_mtime(sidecar_path, 1_000_000_000)

        assert admin_task_runtime.current_fetch_output_count(root_mod=_root(report_path)) == 5

    def test_unchanged_report_is_parsed_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(output=5))
        reads: list[Path] = []
        real_loader = admin_task_runtime.load_runtime_evidence

        def recording_loader(path: Any, default: Any = None) -> Any:
            reads.append(Path(path))
            return real_loader(path, default)

        monkeypatch.setattr(admin_task_runtime, "load_runtime_evidence", recording_loader)
        root = _root(report_path)

        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 5
        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 5
        assert len(reads) == 1

    def test_report_rewrite_invalidates_the_memo(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        _write(report_path, _report(output=5))
        root = _root(report_path)
        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 5

        _write(report_path, _report(output=6))
        _shift_mtime(report_path, 1_000_000)

        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 6

    def test_removing_the_sidecar_invalidates_the_memo(self, tmp_path: Path) -> None:
        report_path = tmp_path / "jobs-fetch-report.json"
        sidecar_path = tmp_path / FETCH_REPORT_SUMMARY_FILE_NAME
        _write(report_path, _report(output=5))
        _write(sidecar_path, _report(output=41))
        _shift_mtime(sidecar_path, 1_000_000_000)
        root = _root(report_path)
        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 41

        sidecar_path.unlink()

        assert admin_task_runtime.current_fetch_output_count(root_mod=root) == 5
