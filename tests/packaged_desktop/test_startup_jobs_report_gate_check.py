"""Packaged smoke must fail when the launch gate parsed the full fetch report."""

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from src.ship import runtime_launcher, startup_profile, startup_telemetry
from src.ship.jobs_first_run_state import (
    has_successful_runtime_jobs_report,
    load_jobs_fetch_report_metadata,
)
from src.ship.packaged_smoke import startup_metrics
from src.ship.startup_telemetry import read_startup_metrics
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging]


def _gate_row(reader: str, process: str = "site", **fields: object) -> dict[str, Any]:
    return {
        "event": "desktop_launch_jobs_report_gate",
        "fields": {
            "reader": reader,
            "process": process,
            "durationMs": 12.5,
            "reportRunId": "run-1",
            "reportFinishedAt": "2026-10-19T00:00:00+00:00",
            "reportMetadataOk": True,
            "feedArtifactsOk": True,
            "success": True,
            **fields,
        },
    }


def _launch_gate_rows(reader: str = "summary-sidecar", **fields: object) -> list[dict[str, Any]]:
    """What a healthy start journals: the site and bridge halves each read the gate."""
    return [_gate_row(reader, "site", **fields), _gate_row(reader, "bridge", **fields)]


def _skip_row(process: str, *, reason: str = "no_artifacts") -> dict[str, Any]:
    return {
        "event": "desktop_launch_jobs_report_gate_skipped",
        "fields": {"process": process, "reason": reason, "restored": 0},
    }


def _launch_skip_rows() -> list[dict[str, Any]]:
    """What a start with no row artifacts journals: both halves report the skip."""
    return [_skip_row("site"), _skip_row("bridge")]


def _quarantine_failure_row(process: str, *, error: str = "quarantine crashed") -> dict[str, Any]:
    return {
        "event": "jobs_row_artifact_quarantine_failed",
        "fields": {"process": process, "dataDir": "C:/data", "error": error},
    }


def test_gate_journal_event_names_have_one_definition() -> None:
    assert startup_metrics.JOBS_REPORT_GATE_EVENT is startup_telemetry.JOBS_REPORT_GATE_EVENT
    assert (
        startup_metrics.JOBS_REPORT_GATE_SKIPPED_EVENT
        is startup_telemetry.JOBS_REPORT_GATE_SKIPPED_EVENT
    )
    assert (
        startup_profile.JOBS_ROW_ARTIFACT_QUARANTINE_FAILED_EVENT
        is startup_telemetry.JOBS_ROW_ARTIFACT_QUARANTINE_FAILED_EVENT
    )
    assert (
        smoke.packaged_smoke_orchestrator_mod.JOBS_ROW_ARTIFACT_QUARANTINE_FAILED_EVENT
        is startup_telemetry.JOBS_ROW_ARTIFACT_QUARANTINE_FAILED_EVENT
    )


def test_jobs_report_gate_full_report_read_finds_fallback_row() -> None:
    rows = [
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        _gate_row("summary-sidecar"),
        _gate_row("full-report", durationMs=321.0),
    ]

    fields = smoke.jobs_report_gate_full_report_read(rows)

    assert fields is not None
    assert fields["reader"] == "full-report"
    assert fields["durationMs"] == 321.0


def test_jobs_report_gate_full_report_read_ignores_sidecar_rows() -> None:
    rows = [_gate_row("summary-sidecar"), {"event": "desktop_site_ready"}]

    assert smoke.jobs_report_gate_full_report_read(rows) is None


def test_seeded_jobs_pipeline_feed_settles_the_real_gate_via_sidecar() -> None:
    with workspace_tmpdir("packaged-smoke-seed") as tmp:
        data_dir = Path(tmp) / "data"

        smoke._seed_jobs_pipeline_smoke_feed(  # noqa: SLF001
            data_dir, finished_at="2026-10-19T00:00:00+00:00"
        )

        metadata, reader = load_jobs_fetch_report_metadata(data_dir)
        assert reader == "summary-sidecar"
        assert metadata["runId"] == "packaged-smoke-seed-run"
        assert has_successful_runtime_jobs_report(data_dir) is True


def test_apply_jobs_report_gate_check_fails_report_on_full_report_read() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = _launch_gate_rows("full-report", durationMs=812.0)

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert report["ok"] is False
    assert report["failure"]["step"] == "startup-jobs-report-gate"
    assert report["failure"]["category"] == "startup_jobs_report_gate_full_report"
    assert "812" in report["failure"]["message"]
    scenario = report["scenarios"][-1]
    assert scenario["slug"] == "startup-jobs-report-gate"
    assert scenario["status"] == "failed"
    assert scenario["durationMs"] == 812
    assert scenario["reportRunId"] == "run-1"


def test_apply_jobs_report_gate_check_leaves_sidecar_report_alone() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = _launch_gate_rows()

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is False
    assert report["ok"] is True
    assert report["failure"] is None
    assert report["scenarios"] == []


def test_real_launch_gate_row_settles_the_seeded_fixture_via_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("packaged-smoke-gate-real") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        smoke._seed_jobs_pipeline_smoke_feed(  # noqa: SLF001
            data_dir, finished_at="2026-10-19T00:00:00+00:00"
        )
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_DESKTOP_STARTUP_TRACE", "1")

        runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="site")
        result = runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="bridge")
        rows = read_startup_metrics(data_dir)

        assert result["skipped"] == "successful_runtime_report"
        assert result["reportGate"]["reader"] == "summary-sidecar"
        assert [row["event"] for row in rows] == ["desktop_launch_jobs_report_gate"] * 2
        assert [row["fields"]["reader"] for row in rows] == ["summary-sidecar"] * 2
        assert [row["fields"]["process"] for row in rows] == ["site", "bridge"]

        report: dict[str, Any] = {
            "ok": True,
            "failure": None,
            "scenarios": [],
            "startupMetrics": rows,
        }
        assert (
            smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
                report, require_row=True
            )
            is False
        )
        assert report["ok"] is True


def test_real_crashed_quarantine_is_reported_by_its_own_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("packaged-smoke-gate-crash") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        smoke._seed_jobs_pipeline_smoke_feed(  # noqa: SLF001
            data_dir, finished_at="2026-10-19T00:00:00+00:00"
        )
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_DESKTOP_STARTUP_TRACE", "1")

        def _crash(_data_dir: Path) -> tuple[bool, dict[str, object]]:
            raise RuntimeError("report exploded")

        monkeypatch.setattr(runtime_launcher, "evaluate_runtime_jobs_report", _crash)
        runtime_launcher.safe_quarantine_stale_jobs_row_artifacts(data_dir, process="site")
        runtime_launcher.safe_quarantine_stale_jobs_row_artifacts(data_dir, process="bridge")
        rows = read_startup_metrics(data_dir)

        failures = [row for row in rows if row["event"] == "jobs_row_artifact_quarantine_failed"]
        assert [row["fields"]["process"] for row in failures] == ["site", "bridge"]
        assert {row["fields"]["error"] for row in failures} == {"report exploded"}

        profile = smoke.summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
        assert profile["jobsReportGate"] == {
            "quarantineFailureCount": 2,
            "quarantineFailureProcesses": ["bridge", "site"],
        }

        report: dict[str, Any] = {
            "ok": True,
            "failure": None,
            "scenarios": [],
            "startupMetrics": rows,
        }
        assert (
            smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
                report
            )
            is True
        )
        assert report["failure"]["category"] == "jobs_row_artifact_quarantine_failed"
        assert "report exploded" in report["failure"]["message"]
        assert [failure["process"] for failure in report["scenarios"][-1]["failures"]] == [
            "site",
            "bridge",
        ]


def test_real_launch_without_artifacts_journals_a_skip_from_both_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("packaged-smoke-gate-empty") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        data_dir.mkdir(parents=True)
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_DESKTOP_STARTUP_TRACE", "1")

        result = runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="site")
        runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="bridge")
        rows = read_startup_metrics(data_dir)

        assert result["skipped"] == "no_artifacts"
        assert [row["event"] for row in rows] == ["desktop_launch_jobs_report_gate_skipped"] * 2
        assert [row["fields"]["process"] for row in rows] == ["site", "bridge"]
        assert {str(row["fields"]["reason"]) for row in rows} == {"no_artifacts"}

        report: dict[str, Any] = {
            "ok": True,
            "failure": None,
            "scenarios": [],
            "startupMetrics": rows,
        }
        assert (
            smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
                report
            )
            is False
        )
        assert report["ok"] is True


def test_real_launch_gate_row_fails_the_smoke_without_the_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("packaged-smoke-gate-real") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        smoke._seed_jobs_pipeline_smoke_feed(  # noqa: SLF001
            data_dir, finished_at="2026-10-19T00:00:00+00:00"
        )
        (data_dir / "jobs-fetch-report-summary.json").unlink()
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_DESKTOP_STARTUP_TRACE", "1")

        runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="site")
        runtime_launcher.quarantine_stale_jobs_row_artifacts(data_dir, process="bridge")
        rows = read_startup_metrics(data_dir)

        assert {str(row["fields"]["reader"]) for row in rows} == {"full-report"}
        report: dict[str, Any] = {
            "ok": True,
            "failure": None,
            "scenarios": [],
            "startupMetrics": rows,
        }
        assert (
            smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
                report, require_row=True
            )
            is True
        )
        assert report["failure"]["category"] == "startup_jobs_report_gate_full_report"
        assert report["failure"]["step"] == "startup-jobs-report-gate"


def test_apply_jobs_report_gate_check_requires_a_row_when_asked() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report, require_row=True
    )

    assert failed is True
    assert report["ok"] is False
    assert report["failure"]["category"] == "startup_jobs_report_gate_missing"
    assert "no reader row for site, bridge" in report["failure"]["message"]
    assert report["scenarios"][-1]["missingProcesses"] == ["site", "bridge"]


def test_apply_jobs_report_gate_check_seeded_lane_needs_a_reader_row() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [
        _gate_row("summary-sidecar", "site"),
        _skip_row("bridge"),
    ]

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report, require_row=True
    )

    assert failed is True
    assert report["failure"]["category"] == "startup_jobs_report_gate_missing"
    assert "no reader row for bridge" in report["failure"]["message"]
    assert report["scenarios"][-1]["missingProcesses"] == ["bridge"]


def test_apply_jobs_report_gate_check_fails_on_a_crashed_quarantine() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [_quarantine_failure_row("bridge", error="boom")]

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert report["ok"] is False
    assert report["failure"]["step"] == "jobs-row-artifact-quarantine"
    assert report["failure"]["category"] == "jobs_row_artifact_quarantine_failed"
    assert "bridge" in report["failure"]["message"]
    assert "boom" in report["failure"]["message"]
    scenario = report["scenarios"][-1]
    assert scenario["slug"] == "jobs-row-artifact-quarantine"
    assert scenario["failures"] == [{"process": "bridge", "dataDir": "C:/data", "error": "boom"}]
    assert all(row.get("slug") != "startup-jobs-report-gate" for row in report["scenarios"])


def test_apply_jobs_report_gate_check_lists_every_crashed_half() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [
        _quarantine_failure_row("site", error="site boom"),
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        _quarantine_failure_row("bridge", error="bridge boom"),
    ]

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    scenario = report["scenarios"][-1]
    assert [failure["process"] for failure in scenario["failures"]] == ["site", "bridge"]
    assert "site boom" in report["failure"]["message"]
    assert "bridge boom" in report["failure"]["message"]
    assert len(report["scenarios"]) == 1


def test_apply_jobs_report_gate_check_dedupes_identical_crash_rows() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [_quarantine_failure_row("site", error="boom")] * 2

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert len(report["scenarios"][-1]["failures"]) == 1


def test_apply_jobs_report_gate_check_fails_when_one_process_stops_reporting() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [_gate_row("summary-sidecar", "site")]

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert report["ok"] is False
    assert report["failure"]["step"] == "startup-jobs-report-gate"
    assert report["failure"]["category"] == "startup_jobs_report_gate_missing"
    assert "no desktop_launch_jobs_report_gate row for bridge" in report["failure"]["message"]
    assert report["scenarios"][-1]["missingProcesses"] == ["bridge"]


def test_apply_jobs_report_gate_check_treats_unattributed_rows_as_missing() -> None:
    row = {"event": "desktop_launch_jobs_report_gate", "fields": {"reader": "summary-sidecar"}}
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": [row]}

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert report["failure"]["category"] == "startup_jobs_report_gate_missing"
    assert report["scenarios"][-1]["missingProcesses"] == ["site", "bridge"]


def test_apply_jobs_report_gate_check_passes_when_both_halves_journal_a_skip() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = _launch_skip_rows()

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is False
    assert report["ok"] is True
    assert report["scenarios"] == []


def test_apply_jobs_report_gate_check_fails_when_one_half_never_journals() -> None:
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}
    report["startupMetrics"] = [_skip_row("bridge")]

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is True
    assert report["failure"]["category"] == "startup_jobs_report_gate_missing"
    assert "no desktop_launch_jobs_report_gate row for site" in report["failure"]["message"]
    assert report["scenarios"][-1]["missingProcesses"] == ["site"]


def test_apply_jobs_report_gate_check_leaves_a_launch_that_never_journaled_gate_events_alone() -> (
    None
):
    report: dict[str, Any] = {"ok": True, "failure": None, "scenarios": [], "startupMetrics": []}

    failed = smoke.packaged_smoke_orchestrator_mod._apply_jobs_report_gate_check(  # noqa: SLF001
        report
    )

    assert failed is False
    assert report["ok"] is True
    assert report["scenarios"] == []


def test_jobs_pipeline_lane_seeds_only_for_the_jobs_pipeline_script() -> None:
    with workspace_tmpdir("packaged-smoke-gate-seed") as tmp:
        data_dir = Path(tmp) / "runtime-data"
        jobs_pipeline_script = Path(
            "tests/frontend/packaged-desktop-smoke.jobs-pipeline.mjs"
        ).resolve()

        seeded = smoke.packaged_smoke_orchestrator_mod._seed_jobs_pipeline_smoke_feed_if_needed(  # noqa: SLF001
            node_smoke_script=jobs_pipeline_script,
            jobs_pipeline_script=jobs_pipeline_script,
            data_dir=data_dir,
            finished_at="2026-10-19T00:00:00+00:00",
        )
        not_seeded = smoke.packaged_smoke_orchestrator_mod._seed_jobs_pipeline_smoke_feed_if_needed(  # noqa: SLF001
            node_smoke_script=jobs_pipeline_script.with_name("other.mjs"),
            jobs_pipeline_script=jobs_pipeline_script,
            data_dir=data_dir / "other",
            finished_at="2026-10-19T00:00:00+00:00",
        )

    assert seeded is True
    assert not_seeded is False


def _run_profile_smoke(*, record_only: bool, startup_metrics: list[dict[str, object]]):
    with workspace_tmpdir("packaged-smoke-gate") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        args = [
            "--exe-path",
            str(exe_path),
            "--report-path",
            str(report_path),
            "--artifacts-dir",
            str(artifacts_dir),
            "--startup-probe",
            "--profile-only",
            "--open-path",
            "jobs.html",
        ]
        if record_only:
            args.append("--profile-record-only")
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={"browserName": "chrome", "browserPath": "C:/Chrome/chrome.exe"},
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(process, mock.Mock(), mock.Mock()),
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(smoke, "wait_for_runtime_events", return_value=startup_metrics),
            mock.patch.object(smoke, "capture_performance_profile_snapshot", return_value={}),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "passed",
                    "classification": "ok",
                    "firstUsableEvent": "jobs_first_interactive",
                    "firstUsableMs": 2100,
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            return smoke.run_packaged_smoke(smoke.parse_args(args))


@pytest.mark.parametrize("record_only", [False, True])
def test_packaged_smoke_fails_when_startup_journal_used_full_report(record_only: bool) -> None:
    startup_metrics: list[dict[str, Any]] = [
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        *_launch_gate_rows("full-report", durationMs=1500.0),
        {"event": "jobs_first_interactive", "payload": {"elapsedMs": 2100}},
        {"event": "jobs_first_render", "payload": {"elapsedMs": 1800}},
        {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
        {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 900}},
        {"event": "jobs_local_data_init_ready", "payload": {"elapsedMs": 1300}},
        {"event": "jobs_module_boot_start", "payload": {"elapsedMs": 1100}},
    ]

    payload = _run_profile_smoke(record_only=record_only, startup_metrics=startup_metrics)

    assert payload["ok"] is False
    assert payload["failure"]["step"] == "startup-jobs-report-gate"
    assert payload["failure"]["category"] == "startup_jobs_report_gate_full_report"
    gate_rows = [
        row for row in payload["scenarios"] if row.get("slug") == "startup-jobs-report-gate"
    ]
    assert len(gate_rows) == 1


@pytest.mark.parametrize("record_only", [False, True])
def test_packaged_smoke_fails_when_quarantine_crashed(record_only: bool) -> None:
    startup_metrics = [
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        _quarantine_failure_row("site", error="quarantine crashed"),
        *_launch_skip_rows(),
        {"event": "jobs_first_interactive", "payload": {"elapsedMs": 2100}},
    ]

    payload = _run_profile_smoke(record_only=record_only, startup_metrics=startup_metrics)

    assert payload["ok"] is False
    assert payload["failure"]["step"] == "jobs-row-artifact-quarantine"
    assert payload["failure"]["category"] == "jobs_row_artifact_quarantine_failed"


def test_packaged_smoke_passes_when_startup_journal_used_sidecar() -> None:
    startup_metrics: list[dict[str, Any]] = [
        {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
        *_launch_gate_rows(),
        {"event": "jobs_first_interactive", "payload": {"elapsedMs": 2100}},
    ]

    payload = _run_profile_smoke(record_only=False, startup_metrics=startup_metrics)

    assert payload["ok"] is True
    assert not payload["failure"]
    assert all(row.get("slug") != "startup-jobs-report-gate" for row in payload["scenarios"])
