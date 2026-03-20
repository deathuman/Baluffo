from __future__ import annotations

from pathlib import Path

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService


def test_trigger_discovery_task_uncapped_uses_explicit_uncapped_args(tmp_path: Path) -> None:
    calls: list[tuple[str, list[str] | None]] = []

    def run_background_script(script_name: str, args: list[str] | None = None) -> int:
        calls.append((script_name, list(args or [])))
        return 123

    service = DiscoveryService(
        paths=DiscoveryPaths(
            report=tmp_path / "source-discovery-report.json",
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            log=tmp_path / "source-discovery.log",
        ),
        deps=DiscoveryDeps(
            schema_version=1,
            now_iso=lambda: "2026-03-20T12:00:00Z",
            now_utc=lambda: None,
            parse_iso=lambda value: None,
            pid_is_running=lambda pid: False,
            bridge_log=lambda *args, **kwargs: None,
            load_json_object=lambda path, default: default,
            save_json_atomic=lambda path, payload: Path(path).write_text("{}", encoding="utf-8"),
            run_background_script=run_background_script,
            append_run_history=lambda payload: payload,
            normalize_discovery_report_contract=lambda payload: payload,
            load_sync_runtime_state=lambda: {},
            maybe_trigger_auto_sync_push=lambda reason: False,
            mark_discovery_sync_finished=lambda finished_at: None,
        ),
    )

    status_code, result = service.trigger_discovery_task(
        route_name="/tasks/run-discovery",
        payload={"preset": "uncapped"},
        enable_auto_sync_watch=False,
    )

    assert status_code == 200
    assert result["started"] is True
    assert result["preset"] == "uncapped"
    assert result["args"] == ["--mode", "dynamic", "--top", "0"]
    assert calls == [("source_discovery.py", ["--mode", "dynamic", "--top", "0"])]

