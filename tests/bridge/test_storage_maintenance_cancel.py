from __future__ import annotations

from pathlib import Path

from src.bridge.storage_health import cancel_storage_maintenance


def test_cancel_storage_maintenance_forwards_the_running_pass(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    class FakeStore:
        def cancel_runtime_retention(self, run_id=""):
            calls.append(run_id)
            return {
                "requested": True,
                "cancelling": True,
                "stale": False,
                "reason": "",
                "runtimeRetention": {
                    "status": "cancelling",
                    "phase": "cancelling",
                    "running": True,
                },
            }

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: FakeStore(),
    )

    result = cancel_storage_maintenance(tmp_path, "retention_abc123")

    assert calls == ["retention_abc123"]
    assert result["ok"] is True
    assert result["requested"] is True
    assert result["cancelling"] is True
    assert result["runtimeRetention"]["status"] == "cancelling"
    assert result["compaction"]["status"] == "not-run"


def test_cancel_storage_maintenance_reports_when_no_pass_is_running(
    tmp_path: Path, monkeypatch
) -> None:
    class IdleStore:
        def cancel_runtime_retention(self, run_id=""):
            return {
                "requested": False,
                "cancelling": False,
                "stale": False,
                "reason": "no-running-pass",
                "runtimeRetention": {"status": "not-run", "running": False, "runId": run_id},
            }

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: IdleStore(),
    )

    result = cancel_storage_maintenance(tmp_path)

    assert result["ok"] is True
    assert result["requested"] is False
    assert result["cancelling"] is False
    assert result["stale"] is False
    assert result["runtimeRetention"]["status"] == "not-run"
