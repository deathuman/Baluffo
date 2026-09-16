from pathlib import Path
from typing import Any, cast

import pytest

from src import admin_bridge
from src.app_version import APP_VERSION


def test_compute_fetcher_metrics_uses_history_window() -> None:
    original_get_ops_api = admin_bridge._get_ops_api
    try:

        class _FakeOpsApi:
            @staticmethod
            def compute_fetcher_metrics(*, window_runs: int = 20):
                assert window_runs == 2
                return {
                    "latestRun": {
                        "duplicateRate": 0.2,
                        "failedSources": 1,
                    },
                    "history": {
                        "windowRuns": 2,
                        "medianDurationMs": 2500,
                    },
                }

        cast(Any, admin_bridge)._get_ops_api = lambda: _FakeOpsApi()
        metrics = admin_bridge.compute_fetcher_metrics(window_runs=2)
        assert int((metrics.get("history") or {}).get("windowRuns") or 0) == 2
        assert int((metrics.get("history") or {}).get("medianDurationMs") or 0) == 2500
        latest = metrics.get("latestRun") or {}
        assert float(latest.get("duplicateRate") or 0.0) == 0.2
        assert int(latest.get("failedSources") or 0) == 1
    finally:
        admin_bridge._get_ops_api = original_get_ops_api


def test_jobs_pipeline_status_payload_includes_app_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Isolation: the status payload resolves the sync config through the packaged
    # loader, which writes the wrapped-key cache next to whichever config it
    # loads. Without an override this test rewrote the repo's real
    # packaging/github-app-sync-config.localkey.json — a build input, so every
    # later preflight portable-build lane missed its cache. Point the loader at
    # an existing-but-invalid config (fresh-checkout condition): the parse fails
    # and the loader returns None before any key derivation or file write.
    invalid_config = tmp_path / "invalid-packaged-sync-config.json"
    invalid_config.write_text("{not json", encoding="utf-8")
    local_key = Path("packaging/github-app-sync-config.localkey.json")
    key_mtime_before = local_key.stat().st_mtime if local_key.exists() else None
    monkeypatch.setenv("BALUFFO_SYNC_APP_CONFIG_PATH", str(invalid_config))

    payload = admin_bridge.get_jobs_pipeline_status_payload()
    assert payload.get("appVersion") == APP_VERSION
    assert (local_key.stat().st_mtime if local_key.exists() else None) == key_mtime_before
