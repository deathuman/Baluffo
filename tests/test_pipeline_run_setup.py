from __future__ import annotations

import json
from pathlib import Path

from src.jobs.pipeline_run_setup import prepare_pipeline_run


class _NoopRedirectResolver:
    def seed_cache(self, _cache: dict[str, str]) -> None:
        return None


def test_prepare_pipeline_run_writes_prep_progress_before_source_execution(
    tmp_path: Path,
) -> None:
    setup = prepare_pipeline_run(
        output_dir=tmp_path,
        run_id="fetch_prep_1",
        started_at_override="2026-07-02T10:00:00+00:00",
        source_loaders=[],
        default_source_loaders=lambda **_kwargs: [],
        build_redirect_resolver_fn=lambda **_kwargs: _NoopRedirectResolver(),
    )
    try:
        task_state = json.loads(setup.paths.task_state_path.read_text(encoding="utf-8"))
        progress = task_state["taskProgress"]
        assert progress["phaseKey"] == "initializing_runtime"
        assert progress["phaseLabel"] == "Initializing fetch runtime"
        assert task_state["workItems"] == []

        report = json.loads(setup.paths.report_path.read_text(encoding="utf-8"))
        setup_timing = (report.get("runtime") or {}).get("setupTiming") or {}
        # ponytail: phaseOrder is append-only and may include interleaved
        # sub-stage keys like "loading_state/read_source_state" that the bench
        # uses to attribute inside-loading-state cost. Assert the anchor
        # sequence is preserved as a subsequence rather than pinning an exact
        # list, so new observability emits don't churn this test.
        expected_anchors = [
            "loading_state",
            "seeding_existing_output",
            "selecting_sources",
            "applying_exclusions",
            "initializing_runtime",
        ]
        phase_order = setup_timing["phaseOrder"]
        cursor = 0
        for anchor in expected_anchors:
            while cursor < len(phase_order) and phase_order[cursor] != anchor:
                cursor += 1
            assert cursor < len(phase_order), f"missing anchor phase: {anchor}"
            cursor += 1
        assert "totalSetupMs" in setup_timing
        assert setup_timing["counts"]["selectedSourceCount"] == 0
    finally:
        setup.stop_progress_reporter()
