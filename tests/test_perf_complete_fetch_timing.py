from __future__ import annotations

import json
from pathlib import Path

from scripts import perf_complete


def test_fetch_payload_benchmark_extracts_source_timing(tmp_path: Path, monkeypatch) -> None:
    def _fake_run(command, *, stdout_path: Path, stderr_path: Path, env=None):
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        stdout_path.write_text(
            json.dumps(
                {
                    "totalDurationMs": 1000,
                    "sourceTimingSignals": {
                        "firstRunSlowestSources": [
                            {"name": "lever_sources", "adapter": "lever", "durationMs": 900}
                        ],
                        "firstRunSlowestProviderBoards": [
                            {
                                "source": "lever_sources",
                                "adapter": "lever",
                                "name": "Slow Board",
                                "status": "ok",
                                "cacheDecision": "miss",
                                "durationMs": 700,
                                "fetchMs": 600,
                                "parseMs": 80,
                            }
                        ],
                    },
                    "nextOptimizationTargets": [
                        {
                            "name": "lever_sources",
                            "action": "source_policy_review",
                            "priority": 100,
                            "durationMs": 900,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        stderr_path.write_text("", encoding="utf-8")
        return {"command": command, "exitCode": 0, "memoryMetrics": {}}

    monkeypatch.setattr(perf_complete, "run_monitored_command", _fake_run)
    summary, exit_code = perf_complete.run_repeated_payload_benchmark(
        mode="fetch",
        steps=[("run-1", ["benchmark"], tmp_path / "run-1.txt", tmp_path / "run-1.err")],
        output_dir=tmp_path,
        baseline_dir=tmp_path / "baseline",
    )

    assert exit_code == 0
    assert summary["sourceTiming"]["topSourcesByDuration"][0]["name"] == "lever_sources"
    assert summary["sourceTiming"]["topProviderBoardsByDuration"][0]["name"] == "Slow Board"
    assert summary["sourceTiming"]["providerSourceBreakdown"][0]["source"] == "lever_sources"
    assert summary["sourceTiming"]["providerSourceBreakdown"][0]["totalFetchMs"] == 600
    assert summary["sourceTiming"]["providerSourceBreakdown"][0]["statuses"] == {"ok": 1}
    assert summary["sourceTiming"]["cacheDecisionBreakdown"]["miss"] == 1
    assert summary["sourceTiming"]["nextOptimizationTargets"][0]["action"] == (
        "source_policy_review"
    )
