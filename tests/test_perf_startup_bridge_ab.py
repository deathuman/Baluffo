from __future__ import annotations

import json
from pathlib import Path

from scripts import perf_startup_bridge_ab


def _write_report(path: Path, first_usable_ms: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"startupProfile": {"firstUsableMs": first_usable_ms}}),
        encoding="utf-8",
    )


def test_bridge_ab_summary_promotes_only_when_threshold_is_met(tmp_path: Path, monkeypatch) -> None:
    durations = {
        "default": [(3000, 2800), (2900, 2750)],
        "parallel": [(2500, 2300), (2400, 2200)],
    }
    counters = {"default": 0, "parallel": 0}

    def _fake_pair(*, artifact_root, summary_path, open_path, **_kwargs):
        mode = "parallel" if "parallel" in str(artifact_root) else "default"
        cold_ms, warm_ms = durations[mode][counters[mode]]
        counters[mode] += 1
        cold_path = Path(artifact_root) / "cold-report.json"
        warm_path = Path(artifact_root) / "warm-report.json"
        _write_report(cold_path, cold_ms)
        _write_report(warm_path, warm_ms)
        Path(summary_path).parent.mkdir(parents=True, exist_ok=True)
        Path(summary_path).write_text(
            json.dumps(
                {
                    "ok": True,
                    "coldReportPath": str(cold_path),
                    "warmReportPath": str(warm_path),
                }
            ),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(perf_startup_bridge_ab, "run_startup_probe_pair", _fake_pair)

    report, exit_code = perf_startup_bridge_ab.run_bridge_ab(
        output_dir=tmp_path,
        pairs=2,
        runtime_timeout_s=60,
        pages=["jobs"],
    )

    assert exit_code == 0
    decision = report["pages"]["jobs"]["summary"]["decision"]
    assert decision["promoteParallelDefault"] is True
    assert decision["coldImprovementMs"] >= 250
    assert Path(report["reportPath"]).is_file()


def test_bridge_ab_decision_rejects_failed_parallel_samples() -> None:
    decision = perf_startup_bridge_ab._decision(
        {
            "sampleCount": 5,
            "passedCount": 5,
            "coldMedianMs": 8358,
            "warmMedianMs": 8199,
        },
        {
            "sampleCount": 5,
            "passedCount": 0,
            "coldMedianMs": 0,
            "warmMedianMs": 0,
        },
    )

    assert decision["promoteParallelDefault"] is False
    assert "failures" in decision["reason"]
