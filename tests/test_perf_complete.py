from __future__ import annotations

import json
from pathlib import Path

from scripts import perf_complete
from src.shared.process_memory import summarize_memory_samples


def test_summarize_artifacts_deduplicates_key_paths(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    first = root / "a.txt"
    second = root / "nested" / "b.txt"
    second.parent.mkdir()
    first.write_bytes(b"aaa")
    second.write_bytes(b"bbbb")

    summary = perf_complete.summarize_artifacts(roots=[root], key_paths=[first])

    assert summary["totalBytes"] == 7
    assert summary["fileCount"] == 2
    assert summary["keyArtifacts"][0]["sizeBytes"] == 3
    assert summary["largestFiles"][0]["sizeBytes"] == 4


def test_memory_summary_preserves_unsupported_reason() -> None:
    summary = summarize_memory_samples(
        [{"unsupportedReason": "process-tree memory sampling unsupported"}]
    )

    assert summary["sampleCount"] == 0
    assert summary["peakWorkingSetBytes"] == 0
    assert summary["unsupportedReason"] == "process-tree memory sampling unsupported"


def test_repeated_payload_benchmark_aggregates_runs(tmp_path: Path, monkeypatch) -> None:
    durations = [300, 100, 200]

    def _fake_run(command, *, stdout_path: Path, stderr_path: Path, env=None):
        duration = durations.pop(0)
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        stdout_path.write_text(
            json.dumps(
                {
                    "totalDurationMs": duration,
                    "stageDurationsMs": {"work": duration // 2},
                }
            ),
            encoding="utf-8",
        )
        stderr_path.write_text("", encoding="utf-8")
        return {
            "command": command,
            "exitCode": 0,
            "stdoutPath": str(stdout_path),
            "stderrPath": str(stderr_path),
            "memoryMetrics": {
                "sampleCount": 1,
                "peakWorkingSetBytes": duration,
                "peakRssBytes": 0,
                "maxProcessCount": 2,
                "skippedProcessCount": 0,
                "unsupportedReason": "",
            },
        }

    monkeypatch.setattr(perf_complete, "run_monitored_command", _fake_run)
    steps = [
        (
            f"run-{index}",
            ["benchmark", str(index)],
            tmp_path / f"run-{index}.txt",
            tmp_path / f"run-{index}.err",
        )
        for index in range(1, 4)
    ]

    summary, exit_code = perf_complete.run_repeated_payload_benchmark(
        mode="discovery",
        steps=steps,
        output_dir=tmp_path,
        baseline_dir=tmp_path / "baseline",
    )

    assert exit_code == 0
    assert summary["medianDurationMs"] == 200
    assert summary["stageMedianDurationsMs"] == {"work": 100}
    assert summary["memoryMetrics"]["peakWorkingSetBytes"] == 300
    assert summary["comparison"]["status"] == "baseline_missing"


def test_repeated_memory_summary_preserves_attribution() -> None:
    summary = perf_complete._summarize_memory_runs(
        [
            {
                "memoryMetrics": {
                    "sampleCount": 1,
                    "peakWorkingSetBytes": 100,
                    "peakRssBytes": 0,
                    "maxProcessCount": 1,
                    "skippedProcessCount": 0,
                    "unsupportedReason": "",
                    "peakSample": {
                        "workingSetBytes": 100,
                        "rssBytes": 0,
                        "memoryBytes": 100,
                        "processes": [
                            {
                                "pid": 1,
                                "name": "python.exe",
                                "category": "python",
                                "memoryBytes": 100,
                            }
                        ],
                    },
                    "topProcesses": [
                        {
                            "pid": 1,
                            "name": "python.exe",
                            "category": "python",
                            "peakBytes": 100,
                        }
                    ],
                    "categoryPeaks": {"python": 100},
                }
            },
            {
                "memoryMetrics": {
                    "sampleCount": 1,
                    "peakWorkingSetBytes": 200,
                    "peakRssBytes": 0,
                    "maxProcessCount": 2,
                    "skippedProcessCount": 0,
                    "unsupportedReason": "",
                    "peakSample": {
                        "workingSetBytes": 200,
                        "rssBytes": 0,
                        "memoryBytes": 200,
                        "processes": [
                            {
                                "pid": 2,
                                "name": "chrome.exe",
                                "category": "browser",
                                "memoryBytes": 200,
                            }
                        ],
                    },
                    "topProcesses": [
                        {
                            "pid": 2,
                            "name": "chrome.exe",
                            "category": "browser",
                            "peakBytes": 200,
                        }
                    ],
                    "categoryPeaks": {"browser": 200},
                }
            },
        ]
    )

    assert summary["peakWorkingSetBytes"] == 200
    assert summary["peakSample"]["processes"][0]["name"] == "chrome.exe"
    assert summary["topProcesses"][0]["name"] == "chrome.exe"
    assert summary["categoryPeaks"]["browser"] == 200


def test_startup_summary_extracts_first_usable_and_runtime_memory(tmp_path: Path) -> None:
    report_path = tmp_path / "startup-report.json"
    artifacts_dir = tmp_path / "startup-artifacts"
    artifacts_dir.mkdir()
    report_path.write_text(
        json.dumps(
            {
                "ok": True,
                "startupProfile": {
                    "status": "passed",
                    "firstUsableMs": 1234,
                    "firstUsableEvent": "jobs_first_interactive",
                    "classification": "",
                    "stages": [{"key": "total_launch_to_first_usable_ui", "durationMs": 1234}],
                },
                "memoryMetrics": {"sampleCount": 2, "peakWorkingSetBytes": 5000},
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    summary = perf_complete._startup_summary(
        mode="cold",
        report_path=report_path,
        artifacts_dir=artifacts_dir,
        command_result={"exitCode": 0},
        baseline_dir=tmp_path / "baseline",
    )

    assert summary["durationMs"] == 1234
    assert summary["status"] == "baseline_missing"
    assert summary["startupProfileStatus"] == "passed"
    assert summary["stageDurationsMs"] == {"total_launch_to_first_usable_ui": 1234}
    assert summary["memoryMetrics"]["peakWorkingSetBytes"] == 5000


def test_sync_rehearsal_extracts_push_pull_timing(tmp_path: Path, monkeypatch) -> None:
    def _fake_run(command, *, stdout_path: Path, stderr_path: Path, env=None):
        report_path = Path(command[command.index("--report-path") + 1])
        artifacts_dir = Path(command[command.index("--artifacts-dir") + 1])
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_stdout = artifacts_dir / "runtime.stdout.log"
        runtime_stderr = artifacts_dir / "runtime.stderr.log"
        runtime_stdout.write_text("", encoding="utf-8")
        runtime_stderr.write_text("", encoding="utf-8")
        report_path.write_text(
            json.dumps(
                {
                    "ok": True,
                    "scenarios": [
                        {
                            "slug": "packaged-sync-rehearsal",
                            "status": "passed",
                            "durationMs": 900,
                            "memoryMetrics": {
                                "sampleCount": 3,
                                "peakWorkingSetBytes": 7000,
                            },
                            "details": {
                                "tokenRequests": 1,
                                "contentRequests": 2,
                                "putRequests": 3,
                                "deleteRequests": 0,
                                "bytesWritten": 42,
                                "pushTiming": {
                                    "totalDurationMs": 111,
                                    "stageTotalsMs": {"pushRemote": 100},
                                },
                                "pullTiming": {
                                    "totalDurationMs": 222,
                                    "stageTotalsMs": {"pullMergeRemote": 200},
                                },
                                "runtimeStdout": str(runtime_stdout),
                                "runtimeStderr": str(runtime_stderr),
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return {"exitCode": 0, "memoryMetrics": {"sampleCount": 1}}

    monkeypatch.setattr(perf_complete, "run_monitored_command", _fake_run)

    summary, exit_code = perf_complete.run_sync_rehearsal(
        output_dir=tmp_path,
        runtime_timeout_s=5,
        baseline_dir=tmp_path / "baseline",
        exe_path=tmp_path / "Baluffo.exe",
    )

    assert exit_code == 0
    assert summary["pushTiming"]["totalDurationMs"] == 111
    assert summary["pullTiming"]["stageTotalsMs"] == {"pullMergeRemote": 200}
    assert summary["memoryMetrics"]["peakWorkingSetBytes"] == 7000
    assert summary["comparisons"]["push"]["status"] == "baseline_missing"


def test_record_complete_rows_writes_baselines(tmp_path: Path) -> None:
    summary = {
        "summaryPath": str(tmp_path / "summary.json"),
        "benchmarks": {
            "discovery": {
                "medianDurationMs": 100,
                "stageMedianDurationsMs": {"probe": 90},
                "comparison": {"status": "baseline_missing"},
            },
            "fetch": {
                "medianDurationMs": 200,
                "stageMedianDurationsMs": {"fetchAndParse": 180},
                "comparison": {"status": "baseline_missing"},
            },
            "frontendBoot": {
                "durationMs": 300,
                "pages": [{"page": "jobs", "durationMs": 300}],
                "comparison": {"status": "baseline_missing"},
            },
            "startup": {
                "cold": {
                    "durationMs": 400,
                    "stageDurationsMs": {"total": 400},
                    "comparison": {"status": "baseline_missing"},
                },
                "warm": {
                    "durationMs": 500,
                    "stageDurationsMs": {"total": 500},
                    "comparison": {"status": "baseline_missing"},
                },
            },
            "sync": {
                "pushTiming": {
                    "totalDurationMs": 600,
                    "stageTotalsMs": {"pushRemote": 600},
                },
                "pullTiming": {
                    "totalDurationMs": 700,
                    "stageTotalsMs": {"pullMergeRemote": 700},
                },
                "comparisons": {
                    "push": {"status": "baseline_missing"},
                    "pull": {"status": "baseline_missing"},
                },
            },
        },
    }

    perf_complete._record_complete_rows(
        summary,
        baseline_dir=tmp_path / "baseline",
        trend_path=tmp_path / "trend.ndjson",
        record_baseline=True,
        record_trend=False,
    )

    assert (tmp_path / "baseline" / "startup-cold-baseline.json").is_file()
    assert (tmp_path / "baseline" / "sync-push-baseline.json").is_file()
    assert (tmp_path / "trend.ndjson").is_file()


def test_console_summary_reports_comparison_status_for_startup_and_sync(capsys) -> None:
    empty_section = {
        "comparison": {"status": "baseline_missing"},
        "artifactSizes": {"totalBytes": 0},
        "memoryMetrics": {},
    }
    summary = {
        "benchmarks": {
            "discovery": {"medianDurationMs": 1, **empty_section},
            "fetch": {"medianDurationMs": 2, **empty_section},
            "frontendBoot": {"durationMs": 3, **empty_section},
            "startup": {
                "cold": {
                    "durationMs": 4,
                    "status": "baseline_missing",
                    "startupProfileStatus": "failed",
                    "artifactSizes": {"totalBytes": 40},
                    "memoryMetrics": {"peakWorkingSetBytes": 400},
                    "comparison": {"status": "baseline_missing"},
                },
                "warm": {
                    "durationMs": 5,
                    "status": "baseline_missing",
                    "startupProfileStatus": "failed",
                    "artifactSizes": {"totalBytes": 50},
                    "memoryMetrics": {"peakWorkingSetBytes": 500},
                    "comparison": {"status": "baseline_missing"},
                },
            },
            "sync": {
                "status": "passed",
                "pushTiming": {"totalDurationMs": 6},
                "pullTiming": {"totalDurationMs": 7},
                "artifactSizes": {"totalBytes": 60},
                "memoryMetrics": {
                    "peakWorkingSetBytes": 600,
                    "peakSample": {
                        "processes": [
                            {
                                "name": "chrome.exe",
                                "category": "browser",
                                "memoryBytes": 2 * 1024 * 1024,
                            }
                        ]
                    },
                },
                "comparisons": {
                    "push": {"status": "baseline_missing"},
                    "pull": {"status": "baseline_missing"},
                },
            },
        }
    }

    perf_complete._print_console_summary(summary)

    output = capsys.readouterr().out
    assert "startup.cold,4,400,40,baseline_missing" in output
    assert "sync.push,6,600,60,baseline_missing" in output
    assert "chrome.exe[browser]=2.0MiB" in output
