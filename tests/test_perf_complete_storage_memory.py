from __future__ import annotations

import json
from pathlib import Path

from scripts import perf_complete


def test_perf_complete_aggregates_storage_read_profile(tmp_path: Path) -> None:
    storage_path = tmp_path / "storage-metrics.startup.json"
    storage_path.write_text(
        json.dumps(
            {
                "ok": True,
                "storageMetrics": {
                    "reads": {
                        "readCount": 2,
                        "failedReadCount": 1,
                        "surfaceCount": 1,
                        "surfaces": [
                            {
                                "surface": "jobsFeed.staticLight",
                                "artifact": "jobs-unified-light.json",
                                "storageKind": "file",
                                "readCount": 2,
                                "failedReadCount": 1,
                                "durationMs": {"max": 42, "total": 50},
                                "bytesRead": {"max": 4096, "total": 8192},
                                "rowCount": {"max": 300, "total": 600},
                                "memoryDeltaBytes": {"max": 128, "total": 128},
                            }
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    startup = {
        "jobs": {
            "cold": {"report": {"artifacts": {"storageMetricsSnapshot": str(storage_path)}}},
            "warm": {},
        },
        "admin": {"cold": {}, "warm": {}},
    }

    profile = perf_complete.build_storage_read_profile_summary(startup=startup, sync={})
    targets = perf_complete.build_optimization_targets({"storageReadProfile": profile})

    assert profile["samples"][0]["readCount"] == 2
    assert profile["topReadsByDuration"][0]["surface"] == "jobsFeed.staticLight"
    assert profile["topReadsByBytes"][0]["artifact"] == "jobs-unified-light.json"
    assert profile["failedReads"][0]["failedReadCount"] == 1
    assert any(row["kind"] == "storage-read-duration" for row in targets)
    assert any(
        row["kind"] == "storage-read-bytes" and row["rankUnit"] == "bytes" for row in targets
    )
    assert any(row["kind"] == "storage-read-failure" for row in targets)


def test_perf_complete_aggregates_memory_profile_and_targets() -> None:
    benchmarks = {
        "discovery": {
            "memoryMetrics": {
                "sampleCount": 1,
                "peakWorkingSetBytes": 100,
                "firstSample": {"memoryBytes": 70},
                "lastSample": {"memoryBytes": 80},
                "categoryPeaks": {"python": 100},
                "topProcesses": [{"name": "python.exe", "category": "python", "peakBytes": 100}],
            }
        },
        "startup": {
            "jobs": {
                "cold": {
                    "memoryMetrics": {
                        "sampleCount": 1,
                        "peakWorkingSetBytes": 300,
                        "firstSample": {"memoryBytes": 150},
                        "lastSample": {"memoryBytes": 250},
                        "categoryPeaks": {"browser": 300},
                        "topProcesses": [
                            {"name": "chrome.exe", "category": "browser", "peakBytes": 300}
                        ],
                    }
                },
                "warm": {},
            },
            "admin": {"cold": {}, "warm": {}},
        },
    }

    profile = perf_complete.build_memory_profile_summary(benchmarks)
    targets = perf_complete.build_optimization_targets({"memoryProfile": profile})

    assert profile["topSamplesByPeakRam"][0]["source"] == "startup.jobs.cold"
    assert profile["topSamplesBySteadyStateRam"][0]["lastSampleBytes"] == 250
    assert profile["topSamplesByRetainedPeakRam"][0]["peakToLastDeltaBytes"] == 50
    assert profile["topCategoryPeaks"][0]["category"] == "browser"
    assert profile["topProcesses"][0]["name"] == "chrome.exe"
    assert any(
        row["kind"] == "memory-peak" and row["rankUnit"] == "bytes" and row["durationMs"] == 0
        for row in targets
    )
    assert any(
        row["kind"] == "memory-steady-state"
        and row["rankUnit"] == "bytes"
        and row["rankValue"] == 250
        for row in targets
    )
