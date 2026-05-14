from __future__ import annotations

from src.shared.process_memory import summarize_memory_samples


def test_summarize_memory_samples_reports_peak_process_attribution() -> None:
    summary = summarize_memory_samples(
        [
            {
                "rootPid": 10,
                "platform": "nt",
                "processCount": 2,
                "skippedProcessCount": 0,
                "workingSetBytes": 300,
                "rssBytes": 0,
                "processes": [
                    {"pid": 10, "parentPid": 0, "name": "python.exe", "workingSetBytes": 100},
                    {"pid": 11, "parentPid": 10, "name": "chrome.exe", "workingSetBytes": 200},
                ],
            },
            {
                "rootPid": 10,
                "platform": "nt",
                "processCount": 3,
                "skippedProcessCount": 1,
                "workingSetBytes": 500,
                "rssBytes": 0,
                "processes": [
                    {"pid": 10, "parentPid": 0, "name": "python.exe", "workingSetBytes": 150},
                    {"pid": 11, "parentPid": 10, "name": "chrome.exe", "workingSetBytes": 300},
                    {"pid": 12, "parentPid": 10, "name": "node.exe", "workingSetBytes": 50},
                ],
            },
        ]
    )

    assert summary["sampleCount"] == 2
    assert summary["peakWorkingSetBytes"] == 500
    assert summary["skippedProcessCount"] == 1
    assert summary["peakSample"]["memoryBytes"] == 500
    assert summary["peakSample"]["processes"][0]["name"] == "chrome.exe"
    assert summary["topProcesses"][0]["peakBytes"] == 300
    assert summary["categoryPeaks"]["browser"] == 300
    assert summary["categoryPeaks"]["python"] == 150
    assert summary["categoryPeaks"]["node"] == 50


def test_summarize_memory_samples_attribution_fallback_is_empty_when_unsupported() -> None:
    summary = summarize_memory_samples(
        [{"unsupportedReason": "process-tree memory sampling unsupported"}]
    )

    assert summary["sampleCount"] == 0
    assert summary["peakSample"] == {}
    assert summary["topProcesses"] == []
    assert summary["categoryPeaks"] == {}
