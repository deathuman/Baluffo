from __future__ import annotations

from scripts import perf_adapter_summary


def test_summarize_adapter_durations_uses_network_wait_counters() -> None:
    rows = perf_adapter_summary.summarize_adapter_durations(
        [
            {"networkWaitCounters": {"adapterDurationsMs": {"lever": 1000, "greenhouse": 500}}},
            {"networkWaitCounters": {"adapterDurationsMs": {"lever": 250}}},
        ]
    )

    assert rows == [
        {"adapter": "lever", "durationMs": 1250},
        {"adapter": "greenhouse", "durationMs": 500},
    ]


def test_summarize_adapter_durations_reads_nested_timing_summaries() -> None:
    rows = perf_adapter_summary.summarize_adapter_durations(
        [
            {
                "firstRun": {
                    "runtime": {
                        "timingSummary": {
                            "adapterTimings": [
                                {"adapter": "ashby", "durationMs": 200},
                                {"adapter": "lever", "durationMs": 100},
                            ]
                        }
                    }
                },
                "runtime": {"adapterTimings": [{"adapter": "static", "durationMs": 300}]},
            }
        ]
    )

    assert rows == [
        {"adapter": "static", "durationMs": 300},
        {"adapter": "ashby", "durationMs": 200},
        {"adapter": "lever", "durationMs": 100},
    ]


def test_format_adapter_summary_handles_missing_data() -> None:
    assert perf_adapter_summary.format_adapter_summary([]) == "No adapter duration data found."


def test_format_adapter_summary_prints_compact_table() -> None:
    table = perf_adapter_summary.format_adapter_summary(
        [{"adapter": "lever", "durationMs": 1250}],
        limit=1,
    )

    assert "adapter" in table
    assert "lever" in table
    assert "1.2s" in table
