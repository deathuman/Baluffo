"""The spawn-clock tool's deliverable is a number, so the rules that produce it are pinned.

Three rules carry the weight:

* the journal is read from the previous offset, so a warm run cannot inherit an
  earlier run's marks;
* a run missing the window or interactive mark is reported incomplete, not zero;
* the report names the clock each mark used, because a page row's ``ts`` is the
  bridge arrival time and a POST delay is not page work.

The clock arithmetic itself lives in ``src/ship/startup_spawn_clock.py``, shared
with the packaged probe, and is pinned by ``tests/test_startup_spawn_clock.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.desktop import spawn_startup  # noqa: E402


def test_read_new_rows_advances_past_a_torn_line(tmp_path: Path) -> None:
    journal = tmp_path / "metrics.jsonl"
    journal.write_text(
        json.dumps({"event": "a"}) + "\n" + '{"event": "b"\n' + json.dumps({"event": "c"}) + "\n",
        encoding="utf-8",
    )

    rows, offset = spawn_startup.read_new_rows(journal, 0)
    assert [row["event"] for row in rows] == ["a", "c"]
    assert offset == journal.stat().st_size

    again, offset_again = spawn_startup.read_new_rows(journal, offset)
    assert again == []
    assert offset_again == offset


def test_read_new_rows_returns_nothing_for_a_missing_journal(tmp_path: Path) -> None:
    rows, offset = spawn_startup.read_new_rows(tmp_path / "absent.jsonl", 7)

    assert rows == []
    assert offset == 7


def test_read_new_rows_does_not_replay_a_warm_run(tmp_path: Path) -> None:
    journal = tmp_path / "metrics.jsonl"
    journal.write_text(json.dumps({"event": "desktop_launch_start"}) + "\n", encoding="utf-8")
    _, offset = spawn_startup.read_new_rows(journal, 0)

    with journal.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"event": "desktop_shell_window_shown"}) + "\n")

    rows, _ = spawn_startup.read_new_rows(journal, offset)

    assert [row["event"] for row in rows] == ["desktop_shell_window_shown"]


def test_summary_medians_ignore_incomplete_runs() -> None:
    summary = spawn_startup.summarize_runs(
        [
            {"bootMs": 100, "windowShownMs": 1000, "firstInteractiveMs": 1100},
            {"bootMs": 140, "windowShownMs": 1200, "firstInteractiveMs": None},
        ]
    )

    assert summary["runs"] == 2
    assert summary["bootMs"] == {"values": [100, 140], "median": 120, "min": 100, "max": 140}
    assert summary["firstInteractiveMs"] == {
        "values": [1100],
        "median": 1100,
        "min": 1100,
        "max": 1100,
    }


def test_markdown_reports_each_clock_and_the_median_row() -> None:
    runs = [
        {
            "label": "cold-1",
            "mode": "cold",
            "bootMs": 100,
            "windowShownMs": 1000,
            "firstInteractiveMs": 1100,
            "firstInteractiveClock": "page",
        },
        {
            "label": "warm-1",
            "mode": "warm",
            "bootMs": 140,
            "windowShownMs": 1200,
            "firstInteractiveMs": 1250,
            "firstInteractiveClock": "arrival",
        },
    ]

    rendered = spawn_startup.render_markdown(
        {"runs": runs, "summary": spawn_startup.summarize_runs(runs)}
    )

    assert "| cold-1 | cold | 100 ms | 1000 ms | 1100 ms | page |" in rendered
    assert "| warm-1 | warm | 140 ms | 1200 ms | 1250 ms | arrival |" in rendered
    assert "| **median** | 2 runs | 120 ms | 1100 ms | 1175 ms | |" in rendered


def test_markdown_marks_a_missing_mark_as_not_available() -> None:
    run = {
        "label": "cold-1",
        "mode": "cold",
        "bootMs": 100,
        "windowShownMs": None,
        "firstInteractiveMs": None,
        "firstInteractiveClock": "",
    }

    rendered = spawn_startup.render_markdown(
        {"runs": [run], "summary": spawn_startup.summarize_runs([run])}
    )

    assert "| cold-1 | cold | 100 ms | n/a | n/a | n/a |" in rendered
    assert "| **median** | 1 runs | 100 ms | n/a | n/a | |" in rendered


def test_free_ports_are_distinct_and_usable() -> None:
    site, bridge = spawn_startup._free_ports()

    assert site != bridge
    assert 0 < site < 65536
    assert 0 < bridge < 65536


def test_missing_executable_is_a_usage_error(tmp_path: Path) -> None:
    exit_code = spawn_startup.main(["--exe", str(tmp_path / "absent.exe")])

    assert exit_code == 2
