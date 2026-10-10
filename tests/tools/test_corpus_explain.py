"""The explain page is only worth printing if its numbers survive an ugly run.

So the tests use a run directory written to ``tmp_path`` rather than a hand-built mapping: the
point of this tool is that it reads what a run actually left on disk, and a fixture that never
touches disk would not test that. The pure functions are pinned separately, because the ranking
and the counter-off case are where a plausible-looking page can still be wrong -- ranking a stage
that does not exist, or printing ``0.0%`` retry time for a run whose counters were never on.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import explain  # noqa: E402


def _report(counters: bool) -> dict[str, Any]:
    slow: dict[str, Any] = {
        "name": "google_sheets",
        "durationMs": 8000,
        "keptCount": 30,
        "status": "ok",
        "failureBucket": "",
        "stageTimingsMs": {"fetchAndParse": 3000, "parseCsv": 4000},
    }
    empty: dict[str, Any] = {
        "name": "static_source::static:listing_url:https://empty.example/jobs",
        "durationMs": 2000,
        "keptCount": 0,
        "status": "ok",
        "failureBucket": "site_changed",
        "stageTimingsMs": {"fetchAndParse": 2000},
    }
    if counters:
        slow["httpCounters"] = {"attempts": 3, "failed": 2}
        empty["httpCounters"] = {"attempts": 1, "failed": 0}
    return {
        "runId": "replay-20261008-190121",
        "startedAt": "2026-10-08T19:01:21.900733+00:00",
        "finishedAt": "2026-10-08T19:02:35.140763+00:00",
        "runtime": {
            "maxWorkers": 1,
            "fetchClient": "custom",
            "adapterHttpConcurrency": 48,
            "forceRefreshAll": True,
            "browserFallbackEnabled": False,
            "selectedSourceCount": 2,
        },
        "summary": {"inputCount": 31, "mergedCount": 1, "outputCount": 30},
        "taskProgress": {
            "counts": {
                "sourceCount": 2,
                "resolvedSources": 2,
                "failedSources": 0,
                "excludedSources": 0,
                "outputCount": 30,
            }
        },
        "sources": [slow, empty],
    }


def _write_run(tmp_path: Path, *, replay: bool = True, counters: bool = True) -> Path:
    name = "replay-20261008-190121" if replay else "record-20261008-190121"
    run_dir = tmp_path / name
    data_dir = run_dir / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "jobs-fetch-report.json").write_text(
        json.dumps(_report(counters)), encoding="utf-8"
    )
    parity = {
        "mode": "replay" if replay else "record",
        "status": "parity-ok",
        "epochId": "corpus-subset-20261008-183226",
        "label": "unit fixture",
        "runConfig": {"mode": "replay" if replay else "record", "maxWorkers": 1},
    }
    (run_dir / explain.PARITY_FILE).write_text(json.dumps(parity), encoding="utf-8")
    return run_dir


def test_rank_stages_keeps_every_stage_and_its_share() -> None:
    ranked = explain.rank_stages(
        {
            "totalSourceMs": 100,
            "byStageMs": {"parseCsv": 30, "fetchAndParse": 60, "unattributedMs": 10},
        }
    )

    assert [row["stage"] for row in ranked] == ["fetchAndParse", "parseCsv", "unattributedMs"]
    assert [row["sharePct"] for row in ranked] == [60.0, 30.0, 10.0]
    assert ranked[0]["ms"] == 60


def test_rank_stages_tolerates_a_missing_payload() -> None:
    assert explain.rank_stages({}) == []
    assert explain.rank_stages({"totalSourceMs": 10, "byStageMs": "nonsense"}) == []


def test_top_sources_takes_the_largest_and_skips_non_mappings() -> None:
    rows: list[Any] = [
        {"name": "small", "durationMs": 10, "keptCount": 1},
        "not a mapping",
        None,
        {"name": "big", "durationMs": 9000, "keptCount": 0, "status": "ok"},
        {"name": "mid", "durationMs": 500, "keptCount": 4},
    ]

    ranked = explain.top_sources(rows, top=2)

    assert [row["name"] for row in ranked] == ["big", "mid"]
    # Shares are of the *readable* total (9510), so a skipped row cannot inflate them.
    assert ranked[0]["sharePct"] == 94.64
    assert ranked[0]["zeroKept"] is True
    assert ranked[1]["zeroKept"] is False


def test_zero_kept_cost_aggregates_and_flags_unknown_kept_counts() -> None:
    rows: list[Any] = [
        {"name": "a", "durationMs": 100, "keptCount": 0},
        {"name": "b", "durationMs": 300, "keptCount": 5},
        {"name": "c", "durationMs": 200, "keptCount": 0},
        {"name": "d", "durationMs": 400},
        "not a mapping",
    ]

    cost = explain.zero_kept_cost(rows)

    assert cost["zeroKeptSources"] == 2
    assert cost["zeroKeptMs"] == 300
    assert cost["totalSourceMs"] == 1000
    assert cost["sharePct"] == 30.0
    assert cost["keptCountUnknownSources"] == 1


def test_share_pct_returns_zero_rather_than_raising_on_a_zero_total() -> None:
    assert explain.share_pct(5, 0) == 0.0
    assert explain.share_pct(5, None) == 0.0
    assert explain.share_pct("nonsense", "nonsense") == 0.0
    assert explain.share_pct(1, 4) == 25.0


def test_retry_exposure_states_the_counters_were_off() -> None:
    exposure = explain.retry_exposure({"sourcesWithCounters": 0, "sharePct": 0.0})

    assert exposure["available"] is False
    assert "counters were off" in str(exposure["reason"])
    assert "sharePct" not in exposure


def test_retry_exposure_reports_the_share_when_the_counters_ran() -> None:
    exposure = explain.retry_exposure(
        {
            "sourcesWithCounters": 4,
            "repeatedFetchSources": 2,
            "repeatedFetchShareMs": 1500,
            "sharePct": 12.5,
            "proxy": "sources whose time contains a repeated fetch of the same URL",
        }
    )

    assert exposure["available"] is True
    assert exposure["sharePct"] == 12.5
    assert exposure["repeatedFetchSources"] == 2


def test_render_markdown_names_the_counters_off_case_instead_of_a_zero_share() -> None:
    analysis = {
        "run": {"name": "replay-x", "isReplay": True, "dir": "/out/replay-x"},
        "wall": {"reportWindowMs": 1000, "summedSourceMs": 1000, "parallelismRatio": 1.0},
        "counts": {},
        "stages": [{"stage": "fetchAndParse", "ms": 900, "sharePct": 90.0}],
        "topSources": [],
        "zeroKept": {"zeroKeptSources": 0, "zeroKeptMs": 0, "sharePct": 0.0},
        "retry": explain.retry_exposure({"sourcesWithCounters": 0}),
        "config": {},
    }

    page = explain.render_markdown(analysis)

    retry_section = page.split("## Retry / backoff exposure", 1)[1].split("## Run configuration")[0]
    assert "counters were off" in retry_section
    # No fabricated share: a percentage here would read as "no source retried".
    assert "%" not in retry_section
    assert explain.REPLAY_BANNER in page


def test_analyze_reads_a_run_directory(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)

    analysis = explain.analyze(run_dir)

    assert analysis["run"]["isReplay"] is True
    assert analysis["run"]["epochId"] == "corpus-subset-20261008-183226"
    assert analysis["wall"]["reportWindowMs"] == 73240
    assert analysis["wall"]["summedSourceMs"] == 10000
    assert analysis["wall"]["parallelismRatio"] == 0.137
    # fetchAndParse 3000+2000, parseCsv 4000, and the 1000 ms the slow source never labelled.
    assert [row["stage"] for row in analysis["stages"]] == [
        "fetchAndParse",
        "parseCsv",
        "unattributedMs",
    ]
    assert [row["sharePct"] for row in analysis["stages"]] == [50.0, 40.0, 10.0]
    assert analysis["topSources"][0]["name"] == "google_sheets"
    assert analysis["zeroKept"]["zeroKeptSources"] == 1
    assert analysis["retry"]["available"] is True
    assert analysis["config"]["fetchClient"] == "custom"
    assert analysis["config"]["adapterHttpConcurrency"] == 48
    assert analysis["counts"]["outputCount"] == 30


def test_analyze_marks_missing_configuration_as_not_present(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path, counters=False)
    data_dir = run_dir / "data"
    report = json.loads((data_dir / "jobs-fetch-report.json").read_text(encoding="utf-8"))
    report.pop("runtime")
    (data_dir / "jobs-fetch-report.json").write_text(json.dumps(report), encoding="utf-8")

    page = explain.render_markdown(explain.analyze(run_dir))

    assert explain.NOT_PRESENT in page
    assert "fetch client: not present in this run" in page


def test_main_writes_artifacts_and_prints_the_page(tmp_path: Path, capsys: Any) -> None:
    run_dir = _write_run(tmp_path)
    out_dir = tmp_path / "out"

    exit_code = explain.main(["--run", str(run_dir), "--out", str(out_dir), "--top", "1"])

    assert exit_code == explain.EXIT_OK
    printed = capsys.readouterr().out
    assert "# Where did the time go?" in printed
    payload = json.loads((out_dir / "explain.json").read_text(encoding="utf-8"))
    assert payload["run"]["name"] == run_dir.name
    assert len(payload["topSources"]) == 1
    assert "## Zero-kept cost" in (out_dir / "explain.md").read_text(encoding="utf-8")


def test_main_resolves_a_bare_token_under_the_corpus_parent(tmp_path: Path, capsys: Any) -> None:
    run_dir = _write_run(tmp_path)

    exit_code = explain.main(["--run", run_dir.name, "--corpus-parent", str(tmp_path)])

    assert exit_code == explain.EXIT_OK
    assert "# Where did the time go?" in capsys.readouterr().out


def test_main_returns_two_for_a_run_that_does_not_exist(tmp_path: Path, capsys: Any) -> None:
    exit_code = explain.main(["--run", "no-such-run", "--corpus-parent", str(tmp_path)])

    assert exit_code == explain.EXIT_INFRASTRUCTURE
    assert "no run directory" in capsys.readouterr().err


def test_main_returns_two_when_the_report_is_missing(tmp_path: Path, capsys: Any) -> None:
    run_dir = tmp_path / "replay-no-report"
    run_dir.mkdir()

    exit_code = explain.main(["--run", str(run_dir)])

    assert exit_code == explain.EXIT_INFRASTRUCTURE
    assert "no jobs-fetch-report.json" in capsys.readouterr().err


def test_is_replay_prefers_artifacts_over_the_directory_name() -> None:
    assert explain.is_replay("record-20261008", {"mode": "replay"}, {}) is True
    assert explain.is_replay("replay-20261008", {}, {}) is True
    assert explain.is_replay("record-20261008", {"mode": "record"}, {"mode": "record"}) is False
    assert explain.is_replay("timing-benchmark-20260925", {"mode": "record"}, {}) is False
