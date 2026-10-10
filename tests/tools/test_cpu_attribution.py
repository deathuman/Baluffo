"""The report is the deliverable, so what it claims is pinned here.

Three rules carry the most weight:

* a frame is bucketed by *what it is*, and the first matching rule wins -- a
  per-domain table that silently reclassifies httpx as orchestration would
  invert the protocol-versus-parsing split the report exists to make;
* the unmapped bucket is printed with its top frames when material, so the
  domain table can never quietly describe only the part of the run we recognised;
* an elapsed-weighted capture yields ``inconclusive``, never a shape. Reading
  elapsed time as CPU is how a blocked thread gets reported as a busy one.
"""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.pipeline import cpu_attribution  # noqa: E402

HTTPX_FRAME = "/usr/local/lib/python3.13/site-packages/httpx/_client.py:100:get"
WORKER_FRAME = "/app/src/jobs/adapters/static_sources.py:10:parse_listing"
UNMAPPED_FRAME = "/usr/local/lib/python3.13/site-packages/some_dep/core.py:5:work"


def _samples_payload(
    *,
    weighting: str = "proc-cpu",
    loop_weight: float = 700.0,
    worker_weight: float = 300.0,
    unmapped_weight: float = 400.0,
) -> dict[str, object]:
    return {
        "profile": "pipeline_abc",
        "intervalMs": 10,
        "cpuWeighted": weighting == "proc-cpu",
        "weighting": weighting,
        "samples": 1234,
        "durationMs": 60000,
        "threads": [
            {
                "tid": 11,
                "name": "Thread-3",
                "samples": 900,
                "weightSeconds": loop_weight,
                "isSampler": False,
                "topSelfFrames": [
                    {"frame": HTTPX_FRAME, "weightSeconds": loop_weight, "samples": 900}
                ],
            },
            {
                "tid": 12,
                "name": "ThreadPoolExecutor-0_0",
                "samples": 400,
                "weightSeconds": worker_weight,
                "isSampler": False,
                "topSelfFrames": [
                    {"frame": WORKER_FRAME, "weightSeconds": worker_weight, "samples": 400}
                ],
            },
            {
                "tid": 13,
                "name": "baluffo-cpu-sampler",
                "samples": 2000,
                "weightSeconds": 1.5,
                "isSampler": True,
                "topSelfFrames": [],
            },
        ],
        "selfWeight": [
            {"frame": HTTPX_FRAME, "weightSeconds": loop_weight, "samples": 900},
            {"frame": WORKER_FRAME, "weightSeconds": worker_weight, "samples": 400},
            {"frame": UNMAPPED_FRAME, "weightSeconds": unmapped_weight, "samples": 500},
        ],
        "cumulativeWeight": [
            {
                "frame": "/app/src/jobs/pipeline_source_loop.py:1:run",
                "weightSeconds": 1000.0,
                "samples": 1300,
            },
            {"frame": HTTPX_FRAME, "weightSeconds": 700.0, "samples": 900},
        ],
    }


def _fetch_report() -> dict[str, object]:
    return {
        "sources": [
            {
                "name": "static_source::a",
                "status": "ok",
                "durationMs": 90000,
                "keptCount": 12,
                "stageTimingsMs": {"detailFetch": 30000, "parse": 1000},
            },
            {
                "name": "static_source::b",
                "status": "error",
                "durationMs": 10000,
                "keptCount": 0,
                "stageTimingsMs": {"detailFetch": 9000},
            },
            {
                "name": "static_source::c",
                "status": "ok",
                "durationMs": 5000,
                "keptCount": 3,
                "stageTimingsMs": {"detailFetch": 4000},
            },
        ]
    }


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (HTTPX_FRAME, "protocol-tls"),
        ("/usr/local/lib/python3.13/site-packages/ssl.py:1:read", "protocol-tls"),
        ("/usr/local/lib/python3.13/asyncio/base_events.py:1:_run_once", "protocol-tls"),
        ("/usr/lib/python3.13/zlib.py:1:decompress", "body-decode"),
        (WORKER_FRAME, "html-parse"),
        ("/app/src/jobs/dedupe.py:1:dedupe", "canonicalise-dedup"),
        ("/app/src/jobs/state_source_state.py:1:read", "state-io"),
        ("/usr/local/lib/python3.13/json/decoder.py:1:decode", "state-io"),
        ("/app/src/jobs/pipeline_source_loop.py:1:run", "orchestration"),
        ("/app/src/shared/profile_utils.py:1:_sample_once", "sampler"),
        (UNMAPPED_FRAME, "unmapped"),
    ],
)
def test_classify_frame_puts_protocol_work_before_its_callers(frame: str, expected: str) -> None:
    assert cpu_attribution.classify_frame(frame) == expected


def test_classify_frame_handles_windows_separators() -> None:
    assert (
        cpu_attribution.classify_frame("C:\\app\\src\\jobs\\dedupe.py:1:dedupe")
        == "canonicalise-dedup"
    )


def test_bucket_weights_shares_add_up() -> None:
    payload = _samples_payload()
    entries = payload["selfWeight"]
    assert isinstance(entries, list)

    rows = cpu_attribution.bucket_weights(entries)

    assert [row["domain"] for row in rows] == ["protocol-tls", "unmapped", "html-parse"]
    assert sum(float(row["sharePct"]) for row in rows) == pytest.approx(100.0, abs=0.01)


def test_unmapped_frames_are_returned_worst_first() -> None:
    payload = _samples_payload()
    entries = payload["selfWeight"]
    assert isinstance(entries, list)

    rows = cpu_attribution.unmapped_frames(entries)

    assert [row["frame"] for row in rows] == [UNMAPPED_FRAME]


def test_stage_share_measures_the_tail_and_the_zero_kept_share() -> None:
    share = cpu_attribution.stage_share(_fetch_report())

    assert share["sourceCount"] == 3
    assert share["totalDurationMs"] == 105000
    assert share["zeroKept"]["count"] == 1
    assert share["zeroKept"]["durationSharePct"] == pytest.approx(9.52, abs=0.01)
    assert share["errored"]["count"] == 1
    assert share["stageTotalsMs"] == {"detailFetch": 43000, "parse": 1000}
    assert share["topSources"][0]["name"] == "static_source::a"
    assert share["durationStatsMs"]["max"] == 90000


def test_thread_roles_come_from_the_frames_not_only_the_name() -> None:
    rows = cpu_attribution.thread_table(_samples_payload())
    by_tid = {row["tid"]: row for row in rows}

    # Unnamed by construction in transport.py, so only its frames can call it the loop.
    assert by_tid[11]["role"] == "async-transport-loop"
    assert by_tid[12]["role"] == "source-worker"
    assert by_tid[13]["role"] == "sampler"
    assert by_tid[13]["sharePct"] == 0.0
    assert by_tid[11]["sharePct"] == pytest.approx(70.0, abs=0.01)


def test_verdict_names_a_single_thread_bottleneck_when_one_thread_dominates() -> None:
    tables = cpu_attribution.thread_table(_samples_payload())

    verdict = cpu_attribution.build_verdict(
        weighting="proc-cpu", tables=tables, wall_clock_seconds=600.0, cpu_quota=2.0
    )

    assert verdict["shape"] == "single-thread-transport-bottleneck"
    assert verdict["confidence"] == "High"
    assert any("asyncTransportLoopShare" in item for item in verdict["evidence"])


def test_verdict_refuses_a_shape_on_elapsed_weighting() -> None:
    tables = cpu_attribution.thread_table(_samples_payload(weighting="elapsed"))

    verdict = cpu_attribution.build_verdict(
        weighting="elapsed", tables=tables, wall_clock_seconds=600.0, cpu_quota=2.0
    )

    assert verdict["shape"] == "inconclusive-no-cpu-weighting"
    assert verdict["confidence"] == "Low"


def test_verdict_prefers_the_wider_cpu_total_and_reports_coverage() -> None:
    """The in-process sampler is killed with the child; utilisation must not inherit that blind spot."""
    tables = cpu_attribution.thread_table(
        _samples_payload(loop_weight=20.0, worker_weight=80.0, unmapped_weight=0.0)
    )

    verdict = cpu_attribution.build_verdict(
        weighting="proc-cpu",
        tables=tables,
        wall_clock_seconds=100.0,
        cpu_quota=2.0,
        # The thread table accounts 100 s; the process-wide sampler saw 145 s.
        total_cpu_seconds=145.0,
        coverage={
            "inProcessCoverageSeconds": 60.0,
            "externalCoverageSeconds": 100.0,
            "uncoveredSeconds": 40.0,
        },
    )

    assert verdict["shape"] == "thread-contended-python-work"
    assert any("measuredCpuSeconds=145.0" in item for item in verdict["evidence"])
    assert any("threadTableCpuSeconds=100.0" in item for item in verdict["evidence"])
    assert any("uncoveredSeconds=40.0" in item for item in verdict["evidence"])


def test_verdict_separates_latency_bound_from_thread_contention() -> None:
    """Same shape, two seats: an idle quota is latency-bound, a busy one is contended."""
    tables = cpu_attribution.thread_table(
        _samples_payload(loop_weight=20.0, worker_weight=80.0, unmapped_weight=0.0)
    )

    latency = cpu_attribution.build_verdict(
        weighting="proc-cpu", tables=tables, wall_clock_seconds=600.0, cpu_quota=2.0
    )
    contended = cpu_attribution.build_verdict(
        weighting="proc-cpu", tables=tables, wall_clock_seconds=60.0, cpu_quota=2.0
    )

    assert latency["shape"] == "latency-bound"
    assert contended["shape"] == "thread-contended-python-work"


def test_verdict_does_not_claim_gil_contention_on_a_half_idle_seat() -> None:
    """Measured case: 50.4% of the seat, loop idle. The report must not overclaim."""
    tables = cpu_attribution.thread_table(
        _samples_payload(loop_weight=20.0, worker_weight=484.0, unmapped_weight=0.0)
    )

    verdict = cpu_attribution.build_verdict(
        weighting="proc-cpu", tables=tables, wall_clock_seconds=500.0, cpu_quota=2.0
    )

    assert verdict["shape"] == "thread-contended-python-work"
    # 504 s of CPU over a 1,000 s seat: above the latency-bound cut, but only just.
    assert verdict["confidence"] == "Medium"
    assert "50.4%" in verdict["reason"]
    assert "close to half idle" in verdict["reason"]
    assert "GIL story" not in verdict["reason"]
    levers = " ".join(verdict["levers"])
    assert "Shard work across processes rather than adding threads" not in levers
    assert "larger worker pool frees nothing" in levers


def test_verdict_raises_confidence_when_the_seat_really_is_contended() -> None:
    tables = cpu_attribution.thread_table(
        _samples_payload(loop_weight=20.0, worker_weight=480.0, unmapped_weight=0.0)
    )

    verdict = cpu_attribution.build_verdict(
        weighting="proc-cpu", tables=tables, wall_clock_seconds=200.0, cpu_quota=2.0
    )

    assert verdict["shape"] == "thread-contended-python-work"
    assert verdict["confidence"] == "High"
    assert "Contention, not idleness" in verdict["reason"]
    assert "shared state serialises" in " ".join(verdict["levers"])


def test_main_writes_a_report_that_flags_a_material_unmapped_bucket(tmp_path: Path) -> None:
    run_dir = tmp_path / "s1-run"
    run_dir.mkdir()
    (run_dir / "cpu-samples-pipeline_abc.json").write_text(
        json.dumps(_samples_payload()), encoding="utf-8"
    )
    (run_dir / "jobs-fetch-report-abc.json.gz").write_bytes(
        gzip.compress(json.dumps(_fetch_report()).encode("utf-8"))
    )
    (run_dir / "thread-cpu-summary.json").write_text(
        json.dumps(
            {
                "tickCount": 120,
                "spanSeconds": 118.0,
                "maxGapSeconds": 3.0,
                "tickRateHz": 1.02,
                "malformedLines": 0,
                "threads": [{"tid": 11, "comm": "python3", "cpuSeconds": 690.0, "ticks": 120}],
                "reconciliation": {
                    "processCpuSeconds": 1000.0,
                    "summedThreadCpuSeconds": 1002.0,
                    "deltaSeconds": 2.0,
                    "deltaPct": 0.2,
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = cpu_attribution.main(
        ["--run-dir", str(run_dir), "--wall-clock-seconds", "600", "--cpu-quota", "2.0"]
    )

    assert exit_code == 0
    markdown = (run_dir / "CPU-ATTRIBUTION.md").read_text(encoding="utf-8")
    assert "## Verdict" in markdown
    assert "single-thread-transport-bottleneck" in markdown
    assert "Unmapped is material" in markdown
    assert UNMAPPED_FRAME.split("/")[-1] in markdown
    assert "fetch lane only" in markdown
    # The in-process window (60 s) is shorter than the external one (118 s), and
    # the report has to say so rather than imply full coverage.
    assert "Coverage: the in-process sampler spans 60 s" in markdown
    assert "is **not** an OS thread id" in markdown
    payload = json.loads((run_dir / "cpu-attribution.json").read_text(encoding="utf-8"))
    assert payload["inputs"]["status"]["fetchReport"] == "discovered"
    assert payload["inputs"]["status"]["cpuSamples"] == "discovered"
    assert payload["verdict"]["shape"] == "single-thread-transport-bottleneck"


def test_main_reports_missing_inputs_instead_of_inventing_them(tmp_path: Path) -> None:
    run_dir = tmp_path / "empty-run"
    run_dir.mkdir()

    exit_code = cpu_attribution.main(["--run-dir", str(run_dir)])

    assert exit_code == 0
    payload = json.loads((run_dir / "cpu-attribution.json").read_text(encoding="utf-8"))
    assert payload["inputs"]["status"] == {
        "fetchReport": "missing",
        "cpuSamples": "missing",
        "threadCpu": "missing",
    }
    assert payload["verdict"]["shape"] == "inconclusive-no-cpu-weighting"
    assert "No fetch report was found" in (run_dir / "CPU-ATTRIBUTION.md").read_text(
        encoding="utf-8"
    )


def test_main_refuses_a_run_dir_that_does_not_exist(tmp_path: Path) -> None:
    assert cpu_attribution.main(["--run-dir", str(tmp_path / "nope")]) == 2
