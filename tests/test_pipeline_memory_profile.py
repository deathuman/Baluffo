from __future__ import annotations

import json
from pathlib import Path

from tools.measurements.pipeline.full_pipeline_memory import _sampler_summary
from tools.measurements.pipeline.memory_profile import (
    detect_cgroup_version,
    run_sampler,
    sample_cgroup,
    sample_process,
    sample_processes,
)


def test_sample_cgroup_v2_reads_peak_events_and_memory_split(tmp_path: Path) -> None:
    root = tmp_path / "cgroup"
    root.mkdir()
    (root / "cgroup.controllers").write_text("cpu memory\n", encoding="utf-8")
    (root / "memory.current").write_text("1234\n", encoding="utf-8")
    (root / "memory.max").write_text("4096\n", encoding="utf-8")
    (root / "memory.peak").write_text("3000\n", encoding="utf-8")
    (root / "memory.swap.current").write_text("7\n", encoding="utf-8")
    (root / "memory.swap.max").write_text("8\n", encoding="utf-8")
    (root / "memory.events").write_text(
        "low 0\nhigh 1\nmax 2\noom 1\noom_kill 1\n", encoding="utf-8"
    )
    (root / "memory.stat").write_text("anon 100\nfile 200\nshmem 3\nslab 4\n", encoding="utf-8")

    sample = sample_cgroup(root)

    assert detect_cgroup_version(root) == 2
    assert sample["available"] is True
    assert sample["memoryCurrentBytes"] == 1234
    assert sample["memoryPeakBytes"] == 3000
    assert sample["memoryEvents"]["oom_kill"] == 1
    assert sample["memoryStat"] == {"anon": 100, "file": 200, "shmem": 3, "slab": 4}


def test_sample_cgroup_v1_fallback_reads_usage_and_limit(tmp_path: Path) -> None:
    root = tmp_path / "cgroup"
    memory = root / "memory"
    memory.mkdir(parents=True)
    (memory / "memory.usage_in_bytes").write_text("200\n", encoding="utf-8")
    (memory / "memory.limit_in_bytes").write_text("max\n", encoding="utf-8")
    (memory / "memory.max_usage_in_bytes").write_text("350\n", encoding="utf-8")
    (memory / "memory.failcnt").write_text("2\n", encoding="utf-8")
    (memory / "memory.stat").write_text("cache 20\nrss 30\n", encoding="utf-8")

    sample = sample_cgroup(root)

    assert detect_cgroup_version(root) == 1
    assert sample["memoryCurrentBytes"] == 200
    assert sample["memoryPeakBytes"] == 350
    assert sample["memoryMaxBytes"] is None
    assert sample["memoryEvents"] == {"failcnt": 2}
    assert sample["memoryStat"]["anon"] == 30
    assert sample["memoryStat"]["file"] == 20


def test_sample_process_reads_status_smaps_and_proc_metadata(tmp_path: Path) -> None:
    proc = tmp_path / "123"
    proc.mkdir()
    (proc / "stat").write_text("123 (worker) S 1 1 0 0 -1 0\n", encoding="utf-8")
    (proc / "status").write_text(
        "Name:\tworker\nVmRSS:\t10 kB\nVmHWM:\t20 kB\nRssAnon:\t8 kB\nRssFile:\t2 kB\n",
        encoding="utf-8",
    )
    (proc / "smaps_rollup").write_text(
        "Rss: 12 kB\nPss: 9 kB\nPrivate_Clean: 4 kB\nPrivate_Dirty: 5 kB\nSwap: 1 kB\n",
        encoding="utf-8",
    )
    (proc / "cmdline").write_bytes(b"python\0worker.py\0")
    (proc / "cgroup").write_text("0::/docker/abc\n", encoding="utf-8")

    sample = sample_process(123, tmp_path)

    assert sample is not None
    assert sample["ppid"] == 1
    assert sample["command"] == "python worker.py"
    assert sample["rssBytes"] == 10 * 1024
    assert sample["pssBytes"] == 9 * 1024
    assert sample["cgroup"] == "0::/docker/abc"
    assert sample_processes(tmp_path, limit=5)[0]["pid"] == 123


def test_run_sampler_writes_bounded_ndjson_and_honors_stop_file(tmp_path: Path) -> None:
    root = tmp_path / "cgroup"
    root.mkdir()
    (root / "cgroup.controllers").write_text("memory\n", encoding="utf-8")
    (root / "memory.current").write_text("1\n", encoding="utf-8")
    stop_file = tmp_path / "stop"
    stop_file.write_text("stop\n", encoding="utf-8")
    output = tmp_path / "samples.ndjson"

    count = run_sampler(
        output_path=output,
        cgroup_root=root,
        proc_root=tmp_path / "missing-proc",
        stop_file=stop_file,
        interval_s=0.01,
    )

    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert count == 1
    assert len(rows) == 1
    assert rows[0]["cgroup"]["memoryCurrentBytes"] == 1


def test_sampler_summary_preserves_terminal_cgroup_evidence(tmp_path: Path) -> None:
    output = tmp_path / "cgroup-process.ndjson"
    output.write_text(
        json.dumps(
            {
                "sampledAt": "2026-09-24T12:00:00+00:00",
                "cgroup": {"memoryPeakBytes": 1234, "memoryEvents": {"oom_kill": 0}},
                "processes": [{"pid": 1, "pssBytes": 99}],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    summary = _sampler_summary(tmp_path)

    assert summary["sampleCount"] == 1
    assert summary["peakMemoryBytes"] == 1234
    assert summary["terminalCgroup"]["memoryEvents"]["oom_kill"] == 0
    assert summary["terminalProcesses"][0]["pid"] == 1
