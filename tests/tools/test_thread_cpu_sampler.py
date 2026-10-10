"""The in-container payload reads another process's /proc, so it is tested against a fake /proc.

Two rules are load-bearing:

* the duplicated ``stat`` parser must agree with
  ``src.shared.profile_utils._parse_task_stat_cpu_seconds``. It is duplicated on
  purpose (a ``/tmp`` script in the container has no repo on ``sys.path``), so
  that agreement is pinned by a parity test living beside the production helper,
  in ``tests/test_profile_utils.py``.
* the target is found by matching the command line, never a pid handed in. The
  fetch child is spawned by the container's own service, so a pid we "know" is a
  pid we guessed.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.pipeline import thread_cpu_sampler as sampler  # noqa: E402


def _stat_line(pid: int, comm: str, *, utime: int, stime: int, state: str = "R") -> str:
    body = " ".join([state, *[str(index) for index in range(1, 11)], str(utime), str(stime)])
    return f"{pid} ({comm}) {body}\n"


def _write_proc_tree(
    root: Path, pid: int, *, comm: str, cmdline: list[str], threads: dict[int, tuple[str, int, int]]
) -> None:
    process_dir = root / str(pid)
    (process_dir / "task").mkdir(parents=True, exist_ok=True)
    (process_dir / "cmdline").write_bytes("\x00".join(cmdline).encode("utf-8") + b"\x00")
    first = next(iter(threads.values()))
    (process_dir / "stat").write_text(
        _stat_line(pid, comm, utime=first[1], stime=first[2]), encoding="utf-8"
    )
    for tid, (tid_comm, utime, stime) in threads.items():
        thread_dir = process_dir / "task" / str(tid)
        thread_dir.mkdir(parents=True, exist_ok=True)
        (thread_dir / "stat").write_text(
            _stat_line(tid, tid_comm, utime=utime, stime=stime), encoding="utf-8"
        )
        (thread_dir / "comm").write_text(tid_comm + "\n", encoding="utf-8")


@pytest.mark.parametrize("raw", ["", "no parens", "1 (x) R 1 2", "1 (x) R a b c"])
def test_stat_parser_refuses_shapes_it_cannot_read(raw: str) -> None:
    assert sampler.parse_stat_cpu_seconds(raw) is None


def test_stat_state_is_read_from_after_the_last_paren() -> None:
    assert sampler.parse_stat_state(_stat_line(9, "x ) y", utime=1, stime=2, state="D")) == "D"
    assert sampler.parse_stat_state("garbage") == ""


def test_target_is_found_by_command_line_and_excludes_the_sampler(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_proc_tree(
        proc_root,
        101,
        comm="python3",
        cmdline=["python", "src/jobs_fetcher.py"],
        threads={101: ("python3", 10, 5)},
    )
    _write_proc_tree(
        proc_root,
        202,
        comm="python3",
        cmdline=["python", "/tmp/baluffo-thread-cpu.py"],
        threads={202: ("python3", 1, 1)},
    )
    _write_proc_tree(
        proc_root,
        303,
        comm="sh",
        cmdline=["sh", "-c", "echo jobs_fetcher"],
        threads={303: ("sh", 1, 1)},
    )

    found = sampler.find_target_pids(proc_root, "jobs_fetcher", exclude=set())

    # 303 matches on a shell command line; the sampler only claims processes it
    # can actually attribute, and its own pid is never a target.
    assert found == [101, 303]
    assert sampler.find_target_pids(proc_root, "jobs_fetcher", exclude={101, 303}) == []


def test_sample_process_reports_every_thread_and_a_gone_process(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_proc_tree(
        proc_root,
        101,
        comm="python3",
        cmdline=["python", "src/jobs_fetcher.py"],
        threads={101: ("python3", 700, 300), 102: ("python3", 40, 10)},
    )

    payload = sampler.sample_process(proc_root, 101)

    assert payload["gone"] is False
    assert "jobs_fetcher" in payload["cmdline"]
    assert payload["processCpuSeconds"] == pytest.approx(
        (700 + 300) / sampler._clock_ticks_per_second()
    )
    by_tid = {row["tid"]: row for row in payload["threads"]}
    assert set(by_tid) == {101, 102}
    assert by_tid[102]["comm"] == "python3"
    assert by_tid[102]["state"] == "R"
    assert sampler.sample_process(proc_root, 999) == {"pid": 999, "gone": True}


def test_sample_process_skips_threads_whose_stat_cannot_be_read(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    _write_proc_tree(
        proc_root, 101, comm="python3", cmdline=["python", "x"], threads={101: ("python3", 1, 1)}
    )
    broken = proc_root / "101" / "task" / "777"
    broken.mkdir(parents=True)
    (broken / "stat").write_text("not a stat line", encoding="utf-8")

    payload = sampler.sample_process(proc_root, 101)

    assert [row["tid"] for row in payload["threads"]] == [101]


def test_payload_main_emits_one_json_object_per_target_per_tick(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "self").mkdir(parents=True)
    _write_proc_tree(
        proc_root,
        101,
        comm="python3",
        cmdline=["python", "src/jobs_fetcher.py"],
        threads={101: ("python3", 100, 50)},
    )

    exit_code = sampler.main(
        [
            "--proc-root",
            str(proc_root),
            "--match",
            "jobs_fetcher",
            "--ticks",
            "1",
            "--interval",
            "0.05",
        ]
    )

    assert exit_code == 0
    emitted = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(emitted) == 1
    payload: dict[str, Any] = emitted[0]
    assert payload["pid"] == 101
    assert payload["tick"] == 1
    assert payload["threads"][0]["cpuSeconds"] == pytest.approx(
        150 / sampler._clock_ticks_per_second()
    )


def test_payload_refuses_a_root_without_proc(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = sampler.main(["--proc-root", str(tmp_path / "empty"), "--ticks", "1"])

    assert exit_code == 2
    assert "Linux-only" in capsys.readouterr().err
