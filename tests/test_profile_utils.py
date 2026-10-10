from __future__ import annotations

import json
import threading
import time

import pytest

from src.shared import profile_utils
from tests.helpers.temp_paths import workspace_tmpdir
from tools.measurements.pipeline import thread_cpu_sampler


def _spin(*, seconds: float = 0.25) -> int:
    """Yield the GIL often, so the sampler thread reliably gets scheduled."""
    deadline = time.monotonic() + seconds
    ticks = 0
    while time.monotonic() < deadline:
        ticks += 1
        time.sleep(0.005)
    return ticks


def test_run_profiled_is_transparent_when_disabled(monkeypatch) -> None:
    with workspace_tmpdir("profile-disabled") as data_dir:
        monkeypatch.delenv("BALUFFO_PROFILE", raising=False)
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        result = profile_utils.run_profiled(lambda left, right: left + right, 2, 3)

        assert result == 5
        assert not (data_dir / "perf-profiles").exists()


def test_run_profiled_writes_profile_artifacts_when_enabled(monkeypatch) -> None:
    with workspace_tmpdir("profile-enabled") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "1")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        result = profile_utils.run_profiled(
            lambda value: sum(range(value)),
            10,
            profile_name="adapter_static_source::Example Jobs",
        )

        profile_dir = data_dir / "perf-profiles"
        assert result == 45
        assert (profile_dir / "adapter_static_source_Example_Jobs.prof").exists()
        assert (profile_dir / "adapter_static_source_Example_Jobs.prof.txt").exists()


def test_run_profiled_dumps_profile_and_reraises_exception(monkeypatch) -> None:
    with workspace_tmpdir("profile-exception") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "yes")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            profile_utils.run_profiled(fail, profile_name="discovery/full run")

        profile_dir = data_dir / "perf-profiles"
        assert (profile_dir / "discovery_full_run.prof").exists()
        assert (profile_dir / "discovery_full_run.prof.txt").exists()


def test_run_profiled_does_not_fail_when_text_summary_render_fails(monkeypatch) -> None:
    with workspace_tmpdir("profile-render-failure") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "1")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        class BrokenStats:
            def __init__(self, *_args, **_kwargs) -> None:
                raise TypeError("broken stats")

        monkeypatch.setattr(profile_utils.pstats, "Stats", BrokenStats)

        result = profile_utils.run_profiled(lambda: "ok", profile_name="adapter lever")

        profile_dir = data_dir / "perf-profiles"
        assert result == "ok"
        assert (profile_dir / "adapter_lever.prof").exists()
        assert "Unable to render profile summary" in (
            profile_dir / "adapter_lever.prof.txt"
        ).read_text(encoding="utf-8")


def test_run_profiled_propagates_unexpected_summary_render_failure(monkeypatch) -> None:
    with workspace_tmpdir("profile-render-unexpected-failure") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "1")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        class BrokenStats:
            def __init__(self, *_args, **_kwargs) -> None:
                raise RuntimeError("unexpected stats bug")

        monkeypatch.setattr(profile_utils.pstats, "Stats", BrokenStats)

        with pytest.raises(RuntimeError, match="unexpected stats bug"):
            profile_utils.run_profiled(lambda: "ok", profile_name="adapter lever")

        profile_dir = data_dir / "perf-profiles"
        assert (profile_dir / "adapter_lever.prof").exists()
        assert not (profile_dir / "adapter_lever.prof.txt").exists()


def test_sanitize_profile_name_falls_back_to_default() -> None:
    assert profile_utils.sanitize_profile_name("???") == "default"


def test_sample_profile_is_off_unless_requested(monkeypatch) -> None:
    """Disabled is the production shape: no thread, no directory, no allocation."""
    with workspace_tmpdir("sample-off") as data_dir:
        monkeypatch.delenv("BALUFFO_PROFILE_SAMPLE", raising=False)
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        assert profile_utils.sample_interval_ms() == 0
        assert profile_utils.start_sample_profile() is None
        assert profile_utils.stop_sample_profile(None) is None
        assert not (data_dir / "perf-profiles").exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("", 0), ("0", 0), ("nonsense", 0), ("-5", 0), ("10", 10), ("99999", 2000)],
)
def test_sample_interval_parsing(monkeypatch, raw: str, expected: int) -> None:
    """A malformed value disables the sampler rather than defaulting it on."""
    monkeypatch.setenv("BALUFFO_PROFILE_SAMPLE", raw)
    assert profile_utils.sample_interval_ms() == expected


def test_parse_task_stat_counts_fields_after_last_paren() -> None:
    """comm is user text: it may contain spaces and parentheses of its own."""
    body = " ".join(["R", *[str(index) for index in range(1, 11)], "250", "150"])
    line = f"4321 (weird ) name) {body}"

    seconds = profile_utils._parse_task_stat_cpu_seconds(line)

    assert seconds == pytest.approx((250 + 150) / profile_utils._clock_ticks_per_second())


@pytest.mark.parametrize("raw", ["", "no parens here", "1234 (short) R 1 2", "1 (x) R a b c"])
def test_parse_task_stat_returns_none_on_shapes_it_does_not_understand(raw: str) -> None:
    assert profile_utils._parse_task_stat_cpu_seconds(raw) is None


def test_thread_cpu_seconds_is_none_without_proc(monkeypatch) -> None:
    """No /proc means elapsed weighting, not a wrong CPU number."""
    monkeypatch.setattr(profile_utils, "_PROC_TASK_DIR_AVAILABLE", False)
    assert profile_utils._thread_cpu_seconds(1234) is None


def test_native_thread_ids_map_python_idents_to_kernel_tids() -> None:
    """The ident from ``sys._current_frames()`` is not necessarily a kernel tid.

    Reading ``/proc/self/task/<ident>/stat`` fails for every thread on Linux and
    looks exactly like a host without ``/proc``, so the sampler reported elapsed
    time as per-thread CPU until this mapping existed. ``native_id`` is the tid
    and is also the join key to a sampler reading the same child from outside.

    The invariant is per live thread, not an inequality: on Windows the two
    identifiers happen to coincide, and a test that asserted they differ would
    fail there for a reason that has nothing to do with the sampler.
    """
    seen: dict[str, int] = {}
    release = threading.Event()

    def hold() -> None:
        seen["ident"] = threading.get_ident()
        seen["native"] = threading.get_native_id()
        release.wait(timeout=5)

    worker = threading.Thread(target=hold, name="native-id-probe")
    worker.start()
    deadline = time.monotonic() + 5
    while "ident" not in seen and time.monotonic() < deadline:
        time.sleep(0.01)
    try:
        mapping = profile_utils.native_thread_ids()
        assert seen["ident"] in mapping
        assert mapping[seen["ident"]] == seen["native"]
    finally:
        release.set()
        worker.join(timeout=5)

    assert mapping[threading.get_ident()] == threading.get_native_id()


def test_sample_profile_records_threads_and_function_level_frames(monkeypatch) -> None:
    with workspace_tmpdir("sample-on") as data_dir:
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_PROFILE_SAMPLE", "10")

        sampler = profile_utils.start_sample_profile(profile_name="unit run")
        assert sampler is not None
        workers = [threading.Thread(target=_spin, name=f"spin-{index}") for index in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        path = profile_utils.stop_sample_profile(sampler)

        assert path is not None
        assert path.name == "cpu-samples-unit_run.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        # Which weighting applies is platform-dependent; that it is named is not.
        assert payload["weighting"] in {"proc-cpu", "elapsed"}
        assert payload["samples"] >= 1
        assert payload["selfWeight"]
        names = {row["name"] for row in payload["threads"]}
        assert "MainThread" in names
        assert "baluffo-cpu-sampler" in names
        # Function-level keys, not line-level: a hot function must not be split.
        for row in payload["selfWeight"]:
            _, _, function = row["frame"].rpartition(":")
            assert function
            assert row["weightSeconds"] >= 0.0


def test_sample_profile_excludes_its_own_frames_from_the_rankings(monkeypatch) -> None:
    with workspace_tmpdir("sample-self") as data_dir:
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))
        monkeypatch.setenv("BALUFFO_PROFILE_SAMPLE", "10")

        sampler = profile_utils.start_sample_profile(profile_name="self frames")
        assert sampler is not None
        _spin(seconds=0.2)
        payload = sampler.snapshot() if sampler else {}
        profile_utils.stop_sample_profile(sampler)

        frames = " ".join(row["frame"] for row in payload["selfWeight"])
        assert "_sample_once" not in frames


@pytest.mark.parametrize(
    "raw",
    [
        "4321 (python3) R 1 2 3 4 5 6 7 8 9 10 250 150 0 0 0 0 0 0",
        "4321 (weird ) name) R 1 2 3 4 5 6 7 8 9 10 250 150",
    ],
)
def test_stat_parser_agrees_with_the_container_payload(raw: str) -> None:
    """The container payload duplicates this parser, so the copies must not drift."""
    assert thread_cpu_sampler.parse_stat_cpu_seconds(
        raw
    ) == profile_utils._parse_task_stat_cpu_seconds(raw)
