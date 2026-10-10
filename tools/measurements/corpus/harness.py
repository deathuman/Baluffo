"""Launch replayed runs and read their receipts: the one place that knows the driver's shape.

S6, S7 and S0 all do the same two things -- start a replay of an epoch under a stated
configuration, then read what that run produced -- and all three would silently disagree if
each implemented them separately. So they share this module instead.

Two decisions live here and are worth stating once:

**A run is a fresh process, not a function call.** ``BALUFFO_FETCH_COUNTERS`` and the profiling
hook resolve to module-level constants at import, so an in-process "arm" could inherit the
previous arm's setting. Each measurement is therefore a subprocess of
``tools/measurements/corpus/run_fetch.py``, which is also how the shipped claim was measured.
The wall clock is taken around the subprocess, so it includes interpreter start-up -- a shared,
uniform cost in both arms, and one the report states rather than hides.

**A receipt is read, never inferred.** Parity, guard state, ledger and per-source rows come out
of the artifacts the run wrote (``replay-parity.json``, ``jobs-fetch-report.json``); a missing
artifact is reported as missing rather than as a zero. That matters most for the two figures the
later verdicts rest on -- rows and the egress count -- because a confident zero is exactly the
failure this whole spine exists to prevent.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.shared.json_io import existing_json_candidate  # noqa: E402
from src.shared.json_io import read_json as read_product_json

DRIVER_RELATIVE_PATH = "tools/measurements/corpus/run_fetch.py"
REPORT_FILE = "jobs-fetch-report.json"
PARITY_FILE = "replay-parity.json"
# The run artifacts by either of their storage names: rows are gzip-backed on real runs, and a
# reader that knew only the plain name would find nothing and report a confident zero rows.
ROWS_FILE_CANDIDATES: tuple[str, ...] = ("jobs-unified.json", "jobs.json")
REPORT_FILE_CANDIDATES: tuple[str, ...] = ("jobs-fetch-report.json", "fetch-report.json")
COUNTERS_ENV = "BALUFFO_FETCH_COUNTERS"
PROFILE_ENV = "BALUFFO_PROFILE"
EXIT_OK = 0
EXIT_PARITY_FAILED = 1
EXIT_INFRASTRUCTURE = 2


class HarnessError(RuntimeError):
    """The driver could not be launched, or its artifacts are not readable."""


def driver_path() -> Path:
    return _ROOT / DRIVER_RELATIVE_PATH


def _artifact(rundir: Path, name: str) -> Path | None:
    candidate = existing_json_candidate(Path(rundir) / name)
    return candidate if candidate is not None and candidate.is_file() else None


def read_json_artifact(rundir: Path, name: str) -> Any:
    """Read one artifact, or ``None`` when it is absent. Absence is never a zero."""

    path = _artifact(rundir, name)
    return None if path is None else read_product_json(path, None)


def artifact_dir(rundir: Path | str) -> Path:
    """Where a run's artifacts live: the run directory, or its ``data/`` when it has one.

    Both spellings are real -- the driver's ``--run-dir`` holds ``data/`` while its
    ``--output-dir`` *is* that directory -- so every reader normalises here instead of each one
    probing for a ``data`` subdirectory of its own.
    """

    root = Path(rundir)
    data = root / "data"
    return data if data.is_dir() else root


def first_artifact(rundir: Path | str, names: Sequence[str]) -> Path | None:
    """The first of ``names`` present under a run dir, as ``x.json`` or its ``x.json.gz`` form."""

    directory = artifact_dir(rundir)
    for name in names:
        candidate = existing_json_candidate(directory / name)
        if candidate is not None and candidate.is_file():
            return candidate
    return None


def run_rows(rundir: Path | str) -> list[dict[str, Any]]:
    """The rows a run kept, read the way the record-time truncation audit reads them.

    An unreadable or absent rows file reads as an empty list, because the redirect join needs a
    list of rows and has no better answer than "none". A caller that must not confuse "no rows"
    with "no file" reads the artifact itself.
    """

    path = first_artifact(rundir, ROWS_FILE_CANDIDATES)
    rows = read_product_json(path, None) if path is not None else None
    return [row for row in rows or [] if isinstance(row, dict)]


def run_report(rundir: Path | str) -> dict[str, Any]:
    """A run's fetch report, or ``{}`` when it is absent. Absence is never a zero."""

    path = first_artifact(rundir, REPORT_FILE_CANDIDATES)
    report = read_product_json(path, None) if path is not None else None
    return dict(report) if isinstance(report, Mapping) else {}


def run_summary(rundir: Path) -> dict[str, Any]:
    """The rows half of a run: row count, identity digest, projection digest, source statuses.

    Deliberately **no projected rows**. An earlier version attached the whole projection, which put
    11.8 MB of restated rows into every stored sample -- a 53 MB determinism receipt whose every
    consumer reads four digests. The projection digest is the comparison; the rows are regenerable
    from the run directory at any time.
    """

    from tools.measurements.corpus.run_fetch import run_summary as summary

    return summary(artifact_dir(rundir))


def source_rows(rundir: Path) -> list[dict[str, Any]]:
    """The report's per-source rows, or an empty list when the report is missing."""

    report = read_json_artifact(artifact_dir(rundir), REPORT_FILE)
    if not isinstance(report, Mapping):
        return []
    return [dict(row) for row in (report.get("sources") or []) if isinstance(row, Mapping)]


def report_summary(rundir: Path) -> dict[str, Any]:
    report = read_json_artifact(artifact_dir(rundir), REPORT_FILE)
    if not isinstance(report, Mapping):
        return {}
    summary = report.get("summary")
    return dict(summary) if isinstance(summary, Mapping) else {}


STAGE_TIMING_KEYS: tuple[str, ...] = ("stageTimingsMs", "lastStageTimingsMs")


def stage_shares(rundir: Path) -> dict[str, Any]:
    """Per-stage time from the report's own stage timings, so 'where did it go' is answerable.

    A source's ``durationMs`` is split across the stages it names; whatever it does not name is
    reported as ``unattributedMs`` rather than folded into a stage it never claimed, and sources
    with no stage timings at all (the one that failed before parsing, for instance) land there.
    """

    stages: dict[str, int] = {}
    total = 0
    for row in source_rows(rundir):
        duration = to_int(row.get("durationMs"))
        total += duration
        timings: Mapping[str, Any] | None = None
        for key in STAGE_TIMING_KEYS:
            candidate = row.get(key)
            if isinstance(candidate, Mapping):
                timings = candidate
                break
        if timings is None:
            stages["unattributedMs"] = stages.get("unattributedMs", 0) + duration
            continue
        labelled = 0
        for name, value in timings.items():
            amount = to_int(value)
            if amount <= 0:
                continue
            stages[str(name)] = stages.get(str(name), 0) + amount
            labelled += amount
        if duration > labelled:
            stages["unattributedMs"] = stages.get("unattributedMs", 0) + (duration - labelled)
    ordered = dict(sorted(stages.items(), key=lambda item: -item[1]))
    return {"totalSourceMs": total, "byStageMs": ordered}


def run_wall_ms(rundir: Path) -> int:
    """The run's own wall clock from its report window, or 0 when the report is unreadable."""

    report = read_json_artifact(artifact_dir(rundir), REPORT_FILE)
    if not isinstance(report, Mapping):
        return 0
    started = str(report.get("startedAt") or "")
    finished = str(report.get("finishedAt") or "")
    if not started or not finished:
        return 0
    try:
        delta = datetime.fromisoformat(finished) - datetime.fromisoformat(started)
    except ValueError:
        return 0
    return max(0, int(delta.total_seconds() * 1000))


def parallelism(rundir: Path) -> dict[str, Any]:
    """Summed source time over wall clock: >1 means sources overlapped, ~1 means they did not.

    Useful because it separates "the engine is slow" from "the engine is waiting": a run with
    one worker serialises sources, so this ratio sits at ~1 and any wall-clock change has to
    come from per-source work.
    """

    source_ms = int(stage_shares(rundir)["totalSourceMs"])
    wall = run_wall_ms(rundir)
    return {
        "sourceMs": source_ms,
        "reportWallMs": wall,
        "ratio": round(source_ms / wall, 3) if wall else 0.0,
    }


def backoff_exposure(rundir: Path) -> dict[str, Any]:
    """The share of source time spent by sources that repeated a fetch (the risk table's item).

    A source whose wall time contains retry sleeps is one that asked the transport for the *same
    URL* more than once -- ``repeatFetches``, the counter that counts attempts beyond the first per
    URL. The earlier ``attempts > 1`` test was wrong in a way that mattered: a source fetching 1 200
    URLs has 1 200 attempts and had usually retried nothing, so that reading reported ~99% of every
    run as retry exposure. Replay removes none of the sleeping -- ``retries``/``backoff_s`` still
    sleep -- so the share is reported next to every A/B verdict rather than left inside the wall
    clock. Still a proxy: it measures whose time *contains* a repeated fetch, not how much was
    sleep, and the counter itself cannot separate a retry from an intentional re-fetch.
    """

    rows = source_rows(rundir)
    repeated = [
        row for row in rows if to_int((row.get("httpCounters") or {}).get("repeatFetches")) > 0
    ]
    total = sum(to_int(row.get("durationMs")) for row in rows)
    repeated_ms = sum(to_int(row.get("durationMs")) for row in repeated)
    return {
        "sourcesWithCounters": sum(1 for row in rows if row.get("httpCounters")),
        "repeatedFetchSources": len(repeated),
        "repeatedFetchShareMs": repeated_ms,
        "totalSourceMs": total,
        "sharePct": round((repeated_ms / total) * 100.0, 2) if total else 0.0,
        "proxy": (
            "sources whose time contains a repeated fetch of the same URL (a retry or an "
            "intentional re-fetch, which the seam cannot tell apart), not the sleep inside them"
        ),
    }


def to_int(value: Any, *, default: int = 0) -> int:
    """A non-negative int read out of a report value, or ``default`` when it cannot be read.

    The corpus tools share this one reader: three identical private copies is how a rounding rule
    starts to drift between the report that is printed and the number the verdict uses. ``default``
    exists for callers that must not confuse "zero" with "unreadable" -- a counter that sits at 0 is
    a measurement, a value that failed to parse is not -- and a boolean is not a measurement either,
    so ``True`` reads as ``default`` rather than as 1.
    """

    if value is None or isinstance(value, bool):
        return default
    try:
        parsed = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if parsed > 0 else default


def build_command(
    *,
    mode: str,
    epoch_dir: Path | str,
    run_dir: Path | str,
    scope: str = "subset",
    label: str = "",
    sources: int = 0,
    families: str = "",
    only_sources: str = "",
    all_sources: bool = False,
    max_workers: int = 1,
    adapter_http_concurrency: int = 0,
    google_sheets_redirect_concurrency: int = 0,
    driver_args: Sequence[str] = (),
) -> list[str]:
    """The driver invocation for one arm, in one place so both arms are spelled identically."""

    command = [
        sys.executable,
        str(driver_path()),
        "--mode",
        str(mode),
        "--scope",
        str(scope),
        "--run-dir",
        str(run_dir),
        "--max-workers",
        str(max(1, int(max_workers))),
    ]
    if str(mode) == "replay" and str(epoch_dir):
        command.extend(["--corpus-dir", str(epoch_dir)])
    if label:
        command.extend(["--label", str(label)])
    if int(sources or 0) > 0:
        command.extend(["--sources", str(int(sources))])
    if families:
        command.extend(["--families", str(families)])
    if only_sources:
        command.extend(["--only-sources", str(only_sources)])
    if all_sources:
        command.append("--all-sources")
    if int(adapter_http_concurrency or 0) > 0:
        command.extend(["--adapter-http-concurrency", str(int(adapter_http_concurrency))])
    if int(google_sheets_redirect_concurrency or 0) > 0:
        command.extend(
            [
                "--google-sheets-redirect-concurrency",
                str(int(google_sheets_redirect_concurrency)),
            ]
        )
    command.extend(str(item) for item in driver_args)
    return command


def run_driver(
    command: Sequence[str],
    *,
    env_extra: Mapping[str, str] | None = None,
    timeout_s: int = 3600,
    cwd: Path | str | None = None,
) -> dict[str, Any]:
    """Run one driver invocation and return its exit status, wall clock and output.

    The environment is inherited with ``env_extra`` applied on top, so a setting the caller did
    not name cannot leak in from the shell -- an A/B whose arms differ by an unstated variable
    would be worse than no measurement.
    """

    env = dict(os.environ)
    env.pop(COUNTERS_ENV, None)
    env.pop(PROFILE_ENV, None)
    for name, value in (env_extra or {}).items():
        env[str(name)] = str(value)
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            [str(item) for item in command],
            check=False,
            capture_output=True,
            text=True,
            env=env,
            cwd=str(cwd or _ROOT),
            timeout=max(1, int(timeout_s)),
        )
    except subprocess.TimeoutExpired as exc:
        raise HarnessError(f"driver timed out after {timeout_s}s: {' '.join(command)}") from exc
    except OSError as exc:
        raise HarnessError(f"driver could not start: {exc}") from exc
    return {
        "command": [str(item) for item in command],
        "exitCode": int(completed.returncode),
        "wallMs": int((time.perf_counter() - started) * 1000),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "envExtra": {str(name): str(value) for name, value in (env_extra or {}).items()},
    }


def observe(rundir: Path | str) -> dict[str, Any]:
    """Everything a verdict may rely on, read from the receipts the run wrote."""

    rundir = Path(rundir)
    parity = read_json_artifact(rundir, PARITY_FILE)
    summary = run_summary(rundir)
    return {
        "runDir": str(rundir),
        "parity": parity if isinstance(parity, Mapping) else {},
        "summary": summary,
        "stageShares": stage_shares(rundir),
        "backoff": backoff_exposure(rundir),
        "wallMs": run_wall_ms(rundir),
        "parallelism": parallelism(rundir),
        "runConfig": dict((parity or {}).get("runConfig") or {})
        if isinstance(parity, Mapping)
        else {},
    }


def epoch_summary(epoch_dir: Path | str) -> dict[str, Any]:
    """The epoch's recorded summary, which every replay is graded against."""

    manifest = read_json_artifact(Path(epoch_dir), "EPOCH.json")
    if not isinstance(manifest, Mapping):
        raise HarnessError(f"no readable EPOCH.json under {epoch_dir}")
    recorded = manifest.get("recordedSummary")
    return {
        "epochId": manifest.get("epochId") or Path(epoch_dir).name,
        "scope": str(manifest.get("scope") or ""),
        "recordedSummary": dict(recorded) if isinstance(recorded, Mapping) else {},
        "recordedConfig": dict(manifest.get("recordedConfig") or {}),
        "counts": dict(manifest.get("counts") or {}),
        "gitSha": str(manifest.get("gitSha") or ""),
        "recordedAt": str(manifest.get("recordedAt") or ""),
        "supersededBy": str(manifest.get("supersededBy") or ""),
    }


def resolve_epoch(epoch_dir: Path | str = "", corpus_parent: Path | str = "") -> Path:
    """An explicit epoch, or the current one by the S3 rule (newest ``recordedAt``)."""

    if str(epoch_dir):
        candidate = Path(epoch_dir)
        if not (candidate / "EPOCH.json").is_file():
            raise HarnessError(f"{candidate} has no EPOCH.json")
        return candidate
    from tools.measurements.corpus import epoch as epoch_mod

    parent = Path(corpus_parent) if str(corpus_parent) else Path("_out/perf-runs")
    resolved = epoch_mod.resolve_current(parent)
    if resolved is None:
        raise HarnessError(f"no epoch found under {parent}")
    return Path(resolved)


__all__ = [
    "COUNTERS_ENV",
    "EXIT_INFRASTRUCTURE",
    "EXIT_OK",
    "EXIT_PARITY_FAILED",
    "HarnessError",
    "PROFILE_ENV",
    "artifact_dir",
    "backoff_exposure",
    "build_command",
    "first_artifact",
    "driver_path",
    "epoch_summary",
    "observe",
    "read_json_artifact",
    "report_summary",
    "resolve_epoch",
    "run_driver",
    "run_report",
    "run_rows",
    "run_summary",
    "source_rows",
    "parallelism",
    "run_wall_ms",
    "stage_shares",
]
