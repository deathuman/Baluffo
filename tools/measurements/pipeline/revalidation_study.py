#!/usr/bin/env python3
"""Seed, run and judge a real conditional-revalidation study over the fetch pipeline.

The shipped revalidation work is only observable on the wire: a 304 saves a download, and a
replay harness that removes the wire cannot see it. This tool therefore drives *live* runs,
but everything around the run is pinned:

* ``seed`` copies a registry and state into the study, selects the population, optionally
  ages every clock past its window, and records the input fingerprints. Nothing reads or
  writes the repo's ``data/``.
* ``arm`` gives one arm its own data dir, runs ``live_run.py`` from a chosen code tree
  (a worktree, for a before/after pair) and keeps the run's own summary. Arms are
  independent: a run that changed state in arm A cannot leak into arm B.
* ``census`` describes what the arm's state contains after the run -- kept, zero-kept with
  and without a validator, boards with no success history, multi-page boards -- priced
  from the run's own per-source durations, never from ``lastDurationMs`` (which reads 0
  for a row whose last run was a skip).
* ``probe`` asks the tagless zero-kept cohort three conditional questions, so whether a
  server honours ``If-Modified-Since`` without publishing a validator is measured rather
  than assumed.
* ``report`` renders the arms, the paired deltas over the same seeded state, and -- when
  the arms did not produce the same work -- refuses a verdict instead of naming a winner.

Reused rather than re-implemented: state aging is ``provider_revalidation_smoke.age_state``;
the paired statistic, interval and verdict are ``measurements/corpus/ab.py``'s; report and
receipt reading is ``measurements/corpus/harness.py``'s. This tool adds only the seeding,
the arm lifecycle and the cohort census the other tools do not have.

Usage::

  python tools/measurements/pipeline/revalidation_study.py seed --study .tmp/study \\
      --state .tmp/fetch-timing/sweep/w12/jobs-source-state.json.gz \\
      --registry .tmp/fetch-timing/sweep/w12/source-registry-active.json \\
      --all-static --age-minutes 900
  python tools/measurements/pipeline/revalidation_study.py arm --study .tmp/study --name a
  python tools/measurements/pipeline/revalidation_study.py census --study .tmp/study --name a
  python tools/measurements/pipeline/revalidation_study.py probe --study .tmp/study --name a
  python tools/measurements/pipeline/revalidation_study.py report --study .tmp/study
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import statistics
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import ab as corpus_ab  # noqa: E402
from tools.provider_revalidation_smoke import age_state  # noqa: E402

STATE_FILE = "jobs-source-state.json"
REGISTRY_FILE = "source-registry-active.json"
VALIDATOR_KEYS = ("lastHttpEtag", "lastHttpLastModified", "lastHttpPageValidators")
STATIC_LOADER_PREFIX = "static_source::"
_LIVE_RUN = Path(__file__).resolve().with_name("live_run.py")


# --------------------------------------------------------------------------- files


def read_json_any(path: Path) -> Any:
    """Read a JSON artifact in either storage spelling (``x.json`` / ``x.json.gz``)."""

    raw = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
    return json.loads(raw.decode("utf-8", errors="replace"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def fingerprint(path: Path) -> dict[str, Any]:
    data = path.read_bytes()
    return {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def load_state(path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json_any(path)
    rows = payload.get("sources") if isinstance(payload, dict) and "sources" in payload else payload
    if not isinstance(rows, dict):
        raise SystemExit(f"{path} is not a source-state map")
    return {str(name): dict(row) for name, row in rows.items() if isinstance(row, dict)}


def state_artifact(directory: Path) -> Path:
    """The state file a data dir holds, in either storage spelling.

    ``write_source_state`` always lands the gzip-backed name, while readers resolve it from
    the plain one. A study that hard-codes either spelling seeds a dir its own arm cannot read.
    """

    for name in (STATE_FILE, f"{STATE_FILE}.gz"):
        candidate = Path(directory) / name
        if candidate.is_file():
            return candidate
    return Path(directory) / STATE_FILE


def write_state(path: Path, rows: dict[str, dict[str, Any]]) -> None:
    """Write state through the product's own writer.

    The reader is ``read_source_state``, which only sees rows under a ``sources`` envelope, so
    a bare map written by hand reads back as "no cache state" and every board looks cold.
    """

    from src.jobs.state_source_records import write_source_state

    write_source_state(path, rows)


def load_registry(path: Path) -> list[dict[str, Any]]:
    payload = read_json_any(path)
    if not isinstance(payload, list):
        raise SystemExit(f"{path} is not a registry list")
    return [row for row in payload if isinstance(row, dict)]


# ------------------------------------------------------------------------- selection


def static_loader_name(row: dict[str, Any]) -> str:
    return f"{STATIC_LOADER_PREFIX}{str(row.get('id') or '').strip()}"


def select_names(
    registry: Sequence[dict[str, Any]],
    *,
    sources_from: Sequence[str] = (),
    only_sources: Sequence[str] = (),
    adapter: str = "static",
) -> list[str]:
    """Loader names for the population, in registry order, de-duplicated.

    A name is the registry ``id`` with the adapter prefix, because that is what the pipeline
    dispatches on -- never the studio label, which is not unique.
    """

    if sources_from:
        wanted = {str(name).strip() for name in sources_from if str(name).strip()}
        return [static_loader_name(row) for row in registry if static_loader_name(row) in wanted]
    if only_sources:
        wanted = {str(name).strip() for name in only_sources if str(name).strip()}
        names: list[str] = []
        for row in registry:
            name = static_loader_name(row)
            source_id = str(row.get("id") or "").strip()
            if name in wanted or source_id in wanted or str(row.get("name") or "") in wanted:
                names.append(name)
        return list(dict.fromkeys(names))
    return list(
        dict.fromkeys(
            static_loader_name(row) for row in registry if str(row.get("adapter") or "") == adapter
        )
    )


def has_validator(row: dict[str, Any]) -> bool:
    if (
        str(row.get("lastHttpEtag") or "").strip()
        or str(row.get("lastHttpLastModified") or "").strip()
    ):
        return True
    pages = row.get("lastHttpPageValidators")
    return bool(isinstance(pages, dict) and pages)


def strip_validators(rows: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """State a tree without validator capture could have left, everything else identical."""

    return {
        name: {key: value for key, value in row.items() if key not in VALIDATOR_KEYS}
        for name, row in rows.items()
    }


def page_count(row: dict[str, Any]) -> int:
    pages = row.get("pages")
    return len([page for page in pages if isinstance(page, str)]) if isinstance(pages, list) else 0


# ------------------------------------------------------------------------------ seed


def seed_study(
    study: Path,
    *,
    state_path: Path,
    registry_path: Path,
    names: Sequence[str],
    age_minutes: int,
) -> dict[str, Any]:
    """Write the study's frozen population: one registry, one state, one name list."""

    registry = load_registry(registry_path)
    state = load_state(state_path)
    wanted = set(names)
    # A board has two state keys: the loader name the pipeline dispatches on and the display
    # name its incremental decision reads. Seed both, or the arm starts cold while the
    # registry looks populated.
    selected_keys: dict[str, set[str]] = {name: {name} for name in names}
    for row in registry:
        loader = static_loader_name(row)
        if loader in selected_keys and str(row.get("name") or ""):
            selected_keys[loader].add(str(row["name"]))
    selected_state = {
        key: dict(state[key]) for keys in selected_keys.values() for key in keys if key in state
    }

    seed_dir = study / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    write_json(seed_dir / REGISTRY_FILE, registry)
    aged = age_state(selected_state, int(age_minutes)) if int(age_minutes) > 0 else selected_state
    write_state(seed_dir / STATE_FILE, aged)
    (study / "sources.txt").write_text("\n".join(names) + "\n", encoding="utf-8")

    payload = {
        "sourceCount": len(names),
        "stateRows": len(selected_state),
        "missingStateRows": sorted(wanted - set(state)),
        "withValidator": sum(1 for row in selected_state.values() if has_validator(row)),
        "kept": sum(1 for row in selected_state.values() if int(row.get("lastKeptCount") or 0) > 0),
        "zeroKept": sum(
            1 for row in selected_state.values() if int(row.get("lastKeptCount") or 0) <= 0
        ),
        "zeroKeptStreak": sum(
            1 for row in selected_state.values() if int(row.get("consecutiveZeroKept") or 0) > 0
        ),
        "multiPage": sum(
            1 for row in registry if static_loader_name(row) in wanted and page_count(row) > 1
        ),
        "ageMinutes": int(age_minutes),
        "sources": fingerprint(study / "sources.txt"),
        "registry": fingerprint(registry_path),
        "state": fingerprint(state_path),
        "seededAt": datetime.now(UTC).isoformat(),
    }
    write_json(study / "seed.json", payload)
    return payload


# ------------------------------------------------------------------------------- arm


def arm_command(
    study: Path,
    name: str,
    *,
    code: Path,
    workers: int,
    tag: str,
) -> list[str]:
    # One directory, deliberately: the pipeline resolves both its registry data dir and every
    # artifact path from a single root, so splitting them is how a run ends up reading no state.
    arm_dir = study / "arms" / name
    return [
        sys.executable,
        str(_LIVE_RUN),
        "--code",
        str(code),
        "--data-dir",
        str(arm_dir),
        "--output-dir",
        str(arm_dir),
        "--sources-from",
        str(study / "sources.txt"),
        "--tag",
        str(tag),
        "--workers",
        str(max(1, int(workers))),
    ]


def prepare_arm(
    study: Path,
    name: str,
    *,
    strip: bool = False,
    age_minutes: int = 0,
) -> Path:
    """Give the arm its own copy of the seed data, then apply the arm's own adjustments."""

    seed_dir = study / "seed"
    seeded_state = state_artifact(seed_dir)
    if not seeded_state.is_file():
        raise SystemExit(f"{study} has no seed; run `seed` first")
    arm_dir = study / "arms" / name
    if arm_dir.exists():
        raise SystemExit(f"{arm_dir} exists; remove it or choose another arm name")
    arm_dir.mkdir(parents=True)
    (arm_dir / REGISTRY_FILE).write_bytes((seed_dir / REGISTRY_FILE).read_bytes())
    rows = load_state(seeded_state)
    if strip:
        rows = strip_validators(rows)
    if int(age_minutes) > 0:
        rows = age_state(rows, int(age_minutes))
    write_state(arm_dir / STATE_FILE, rows)
    return arm_dir


def run_arm(
    study: Path,
    name: str,
    *,
    code: Path,
    workers: int,
    tag: str,
    strip: bool = False,
    age_minutes: int = 0,
) -> dict[str, Any]:
    arm_dir = prepare_arm(study, name, strip=strip, age_minutes=age_minutes)
    command = arm_command(study, name, code=code, workers=workers, tag=tag)
    started = time.perf_counter()
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    elapsed = time.perf_counter() - started
    run_path = arm_dir / f"{tag}.run.json"
    payload: dict[str, Any] = {
        "arm": name,
        "tag": tag,
        "code": str(code),
        "command": command,
        "exitCode": int(completed.returncode),
        "processWallS": round(elapsed, 1),
        "strippedValidators": bool(strip),
        "agedMinutes": int(age_minutes),
        "stdoutTail": completed.stdout[-2000:],
        "stderrTail": completed.stderr[-2000:],
    }
    if run_path.is_file():
        payload["run"] = json.loads(run_path.read_text(encoding="utf-8"))
    write_json(arm_dir / "arm.json", payload)
    return payload


# ---------------------------------------------------------------------------- census


def _by_names(run: dict[str, Any] | None, field: str) -> dict[str, Any]:
    """Index a run's sources under both names they carry.

    A work item is named by the loader (``static_source::...``); the state row the decision
    reads is named by the source's display name. Indexing both is what lets a cohort priced
    from the run meet a cohort selected from state.
    """

    indexed: dict[str, Any] = {}
    for row in (run or {}).get("sources") or []:
        value = row.get(field)
        for name in (row.get("name"), row.get("sourceName")):
            if str(name or ""):
                indexed[str(name)] = value
    return indexed


def _duration_by_name(run: dict[str, Any] | None) -> dict[str, int]:
    return {name: int(value or 0) for name, value in _by_names(run, "durationMs").items()}


def _decision_by_name(run: dict[str, Any] | None) -> dict[str, str]:
    return {name: str(value or "") for name, value in _by_names(run, "cacheDecision").items()}


def census(
    rows: dict[str, dict[str, Any]],
    registry: Sequence[dict[str, Any]],
    run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Split the population by what the next decision can actually do with it.

    ``lastDurationMs`` is deliberately not a source here: it reads 0 for a row whose last
    run was a skip, so pricing a cohort with it silently prices skipped boards at zero.
    """

    durations = _duration_by_name(run)
    decisions = _decision_by_name(run)
    pages_by_name: dict[str, int] = {}
    for row in registry:
        if row.get("adapter") != "static":
            continue
        count = page_count(row)
        for key in (static_loader_name(row), str(row.get("name") or "")):
            if key:
                pages_by_name[key] = count

    def _group(predicate: Any) -> dict[str, Any]:
        names = sorted(name for name, row in rows.items() if predicate(name, row))
        costs = [durations[name] for name in names if durations.get(name, 0) > 0]
        return {
            "count": len(names),
            "withRunCost": len(costs),
            "readSeconds": round(sum(costs) / 1000.0, 1),
            "medianReadMs": int(statistics.median(costs)) if costs else 0,
            "answered304": sum(1 for name in names if decisions.get(name) == "revalidate_only"),
            "names": names,
        }

    kept = _group(lambda _name, row: int(row.get("lastKeptCount") or 0) > 0)
    zero = _group(lambda _name, row: int(row.get("lastKeptCount") or 0) <= 0)
    zero_with_validator = _group(
        lambda _name, row: int(row.get("lastKeptCount") or 0) <= 0 and has_validator(row)
    )
    zero_tagless = _group(
        lambda _name, row: int(row.get("lastKeptCount") or 0) <= 0 and not has_validator(row)
    )
    no_success = _group(lambda _name, row: not str(row.get("lastSuccessAt") or "").strip())
    multi_page = _group(lambda name, _row: pages_by_name.get(name, 0) > 1)
    return {
        "rows": len(rows),
        "kept": kept,
        "zeroKept": zero,
        "zeroKeptWithValidator": zero_with_validator,
        "zeroKeptTagless": zero_tagless,
        "noSuccessHistory": no_success,
        "multiPage": multi_page,
        "withValidator": sum(1 for row in rows.values() if has_validator(row)),
    }


# ----------------------------------------------------------------------------- probe


def _rfc1123(value: Any) -> str:
    from src.jobs.common.datetime_utils import parse_datetime

    parsed = parse_datetime(value)
    if parsed is None:
        return ""
    return format_datetime(parsed.astimezone(UTC), usegmt=True)


def head_probe(url: str, *, since: str = "", timeout_s: int = 10) -> dict[str, Any]:
    """One HEAD, optionally conditional on a date. A measurement probe, not the flow's."""

    import httpx

    from src.jobs.transport import DEFAULT_HTTP_HEADERS

    headers = dict(DEFAULT_HTTP_HEADERS)
    if since:
        headers["If-Modified-Since"] = since
    started = time.perf_counter()
    try:
        with httpx.Client(
            follow_redirects=True, headers=headers, timeout=httpx.Timeout(float(timeout_s))
        ) as client:
            response = client.request("HEAD", url)
        status = int(response.status_code)
        etag = str(response.headers.get("ETag") or "")
        last_modified = str(response.headers.get("Last-Modified") or "")
        final_url = str(response.url)
    except Exception as exc:  # noqa: BLE001 - a probe failure is a measurement, not a crash
        return {
            "url": url,
            "status": 0,
            "notModified": False,
            "error": f"{type(exc).__name__}: {exc}",
            "durationMs": int((time.perf_counter() - started) * 1000),
        }
    return {
        "url": url,
        "status": status,
        "notModified": status == 304,
        "etag": etag,
        "lastModified": last_modified,
        "finalUrl": final_url,
        "durationMs": int((time.perf_counter() - started) * 1000),
    }


def tagless_cohort(
    rows: dict[str, dict[str, Any]], run: dict[str, Any] | None
) -> dict[str, dict[str, Any]]:
    """Zero-kept, tagless boards whose cost this run actually observed."""

    durations = _duration_by_name(run)
    cohort: dict[str, dict[str, Any]] = {}
    for name, row in rows.items():
        if int(row.get("lastKeptCount") or 0) > 0:
            continue
        if has_validator(row):
            continue
        if not str(row.get("lastSuccessAt") or "").strip():
            continue
        if durations.get(name, 0) <= 0:
            continue
        cohort[name] = row
    return cohort


def probe_tagless(
    cohort: dict[str, dict[str, Any]],
    *,
    registry: Sequence[dict[str, Any]],
    run: dict[str, Any] | None,
    concurrency: int = 6,
    timeout_s: int = 10,
) -> dict[str, Any]:
    """Ask every cohort page three questions: plain HEAD, read-date IMS, ancient IMS.

    The ancient date is the discriminator: a host that answers 200 to a date years before
    its copy ignores the header, so a 200 in the read-date arm is that server's answer
    rather than the condition being too recent.
    """

    from concurrent.futures import ThreadPoolExecutor

    pages_by_name: dict[str, list[str]] = {}
    for row in registry:
        if row.get("adapter") != "static":
            continue
        pages = [page for page in (row.get("pages") or []) if isinstance(page, str)]
        if not pages:
            continue
        for key in (static_loader_name(row), str(row.get("name") or "")):
            if key:
                pages_by_name[key] = pages
    durations = _duration_by_name(run)
    ancient = "Thu, 01 Jan 2015 00:00:00 GMT"

    jobs: list[tuple[str, str, str, str]] = []
    for name, row in cohort.items():
        pages = pages_by_name.get(name) or [str(row.get("listingUrl") or "").strip()]
        since = _rfc1123(row.get("lastSuccessAt"))
        for page in [page for page in pages if page.strip()]:
            jobs.append((name, page, "plain", ""))
            if since:
                jobs.append((name, page, "since", since))
            jobs.append((name, page, "old", ancient))

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(concurrency))) as pool:
        futures = [
            pool.submit(head_probe, page, since=condition, timeout_s=timeout_s)
            for _name, page, _mode, condition in jobs
        ]
        for (_name, _page, mode, _condition), future in zip(jobs, futures, strict=True):
            answer = future.result()
            answer["mode"] = mode
            answer["source"] = _name
            results.append(answer)

    per_board: dict[str, dict[str, Any]] = {}
    for name, row in cohort.items():
        boards = [item for item in results if item["source"] == name]
        entry: dict[str, Any] = {
            "readSeconds": round(durations.get(name, 0) / 1000.0, 2),
            "streak": int(row.get("consecutiveZeroKept") or 0),
            "pages": len({item["url"] for item in boards}),
        }
        for mode in ("plain", "since", "old"):
            answers = [item for item in boards if item["mode"] == mode]
            entry[mode] = {
                "pages": len(answers),
                "notModified": sum(1 for item in answers if item["notModified"]),
                "ok": sum(1 for item in answers if 200 <= int(item["status"]) < 300),
                "errors": sum(1 for item in answers if int(item["status"]) == 0),
                "allUnchanged": bool(answers) and all(item["notModified"] for item in answers),
            }
        per_board[name] = entry

    def _summary(mode: str) -> dict[str, Any]:
        answers = [item for item in results if item["mode"] == mode]
        boards = [entry for entry in per_board.values() if entry[mode]["pages"]]
        return {
            "pages": len(answers),
            "notModified": sum(1 for item in answers if item["notModified"]),
            "ok": sum(1 for item in answers if 200 <= int(item["status"]) < 300),
            "errors": sum(1 for item in answers if int(item["status"]) == 0),
            "boards": len(boards),
            "allUnchangedBoards": sum(1 for entry in boards if entry[mode]["allUnchanged"]),
            "probeSeconds": round(sum(int(item["durationMs"]) for item in answers) / 1000.0, 1),
        }

    unchanged = [entry for entry in per_board.values() if entry["since"]["allUnchanged"]]
    probe_cost_s = _summary("since")["probeSeconds"]
    read_cost_s = round(sum(entry["readSeconds"] for entry in unchanged), 1)
    return {
        "cohort": len(cohort),
        "plain": _summary("plain"),
        "since": _summary("since"),
        "old": _summary("old"),
        "netSecondsPerEmptyWindow": round(read_cost_s - probe_cost_s, 1),
        "unchangedReadSeconds": read_cost_s,
        "boards": per_board,
        "results": results,
    }


# ---------------------------------------------------------------------------- report


def _pairs(study: Path, specs: Sequence[str]) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for spec in specs:
        left, _, right = str(spec).partition(":")
        if not left or not right:
            raise SystemExit(f"--compare wants armA:armB, got {spec!r}")
        runs: dict[str, Any] = {}
        for side, name in (("a", left), ("b", right)):
            arm_file = study / "arms" / name / "arm.json"
            if not arm_file.is_file():
                raise SystemExit(f"{arm_file} is missing; run arm {name!r} first")
            payload = json.loads(arm_file.read_text(encoding="utf-8"))
            wall_s = float((payload.get("run") or {}).get("wallS") or 0)
            # The paired statistic reads a millisecond clock at the top level; the arm file
            # keeps the run's own window under ``run``. Normalise once, here.
            runs[side] = {
                **payload,
                "reportWallMs": int(wall_s * 1000),
                "wallMs": int(float(payload.get("processWallS") or 0) * 1000),
            }
        pairs.append(runs)
    return pairs


def report(
    study: Path,
    *,
    compare: Sequence[str] = (),
    min_effect_pct: float = 2.0,
    allow_output_drift: bool = False,
) -> dict[str, Any]:
    """Render every arm, then the paired verdict the arms' own numbers support."""

    arms: list[dict[str, Any]] = []
    for arm_file in sorted(study.glob("arms/*/arm.json")):
        payload = json.loads(arm_file.read_text(encoding="utf-8"))
        run = payload.get("run") or {}
        arms.append(
            {
                "arm": payload.get("arm"),
                "tag": payload.get("tag"),
                "exitCode": payload.get("exitCode"),
                "wallS": run.get("wallS"),
                "sourceWorkS": run.get("sourceWorkS"),
                "sources": run.get("sourceCount"),
                "kept": run.get("kept"),
                "outputCount": run.get("outputCount"),
                "notModifiedCount": run.get("notModifiedCount"),
                "revalidatedCount": run.get("revalidatedCount"),
                "cacheSkippedCount": run.get("cacheSkippedCount"),
                "failedSources": run.get("failedSources"),
                "reportWallMs": int(float(run.get("wallS") or 0) * 1000),
            }
        )
    payload: dict[str, Any] = {"arms": arms, "pairs": []}
    pairs = _pairs(study, compare)
    if pairs:
        deltas = corpus_ab.paired_deltas(pairs)
        median = round(float(statistics.median(deltas)), 3) if deltas else 0.0
        ci = corpus_ab.bootstrap_ci(deltas)
        failures = [
            {"arm": pair["b"].get("arm"), "exitCode": pair["b"].get("exitCode")}
            for pair in pairs
            if int(pair["a"].get("exitCode") or 0) != 0 or int(pair["b"].get("exitCode") or 0) != 0
        ]
        divergences = []
        if not allow_output_drift:
            for pair in pairs:
                left = pair["a"].get("run") or {}
                right = pair["b"].get("run") or {}
                for unit in ("outputCount", "kept"):
                    if int(left.get(unit) or 0) != int(right.get(unit) or 0):
                        divergences.append(
                            {
                                "arm": pair["b"].get("arm"),
                                "unit": unit,
                                "expected": int(left.get(unit) or 0),
                                "actual": int(right.get(unit) or 0),
                            }
                        )
        status, verdict = corpus_ab.decide(
            divergences,
            failures,
            median=median,
            ci=ci,
            min_effect_pct=min_effect_pct,
        )
        payload["pairs"] = {
            "count": len(pairs),
            "deltas": deltas,
            "medianPct": median,
            "ci95": ci,
            "status": status,
            "verdict": verdict,
            "divergences": divergences,
            "failures": failures,
            "minEffectPct": min_effect_pct,
            "note": "Delta is (B - A) / A: negative means B spent less wall time.",
        }
    write_json(study / "report.json", payload)
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    header = (
        "| arm | exit | wall | source work | sources | kept | output | 304s | revalidated | "
        "skipped | failed |\n|---|---|---|---|---|---|---|---|---|---|---|"
    )
    lines = [header]
    for arm in payload.get("arms") or []:
        lines.append(
            f"| {arm['arm']} | {arm['exitCode']} | {arm['wallS']}s | {arm['sourceWorkS']}s | "
            f"{arm['sources']} | {arm['kept']} | {arm['outputCount']} | "
            f"{arm['notModifiedCount']} | {arm['revalidatedCount']} | "
            f"{arm['cacheSkippedCount']} | {arm['failedSources']} |"
        )
    pairs = payload.get("pairs") or {}
    if pairs:
        lines.append("")
        lines.append(
            f"pairs={pairs['count']}  median delta={pairs['medianPct']}%  "
            f"95% CI={pairs['ci95']}  status={pairs['status']}  verdict={pairs['verdict']}"
        )
        lines.append(str(pairs["note"]))
        for divergence in pairs["divergences"]:
            lines.append(
                f"- parity: {divergence['arm']} {divergence['unit']} "
                f"{divergence['actual']} vs expected {divergence['expected']}"
            )
    return "\n".join(lines)


# ------------------------------------------------------------------------------- cli


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed", help="Freeze a registry/state population for the study.")
    seed.add_argument("--study", required=True)
    seed.add_argument("--state", required=True)
    seed.add_argument("--registry", required=True)
    seed.add_argument("--sources-from", default="")
    seed.add_argument("--only-sources", default="")
    seed.add_argument("--all-static", action="store_true")
    seed.add_argument("--age-minutes", type=int, default=0)

    arm = sub.add_parser("arm", help="Run one arm from a code tree.")
    arm.add_argument("--study", required=True)
    arm.add_argument("--name", required=True)
    arm.add_argument("--code", default=".")
    arm.add_argument("--workers", type=int, default=16)
    arm.add_argument("--tag", default="run")
    arm.add_argument("--strip-validators", action="store_true")
    arm.add_argument("--age-minutes", type=int, default=0)

    census_cmd = sub.add_parser("census", help="Describe an arm's state after its run.")
    census_cmd.add_argument("--study", required=True)
    census_cmd.add_argument("--name", default="")
    census_cmd.add_argument("--tag", default="run")

    probe = sub.add_parser(
        "probe", help="Ask the tagless zero-kept cohort its conditional question."
    )
    probe.add_argument("--study", required=True)
    probe.add_argument("--name", default="")
    probe.add_argument("--tag", default="run")
    probe.add_argument("--concurrency", type=int, default=6)
    probe.add_argument("--timeout", type=int, default=10)

    report_cmd = sub.add_parser("report", help="Arms, paired deltas and a verdict.")
    report_cmd.add_argument("--study", required=True)
    report_cmd.add_argument("--compare", action="append", default=[])
    report_cmd.add_argument("--min-effect-pct", type=float, default=2.0)
    report_cmd.add_argument("--allow-output-drift", action="store_true")

    args = parser.parse_args(list(argv) if argv is not None else None)
    study = Path(args.study).resolve()

    if args.command == "seed":
        registry_path = Path(args.registry).resolve()
        sources_from = (
            [
                line.strip()
                for line in Path(args.sources_from).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            if args.sources_from
            else []
        )
        names = select_names(
            load_registry(registry_path),
            sources_from=sources_from,
            only_sources=[item for item in args.only_sources.split(",") if item.strip()],
        )
        if not names:
            raise SystemExit("the selection is empty; pass --sources-from or --only-sources")
        payload = seed_study(
            study,
            state_path=Path(args.state).resolve(),
            registry_path=registry_path,
            names=names,
            age_minutes=args.age_minutes,
        )
    elif args.command == "arm":
        payload = run_arm(
            study,
            args.name,
            code=Path(args.code).resolve(),
            workers=args.workers,
            tag=args.tag,
            strip=args.strip_validators,
            age_minutes=args.age_minutes,
        )
    elif args.command == "census":
        arm_dir = study / "arms" / args.name
        rows = load_state(state_artifact(arm_dir))
        run_file = arm_dir / f"{args.tag}.run.json"
        run = json.loads(run_file.read_text(encoding="utf-8")) if run_file.is_file() else None
        payload = census(rows, load_registry(study / "seed" / REGISTRY_FILE), run)
        write_json(arm_dir / "census.json", payload)
    elif args.command == "probe":
        arm_dir = study / "arms" / args.name
        rows = load_state(state_artifact(arm_dir))
        run_file = arm_dir / f"{args.tag}.run.json"
        run = json.loads(run_file.read_text(encoding="utf-8")) if run_file.is_file() else None
        cohort = tagless_cohort(rows, run)
        payload = probe_tagless(
            cohort,
            registry=load_registry(study / "seed" / REGISTRY_FILE),
            run=run,
            concurrency=args.concurrency,
            timeout_s=args.timeout,
        )
        write_json(arm_dir / "tagless-probe.json", payload)
        payload.pop("results", None)
        payload.pop("boards", None)
    else:
        payload = report(
            study,
            compare=args.compare,
            min_effect_pct=args.min_effect_pct,
            allow_output_drift=args.allow_output_drift,
        )
        print(render_markdown(payload))
        return 0

    # Never slice this: a JSON cut at a fixed offset is not a smaller answer, it is an
    # unparsable one. The probe drops its bulky per-request lists above, and the census is
    # written whole to ``census.json`` beside the arm either way.
    print(json.dumps(payload, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
