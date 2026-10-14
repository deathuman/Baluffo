"""Source guard: a lane-bounded discovery batch sizes its own thread pool.

`asyncio.to_thread` and `run_in_executor(None, ...)` submit to the event loop's SHARED default
executor, `min(32, cpu_count + 4)` -- 20 on the 16-core box the audit runs use. A batch whose
configured lanes exceed that runs at the machine's worker count instead, silently and with no log
line; whether a run got its lanes or that cap came down to whether its fetcher happened to be
`fetch_text` *by identity*.

  - `src/shared/http_batch.py` ran the audit's homepage and recovery phases at a peak of 20
    in-flight of 32/72 configured until it grew its own `page-fetch` pool.
  - `src/source_discovery/probe_runtime.py` was measured at exactly 20 in-flight of its configured
    probe lane count (40) in both `lane-ceiling` audit runs, with the probe's own semaphores
    waiting behind the shared executor.

The behavioral contracts beside this guard can only see the paths they drive. This reads every call
in every module of the discovery lane (plus the shared page batch), so a new shared-executor
submission fails here rather than in a live phase's occupancy, and it pins the pool each batch owner
creates so the lane contracts' thread-name checks keep telling a batch's pool from the shared one.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Modules that own a lane-bounded batch: (path, the thread prefix its own pool must carry, why).
LANE_BOUND_BATCH_MODULES = (
    (
        "src/shared/http_batch.py",
        "page-fetch",
        "the homepage/recovery/jobs page batch (`fetch_pages_batched`)",
    ),
    (
        "src/source_discovery/probe_runtime.py",
        "probe-fetch",
        "the probe wave (`run_bounded_probe_batch_async`)",
    ),
    (
        "src/source_discovery/browser_recovery.py",
        "browser-fetch",
        "the browser recovery wave (`fetch_browser_recovery_pages_async`)",
    ),
)

# Shared-executor submissions inside a batch's PER-CANDIDATE work rather than in a batch's own lane
# handoff, as `path -> (count, why)`. These helpers are not a batch's lane math, and routing them
# through the batch's pool needs a runner seam their signatures do not take. The count is exact on
# purpose: fixing one has to update this record in the same change, so the list cannot go stale.
PER_CANDIDATE_HOP_ALLOWLIST = {
    "src/source_discovery/probe.py": (
        2,
        "the Workday CXS POST and the playwright fallback: per-candidate blocking helpers, the "
        "playwright one gated by its own narrow semaphore",
    ),
}

# The discovery lane, plus the shared primitive its page batches run through.
LANE_ROOT = "src/source_discovery"
SHARED_BATCH_SOURCE = "src/shared/http_batch.py"


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _is_none_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _shared_executor_submissions(source: str) -> list[str]:
    """Every call in `source` that runs sync work on an executor nobody sized to a batch."""
    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        call = func.attr if isinstance(func, ast.Attribute) else str(getattr(func, "id", ""))
        if call == "to_thread":
            findings.append("asyncio.to_thread() submits to the loop's shared default executor")
        elif call == "run_in_executor" and node.args and _is_none_literal(node.args[0]):
            findings.append("run_in_executor(None, ...) submits to the shared default executor")
        elif call == "ThreadPoolExecutor":
            if "max_workers" not in {keyword.arg for keyword in node.keywords if keyword.arg}:
                findings.append(
                    "ThreadPoolExecutor() without max_workers is sized by the machine, not the batch"
                )
    return findings


def _module_string_constants(source: str) -> dict[str, str]:
    """Module-level `NAME = "literal"` bindings, so one module can name its prefix once."""
    constants: dict[str, str] = {}
    for node in ast.parse(source).body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                constants[target.id] = node.value.value
    return constants


def _pool_thread_prefixes(source: str) -> list[str]:
    """The `thread_name_prefix` of every `ThreadPoolExecutor(...)` call in `source`.

    A module-level string constant is resolved to its value: naming the prefix once and passing it
    is the same contract as inlining it, and the lane contracts key on the value either way. What
    the check refuses is a prefix it cannot read at all -- that is what a caller of
    `_pool_threads_alive()`-style helpers would silently stop matching.
    """
    constants = _module_string_constants(source)
    prefixes: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or getattr(node.func, "id", "") != "ThreadPoolExecutor":
            continue
        prefix = ""
        for keyword in node.keywords:
            if keyword.arg != "thread_name_prefix":
                continue
            if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                prefix = keyword.value.value
            elif isinstance(keyword.value, ast.Name):
                prefix = constants.get(keyword.value.id, "")
        prefixes.append(prefix)
    return prefixes


def _lane_sources() -> list[str]:
    files = [path.relative_to(ROOT).as_posix() for path in (ROOT / LANE_ROOT).rglob("*.py")]
    return sorted([*files, SHARED_BATCH_SOURCE])


def test_lane_bounded_batch_modules_run_sync_work_on_a_pool_they_size() -> None:
    """A batch's lanes are its caller's budget on every machine's core count.

    The cap raises nothing and logs nothing, so the only cheap place to see it is the call itself:
    the batch owner must not touch the shared default executor, every pool it creates must be sized
    (`max_workers=`) and must carry the prefix the behavioral lane contracts key on.
    """
    failures: list[str] = []
    for path, expected_prefix, why in LANE_BOUND_BATCH_MODULES:
        source = _source(path)
        for submission in _shared_executor_submissions(source):
            failures.append(f"{path} ({why}): {submission}")
        prefixes = _pool_thread_prefixes(source)
        if not prefixes:
            failures.append(f"{path} ({why}) no longer creates its own ThreadPoolExecutor")
        for prefix in prefixes:
            if not prefix.startswith(expected_prefix):
                failures.append(
                    f"{path} pool thread prefix is {prefix!r}; the lane contracts read "
                    f"{expected_prefix!r} to tell this batch's pool from the shared default executor "
                    f"(pass a string literal or a module-level string constant)"
                )
    assert not failures, (
        "a lane-bounded batch may only run its sync work on a pool it sizes to the batch: "
        + "; ".join(failures)
    )


def test_discovery_lane_has_no_unlisted_shared_executor_submission() -> None:
    """A new submission anywhere in the lane has to be a deliberate, recorded decision.

    The allowlist records the per-candidate helpers a batch's own lane handoff does not cover; its
    counts are exact, so a fixed helper updates the record in the same change rather than leaving a
    stale exemption behind.
    """
    found: dict[str, list[str]] = {}
    for path in _lane_sources():
        sites = _shared_executor_submissions(_source(path))
        if sites:
            found[path] = sites
    unlisted = sorted(set(found) - set(PER_CANDIDATE_HOP_ALLOWLIST))
    assert not unlisted, (
        "these discovery modules submit sync work to the loop's shared default executor and no "
        "allowlist entry records them: "
        + "; ".join(
            f"{path} ({len(found[path])} site(s): {'; '.join(found[path])})" for path in unlisted
        )
    )
    for path, (expected, why) in PER_CANDIDATE_HOP_ALLOWLIST.items():
        actual = len(found.get(path, []))
        assert actual == expected, (
            f"{path} has {actual} shared-executor submission(s), the allowlist records {expected} "
            f"({why}); update PER_CANDIDATE_HOP_ALLOWLIST in the same change"
        )
