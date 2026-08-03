#!/usr/bin/env python3
"""Benchmark Admin page routes and flows against a seeded local Baluffo container.

Runs three passes against a container started from a benchmark image:

Pass A (cold + warm GET sweep): each admin-facing GET route sampled twice:
    once right after container start (cold) and once after a prior hit (warm).
Pass B (safe mutations): a narrow set of admin mutation routes run with the
    original snapshot preserved so the run can be rolled back if needed.
Pass C (composite flows): multi-request sequences that mirror what the Admin UI
    actually does on common tasks (open admin, drill conflicts, trigger fetch).

Outputs a JSON routes profile, JSON flows profile, and a Markdown report under
`_out/perf-admin-flows/<run-token>/`.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.perf_complete import (  # noqa: E402
    _fetch_live_bridge_request,
    generate_run_token,
)

DEFAULT_OUTPUT_ROOT = REPO_ROOT / "_out" / "perf-admin-flows"
DEFAULT_IMAGE = "baluffo:local-benchmark"
DEFAULT_CONTAINER_NAME = "baluffo-perf-admin"
DEFAULT_PROFILE = "pi4-tight"
PROFILES: dict[str, dict[str, str]] = {
    "pi4-tight": {"cpus": "1.5", "memory": "1g"},
    "pi4-roomy": {"cpus": "2.5", "memory": "2g"},
}
MIN_MEMORY_G = 1

# Cold/warm GET sweep covers every Admin surface a human can navigate to. Additions
# should stay read-only; mutating endpoints belong in MUTATION_SPECS.
GET_ROUTES: tuple[tuple[str, str], ...] = (
    ("admin_page", "/admin.html"),
    ("admin_bootstrap", "/admin/bootstrap"),
    ("ops_tab_counts_summary", "/admin/ops-tab-counts?view=summary"),
    ("registry_summary", "/registry/summary"),
    ("registry_conflicts", "/registry/conflicts"),
    ("registry_conflicts_summary", "/registry/conflicts?view=summary"),
    ("ops_dashboard_health", "/ops/dashboard-health"),
    ("ops_health", "/ops/health"),
    ("ops_fetch_report_summary", "/ops/fetch-report?view=summary"),
    ("ops_fetch_kpis", "/ops/fetch-kpis"),
    ("ops_task_state", "/ops/task-state"),
    ("ops_task_live_fetch", "/ops/task-live/fetch?view=summary"),
    ("ops_task_live_discover", "/ops/task-live/discover?view=summary"),
    ("ops_task_live_sync", "/ops/task-live/sync?view=summary"),
    ("ops_history", "/ops/history"),
    ("ops_performance_profile", "/ops/performance-profile"),
    ("ops_fetcher_metrics", "/ops/fetcher-metrics"),
    ("ops_storage_health", "/ops/storage-health"),
    ("ops_storage_metrics", "/ops/storage-metrics"),
    ("ops_task_failure_attempts", "/ops/task-failure-attempts"),
    ("ops_perf_counters", "/ops/perf-counters"),
    (
        "registry_sources_table",
        "/registry/sources?view=table&buckets=pending,active,rejected",
    ),
    ("discovery_report", "/discovery/report"),
    ("discovery_candidates", "/discovery/candidates"),
    ("discovery_log", "/discovery/log"),
    ("discovery_config", "/discovery/config"),
    ("sync_status", "/sync/status"),
    ("source_policy_recommendations", "/source-policy/recommendations"),
    ("ops_discovery_audit_artifacts", "/ops/discovery-audit-artifacts"),
    ("fetcher_log", "/fetcher/log"),
)

# Mutations are bounded and at-most-then-revert side effects. We snapshot the
# runtime DB before Pass B so a partial run can roll back cleanly.
MUTATION_SPECS: tuple[tuple[str, str, dict[str, Any] | None], ...] = (
    (
        "tasks.run_fetcher_retry_failed",
        "POST /tasks/run-fetcher",
        {"preset": "retry_failed"},
    ),
    ("tasks.abort", "POST /tasks/abort", None),
    ("dedup.review_action", "POST /dedup/review-action", {"action": "dismiss", "jobIds": []}),
)

# Composite flows mirror real Admin UI sessions. Step names stay stable because
# the Markdown report prints them as flow legs.
FLOW_SPECS: dict[str, list[tuple[str, str]]] = {
    "admin.open.cold": [
        ("bootstrap", "/admin/bootstrap"),
        ("ops_tab_counts", "/admin/ops-tab-counts?view=summary"),
        ("registry_summary", "/registry/summary"),
        ("sync_status", "/sync/status"),
    ],
    "admin.open.warm": [
        ("bootstrap", "/admin/bootstrap"),
        ("ops_tab_counts", "/admin/ops-tab-counts?view=summary"),
        ("registry_summary", "/registry/summary"),
        ("sync_status", "/sync/status"),
    ],
    "admin.sources": [
        ("sources_table", "/registry/sources?view=table&buckets=pending,active,rejected"),
        ("sources_summary", "/registry/summary"),
    ],
    "admin.conflicts.drill": [
        ("conflicts_list", "/registry/conflicts"),
        ("conflicts_summary", "/registry/conflicts?view=summary"),
    ],
    "admin.sync.ready": [
        ("sync_status", "/sync/status"),
        ("ops_dashboard_health", "/ops/dashboard-health"),
    ],
}

POLL_INTERVAL_S = 0.25
POLL_TIMEOUT_S = 300.0
DEFAULT_REQUEST_TIMEOUT_S = 120.0


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(cmd)}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(["docker", *args], check=check)


def _stop_container(name: str) -> None:
    _docker("rm", "-f", name, check=False)


def _request(
    base_url: str,
    method: str,
    endpoint: str,
    timeout_s: float,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if method.upper() == "GET":
        sample, parsed = _fetch_live_bridge_request(
            base_url=base_url,
            endpoint=endpoint,
            timeout_s=timeout_s,
        )
        sample = dict(sample)
        sample.setdefault("gatewayHit", sample.get("status") == 504)
        sample["parsedKeys"] = list((parsed or {}).keys())[:20] if isinstance(parsed, dict) else []
        return sample

    import http.client
    import urllib.parse

    started_at = time.perf_counter()
    url = f"{base_url}{endpoint}"
    parsed_url = urllib.parse.urlsplit(url)
    request_path = parsed_url.path or "/"
    if parsed_url.query:
        request_path = f"{request_path}?{parsed_url.query}"
    host = str(parsed_url.hostname or "")
    port = int(parsed_url.port or (443 if parsed_url.scheme == "https" else 80))
    connection = http.client.HTTPConnection(host, port=port, timeout=timeout_s)
    try:
        payload = json.dumps(body or {}).encode("utf-8")
        connection.request(
            method.upper(),
            request_path,
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "BaluffoPerfAdminSampler/1",
            },
        )
        response = connection.getresponse()
        raw = response.read()
        duration_ms = int((time.perf_counter() - started_at) * 1000)
        status = int(response.status or 0)
        return {
            "ok": 200 <= status < 400,
            "endpoint": endpoint,
            "method": method.upper(),
            "status": status,
            "durationMs": duration_ms,
            "timeoutS": timeout_s,
            "sizeBytes": len(raw),
            "gatewayHit": status == 504,
            "phase": "complete",
        }
    except Exception as exc:  # noqa: BLE001 -- record and report
        return {
            "ok": False,
            "endpoint": endpoint,
            "method": method.upper(),
            "status": 0,
            "durationMs": int((time.perf_counter() - started_at) * 1000),
            "timeoutS": timeout_s,
            "sizeBytes": 0,
            "gatewayHit": False,
            "phase": "error",
            "error": str(exc),
        }
    finally:
        with contextlib.suppress(Exception):
            connection.close()


def _wait_ready(base_url: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        sample, _ = _fetch_live_bridge_request(
            base_url=base_url, endpoint="/app/ready", timeout_s=2.0
        )
        if sample.get("ok"):
            return
        time.sleep(0.5)
    raise TimeoutError(f"container at {base_url} did not become healthy within {timeout_s}s")


def _sample_route(
    base_url: str,
    label: str,
    endpoint: str,
    *,
    warm: bool,
    timeout_s: float,
) -> dict[str, Any]:
    sample = _request(base_url, "GET", endpoint, timeout_s)
    return {
        "label": f"GET {endpoint}",
        "route": label,
        "warm": warm,
        **sample,
    }


def _sample_get_sweep(base_url: str, timeout_s: float, warm: bool) -> list[dict[str, Any]]:
    rows = []
    for label, endpoint in GET_ROUTES:
        rows.append(_sample_route(base_url, label, endpoint, warm=warm, timeout_s=timeout_s))
    return rows


def _snapshot_volume(data_volume: Path, output_dir: Path) -> Path | None:
    db_path = data_volume / "baluffo-runtime.db"
    if not db_path.is_file():
        return None
    snap = output_dir / "baluffo-runtime.snapshot.db"
    snap.parent.mkdir(parents=True, exist_ok=True)
    snap.write_bytes(db_path.read_bytes())
    return snap


def _restore_volume(snapshot: Path, data_volume: Path) -> None:
    target = data_volume / snapshot.name.replace(".snapshot.db", ".db")
    target.write_bytes(snapshot.read_bytes())


def _poll_task_live(base_url: str, task: str, timeout_s: float) -> dict[str, Any]:
    endpoint = f"/ops/task-live/{task}?view=summary"
    deadline = time.monotonic() + timeout_s
    samples = 0
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = _request(base_url, "GET", endpoint, timeout_s=5.0)
        samples += 1
        if last.get("ok"):
            parsed_keys = last.get("parsedKeys") or []
            # If the response is the summary view, idle is signaled by no running task.
            # Without a parsed body, treat a 200 + "idle" substring as terminal.
            if "running" in parsed_keys and not last.get("running", True):
                break
        time.sleep(POLL_INTERVAL_S)
    return {
        "endpoint": endpoint,
        "samples": samples,
        "final": last,
        "timedOut": time.monotonic() >= deadline,
    }


def _run_pass_b(
    base_url: str, timeout_s: float, snapshot: Path | None, data_volume: Path
) -> list[dict[str, Any]]:
    rows = []
    for label, method_path, body in MUTATION_SPECS:
        method, endpoint = method_path.split(" ", 1)
        started = time.perf_counter()
        try:
            sample = _request(base_url, method, endpoint, timeout_s, body=body)
            rows.append({"label": label, "endpoint": endpoint, **sample})
        except Exception as exc:  # noqa: BLE001 -- surface and continue
            rows.append(
                {
                    "label": label,
                    "endpoint": endpoint,
                    "ok": False,
                    "error": str(exc),
                    "durationMs": int((time.perf_counter() - started) * 1000),
                }
            )
        time.sleep(0.1)
    if snapshot is not None:
        _restore_volume(snapshot, data_volume)
    return rows


def _run_pass_c(base_url: str, timeout_s: float, cold_open: bool) -> list[dict[str, Any]]:
    flows: list[dict[str, Any]] = []
    flow_names = list(FLOW_SPECS.keys())
    if cold_open:
        flow_names.remove("admin.open.warm")
    else:
        flow_names.remove("admin.open.cold")

    for name in flow_names:
        legs = FLOW_SPECS[name]
        started = time.perf_counter()
        leg_rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=len(legs)) as pool:
            future_map = {
                pool.submit(_request, base_url, "GET", endpoint, timeout_s): leg_name
                for leg_name, endpoint in legs
            }
            for future in as_completed(future_map):
                leg_name = future_map[future]
                leg_rows.append({"leg": leg_name, **future.result()})
        flows.append(
            {
                "flow": name,
                "legs": leg_rows,
                "durationMs": int((time.perf_counter() - started) * 1000),
            }
        )

    fetcher_start = time.perf_counter()
    trigger = _request(
        base_url, "POST", "/tasks/run-fetcher", timeout_s, body={"preset": "retry_failed"}
    )
    poll = _poll_task_live(base_url, "fetch", POLL_TIMEOUT_S)
    abort = _request(base_url, "POST", "/tasks/abort", timeout_s, body=None)
    flows.append(
        {
            "flow": "admin.fetcher.trigger",
            "legs": [
                {"leg": "trigger", **trigger},
                {"leg": "abort", **abort},
            ],
            "durationMs": int((time.perf_counter() - fetcher_start) * 1000),
            "poll": poll,
        }
    )
    return flows


def _summarize_routes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_route: dict[tuple[str, bool], list[int]] = {}
    for row in rows:
        key = (str(row.get("route") or ""), bool(row.get("warm")))
        by_route.setdefault(key, []).append(int(row.get("durationMs") or 0))
    summary = []
    for (route, warm), durations in sorted(by_route.items()):
        durations.sort()
        p50 = durations[len(durations) // 2] if durations else 0
        p95 = durations[min(len(durations) - 1, int(len(durations) * 0.95))] if durations else 0
        summary.append(
            {
                "route": route,
                "warm": warm,
                "samples": len(durations),
                "minMs": durations[0] if durations else 0,
                "p50Ms": p50,
                "p95Ms": p95,
                "maxMs": durations[-1] if durations else 0,
            }
        )
    return summary


def _write_report(
    output_dir: Path,
    *,
    image: str,
    profile: str,
    route_summary: list[dict[str, Any]],
    mutations: list[dict[str, Any]],
    flows_a: list[dict[str, Any]],
    flows_b: list[dict[str, Any]],
) -> None:
    lines = [
        "# Admin Flow Benchmark",
        "",
        f"- image: `{image}`",
        f"- profile: `{profile}`",
        f"- generated: `{datetime.now(UTC).isoformat()}`",
        "",
        "## Route sweep (Pass A)",
        "",
        "| Route | Warm | Samples | p50 (ms) | p95 (ms) | max (ms) |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    for row in route_summary:
        lines.append(
            "| `{route}` | {warm} | {samples} | {p50Ms} | {p95Ms} | {maxMs} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Mutations (Pass B)",
            "",
            "| Mutation | Duration (ms) | Status | Note |",
            "| --- | ---: | --- | --- |",
        ]
    )
    for row in mutations:
        note = row.get("error", "") or row.get("phase", "")
        lines.append(
            f"| `{row.get('label', '')}` | {row.get('durationMs', 0)} | {row.get('status', '')} | {note} |"
        )
    lines.extend(
        [
            "",
            "## Composite flows (Pass C)",
            "",
            "| Flow | Duration (ms) | Legs |",
            "| --- | ---: | --- |",
        ]
    )
    for flow in flows_a + flows_b:
        legs = ", ".join(
            f"`{leg.get('leg', '')}`={leg.get('durationMs', 0)}ms" for leg in flow.get("legs", [])
        )
        lines.append(f"| `{flow.get('flow', '')}` | {flow.get('durationMs', 0)} | {legs} |")
    (output_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES.keys()),
        default=DEFAULT_PROFILE,
    )
    parser.add_argument(
        "--data-volume",
        required=True,
        help="Path to the seeded /data volume on the host.",
    )
    parser.add_argument(
        "--output",
        default="",
        help=f"Output directory (default: {DEFAULT_OUTPUT_ROOT}/<run-token>).",
    )
    parser.add_argument(
        "--container-name",
        default=DEFAULT_CONTAINER_NAME,
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_S,
    )
    parser.add_argument("--port", type=int, default=8878)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_volume = Path(str(args.data_volume)).expanduser().resolve()
    if not data_volume.is_dir():
        raise SystemExit(f"data volume not found: {data_volume}")
    profile = PROFILES[args.profile]
    memory = str(profile["memory"])
    if memory.endswith("g") and float(memory[:-1]) < MIN_MEMORY_G:
        raise SystemExit(f"profile memory must be >= {MIN_MEMORY_G}g")

    run_token = generate_run_token()
    output_dir = (
        Path(str(args.output)).expanduser().resolve()
        if args.output
        else (DEFAULT_OUTPUT_ROOT / run_token)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = f"http://127.0.0.1:{int(args.port)}"
    container_name = str(args.container_name)
    _stop_container(container_name)
    _docker(
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "--cpus",
        str(profile["cpus"]),
        "--memory",
        memory,
        "--memory-swap",
        memory,
        "-p",
        f"{int(args.port)}:8080",
        "-v",
        f"{str(data_volume)}:/data",
        str(args.image),
    )

    def _cleanup() -> None:
        _stop_container(container_name)

    try:
        _wait_ready(base_url)
        cold = _sample_get_sweep(base_url, args.timeout, warm=False)
        warm = _sample_get_sweep(base_url, args.timeout, warm=True)

        snapshot = _snapshot_volume(data_volume, output_dir)
        mutations = _run_pass_b(base_url, args.timeout, snapshot, data_volume)

        flows_cold = _run_pass_c(base_url, args.timeout, cold_open=True)
        flows_warm = _run_pass_c(base_url, args.timeout, cold_open=False)
    finally:
        _cleanup()

    (output_dir / "meta.json").write_text(
        json.dumps(
            {
                "runToken": run_token,
                "image": str(args.image),
                "profile": str(args.profile),
                "dataVolume": str(data_volume),
                "containerName": container_name,
                "generated": datetime.now(UTC).isoformat(),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "routes.json").write_text(
        json.dumps(
            {"cold": cold, "warm": warm, "summary": _summarize_routes(cold + warm)}, indent=2
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "flows.json").write_text(
        json.dumps(
            {
                "cold": flows_cold,
                "warm": flows_warm,
                "mutations": mutations,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_report(
        output_dir,
        image=str(args.image),
        profile=str(args.profile),
        route_summary=_summarize_routes(cold + warm),
        mutations=mutations,
        flows_a=flows_cold,
        flows_b=flows_warm,
    )
    print(f"wrote benchmark output to {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
