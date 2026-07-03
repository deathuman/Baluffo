#!/usr/bin/env python3
"""Record lightweight reachability evidence for the private Umbrel raw-LAN host."""

from __future__ import annotations

import argparse
import json
import math
import platform
import re
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOST = "192.168.50.61"
DEFAULT_UMBREL_PORT = 80
DEFAULT_BALUFFO_PORT = 8877
DEFAULT_TIMEOUT_S = 3.0
DEFAULT_OUTPUT_ROOT = ROOT / "_out" / "umbrel-reachability"
USER_AGENT = "BaluffoUmbrelReachabilityProbe/1.0"

NORMAL_HTTP_PATHS = (
    "/app/ready",
    "/tasks/run-jobs-pipeline-status",
    "/ops/task-state?view=summary",
    "/sync/status?view=summary",
    "/tasks/jobs-pipeline-schedule",
)
DIAGNOSTIC_HTTP_PATHS = ("/ops/health",)
HEAVY_ROUTE_PREFIXES = (
    "/ops/health",
    "/ops/storage-metrics",
    "/ops/storage-health",
    "/ops/fetch-report",
    "/registry/summary",
    "/registry/sources",
    "/discovery/report",
)
MAC_RE = re.compile(r"(?i)(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}")
PING_TIME_RE = re.compile(r"(?i)time[=<]\s*(\d+(?:\.\d+)?)\s*ms")


def utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_dir_name() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def normal_http_paths() -> tuple[str, ...]:
    return NORMAL_HTTP_PATHS


def diagnostic_http_paths() -> tuple[str, ...]:
    return DIAGNOSTIC_HTTP_PATHS


def assert_normal_route_budget(paths: Sequence[str] = NORMAL_HTTP_PATHS) -> None:
    for path in paths:
        if any(path.startswith(prefix) for prefix in HEAVY_ROUTE_PREFIXES):
            raise ValueError(f"normal probe route is too heavy: {path}")


def normalize_mac(value: str | None) -> str | None:
    if not value:
        return None
    match = MAC_RE.search(value)
    if not match:
        return None
    return match.group(0).replace(":", "-").upper()


def _capture(command: Sequence[str], *, timeout_s: float) -> dict[str, object]:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            check=False,
            text=True,
            timeout=max(1.0, timeout_s),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "command": list(command),
            "returncode": None,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "timedOut": True,
        }
    except OSError as exc:
        return {
            "command": list(command),
            "returncode": None,
            "stdout": "",
            "stderr": str(exc),
            "error": exc.__class__.__name__,
        }
    return {
        "command": list(command),
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def parse_neighbor_output(host: str, text: str, *, returncode: int | None = 0) -> dict[str, object]:
    lowered = text.lower()
    mac = normalize_mac(text)
    state = "unknown"
    if mac:
        for candidate in ("reachable", "stale", "delay", "probe", "dynamic", "static"):
            if candidate in lowered:
                state = candidate
                break
    elif "unreachable" in lowered or "failed" in lowered:
        state = "unreachable"
    elif "no arp entries" in lowered or returncode not in (0, None):
        state = "missing"
    return {"host": host, "mac": mac, "state": state, "raw": text.strip()[:1000]}


def probe_neighbor(host: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict[str, object]:
    if platform.system() == "Windows":
        captured = _capture(("arp", "-a", host), timeout_s=timeout_s)
    else:
        captured = _capture(("ip", "neigh", "show", host), timeout_s=timeout_s)
        if captured.get("error"):
            captured = _capture(("arp", "-a", host), timeout_s=timeout_s)
    text = "\n".join(str(captured.get(part) or "") for part in ("stdout", "stderr"))
    parsed = parse_neighbor_output(
        host,
        text,
        returncode=captured.get("returncode")
        if isinstance(captured.get("returncode"), int)
        else None,
    )
    parsed["command"] = captured.get("command")
    parsed["timedOut"] = bool(captured.get("timedOut"))
    if captured.get("error"):
        parsed["error"] = captured["error"]
    return parsed


def _ping_command(host: str, timeout_s: float) -> tuple[str, ...]:
    if platform.system() == "Windows":
        return ("ping", "-n", "1", "-w", str(max(1000, int(timeout_s * 1000))), host)
    return ("ping", "-c", "1", "-W", str(max(1, math.ceil(timeout_s))), host)


def probe_ping(host: str, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict[str, object]:
    captured = _capture(_ping_command(host, timeout_s), timeout_s=timeout_s + 1.0)
    text = "\n".join(str(captured.get(part) or "") for part in ("stdout", "stderr"))
    match = PING_TIME_RE.search(text)
    return {
        "host": host,
        "ok": captured.get("returncode") == 0,
        "latencyMs": float(match.group(1)) if match else None,
        "returncode": captured.get("returncode"),
        "timedOut": bool(captured.get("timedOut")),
        "raw": text.strip()[:1000],
    }


def probe_tcp(host: str, port: int, *, timeout_s: float = DEFAULT_TIMEOUT_S) -> dict[str, object]:
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            ok = True
            error = None
    except OSError as exc:
        ok = False
        error = f"{exc.__class__.__name__}: {exc}"
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return {"host": host, "port": port, "ok": ok, "elapsedMs": elapsed_ms, "error": error}


def _json_summary(body: bytes) -> dict[str, object]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    summary_keys = (
        "service",
        "status",
        "healthy",
        "startupReady",
        "appVersion",
        "active",
        "stage",
        "source",
        "nextRunAt",
    )
    summary = {key: payload[key] for key in summary_keys if key in payload}
    config = payload.get("config")
    if isinstance(config, dict):
        summary["configReady"] = config.get("ready")
        summary["configState"] = config.get("state")
    return summary


def probe_http(
    host: str,
    port: int,
    path: str,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, object]:
    url = f"http://{host}:{port}{path}"
    started = time.perf_counter()
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=timeout_s) as response:
            body = response.read(65536)
            status = int(response.status)
            error = None
    except HTTPError as exc:
        body = exc.read(4096)
        status = int(exc.code)
        error = f"HTTPError: {exc.code}"
    except TimeoutError as exc:
        body = b""
        status = None
        error = f"TimeoutError: {exc}"
    except URLError as exc:
        body = b""
        status = None
        error = f"URLError: {exc.reason}"
    except OSError as exc:
        body = b""
        status = None
        error = f"{exc.__class__.__name__}: {exc}"
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return {
        "url": url,
        "path": path,
        "status": status,
        "ok": status is not None and 200 <= status < 400,
        "elapsedMs": elapsed_ms,
        "error": error,
        "summary": _json_summary(body),
    }


def skipped_http(path: str, reason: str) -> dict[str, object]:
    return {"path": path, "ok": False, "skipped": True, "reason": reason}


def build_sample(
    *,
    host: str = DEFAULT_HOST,
    umbrel_port: int = DEFAULT_UMBREL_PORT,
    baluffo_port: int = DEFAULT_BALUFFO_PORT,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    include_http: bool = True,
    include_diagnostic: bool = False,
) -> dict[str, object]:
    assert_normal_route_budget()
    neighbor = probe_neighbor(host, timeout_s=timeout_s)
    ping = probe_ping(host, timeout_s=timeout_s)
    tcp = {
        str(umbrel_port): probe_tcp(host, umbrel_port, timeout_s=timeout_s),
        str(baluffo_port): probe_tcp(host, baluffo_port, timeout_s=timeout_s),
    }
    http: dict[str, object] = {}
    if include_http:
        if bool(tcp[str(baluffo_port)].get("ok")):
            http = {
                path: probe_http(host, baluffo_port, path, timeout_s=timeout_s)
                for path in NORMAL_HTTP_PATHS
            }
        else:
            http = {path: skipped_http(path, "baluffo-port-down") for path in NORMAL_HTTP_PATHS}
    diagnostic_http: dict[str, object] = {}
    if include_diagnostic and bool(tcp[str(baluffo_port)].get("ok")):
        diagnostic_http = {
            path: probe_http(host, baluffo_port, path, timeout_s=timeout_s)
            for path in DIAGNOSTIC_HTTP_PATHS
        }
    sample = {
        "timestamp": utc_timestamp(),
        "target": {"host": host, "umbrelPort": umbrel_port, "baluffoPort": baluffo_port},
        "neighbor": neighbor,
        "ping": ping,
        "tcp": tcp,
        "http": http,
        "diagnosticHttp": diagnostic_http,
    }
    sample["classification"] = classify_sample(sample)
    return sample


def _tcp_ok(sample: dict[str, object], port: int) -> bool:
    tcp = sample.get("tcp")
    if not isinstance(tcp, dict):
        return False
    row = tcp.get(str(port))
    return isinstance(row, dict) and bool(row.get("ok"))


def _all_tcp_failed(sample: dict[str, object]) -> bool:
    tcp = sample.get("tcp")
    if not isinstance(tcp, dict) or not tcp:
        return True
    return not any(isinstance(row, dict) and row.get("ok") for row in tcp.values())


def _http_failures(sample: dict[str, object]) -> list[dict[str, object]]:
    http = sample.get("http")
    if not isinstance(http, dict):
        return []
    failures: list[dict[str, object]] = []
    for path, row in http.items():
        if not isinstance(row, dict) or row.get("skipped"):
            continue
        if not row.get("ok"):
            failures.append({"path": path, **row})
    return failures


def classify_sample(sample: dict[str, object]) -> dict[str, object]:
    target = sample.get("target") if isinstance(sample.get("target"), dict) else {}
    umbrel_port = int(target.get("umbrelPort") or DEFAULT_UMBREL_PORT)
    baluffo_port = int(target.get("baluffoPort") or DEFAULT_BALUFFO_PORT)
    neighbor = sample.get("neighbor") if isinstance(sample.get("neighbor"), dict) else {}
    ping = sample.get("ping") if isinstance(sample.get("ping"), dict) else {}
    neighbor_missing = not neighbor.get("mac") and neighbor.get("state") in {
        "missing",
        "unreachable",
        "failed",
    }
    if neighbor_missing and not ping.get("ok") and _all_tcp_failed(sample):
        return {
            "status": "unhealthy",
            "domain": "host_network",
            "reason": "host-unreachable",
        }
    if not _tcp_ok(sample, umbrel_port):
        return {
            "status": "unhealthy",
            "domain": "umbrel_proxy",
            "reason": "umbrel-port-down",
        }
    if not _tcp_ok(sample, baluffo_port):
        return {
            "status": "unhealthy",
            "domain": "baluffo_app_proxy",
            "reason": "baluffo-port-down",
        }
    failures = _http_failures(sample)
    if failures:
        statuses = {failure.get("status") for failure in failures}
        if 504 in statuses:
            reason = "baluffo-compact-route-504"
        elif any("Timeout" in str(failure.get("error") or "") for failure in failures):
            reason = "baluffo-compact-route-timeout"
        else:
            reason = "baluffo-compact-route-failure"
        return {
            "status": "unhealthy",
            "domain": "baluffo_container_gateway",
            "reason": reason,
            "failedRoutes": [failure.get("path") for failure in failures],
        }
    return {"status": "healthy", "domain": "reachable", "reason": "compact-routes-ok"}


def should_run_diagnostic_burst(
    sample: dict[str, object],
    *,
    consecutive_failures: int,
    threshold: int,
) -> bool:
    neighbor = sample.get("neighbor") if isinstance(sample.get("neighbor"), dict) else {}
    if not neighbor.get("mac") and neighbor.get("state") in {"missing", "unreachable", "failed"}:
        return True
    classification = sample.get("classification")
    if not isinstance(classification, dict):
        classification = classify_sample(sample)
    unhealthy = isinstance(classification, dict) and classification.get("status") != "healthy"
    return unhealthy and consecutive_failures >= threshold


def write_sample(output_dir: Path, sample: dict[str, object]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "samples.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sample, sort_keys=True, separators=(",", ":")) + "\n")
    (output_dir / "latest.json").write_text(json.dumps(sample, indent=2), encoding="utf-8")
    return path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--umbrel-port", type=int, default=DEFAULT_UMBREL_PORT)
    parser.add_argument("--baluffo-port", type=int, default=DEFAULT_BALUFFO_PORT)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--no-http", action="store_true")
    parser.add_argument(
        "--force-diagnostic-burst",
        action="store_true",
        help="Also call diagnostic routes once; normal monitoring never uses them.",
    )
    parser.add_argument(
        "--diagnostic-after-failures",
        type=int,
        default=3,
        help="Call the diagnostic burst after this many consecutive unhealthy samples.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.run_dir or (args.output_root / run_dir_name())
    consecutive_failures = 0
    print(f"Writing Umbrel reachability samples to {output_dir}")
    for index in range(max(1, args.samples)):
        sample = build_sample(
            host=args.host,
            umbrel_port=args.umbrel_port,
            baluffo_port=args.baluffo_port,
            timeout_s=args.timeout,
            include_http=not args.no_http,
            include_diagnostic=False,
        )
        classification = sample["classification"]
        if isinstance(classification, dict) and classification.get("status") == "healthy":
            consecutive_failures = 0
        else:
            consecutive_failures += 1
        run_diagnostic = args.force_diagnostic_burst or should_run_diagnostic_burst(
            sample,
            consecutive_failures=consecutive_failures,
            threshold=max(1, args.diagnostic_after_failures),
        )
        if run_diagnostic:
            sample["diagnosticHttp"] = {
                path: probe_http(args.host, args.baluffo_port, path, timeout_s=args.timeout)
                for path in DIAGNOSTIC_HTTP_PATHS
                if _tcp_ok(sample, args.baluffo_port)
            }
        write_sample(output_dir, sample)
        print(
            f"{sample['timestamp']} {classification['status']} "
            f"{classification['domain']} {classification['reason']}"
        )
        if index + 1 < args.samples:
            time.sleep(max(0.0, args.interval))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
