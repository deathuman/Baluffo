from __future__ import annotations

import json
from pathlib import Path

from scripts import umbrel_reachability_probe as probe


def _sample(
    *,
    neighbor: dict[str, object] | None = None,
    ping_ok: bool = True,
    port80_ok: bool = True,
    port8877_ok: bool = True,
    http: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "target": {"host": probe.DEFAULT_HOST, "umbrelPort": 80, "baluffoPort": 8877},
        "neighbor": neighbor or {"mac": "98-F2-B3-E7-C9-5A", "state": "reachable"},
        "ping": {"ok": ping_ok},
        "tcp": {"80": {"ok": port80_ok}, "8877": {"ok": port8877_ok}},
        "http": http
        if http is not None
        else {path: {"ok": True, "status": 200} for path in probe.normal_http_paths()},
    }


def test_classifies_host_unreachable_when_arp_ping_and_ports_are_down() -> None:
    sample = _sample(
        neighbor={"mac": None, "state": "missing"},
        ping_ok=False,
        port80_ok=False,
        port8877_ok=False,
    )

    assert probe.classify_sample(sample) == {
        "status": "unhealthy",
        "domain": "host_network",
        "reason": "host-unreachable",
    }


def test_classifies_umbrel_port_down_after_host_is_seen() -> None:
    sample = _sample(port80_ok=False, port8877_ok=True)

    assert probe.classify_sample(sample) == {
        "status": "unhealthy",
        "domain": "umbrel_proxy",
        "reason": "umbrel-port-down",
    }


def test_classifies_baluffo_port_down_after_umbrel_port_is_seen() -> None:
    sample = _sample(port80_ok=True, port8877_ok=False)

    assert probe.classify_sample(sample) == {
        "status": "unhealthy",
        "domain": "baluffo_app_proxy",
        "reason": "baluffo-port-down",
    }


def test_classifies_compact_route_504_without_calling_it_host_unreachable() -> None:
    sample = _sample(
        http={
            "/app/ready": {"ok": True, "status": 200},
            "/ops/task-state?view=summary": {"ok": False, "status": 504},
        }
    )

    assert probe.classify_sample(sample) == {
        "status": "unhealthy",
        "domain": "baluffo_container_gateway",
        "reason": "baluffo-compact-route-504",
        "failedRoutes": ["/ops/task-state?view=summary"],
    }


def test_classifies_compact_route_timeout() -> None:
    sample = _sample(
        http={
            "/app/ready": {"ok": True, "status": 200},
            "/sync/status?view=summary": {
                "ok": False,
                "status": None,
                "error": "TimeoutError: timed out",
            },
        }
    )

    assert probe.classify_sample(sample)["reason"] == "baluffo-compact-route-timeout"


def test_classifies_healthy_when_ports_and_compact_routes_are_ok() -> None:
    assert probe.classify_sample(_sample()) == {
        "status": "healthy",
        "domain": "reachable",
        "reason": "compact-routes-ok",
    }


def test_normal_probe_route_budget_excludes_heavy_routes() -> None:
    normal_paths = probe.normal_http_paths()

    assert "/ops/health" not in normal_paths
    assert "/ops/storage-metrics" not in normal_paths
    assert not any(path.startswith("/ops/fetch-report") for path in normal_paths)
    probe.assert_normal_route_budget(normal_paths)


def test_diagnostic_burst_waits_for_failures_or_missing_neighbor() -> None:
    healthy = _sample()
    failing = _sample(http={"/app/ready": {"ok": False, "status": 504}})
    missing_neighbor = _sample(neighbor={"mac": None, "state": "unreachable"})

    assert not probe.should_run_diagnostic_burst(healthy, consecutive_failures=10, threshold=3)
    assert not probe.should_run_diagnostic_burst(failing, consecutive_failures=2, threshold=3)
    assert probe.should_run_diagnostic_burst(failing, consecutive_failures=3, threshold=3)
    assert probe.should_run_diagnostic_burst(missing_neighbor, consecutive_failures=1, threshold=3)


def test_write_sample_appends_jsonl_and_latest_snapshot(tmp_path: Path) -> None:
    sample = _sample()
    sample["classification"] = probe.classify_sample(sample)

    path = probe.write_sample(tmp_path, sample)

    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    latest = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    assert rows == [sample]
    assert latest == sample
