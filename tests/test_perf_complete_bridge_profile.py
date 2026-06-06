from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from scripts import perf_complete


class _FakeHttpResponse:
    def __init__(
        self, payload: bytes, *, status: int = 200, content_type: str = "application/json"
    ):
        self._payload = payload
        self.status = status
        self.headers = {"content-type": content_type}

    def read(self) -> bytes:
        return self._payload


class _FakeHttpConnection:
    calls: list[str] = []

    def __init__(self, host: str, *, port: int, timeout: float):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.path = ""

    def connect(self) -> None:
        return None

    def putrequest(self, method: str, path: str) -> None:
        self.path = path
        self.calls.append(path)

    def putheader(self, *_args) -> None:
        return None

    def endheaders(self) -> None:
        return None

    def getresponse(self):
        if self.path.endswith("/ops/performance-profile"):
            return _FakeHttpResponse(
                json.dumps({"ok": True, "routeTimings": {"routes": []}}).encode("utf-8")
            )
        if self.path.endswith(".html"):
            return _FakeHttpResponse(b"<html>ok</html>", content_type="text/html")
        return _FakeHttpResponse(json.dumps({"ok": True}).encode("utf-8"))

    def close(self) -> None:
        return None


def _write_profile(path: Path, *, route_p95: int, operation_p95: int, error_count: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "ok": True,
                "generatedAt": "2026-06-05T08:00:00+00:00",
                "routeTimings": {
                    "routes": [
                        {
                            "label": "GET /ops/dashboard-health",
                            "count": 3,
                            "p50Ms": route_p95 // 2,
                            "p95Ms": route_p95,
                            "avgMs": route_p95 // 2,
                            "maxMs": route_p95,
                            "lastMs": route_p95,
                            "lastStatus": 200,
                            "errorCount": error_count,
                        }
                    ]
                },
                "operationTimings": {
                    "operations": [
                        {
                            "label": "ops.dashboard-health.registry",
                            "count": 2,
                            "p50Ms": operation_p95 // 2,
                            "p95Ms": operation_p95,
                            "avgMs": operation_p95 // 2,
                            "maxMs": operation_p95,
                            "lastMs": operation_p95,
                            "lastStatus": 200,
                            "errorCount": 0,
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )


def test_sync_rehearsal_summary_preserves_performance_profile_artifact(
    tmp_path: Path, monkeypatch
) -> None:
    def _fake_run(command, *, stdout_path: Path, stderr_path: Path, env=None):
        report_path = Path(command[command.index("--report-path") + 1])
        artifacts_dir = Path(command[command.index("--artifacts-dir") + 1])
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stderr_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_stdout = artifacts_dir / "runtime.stdout.log"
        runtime_stderr = artifacts_dir / "runtime.stderr.log"
        performance_profile = artifacts_dir / "performance-profile.post-sync.json"
        runtime_stdout.write_text("", encoding="utf-8")
        runtime_stderr.write_text("", encoding="utf-8")
        performance_profile.write_text(
            json.dumps({"ok": True, "routeTimings": {"routes": []}}),
            encoding="utf-8",
        )
        report_path.write_text(
            json.dumps(
                {
                    "ok": True,
                    "scenarios": [
                        {
                            "slug": "packaged-sync-rehearsal",
                            "status": "passed",
                            "durationMs": 900,
                            "memoryMetrics": {
                                "sampleCount": 3,
                                "peakWorkingSetBytes": 7000,
                            },
                            "details": {
                                "pushTiming": {"totalDurationMs": 111},
                                "pullTiming": {"totalDurationMs": 222},
                                "runtimeStdout": str(runtime_stdout),
                                "runtimeStderr": str(runtime_stderr),
                                "performanceProfileSnapshot": str(performance_profile),
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return {"exitCode": 0, "memoryMetrics": {"sampleCount": 1}}

    monkeypatch.setattr(perf_complete, "run_monitored_command", _fake_run)

    summary, exit_code = perf_complete.run_sync_rehearsal(
        output_dir=tmp_path,
        runtime_timeout_s=5,
        baseline_dir=tmp_path / "baseline",
        exe_path=tmp_path / "Baluffo.exe",
    )

    expected_profile = tmp_path / "sync" / "artifacts" / "performance-profile.post-sync.json"
    assert exit_code == 0
    assert summary["scenario"]["details"]["performanceProfileSnapshot"] == str(expected_profile)
    assert summary["artifactSizes"]["keyArtifacts"][-1]["path"] == str(expected_profile.resolve())


def test_bridge_profile_summary_aggregates_snapshot_artifacts(tmp_path: Path) -> None:
    jobs_profile = tmp_path / "jobs" / "performance-profile.startup.json"
    admin_profile = tmp_path / "admin" / "performance-profile.startup.json"
    sync_profile = tmp_path / "sync" / "performance-profile.post-sync.json"
    _write_profile(jobs_profile, route_p95=120, operation_p95=90)
    _write_profile(admin_profile, route_p95=900, operation_p95=700, error_count=1)
    _write_profile(sync_profile, route_p95=300, operation_p95=200)
    startup = {
        "jobs": {
            "cold": {"report": {"artifacts": {"performanceProfileSnapshot": str(jobs_profile)}}},
            "warm": {"report": {"artifacts": {}}},
        },
        "admin": {
            "cold": {"report": {"artifacts": {"performanceProfileSnapshot": str(admin_profile)}}},
            "warm": {"report": {"artifacts": {}}},
        },
    }
    sync = {
        "scenario": {"details": {"performanceProfileSnapshot": str(sync_profile)}},
        "report": {"artifacts": {}},
    }

    summary = perf_complete.build_bridge_profile_summary(
        startup=startup,
        sync=sync,
        output_dir=tmp_path,
    )

    assert summary["samples"][0]["source"] == "startup.jobs.cold"
    assert summary["samples"][0]["routeCount"] == 1
    assert summary["topRoutesByP95"][0]["source"] == "startup.admin.cold"
    assert summary["topRoutesByP95"][0]["p95Ms"] == 900
    assert summary["topOperationsByP95"][0]["label"] == "ops.dashboard-health.registry"
    assert summary["errorRoutes"][0]["errorCount"] == 1
    assert summary["suspectRoutes"][0]["label"] == "GET /ops/dashboard-health"
    assert summary["suspectRoutes"][0]["maxP95Ms"] == 900


def test_bridge_profile_summary_records_external_sample_failure(tmp_path: Path) -> None:
    summary = perf_complete.build_bridge_profile_summary(
        startup={},
        sync={},
        output_dir=tmp_path,
        bridge_base_url="http://127.0.0.1:1",
    )

    external = [row for row in summary["samples"] if row["source"] == "live.bridge"][0]
    assert external["ok"] is False
    assert external["profilePath"].endswith("performance-profile.json")
    assert Path(external["profilePath"]).is_file()


def test_live_bridge_sampler_records_bounded_read_only_sample(tmp_path: Path, monkeypatch) -> None:
    _FakeHttpConnection.calls = []
    monkeypatch.setattr(perf_complete.http.client, "HTTPConnection", _FakeHttpConnection)
    monkeypatch.setattr(
        perf_complete,
        "datetime",
        SimpleNamespace(now=lambda _tz=None: SimpleNamespace(isoformat=lambda: "now")),
    )

    summary = perf_complete.capture_live_bridge_profile(
        bridge_base_url="http://192.168.50.61:8877",
        output_dir=tmp_path,
    )

    assert summary["ok"] is True
    assert len(summary["requests"]) == len(perf_complete.LIVE_BRIDGE_ENDPOINTS)
    assert any(path.endswith("/admin.html") for path in _FakeHttpConnection.calls)
    profile_path = Path(summary["profilePath"])
    assert profile_path.is_file()
    assert json.loads(profile_path.read_text(encoding="utf-8"))["ok"] is True
    html_row = next(row for row in summary["requests"] if row["endpoint"] == "/admin.html")
    assert html_row["sizeBytes"] == len(b"<html>ok</html>")
    assert html_row["topLevelKeys"] == []
    assert html_row["tcpConnectMs"] >= 0
    assert html_row["firstByteMs"] >= 0


def test_live_bridge_sampler_can_compare_multiple_timeouts(tmp_path: Path, monkeypatch) -> None:
    _FakeHttpConnection.calls = []
    monkeypatch.setattr(perf_complete.http.client, "HTTPConnection", _FakeHttpConnection)

    summary = perf_complete.capture_live_bridge_profile(
        bridge_base_url="http://192.168.50.61:8877",
        output_dir=tmp_path,
        timeout_sequence=[3.0, 10.0],
    )

    assert summary["timeoutsS"] == [3.0, 10.0]
    assert len(summary["requests"]) == len(perf_complete.LIVE_BRIDGE_ENDPOINTS) * 2
    assert {row["timeoutS"] for row in summary["requests"]} == {3.0, 10.0}


def test_optimization_targets_include_bridge_profile_rows() -> None:
    benchmarks = {
        "discovery": {"medianDurationMs": 1000},
        "fetch": {"medianDurationMs": 2000},
        "frontendBoot": {
            "pages": [{"page": "admin", "durationMs": 300, "summaryPath": "admin.json"}]
        },
        "startup": {
            "admin": {
                "cold": {
                    "stageDurationsMs": {"total_launch_to_first_usable_ui": 1500},
                    "reportPath": "admin-cold.json",
                }
            }
        },
        "sync": {
            "pushTiming": {
                "totalDurationMs": 400,
                "detailTiming": {
                    "stageTotalsMs": {"writeShardedSnapshot": 350},
                    "stageTop": [{"stage": "writeShardedSnapshot", "durationMs": 350}],
                },
            },
            "pullTiming": {"totalDurationMs": 500},
        },
        "syncDetail": {
            "stageTop": [{"stage": "writeShardedSnapshot", "durationMs": 350}],
            "remoteOperationTop": [{"operation": "pushShard", "durationMs": 275}],
            "remoteSlowestRequests": [
                {"method": "PUT", "operation": "pushShard", "durationMs": 225, "path": "shard"}
            ],
            "reportPath": "sync.json",
        },
        "bridgeProfile": {
            "topRoutesByP95": [
                {
                    "source": "startup.admin.cold",
                    "label": "GET /ops/dashboard-health",
                    "p95Ms": 2500,
                    "profilePath": "profile.json",
                }
            ],
            "topOperationsByP95": [
                {
                    "source": "startup.admin.cold",
                    "label": "ops.dashboard-health.registry",
                    "p95Ms": 1800,
                    "profilePath": "profile.json",
                }
            ],
        },
    }

    targets = perf_complete.build_optimization_targets(benchmarks)

    assert targets[0]["kind"] == "bridge-route"
    assert targets[0]["label"] == "GET /ops/dashboard-health"
    assert any(row["kind"] == "bridge-operation" for row in targets)
    assert any(row["kind"] == "sync-detail" for row in targets)
    assert any(row["kind"] == "sync-remote-operation" for row in targets)
    assert any(row["kind"] == "sync-remote-request" for row in targets)


def test_console_summary_reports_bridge_profile_and_targets(capsys) -> None:
    summary = {
        "benchmarks": {
            "discovery": {"medianDurationMs": 1, "artifactSizes": {}, "memoryMetrics": {}},
            "fetch": {"medianDurationMs": 2, "artifactSizes": {}, "memoryMetrics": {}},
            "frontendBoot": {"durationMs": 3, "artifactSizes": {}, "memoryMetrics": {}},
            "startup": {
                "jobs": {
                    "cold": {"durationMs": 4, "artifactSizes": {}, "memoryMetrics": {}},
                    "warm": {"durationMs": 5, "artifactSizes": {}, "memoryMetrics": {}},
                },
                "admin": {
                    "cold": {"durationMs": 6, "artifactSizes": {}, "memoryMetrics": {}},
                    "warm": {"durationMs": 7, "artifactSizes": {}, "memoryMetrics": {}},
                },
            },
            "sync": {
                "pushTiming": {"totalDurationMs": 8},
                "pullTiming": {"totalDurationMs": 9},
                "artifactSizes": {},
                "memoryMetrics": {},
                "comparisons": {"push": {}, "pull": {}},
            },
            "bridgeProfile": {
                "topRoutesByP95": [
                    {
                        "source": "startup.admin.cold",
                        "label": "GET /ops/dashboard-health",
                        "p95Ms": 99,
                        "avgMs": 50,
                        "count": 2,
                        "errorCount": 0,
                    }
                ],
                "topOperationsByP95": [],
            },
        },
        "optimizationTargets": [
            {
                "kind": "bridge-route",
                "source": "startup.admin.cold",
                "label": "GET /ops/dashboard-health",
                "durationMs": 99,
            }
        ],
    }

    perf_complete._print_console_summary(summary)

    output = capsys.readouterr().out
    assert "Bridge profile top timings" in output
    assert "route,startup.admin.cold,GET /ops/dashboard-health,99,50,2,0" in output
    assert "Optimization targets" in output
    assert "bridge-route,startup.admin.cold,GET /ops/dashboard-health,99" in output
