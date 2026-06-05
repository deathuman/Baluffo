from __future__ import annotations

from src.bridge import performance_profile


def setup_function() -> None:
    performance_profile.clear_performance_profile()


def teardown_function() -> None:
    performance_profile.clear_performance_profile()


def test_performance_profile_aggregates_routes_and_operations() -> None:
    performance_profile.record_route_duration(
        "GET",
        "/ops/dashboard-health?secret=do-not-expose",
        100,
        status=200,
    )
    performance_profile.record_route_duration(
        "GET",
        "/ops/dashboard-health?secret=do-not-expose",
        300,
        status=500,
    )
    performance_profile.record_operation_duration("ops.dashboard.history", 40)

    snapshot = performance_profile.snapshot_performance_profile(
        runtime={"runtimeMode": "container"},
        generated_at="2026-06-05T00:00:00+00:00",
    )

    route = snapshot["routeTimings"]["routes"][0]
    operation = snapshot["operationTimings"]["operations"][0]
    assert snapshot["ok"] is True
    assert snapshot["runtime"]["runtimeMode"] == "container"
    assert route["label"] == "GET /ops/dashboard-health"
    assert route["count"] == 2
    assert route["avgMs"] == 200
    assert route["p95Ms"] == 300
    assert route["errorCount"] == 1
    assert "secret" not in route["label"]
    assert operation["label"] == "ops.dashboard.history"
    assert operation["p50Ms"] == 40


def test_performance_profile_redacts_dynamic_route_segments() -> None:
    performance_profile.record_route_duration(
        "GET",
        "/desktop-local-data/saved-jobs/local_umbrel_smoke_0_2_28/job_123",
        12,
    )

    snapshot = performance_profile.snapshot_performance_profile()
    route = snapshot["routeTimings"]["routes"][0]

    assert route["label"] == "GET /desktop-local-data/saved-jobs/:value/:value"
    assert "local_umbrel" not in route["label"]
    assert "job_123" not in route["label"]


def test_performance_profile_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(performance_profile, "MAX_SAMPLES_PER_CATEGORY", 3)
    performance_profile.clear_performance_profile()

    for value in range(1, 6):
        performance_profile.record_operation_duration("ops.dashboard.sync_status", value)

    snapshot = performance_profile.snapshot_performance_profile()
    operation = snapshot["operationTimings"]["operations"][0]

    assert operation["count"] == 3
    assert operation["sumMs"] == 12
    assert operation["p50Ms"] == 4
    assert operation["maxMs"] == 5
