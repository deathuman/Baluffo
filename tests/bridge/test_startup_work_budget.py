"""Admission rules for the first-paint startup work budget."""

from __future__ import annotations

import threading

from src.bridge import startup_work_budget
from src.bridge.startup_work_budget import StartupWorkBudget


class _Clock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = float(value)

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


def test_barrier_opens_only_after_the_first_request_burst_goes_quiet() -> None:
    clock = _Clock()
    budget = StartupWorkBudget(budget_ms=5000, quiet_ms=350, clock=clock)
    budget.start()

    assert budget.note_accepted(1) is False  # first request arrives
    clock.advance(0.2)
    assert budget.note_accepted(1) is False  # still inside the burst
    clock.advance(0.2)
    assert budget.note_accepted(2) is False  # a new request restarts the window
    clock.advance(0.4)

    assert budget.note_accepted(2) is True
    snapshot = budget.snapshot()
    assert snapshot["barrierOpen"] is True
    assert snapshot["reason"] == "request-quiet"
    assert snapshot["acceptedRequests"] == 2
    assert snapshot["waitedMs"] == 800


def test_barrier_opens_when_the_budget_expires_during_a_continuous_burst() -> None:
    clock = _Clock()
    budget = StartupWorkBudget(budget_ms=600, quiet_ms=350, clock=clock)
    budget.start()

    assert budget.note_accepted(1) is False  # t=0.00
    clock.advance(0.2)
    assert budget.note_accepted(2) is False  # t=0.20
    clock.advance(0.2)
    assert budget.note_accepted(3) is False  # t=0.40
    clock.advance(0.25)

    # t=0.65: requests never pause, so the quiet rule cannot fire and the budget
    # has to admit the work mid-burst.
    assert budget.note_accepted(4) is True
    assert budget.snapshot()["reason"] == "budget-expired"
    assert budget.snapshot()["waitedMs"] == 650


def test_wait_is_not_armed_before_the_listener_binds() -> None:
    budget = StartupWorkBudget(budget_ms=5000, quiet_ms=0)

    assert budget.wait_for_admission() == "not-armed"
    assert budget.snapshot()["barrierOpen"] is False


def test_wait_opens_the_barrier_when_its_own_budget_expires() -> None:
    budget = StartupWorkBudget(budget_ms=5000, quiet_ms=350)
    budget.start()

    assert budget.wait_for_admission(timeout_s=0.05) == "budget-expired"
    assert budget.snapshot()["reason"] == "wait-timeout"
    assert budget.wait_for_admission() == "opened"


def test_deferred_work_runs_after_admission_and_in_submission_order() -> None:
    budget = StartupWorkBudget(budget_ms=5000, quiet_ms=0)
    budget.start()
    order: list[str] = []
    barrier_states: list[bool] = []
    done = threading.Event()

    def work(label: str, *, last: bool = False):
        def run() -> None:
            order.append(label)
            barrier_states.append(budget.is_open())
            if last:
                done.set()

        return run

    assert budget.submit("registry_parse", work("registry_parse")) is True
    assert budget.submit("journal_compaction", work("journal_compaction", last=True)) is True
    assert budget.submit("registry_parse", work("duplicate")) is False

    budget.wait_for_admission(timeout_s=0.1)

    assert done.wait(timeout=5.0) is True
    assert order == ["registry_parse", "journal_compaction"]
    assert barrier_states == [True, True]


def test_failing_deferred_work_is_recorded_and_the_queue_continues() -> None:
    metrics: list[tuple[str, dict[str, object] | None]] = []
    budget = StartupWorkBudget(
        budget_ms=5000,
        quiet_ms=0,
        metric_sink=lambda event, payload: metrics.append((event, payload)),
    )
    budget.start()
    completed = threading.Event()

    def broken() -> None:
        raise RuntimeError("boom")

    def follow_up() -> None:
        completed.set()

    budget.submit("broken", broken)
    budget.submit("follow_up", follow_up)
    budget.wait_for_admission(timeout_s=0.1)

    assert completed.wait(timeout=5.0) is True
    finished = [
        payload
        for event, payload in metrics
        if event == "desktop_bridge_deferred_startup_work_finished"
    ]
    assert [(row["label"], row["ok"], row["error"]) for row in finished if row] == [
        ("broken", False, "boom"),
        ("follow_up", True, ""),
    ]


def test_barrier_metric_records_why_heavy_work_started() -> None:
    metrics: list[tuple[str, dict[str, object] | None]] = []
    clock = _Clock()
    budget = StartupWorkBudget(
        budget_ms=5000,
        quiet_ms=100,
        clock=clock,
        metric_sink=lambda event, payload: metrics.append((event, payload)),
    )
    budget.start()
    budget.note_accepted(1)
    clock.advance(0.15)

    assert budget.note_accepted(1) is True

    assert [event for event, _ in metrics] == ["desktop_bridge_first_paint_barrier_opened"]
    payload = metrics[0][1] or {}
    assert payload["reason"] == "request-quiet"
    assert payload["waitedMs"] == 150
    assert payload["acceptedRequests"] == 1


def test_diagnostics_env_override_sets_the_default_budget(
    monkeypatch,
) -> None:
    monkeypatch.setenv(startup_work_budget.BUDGET_ENV, "0")
    try:
        budget = startup_work_budget.configure_startup_work_budget()
        assert budget.snapshot()["budgetMs"] == 0

        explicit = startup_work_budget.configure_startup_work_budget(budget_ms=1500)
        assert explicit.snapshot()["budgetMs"] == 1500
    finally:
        monkeypatch.delenv(startup_work_budget.BUDGET_ENV, raising=False)
        startup_work_budget.reset_startup_work_budget()


def test_module_budget_is_reconfigured_per_process() -> None:
    metrics: list[tuple[str, dict[str, object] | None]] = []
    startup_work_budget.configure_startup_work_budget(
        budget_ms=10,
        quiet_ms=0,
        metric_sink=lambda event, payload: metrics.append((event, payload)),
    )
    try:
        startup_work_budget.start_startup_work_budget()

        assert startup_work_budget.note_accepted_requests(1) is False
        assert startup_work_budget.open_first_paint("test-open") is True
        assert startup_work_budget.startup_work_budget_snapshot()["barrierOpen"] is True
        assert [event for event, _ in metrics] == ["desktop_bridge_first_paint_barrier_opened"]
        assert (metrics[0][1] or {})["reason"] == "test-open"
    finally:
        startup_work_budget.reset_startup_work_budget()

    assert startup_work_budget.startup_work_budget_snapshot()["barrierOpen"] is False
