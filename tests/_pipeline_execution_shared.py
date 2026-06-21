"""Shared helpers for pipeline execution tests."""

from __future__ import annotations

import datetime as datetime_module
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.bridge.pipeline_service import PipelineRuntime, PipelineService

__all__ = [
    "Any",
    "FakeLock",
    "Path",
    "PipelineRuntime",
    "PipelineService",
    "SimpleNamespace",
    "_install_fake_wait_clock",
    "_pipeline_status_payload",
    "_projection_snapshot",
    "datetime",
    "load_json_object_stub",
    "make_parse_iso",
    "pytest",
    "threading",
]


def load_json_object_stub(_path: Path, default: Any) -> Any:
    return {
        "startedAt": "2026-03-22T12:00:00Z",
        "finishedAt": "2026-03-22T12:00:00Z",
        "summary": {"outputCount": 0},
    }


class FakeLock:
    """Fake lock for testing."""

    def __init__(self):
        self._acquired = False

    def __enter__(self):
        self._acquired = True
        return self

    def __exit__(self, *args):
        self._acquired = False


def make_parse_iso():
    """Create a parse_iso function that returns datetime objects."""

    def parse_iso(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

    return parse_iso


def _projection_snapshot(
    *, task_type: str, run_id: str, active: bool, finished_at: str = "", explicit_dead: bool = False
):
    return SimpleNamespace(
        child_tasks={
            task_type: SimpleNamespace(
                run_id=run_id,
                active=active,
                finished_at=finished_at,
                explicit_dead=explicit_dead,
            )
        }
    )


def _pipeline_status_payload(
    *,
    active: bool,
    run_id: str,
    stage: str,
    current_step: int,
    total_steps: int,
    percent: int,
    label: str,
    started_at: str,
    finished_at: str,
    updates_found: bool,
    refresh_recommended: bool,
    baseline_output_count: int,
    final_output_count: int,
    jobs_page_loaded_count: int,
) -> dict[str, object]:
    return {
        "active": active,
        "runId": run_id,
        "stage": stage,
        "progress": {
            "currentStep": current_step,
            "totalSteps": total_steps,
            "percent": percent,
            "label": label,
        },
        "startedAt": started_at,
        "finishedAt": finished_at,
        "error": "",
        "updatesFound": updates_found,
        "refreshRecommended": refresh_recommended,
        "baselineOutputCount": baseline_output_count,
        "finalOutputCount": final_output_count,
        "jobsPageLoadedCount": jobs_page_loaded_count,
        "appVersion": "1.0.0",
    }


def _install_fake_wait_clock(
    monkeypatch: pytest.MonkeyPatch, *, start_at: str
) -> tuple[dict[str, datetime], list[float]]:
    clock = {"now": datetime.fromisoformat(start_at.replace("Z", "+00:00"))}
    waits: list[float] = []
    real_datetime = datetime_module.datetime

    class FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            current = clock["now"]
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    class FakeEvent:
        def wait(self, delay: float) -> None:
            waits.append(float(delay))
            clock["now"] = clock["now"] + datetime_module.timedelta(seconds=float(delay))

    monkeypatch.setattr(datetime_module, "datetime", FakeDateTime)
    monkeypatch.setattr(threading, "Event", FakeEvent)
    return clock, waits
