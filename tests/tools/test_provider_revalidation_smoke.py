"""What the live revalidation smoke tool must prove, without touching the network.

``tools/provider_revalidation_smoke.py`` exists because a provider feed can publish its
validator on the GET that carries the jobs and nowhere else. The tool's own verdict is
only worth what its two phases check, so these tests pin them:

* the run-1 report must carry a validator AND the state file must keep it -- a capture
  that reaches the report but not the state would still re-read every run;
* after ageing, the real incremental policy (not a stub) must decide
  ``revalidate_only``, otherwise run 2 says nothing about revalidation;
* run 2 must be non-forced, fetch no body, and be recorded as the 304 exclusion;
* a feed that publishes no validator fails on that cause alone, rather than on the
  skip that follows it.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.common.datetime_utils import parse_datetime  # noqa: E402

NAME = "Smoke Board"
LOADER = "greenhouse_boards"
URL = "https://feed.example/jobs"


def _load():
    spec = importlib.util.spec_from_file_location(
        "provider_revalidation_smoke_under_test",
        _ROOT / "tools" / "provider_revalidation_smoke.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


smoke = _load()


def _detail(**overrides: Any) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "name": NAME,
        "adapter": "greenhouse",
        "status": "ok",
        "fetchedCount": 2,
        "keptCount": 2,
        "error": "",
        "httpStatus": 200,
        "httpEtag": '"v1"',
        "cacheDecision": "run_now",
        "cacheDecisionReason": "force_refresh_all",
    }
    detail.update(overrides)
    return detail


def _publish(detail: dict[str, Any]) -> None:
    smoke.diagnostics.SOURCE_DIAGNOSTICS[LOADER] = {"details": [detail], "partialErrors": []}


class _RevalidatingLoader:
    """A runner that honours the state it is handed, like the real one does."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        *,
        fetch_text: Any,
        timeout_s: int,
        retries: int,
        backoff_s: float,
        source_state_rows: dict[str, dict[str, Any]],
        force_refresh_all: bool,
    ) -> list[dict[str, Any]]:
        self.calls.append({"force": force_refresh_all, "rows": source_state_rows})
        if force_refresh_all:
            fetch_text(URL, timeout_s)
            _publish(_detail())
            return [{"title": "one"}, {"title": "two"}]
        assert not force_refresh_all, "the revalidation run must never be forced"
        decision = smoke.get_incremental_cache_decision(
            NAME, source_state_rows, adapter="greenhouse"
        )
        assert decision["cacheDecision"] == "revalidate_only", decision
        _publish(
            _detail(
                status="excluded",
                fetchedCount=0,
                keptCount=0,
                httpStatus=304,
                exclusionReason="cache_not_modified_304",
                cacheDecision="revalidate_only",
                cacheDecisionReason="not_modified_304",
            )
        )
        return []


class _NoValidatorLoader:
    def __call__(self, **_kwargs: Any) -> list[dict[str, Any]]:
        _publish(_detail(httpEtag=""))
        return [{"title": "one"}]


def _run(tmp_path: Path, loader: Any, *, name: str = NAME) -> dict[str, Any]:
    try:
        summary: dict[str, Any] = smoke.run_smoke(
            row={"name": name, "adapter": "greenhouse"},
            loader_name=LOADER,
            loader=loader,
            fetch_text=lambda url, timeout, request=None: "body",
            output_dir=tmp_path / "out",
            age_minutes=30,
        )
        return summary
    finally:
        smoke.diagnostics.SOURCE_DIAGNOSTICS.pop(LOADER, None)


def test_a_revalidating_feed_passes_every_phase(tmp_path: Path) -> None:
    loader = _RevalidatingLoader()

    summary = _run(tmp_path, loader)

    assert summary["ok"], summary["failures"]
    assert summary["stored"]["lastHttpEtag"] == '"v1"'
    assert summary["decision"] == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "provider_stable_revalidate",
    }
    assert summary["run2"]["urls"] == []
    assert summary["run2"]["httpStatus"] == 304
    assert summary["run2"]["exclusionReason"] == "cache_not_modified_304"
    assert summary["storedAfterRun2"]["lastHttpEtag"] == '"v1"'
    assert [call["force"] for call in loader.calls] == [True, False]


def test_a_feed_without_a_validator_fails_on_that_cause_alone(tmp_path: Path) -> None:
    summary = _run(tmp_path, _NoValidatorLoader())

    assert not summary["ok"]
    assert len(summary["failures"]) == 1, summary["failures"]
    assert "no ETag/Last-Modified" in summary["failures"][0]
    assert summary["decision"] == {}, "the second phase must not run without a validator"


def test_ageing_moves_the_run_clocks_and_keeps_the_validator() -> None:
    rows = {
        NAME: {
            "lastCheckedAt": datetime.now(UTC).isoformat(),
            "nextEligibleCheckAt": datetime.now(UTC).isoformat(),
            "lastHttpEtag": '"v1"',
        }
    }
    original_checked_at = rows[NAME]["lastCheckedAt"]

    aged = smoke.age_state(rows, 30)

    aged_checked_at = parse_datetime(aged[NAME]["lastCheckedAt"])
    original_parsed = parse_datetime(original_checked_at)
    assert aged_checked_at is not None and original_parsed is not None
    assert aged_checked_at < original_parsed
    assert aged[NAME]["nextEligibleCheckAt"] != rows[NAME]["nextEligibleCheckAt"]
    assert aged[NAME]["lastHttpEtag"] == '"v1"'
    assert rows[NAME]["lastCheckedAt"] == original_checked_at, "the input is not mutated"


def test_exact_identity_beats_a_substring_match() -> None:
    rows = [
        {"name": "Atari (Greenhouse)", "slug": "atariinc"},
        {"name": "Atari VCS", "slug": "atari-vcs"},
    ]

    assert smoke.match_rows(rows, "atariinc") == [rows[0]]
    assert smoke.match_rows(rows, "Atari") == rows
    assert smoke.match_rows(rows, "missing") == []


def test_the_tool_refuses_to_write_state_into_live_data() -> None:
    import pytest

    with pytest.raises(SystemExit, match="live data"):
        smoke._resolve_output_dir(str(_ROOT / "data" / "scratch"), "greenhouse", {})
