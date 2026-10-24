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


def _summary(**overrides: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "run1": {"status": "ok", "httpStatus": 200, "httpEtag": '"v1"'},
        "run2": {"urls": [], "httpStatus": 304, "exclusionReason": "cache_not_modified_304"},
        "decision": {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "provider_stable_revalidate",
        },
    }
    summary.update(overrides)
    return summary


def test_classification_separates_an_ignored_probe_from_a_refused_one() -> None:
    ignored = smoke.classify_outcome(_summary(), [{"statusCode": 200, "notModified": False}])
    refused = smoke.classify_outcome(_summary(), [{"statusCode": 405, "notModified": False}])
    honoured = smoke.classify_outcome(_summary(), [{"statusCode": 304, "notModified": True}])

    assert ignored["outcome"] == smoke.OUTCOME_IGNORES
    assert "200" in ignored["reason"]
    assert refused["outcome"] == smoke.OUTCOME_PROBE_REFUSED, "405 is not evidence about the tag"
    assert "405" in refused["reason"]
    assert honoured == {"outcome": smoke.OUTCOME_HONOURS, "reason": ""}


def test_classification_never_blames_a_feed_that_was_not_asked() -> None:
    no_probe = smoke.classify_outcome(_summary(), [])
    no_validator = smoke.classify_outcome(_summary(run1={"status": "ok", "httpStatus": 200}), [])
    read_failed = smoke.classify_outcome(
        _summary(run1={"status": "error", "httpStatus": 200, "error": "no jobs extracted"}), []
    )
    inconclusive = smoke.classify_outcome(_summary(decision={}), [])
    empty_window = smoke.classify_outcome(
        _summary(
            run1={"status": "ok", "httpStatus": 200, "httpEtag": '"v1"', "kept": 0},
            decision={
                "cacheDecision": "skip_fresh",
                "cacheDecisionReason": "within_freshness_window",
            },
        ),
        [],
    )

    assert no_probe["outcome"] == smoke.OUTCOME_NO_PROBE
    assert no_validator["outcome"] == smoke.OUTCOME_NO_VALIDATOR
    assert read_failed["outcome"] == smoke.OUTCOME_READ_FAILED
    assert "no jobs extracted" in read_failed["reason"], (
        "a 200 that parsed nothing is not unreachable"
    )
    assert inconclusive["outcome"] == smoke.OUTCOME_INCONCLUSIVE
    assert "empty-source window" in empty_window["reason"], (
        "a zero-kept skip is not a lost validator"
    )


def test_the_probe_recorder_keeps_the_answer_a_report_would_overwrite() -> None:
    from src.jobs.adapters.plugins.provider_api import greenhouse_runner

    original = greenhouse_runner.provider_revalidate_not_modified
    report: dict[str, Any] = {"name": NAME, "status": "ok", "cacheDecision": "revalidate_only"}
    with smoke.record_provider_probes() as recorder:
        handled = greenhouse_runner.provider_revalidate_not_modified(
            entry_report=report,
            url=URL,
            timeout_s=1,
            source_name=NAME,
            source_state_rows={NAME: {"lastHttpEtag": '"v1"'}},
            revalidate_url=lambda *_args, **_kwargs: {
                "statusCode": 405,
                "notModified": False,
                "etag": "",
                "lastModified": "",
            },
        )

    assert handled is False
    assert recorder.probes == [
        {"source": NAME, "url": URL, "statusCode": 405, "notModified": False}
    ]
    assert greenhouse_runner.provider_revalidate_not_modified is original


class _SweepLoader:
    """One injected feed per run, behaving as its row asks and probing like a runner."""

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
        from src.jobs.adapters.plugins.provider_api import greenhouse_runner

        row = greenhouse_runner.registry_entries("greenhouse")[0]
        name = row["name"]
        behaviour = row["behaviour"]
        if force_refresh_all:
            failed = behaviour == "error"
            _publish(
                _detail(
                    name=name,
                    status="error" if failed else "ok",
                    error="boom" if failed else "",
                    httpStatus=0 if failed else 200,
                    httpEtag="" if failed or behaviour == "no-validator" else '"v1"',
                )
            )
            return [] if failed else [{"title": "one"}]
        if behaviour in {"honour", "ignore", "refuse"}:
            code = {"honour": 304, "ignore": 200, "refuse": 405}[behaviour]
            probe_report: dict[str, Any] = {
                "name": name,
                "status": "ok",
                "cacheDecision": "revalidate_only",
            }
            greenhouse_runner.provider_revalidate_not_modified(
                entry_report=probe_report,
                url=URL,
                timeout_s=timeout_s,
                source_name=name,
                source_state_rows=source_state_rows,
                revalidate_url=lambda *_args, **_kwargs: {
                    "statusCode": code,
                    "notModified": code == 304,
                    "etag": '"v1"' if code == 200 else "",
                    "lastModified": "",
                },
            )
            if code == 304:
                _publish(
                    _detail(
                        name=name,
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
        fetch_text(URL, timeout_s)
        _publish(
            _detail(
                name=name,
                fetchedCount=1,
                keptCount=1,
                httpStatus=200,
                cacheDecision="revalidate_only",
            )
        )
        return [{"title": "one"}]


def test_a_sweep_classifies_every_feed_and_names_the_offenders(tmp_path: Path) -> None:
    from src.jobs.adapters.plugins.provider_api import greenhouse_runner

    original_registry = greenhouse_runner.registry_entries
    rows = [
        {"name": "Honest", "slug": "honest", "behaviour": "honour"},
        {"name": "Ignorer", "slug": "ignorer", "behaviour": "ignore"},
        {"name": "Refuser", "slug": "refuser", "behaviour": "refuse"},
        {"name": "Silent", "slug": "silent", "behaviour": "no-probe"},
        {"name": "Empty", "slug": "empty", "behaviour": "no-validator"},
        {"name": "Broken", "slug": "broken", "behaviour": "error"},
    ]
    try:
        sweep = smoke.sweep_adapter(
            adapter="greenhouse",
            rows=rows,
            loader_name=LOADER,
            loader=_SweepLoader(),
            fetch_text=lambda url, timeout, request=None: "body",
            output_root=tmp_path / "sweep",
            age_minutes=30,
        )
    finally:
        smoke.diagnostics.SOURCE_DIAGNOSTICS.pop(LOADER, None)

    assert {result["source"]: result["outcome"] for result in sweep["feeds"]} == {
        "Honest": smoke.OUTCOME_HONOURS,
        "Ignorer": smoke.OUTCOME_IGNORES,
        "Refuser": smoke.OUTCOME_PROBE_REFUSED,
        "Silent": smoke.OUTCOME_NO_PROBE,
        "Empty": smoke.OUTCOME_NO_VALIDATOR,
        "Broken": smoke.OUTCOME_READ_FAILED,
    }
    assert sweep["offenders"] == ["Ignorer"]
    assert sweep["ok"] is False
    assert sweep["counts"][smoke.OUTCOME_HONOURS] == 1
    assert (tmp_path / "sweep" / "ignorer" / "summary.json").exists()
    assert greenhouse_runner.registry_entries is original_registry, (
        "the sweep puts the registry back"
    )
