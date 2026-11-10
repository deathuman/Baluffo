"""The source-refresh sweep: routes, repoints, retirements, and the write gate."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_source_refresh", _ROOT / "tools" / "coverage_source_refresh.py"
)
assert _spec and _spec.loader
refresh: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_source_refresh"] = refresh
_spec.loader.exec_module(refresh)


def _report(*rows: dict[str, Any]) -> dict[str, Any]:
    return {"sources": list(rows)}


def _static_row(
    url: str,
    *,
    name: str = "Studio",
    error: str = "",
    bucket: str = "",
) -> dict[str, Any]:
    source_id = f"static:listing_url:{url}"
    return {
        "name": f"static_source::{source_id}",
        "sourceId": source_id,
        "sourceUrl": url,
        "adapter": "static",
        "studio": name,
        "keptCount": 0,
        "failureBucket": bucket,
        "error": error,
    }


def _registry_row(url: str, *, adapter: str = "static") -> dict[str, Any]:
    source_id = f"static:listing_url:{url}" if adapter == "static" else adapter
    return {
        "id": source_id,
        "name": "Studio",
        "studio": "Studio",
        "adapter": adapter,
        "listing_url": url,
        "careersUrl": url,
        "registryState": "active",
        "enabledByDefault": True,
    }


def _probe(url: str, *, status: int = 200, final_url: str = "", body: str = "") -> dict[str, Any]:
    return {
        "ok": status == 200,
        "status": status,
        "finalUrl": final_url or url,
        "body": body,
        "error": "" if status == 200 else f"HTTP {status}",
    }


# --- ATS routing -----------------------------------------------------------------------


def test_ats_plan_reclassifies_zero_kept_signature_sources() -> None:
    url = "https://acme.example/careers"
    report = _report(
        _static_row(
            url,
            error=(
                "static:Studio:https://acme.example/careers: "
                "HTML contains greenhouse signature — consider adapter reclassification"
            ),
        )
    )
    registry = [_registry_row(url)]

    plan = refresh.build_ats_plan(
        report,
        registry,
        fetch_html=lambda _url, **_kwargs: _probe(
            url,
            final_url=url,
            body='<a href="https://job-boards.greenhouse.io/acme">roles</a>',
        ),
    )

    assert len(plan["actions"]) == 1
    action = plan["actions"][0]
    assert action["action"] == "reclassify"
    assert action["route"]["adapter"] == "greenhouse"
    assert action["route"]["fields"]["slug"] == "acme"


def test_ats_plan_reads_a_structured_route_without_fetching() -> None:
    url = "https://acme.example/careers"
    row = _static_row(url, error="HTML contains workday signature")
    row["atsRoute"] = {
        "adapter": "workday",
        "boardUrl": "https://acme.wd1.myworkdayjobs.com/en-US/External",
        "host": "acme.wd1.myworkdayjobs.com",
        "tenant": "acme",
        "fields": {"listing_url": "https://acme.wd1.myworkdayjobs.com/en-US/External"},
    }
    called = False

    def _fetch(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        raise AssertionError("structured route must not fetch")

    plan = refresh.build_ats_plan(_report(row), [_registry_row(url)], fetch_html=_fetch)

    assert len(plan["actions"]) == 1
    assert called is False


def test_ats_plan_skips_when_a_provider_row_already_covers_the_board() -> None:
    url = "https://acme.example/careers"
    report = _report(_static_row(url, error="HTML contains greenhouse signature"))
    registry = [
        _registry_row(url),
        {
            **_registry_row(url, adapter="greenhouse"),
            "id": "greenhouse:slug:acme",
            "slug": "acme",
        },
    ]

    plan = refresh.build_ats_plan(
        report,
        registry,
        fetch_html=lambda _url, **_kwargs: _probe(
            url,
            body='<a href="https://job-boards.greenhouse.io/acme">roles</a>',
        ),
    )

    assert plan["actions"] == []
    assert plan["skipped"][0]["why"] == "provider row already covers this board"


def test_ats_plan_skips_a_signature_without_a_tenant() -> None:
    url = "https://acme.example/careers"
    report = _report(_static_row(url, error="HTML contains workday signature"))

    plan = refresh.build_ats_plan(
        report,
        [_registry_row(url)],
        fetch_html=lambda _url, **_kwargs: _probe(url, body="myworkdayjobs"),
    )

    assert plan["actions"] == []
    assert "tenant" in plan["skipped"][0]["why"]


# --- site_changed sweep ----------------------------------------------------------------


def test_site_changed_plan_repoints_reclassifies_and_retires() -> None:
    moved = "https://moved.example/jobs"
    ats = "https://ats.example/careers"
    dead = "https://dead.example/jobs"
    report = _report(
        _static_row(moved, name="Moved", bucket="site_changed"),
        _static_row(ats, name="ATS", bucket="site_changed"),
        _static_row(dead, name="Dead", bucket="site_changed"),
    )
    registry = [_registry_row(url) for url in (moved, ats, dead)]

    def _fetch(url: str, **_kwargs: Any) -> dict[str, Any]:
        if url == moved:
            return _probe(url, final_url="https://moved.example/jobs-new")
        if url == ats:
            return _probe(
                url,
                final_url="https://job-boards.greenhouse.io/atsco",
                body="greenhouse.io",
            )
        return _probe(url, status=404)

    plan = refresh.build_site_changed_plan(
        report,
        registry,
        fetch_html=_fetch,
        root_is_terminal=lambda *_args, **_kwargs: (
            True,
            {"ok": False, "status": 404, "final_url": dead},
        ),
        control_probe=lambda **_kwargs: {"ok": True, "root": "control"},
    )

    by_source = {action["sourceId"]: action for action in plan["actions"]}
    assert by_source[f"static:listing_url:{moved}"]["action"] == "repoint"
    assert by_source[f"static:listing_url:{moved}"]["targetUrl"] == (
        "https://moved.example/jobs-new"
    )
    assert by_source[f"static:listing_url:{ats}"]["action"] == "reclassify"
    assert by_source[f"static:listing_url:{ats}"]["route"]["adapter"] == "greenhouse"
    assert by_source[f"static:listing_url:{dead}"]["action"] == "retire"
    assert by_source[f"static:listing_url:{dead}"]["probe"]["terminal"] is True
    assert by_source[f"static:listing_url:{dead}"]["probe"]["control_ok"] is True


def test_site_changed_plan_does_not_retire_without_a_passing_control() -> None:
    url = "https://dead.example/jobs"
    plan = refresh.build_site_changed_plan(
        _report(_static_row(url, bucket="site_changed")),
        [_registry_row(url)],
        fetch_html=lambda _url, **_kwargs: _probe(url, status=404),
        root_is_terminal=lambda *_args, **_kwargs: (
            True,
            {"ok": False, "status": 404, "final_url": url},
        ),
        control_probe=lambda **_kwargs: {"ok": False, "root": "control"},
    )

    assert plan["actions"] == []
    assert "control" in plan["skipped"][0]["why"]


def test_site_changed_plan_skips_an_unreadable_platform_migration() -> None:
    url = "https://acme.example/careers"
    plan = refresh.build_site_changed_plan(
        _report(_static_row(url, bucket="site_changed")),
        [_registry_row(url)],
        fetch_html=lambda _url, **_kwargs: _probe(
            url,
            final_url="https://job-boards.greenhouse.io",
        ),
        control_probe=lambda **_kwargs: {"ok": True, "root": "control"},
    )

    assert plan["actions"] == []
    assert "readable provider tenant" in plan["skipped"][0]["why"]


def test_a_404_body_cannot_reclassify_a_dead_listing() -> None:
    url = "https://dead.example/jobs"
    plan = refresh.build_site_changed_plan(
        _report(_static_row(url, bucket="site_changed")),
        [_registry_row(url)],
        fetch_html=lambda _url, **_kwargs: _probe(
            url,
            status=404,
            final_url="https://job-boards.greenhouse.io/acme",
            body="greenhouse.io",
        ),
        root_is_terminal=lambda *_args, **_kwargs: (
            False,
            {"ok": False, "status": 403, "final_url": url},
        ),
        control_probe=lambda **_kwargs: {"ok": True, "root": "control"},
    )

    assert plan["actions"] == []
    assert plan["skipped"][0]["why"].startswith("terminal listing")


def test_site_changed_plan_does_not_treat_a_live_zero_job_board_as_dead() -> None:
    url = "https://alive.example/jobs"
    plan = refresh.build_site_changed_plan(
        _report(_static_row(url, bucket="site_changed")),
        [_registry_row(url)],
        fetch_html=lambda _url, **_kwargs: _probe(url),
        control_probe=lambda **_kwargs: {"ok": True, "root": "control"},
    )

    assert plan["actions"] == []
    assert plan["skipped"][0]["why"] == (
        "listing still answers; zero jobs is not a retirement signal"
    )


# --- apply and read-back -----------------------------------------------------------------


def test_apply_reclassifies_and_repoints_with_a_backup(tmp_path: Path) -> None:
    moved = "https://moved.example/jobs"
    ats = "https://ats.example/careers"
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(
        json.dumps([_registry_row(moved), _registry_row(ats)]), encoding="utf-8"
    )
    plan = {
        "actions": [
            {
                "action": "repoint",
                "sourceId": f"static:listing_url:{moved}",
                "targetUrl": "https://moved.example/jobs-new",
            },
            {
                "action": "reclassify",
                "sourceId": f"static:listing_url:{ats}",
                "route": {
                    "adapter": "greenhouse",
                    "boardUrl": "https://job-boards.greenhouse.io/atsco",
                    "host": "job-boards.greenhouse.io",
                    "tenant": "atsco",
                    "fields": {"slug": "atsco"},
                },
            },
        ],
    }

    result = refresh.apply_plan(
        plan,
        registry_path,
        expect_rows=2,
        backup_dir=tmp_path / "backups",
        at="2026-11-10T00:00:00+00:00",
    )

    rows = json.loads(registry_path.read_text(encoding="utf-8"))
    by_id = {row["id"]: row for row in rows}
    assert by_id[f"static:listing_url:{moved}"]["listing_url"] == ("https://moved.example/jobs-new")
    assert by_id[f"static:listing_url:{ats}"]["adapter"] == "greenhouse"
    assert Path(result["backup"]).exists()


def test_apply_refuses_an_unexpected_row_count(tmp_path: Path) -> None:
    url = "https://moved.example/jobs"
    registry_path = tmp_path / "registry.json"
    original = json.dumps([_registry_row(url)], indent=2)
    registry_path.write_text(original, encoding="utf-8")
    plan = {
        "actions": [
            {
                "action": "repoint",
                "sourceId": f"static:listing_url:{url}",
                "targetUrl": "https://moved.example/new",
            }
        ]
    }

    with pytest.raises(SystemExit, match="expected 2"):
        refresh.apply_plan(
            plan,
            registry_path,
            expect_rows=2,
            backup_dir=tmp_path / "backups",
            at="2026-11-10T00:00:00+00:00",
        )

    assert registry_path.read_text(encoding="utf-8") == original


def test_apply_retires_only_with_terminal_evidence_and_control(tmp_path: Path) -> None:
    url = "https://dead.example/jobs"
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps([_registry_row(url)]), encoding="utf-8")
    plan = {
        "actions": [
            {
                "action": "retire",
                "sourceId": f"static:listing_url:{url}",
                "classification": "site_gone_or_moved",
                "probe": {"terminal": True, "control_ok": True},
            }
        ]
    }

    refresh.apply_plan(
        plan,
        registry_path,
        expect_rows=1,
        backup_dir=tmp_path / "backups",
        at="2026-11-10T00:00:00+00:00",
    )

    rows = json.loads(registry_path.read_text(encoding="utf-8"))
    assert rows[0]["registryState"] == "rejected"
