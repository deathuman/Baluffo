"""The retirement tool: evidence in, a plan out, and the control decides.

The tool is the only place that reads a fetch report, classifies its refused redirects and
proposes retirements; the gate itself lives in `transition_registry_to_retired`, so these
tests cover the parts the gate cannot see -- the pair parsing, the terminal-only selection,
the host+tenant row matching and the probe's judgement (including the "unproven is not
gone" cases).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_retire", _ROOT / "tools" / "coverage_retire.py"
)
assert _spec and _spec.loader
retire: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_retire"] = retire
_spec.loader.exec_module(retire)


# --- the report's refusal pairs --------------------------------------------------------


def test_extract_redirect_pairs_reads_the_refusal_text() -> None:
    report = {
        "sources": [
            {
                "error": "x: Unsafe static redirect from https://a.example/ to https://b.example/; tail"
            },
            {"error": ""},
            {"error": "Unsafe static redirect from https://c.example/ to https://d.example/"},
        ]
    }
    assert retire.extract_redirect_pairs(report) == [
        ("https://a.example/", "https://b.example/"),
        ("https://c.example/", "https://d.example/"),
    ]


def test_only_site_gone_or_moved_is_terminal() -> None:
    pairs = [
        ("https://www.exozet.com", "https://www.endava.com"),
        ("https://www.bungie.net", "https://careers.bungie.com"),
        ("https://acme.example/jobs", "https://boards.greenhouse.io/acme"),
        ("https://www.ninjatheory.com/careers", "http://www.ninjatheory.com/careers/"),
    ]
    assert list(retire.classified_terminal_sources(pairs)) == ["https://www.exozet.com"]


# --- row matching ----------------------------------------------------------------------


def test_rows_match_by_host_never_by_label() -> None:
    rows = [
        {"id": "static:listing_url:https://dead.example/jobs", "studio": "Same Studio"},
        {"id": "static:listing_url:https://alive.example/jobs", "studio": "Same Studio"},
    ]
    matched = retire.match_registry_rows(rows, "https://dead.example/jobs")
    assert [row["id"] for row in matched] == ["static:listing_url:https://dead.example/jobs"]


def test_an_exact_listing_url_beats_a_sibling_row_on_the_host() -> None:
    rows = [
        {"id": "static:listing_url:https://dead.example/other", "studio": "Studio"},
        {"id": "static:listing_url:https://dead.example/jobs", "studio": "Studio"},
    ]
    matched = retire.match_registry_rows(rows, "https://dead.example/jobs")
    assert [row["id"] for row in matched] == ["static:listing_url:https://dead.example/jobs"]


# --- the probe -------------------------------------------------------------------------


def _probe(monkeypatch: pytest.MonkeyPatch, outcomes: dict[str, dict[str, object]]) -> None:
    monkeypatch.setattr(retire, "probe_root", lambda url, timeout: outcomes[url])


def test_a_serving_root_is_not_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    _probe(
        monkeypatch,
        {
            "https://dead.example/": {
                "ok": True,
                "status": 200,
                "final_url": "https://dead.example/",
            }
        },
    )
    terminal, _probe_result = retire.root_is_terminal("https://dead.example/jobs", timeout=1)
    assert terminal is False


def test_a_404_root_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    _probe(
        monkeypatch,
        {
            "https://dead.example/": {
                "ok": False,
                "status": 404,
                "final_url": "https://dead.example/",
                "error": "HTTP 404",
            }
        },
    )
    terminal, _ = retire.root_is_terminal("https://dead.example/jobs", timeout=1)
    assert terminal is True


def test_a_403_root_is_unproven_not_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A challenge is not a closure; the D-work lesson applies to the probe too."""
    _probe(
        monkeypatch,
        {
            "https://dead.example/": {
                "ok": False,
                "status": 403,
                "final_url": "https://dead.example/",
                "error": "HTTP 403",
            }
        },
    )
    terminal, _ = retire.root_is_terminal("https://dead.example/jobs", timeout=1)
    assert terminal is False


def test_an_unreachable_root_on_both_schemes_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    _probe(
        monkeypatch,
        {
            "https://dead.example/": {
                "ok": False,
                "status": 0,
                "final_url": "",
                "error": "URLError",
            },
            "http://dead.example/": {
                "ok": False,
                "status": 0,
                "final_url": "",
                "error": "URLError",
            },
        },
    )
    terminal, _ = retire.root_is_terminal("https://dead.example/jobs", timeout=1)
    assert terminal is True


def test_a_cross_site_gone_redirect_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    _probe(
        monkeypatch,
        {
            "https://dead.example/": {
                "ok": True,
                "status": 200,
                "final_url": "https://www.endava.com/",
            }
        },
    )
    terminal, _ = retire.root_is_terminal("https://dead.example/", timeout=1)
    assert terminal is True


# --- the control decides ---------------------------------------------------------------


def test_prepare_retirements_refuses_when_the_control_failed() -> None:
    plan = {
        "control": {"ok": False},
        "retired": [
            {
                "row": {"id": "static:listing_url:https://dead.example", "adapter": "static"},
                "classification": "site_gone_or_moved",
                "probe": {"terminal": True},
            }
        ],
    }
    with pytest.raises(ValueError, match="control"):
        retire.prepare_retirements(plan)


def test_build_plan_retires_only_the_terminal_matched_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = {
        "sources": [
            {
                "error": "Unsafe static redirect from https://www.exozet.com to https://www.endava.com"
            },
            {
                "error": "Unsafe static redirect from https://www.bungie.net to https://careers.bungie.com"
            },
        ]
    }
    registry = [
        {"id": "static:listing_url:https://www.exozet.com", "adapter": "static"},
        {"id": "static:listing_url:https://www.bungie.net/jobs", "adapter": "static"},
    ]
    report_path = tmp_path / "report.json"
    registry_path = tmp_path / "registry.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    registry_path.write_text(json.dumps(registry), encoding="utf-8")

    monkeypatch.setattr(
        retire, "_control_probe", lambda **_: {"ok": True, "root": "control", "probe": {}}
    )
    monkeypatch.setattr(
        retire, "root_is_terminal", lambda source_url, **_: (True, {"terminal": True})
    )

    plan = retire.build_plan(report_path, registry_path, timeout=1)
    assert [item["row"]["id"] for item in plan["retired"]] == [
        "static:listing_url:https://www.exozet.com"
    ]
    assert plan["control"]["ok"] is True
    prepared = retire.prepare_retirements(plan)
    assert prepared[0]["registryState"] == "rejected"
