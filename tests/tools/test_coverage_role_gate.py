"""The DG role gate: a miss is only recoverable if the pipeline would keep it.

A raw miss count is not recoverable coverage. The 2026-10-06 re-measurement
sized 24 absent boards at 486 listings, of which 149 (31%) were game roles;
the other 337 were finance, marketing and support vacancies that mixed
companies post beside their games work. Every number sized off the raw count
inherits that inflation unless the split is made where the numbers are
produced: in the audit (GJI-title basis), in the sizing (per board) and in
verification (fetched-listing basis).

The predicate under test is the production row filter itself, so the gate
cannot drift away from the pipeline it claims to reproduce.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, _ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


audit_mod = _load("coverage_audit", "tools/coverage_audit.py")
boards_mod = _load("coverage_boards", "tools/coverage_boards.py")
verify_mod = _load("coverage_verify", "tools/coverage_verify.py")

# A registry row for some *other* board, so a miss on the board under test
# classifies as `unregistered_board` rather than falling through to `unknown`
# (an empty registry ids set makes every miss undecidable).
OTHER_BOARD_REGISTRY = {"static:listing_url:https://other.example"}


def _gji_row(title: str, company: str, url: str) -> dict[str, object]:
    return {"title": title, "company": company, "source_url": url, "country": "DE"}


def _audit(gji_rows: list[dict[str, object]]) -> Any:
    # The tool is loaded by path, so its return type is `Any`; the tests below
    # assert the shape of that payload.
    return audit_mod.audit(
        gji_rows,
        [],  # an empty feed matches nothing, so every GJI row is a miss
        region="EU",
        registry_ids=set(OTHER_BOARD_REGISTRY),
    )


# --- Audit: the GJI-title basis ------------------------------------------


def test_audit_tags_a_game_role_miss() -> None:
    result = _audit(
        [_gji_row("[Voodoo] Senior Game Designer", "Voodoo", "https://voodoo.com/jobs/1")]
    )
    assert result["misses"][0]["gameRole"] is True
    assert result["gjiMissingGameRoles"] == 1
    assert result["gjiMissingNonGameRoles"] == 0


def test_audit_tags_a_non_game_role_miss() -> None:
    # Kambi's actual misses: its betting business, not its games work.
    result = _audit([_gji_row("Compliance Manager", "Kambi", "https://kambi.com/jobs/1")])
    assert result["misses"][0]["gameRole"] is False
    assert result["gjiMissingGameRoles"] == 0
    assert result["gjiMissingNonGameRoles"] == 1


def test_the_split_sums_to_the_miss_total() -> None:
    result = _audit(
        [
            _gji_row("Gameplay Engineer", "Studio", "https://studio.example/jobs/1"),
            _gji_row("Marketing Manager", "Studio", "https://studio.example/jobs/2"),
            _gji_row("Technical Artist", "Studio", "https://studio.example/jobs/3"),
        ]
    )
    assert result["gjiMissing"] == 3
    assert result["gjiMissingGameRoles"] == 2
    assert result["gjiMissingNonGameRoles"] == 1


def test_company_text_is_consulted_like_the_pipeline_does() -> None:
    # The production filter reads title *and* company, so a business title at
    # an employer whose name asserts game scope is kept -- and so is the gate.
    result = _audit(
        [_gji_row("Project Manager", "Game Studios", "https://gamestudios.example/jobs/1")]
    )
    assert result["misses"][0]["gameRole"] is True


def test_the_gate_never_admits_what_the_pipeline_would_drop() -> None:
    # Monotonicity: the pipeline also reads tags, which GJI does not publish,
    # so the gate's game-role set must be a subset of the pipeline's keep set.
    # Every keyword the gate can see is one the pipeline can see too.
    from src.jobs.game_detection import GAME_ROW_KEYWORDS, looks_like_game_job

    for keyword in GAME_ROW_KEYWORDS:
        assert looks_like_game_job(keyword, "Unrelated Employer")
        assert looks_like_game_job(f"Lead {keyword}", "Unrelated Employer")


# --- Boards: the sizing basis --------------------------------------------


def _miss(url: str, title: str, game_role: bool | None) -> dict[str, object]:
    row: dict[str, object] = {
        "sourceUrl": url,
        "company": "Studio",
        "title": title,
    }
    if game_role is not None:
        row["gameRole"] = game_role
    return row


def test_candidates_split_their_misses_by_role() -> None:
    candidates = boards_mod.collect_candidates(
        [
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/1", "Game Designer", True),
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/2", "Accountant", False),
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/3", "Animator", True),
        ],
        set(),
    )
    assert len(candidates) == 1
    row = candidates[0]
    assert row["missingCount"] == 3
    assert row["gameRoleCount"] == 2
    assert row["nonGameRoleCount"] == 1


def test_untagged_misses_count_in_the_total_and_in_neither_split() -> None:
    """A report written before the gate carries no tag; guessing one would
    manufacture a split that was never measured."""
    candidates = boards_mod.collect_candidates(
        [_miss("https://job-boards.greenhouse.io/voodoo/jobs/1", "Game Designer", None)],
        set(),
    )
    assert candidates[0]["missingCount"] == 1
    assert candidates[0]["gameRoleCount"] == 0
    assert candidates[0]["nonGameRoleCount"] == 0
    summary = boards_mod.summarise(candidates)
    assert summary["openingsRecoverable"] == 1
    assert summary["openingsRecoverableGameRoles"] == 0
    assert summary["openingsRecoverableNonGameRoles"] == 0


def test_summarise_reports_the_split_alongside_the_total() -> None:
    candidates = boards_mod.collect_candidates(
        [
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/1", "Game Designer", True),
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/2", "Animator", True),
            _miss("https://job-boards.greenhouse.io/voodoo/jobs/3", "Head of Tax", False),
            _miss("https://jobs.ashbyhq.com/i3d/xyz", "Data Center Lead", False),
            _miss("https://jobs.ashbyhq.com/i3d/abc", "Gameplay Engineer", True),
        ],
        set(),
    )
    summary = boards_mod.summarise(candidates)
    assert summary["openingsRecoverable"] == 5
    assert summary["openingsRecoverableGameRoles"] == 3
    assert summary["openingsRecoverableNonGameRoles"] == 2


# --- Verify: the fetched-listing basis ------------------------------------


def test_verify_counts_game_roles_in_a_greenhouse_payload() -> None:
    payload = json.dumps(
        {
            "jobs": [
                {"id": 1, "title": "Senior Game Designer", "absolute_url": "https://x/1"},
                {"id": 2, "title": "Head of Tax", "absolute_url": "https://x/2"},
                {"id": 3, "title": "Environment Artist", "absolute_url": "https://x/3"},
            ]
        }
    )
    assert verify_mod.game_role_rows(payload) == 2


def test_game_roles_never_exceed_the_row_count() -> None:
    payload = json.dumps(
        {"postings": [{"title": "Gameplay Engineer"}, {"title": "Logistics Director"}]}
    )
    rows, _anchors = verify_mod.count_rows(payload)
    assert rows == 2
    assert verify_mod.game_role_rows(payload) == 1


def test_company_and_tags_are_consulted_like_the_production_filter() -> None:
    # The JSON parsers call looks_like_game_job(title, company, tags), so a
    # generic title at a game-named employer, or a title whose game-ness
    # lives in its tags, is kept -- and the gate keeps it too.
    payload = json.dumps(
        {
            "jobs": [
                {"title": "Designer", "company_name": "Game Studios Inc."},
                {"title": "Specialist", "tags": ["game design", "unreal"]},
                {"title": "Specialist", "tags": ["finance", "audit"]},
            ]
        }
    )
    assert verify_mod.game_role_rows(payload) == 2


def test_verify_game_role_rows_is_none_for_html() -> None:
    # HTML boards carry no generic title field a GET can read; None is the
    # honest answer, and it must not be folded into 0 (which would file a
    # working JS-shell board as serving no game roles).
    assert verify_mod.game_role_rows("<html><body>maintenance</body></html>") is None


def test_verify_game_role_rows_is_none_for_an_unrecognised_envelope() -> None:
    # The Workday SPA redirect stub: a non-empty dict with no listing keys.
    assert verify_mod.game_role_rows({"widget": "redirect", "externalSpa": True}) is None


def test_verify_game_role_rows_empty_listing_is_zero_not_none() -> None:
    assert verify_mod.game_role_rows({"jobs": []}) == 0


def test_verify_game_role_rows_reads_the_parsed_cxs_shape() -> None:
    # Workday's branch holds an already-parsed payload, not response text.
    payload = {"total": 2, "jobPostings": [{"title": "Game Designer"}, {"title": "Recruiter"}]}
    assert verify_mod.game_role_rows(payload) == 1


def test_verify_candidate_reports_game_roles_for_a_json_board(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps({"jobs": [{"title": "Game Designer"}, {"title": "Accountant"}]})
    monkeypatch.setattr(verify_mod.probe, "http_get", lambda *a, **k: (200, payload))
    result = verify_mod.verify_candidate(
        {
            "id": "greenhouse:slug:voodoo",
            "adapter": "greenhouse",
            "slug": "voodoo",
        }
    )
    assert result["verdict"] == verify_mod.VERDICT_COLLECTS
    assert result["rows"] == 2
    assert result["gameRoles"] == 1
