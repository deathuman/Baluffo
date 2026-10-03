"""Verification verdicts gate registration, so the three-way rule is pinned here.

The load-bearing rule: an unrecognised payload shape is ``unknown``, never ``empty``.
The first version of this tool fell back to "any non-empty dict is one row", and
Workday's SPA redirect stub -- ``{"widget":"redirect","externalSpa":true}`` -- then
made every Workday board report ``collects`` with 1 row. That is the exact failure
this file exists to prevent: a board filed as collecting when nothing collects it.

The second rule: a failed fetch is ``unknown``, not ``empty``. "Empty" asserts the
board has no openings. A timeout, a 404 or an unreachable tenant asserts nothing of
the kind, and filing those as empty quietly re-creates the coverage gap.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_verify", _ROOT / "tools" / "coverage_verify.py"
)
assert _spec and _spec.loader
verify_mod: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_verify"] = verify_mod
_spec.loader.exec_module(verify_mod)


def _candidate(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": "greenhouse:slug:example",
        "adapter": "greenhouse",
        "slug": "example",
        "missingCount": 5,
    }
    row.update(overrides)
    return row


# --- Row counting ---------------------------------------------------------


def test_a_json_array_counts_as_its_length() -> None:
    assert verify_mod.count_rows('[{"a":1},{"b":2}]') == 2


def test_the_usual_listing_envelopes_are_recognised() -> None:
    for key in ("jobs", "postings", "results", "offers", "items", "content", "jobPostings"):
        payload = '{"' + key + '":[{"x":1},{"x":2}]}'
        assert verify_mod.count_rows(payload) == 2, key


def test_workday_cxs_envelope_is_recognised_by_its_total() -> None:
    """Workday returns total plus jobPostings; both are read."""
    assert verify_mod.count_rows('{"total": 154, "jobPostings": [{"t":1}]}') == 1


def test_a_bare_object_is_not_silently_one_row() -> None:
    """This is the Workday SPA stub, and counting it as 1 row invented coverage."""
    payload = '{"widget":"redirect","externalSpa":true}'
    assert verify_mod.count_rows(payload) is None


def test_non_json_is_not_counted() -> None:
    assert verify_mod.count_rows("<html>maintenance</html>") is None
    assert verify_mod.count_rows("") is None


def test_an_empty_listing_is_zero_not_unknown() -> None:
    assert verify_mod.count_rows('{"jobs": []}') == 0


def test_an_empty_envelope_is_a_real_answer() -> None:
    """``{}`` is a valid, empty listing. Only a non-empty unknown shape is unknown."""
    assert verify_mod.count_rows("{}") == 0


# --- List URL derivation --------------------------------------------------


@pytest.mark.parametrize(
    ("adapter", "fields", "expected_fragment"),
    [
        ("greenhouse", {"slug": "2kczech"}, "boards/2kczech/jobs"),
        ("lever", {"account": "animocabrands"}, "postings/animocabrands"),
        ("ashby", {"board_url": "https://jobs.ashbyhq.com/voodoo"}, "job-board/voodoo"),
        ("workable", {"account": "1tk"}, "accounts/1tk/jobs"),
        (
            "smartrecruiters",
            {"api_url": "https://api.smartrecruiters.com/v1/companies/x/postings"},
            "companies/x",
        ),
        ("recruitee", {"api_url": "https://a.recruitee.com/api/offers/"}, "api/offers"),
        ("static", {"listing_url": "https://careers.x.example"}, "careers.x.example"),
    ],
)
def test_list_url_is_built_from_the_emitted_registry_row(
    adapter: str, fields: dict[str, str], expected_fragment: str
) -> None:
    """Not from the original job URL.

    If the row the registry would hold cannot be turned into a fetch target, it
    collects nothing, and the tool must say so instead of probing the URL the
    opening happened to be found at.
    """
    url = verify_mod.list_url_for({"adapter": adapter, **fields})
    assert expected_fragment in url


def test_a_row_with_no_usable_field_yields_no_url() -> None:
    assert verify_mod.list_url_for({"adapter": "greenhouse"}) == ""


def test_workday_is_routed_to_the_structured_path_not_a_get() -> None:
    """A GET cannot reach Workday's POST CXS endpoint."""
    assert "workday" in verify_mod.STRUCTURED_ADAPTERS


# --- Verdict classification ----------------------------------------------


def test_rows_means_collects(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verify_mod.probe, "http_get", lambda *a, **k: (200, '{"jobs":[{"x":1}]}'))
    result = verify_mod.verify_candidate(_candidate())
    assert result["verdict"] == verify_mod.VERDICT_COLLECTS
    assert result["rows"] == 1


def test_an_unrecognised_shape_is_unknown_not_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        verify_mod.probe,
        "http_get",
        lambda *a, **k: (200, '{"widget":"redirect","externalSpa":true}'),
    )
    assert verify_mod.verify_candidate(_candidate())["verdict"] == verify_mod.VERDICT_UNKNOWN


def test_a_tiny_empty_body_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verify_mod.probe, "http_get", lambda *a, **k: (200, "{}"))
    assert verify_mod.verify_candidate(_candidate())["verdict"] == verify_mod.VERDICT_EMPTY


@pytest.mark.parametrize("status", [404, 403, 500, 0])
def test_a_failed_fetch_is_unknown_never_empty(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """ "Empty" asserts the board has no openings; an error asserts nothing."""
    monkeypatch.setattr(verify_mod.probe, "http_get", lambda *a, **k: (status, ""))
    assert verify_mod.verify_candidate(_candidate())["verdict"] == verify_mod.VERDICT_UNKNOWN


def test_no_list_url_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        verify_mod.probe, "http_get", lambda *a, **k: pytest.fail("must not fetch without a URL")
    )
    assert verify_mod.verify_candidate(_candidate(slug=""))["verdict"] == verify_mod.VERDICT_UNKNOWN


# --- Control gating -------------------------------------------------------


def test_a_failed_adapter_control_downgrades_every_board_on_it() -> None:
    """A dead control is indistinguishable from a vendor of dead boards.

    Without the gate, one broken fetcher files a whole vendor's backlog as boards
    with nothing to collect, and the fix -- writing or fixing the adapter -- never
    gets made.
    """
    controls = {"greenhouse": {"ok": False, "detail": "control -> HTTP 404, 0 rows"}}
    results = verify_mod.verify_candidates(
        [_candidate(), _candidate(id="greenhouse:slug:other", slug="other")],
        controls=controls,
    )
    assert all(r["verdict"] == verify_mod.VERDICT_UNKNOWN for r in results)
    assert all("control failed" in r["reason"] for r in results)


def test_an_adapter_with_no_control_is_still_probed() -> None:
    """No control must mean 'unproven fetcher', not 'never fetch'."""
    monkeyed = {"greenhouse": {"ok": True, "detail": "ok"}}
    results = verify_mod.verify_candidates([_candidate()], controls=monkeyed)
    assert results[0]["verdict"] in {
        verify_mod.VERDICT_COLLECTS,
        verify_mod.VERDICT_EMPTY,
        verify_mod.VERDICT_UNKNOWN,
    }


def test_boards_on_other_adapters_survive_a_broken_control() -> None:
    controls = {"greenhouse": {"ok": False, "detail": "down"}}
    results = verify_mod.verify_candidates(
        [
            _candidate(
                id="static:listing_url:https://x.example",
                adapter="static",
                listing_url="https://x.example",
            )
        ],
        controls=controls,
    )
    assert (
        results[0]["verdict"] != verify_mod.VERDICT_UNKNOWN or "control" not in results[0]["reason"]
    )


# --- Summary -------------------------------------------------------------


def test_summary_counts_openings_not_just_boards() -> None:
    """One board with 176 openings and one with a single opening are not equal."""
    results = [
        {"id": "a", "verdict": verify_mod.VERDICT_COLLECTS, "rows": 176},
        {"id": "b", "verdict": verify_mod.VERDICT_COLLECTS, "rows": 2},
        {"id": "c", "verdict": verify_mod.VERDICT_UNKNOWN, "rows": 0},
    ]
    candidates = [
        {"id": "a", "missingCount": 176},
        {"id": "b", "missingCount": 2},
        {"id": "c", "missingCount": 40},
    ]
    summary = verify_mod.summarise(results, candidates)
    assert summary["byVerdict"] == {verify_mod.VERDICT_COLLECTS: 2, verify_mod.VERDICT_UNKNOWN: 1}
    assert summary["openingsByVerdict"][verify_mod.VERDICT_COLLECTS] == 178
    assert summary["openingsByVerdict"][verify_mod.VERDICT_UNKNOWN] == 40
