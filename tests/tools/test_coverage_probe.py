"""Live-probe verdicts: what counts as evidence that a role is still open.

The obvious check is wrong. A job's detail page usually stays fetchable long after
the posting closes, so "the URL still resolves" would mark every delisted role as
live. These tests pin the evidence rule that replaces it, plus the two ways the
probe produced confidently wrong answers before: merging tenants that share an ATS
host, and flipping verdicts on a flaky fetch.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "coverage_probe", _ROOT / "tools" / "coverage_probe.py"
)
assert _spec and _spec.loader
probe_mod: ModuleType = importlib.util.module_from_spec(_spec)
sys.modules["coverage_probe"] = probe_mod
_spec.loader.exec_module(probe_mod)


# --- Board identity -------------------------------------------------------


def test_multi_tenant_hosts_do_not_merge_into_one_board() -> None:
    """jobs.smartrecruiters.com carries unrelated companies side by side.

    Grouping by host made the probe fetch only CDPROJEKTRED's list API and report
    Yggdrasil's role as absent from a listing that was never retrieved.
    """
    cdpr = "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000130773989"
    ygg = "https://jobs.smartrecruiters.com/YggdrasilSandbox/744000144765969"
    assert probe_mod.board_key(cdpr) != probe_mod.board_key(ygg)


def test_same_tenant_two_roles_share_one_board() -> None:
    a = "https://job-boards.greenhouse.io/2kczech/jobs/7533146003"
    b = "https://job-boards.greenhouse.io/2kczech/jobs/7533146004"
    assert probe_mod.board_key(a) == probe_mod.board_key(b)


def test_greenhouse_tenants_are_separate_boards() -> None:
    a = "https://job-boards.greenhouse.io/2kczech/jobs/7533146003"
    b = "https://job-boards.greenhouse.io/hangar13/jobs/7527472003"
    assert probe_mod.board_key(a) != probe_mod.board_key(b)


def test_plain_career_pages_group_by_host_root() -> None:
    """HTML boards expose no list API, so one fetch per host must cover all roles."""
    a = "https://www.4a-games.com.mt/senior-technical-artist"
    b = "https://www.4a-games.com.mt/lead-technical-artist-new-ip"
    assert probe_mod.board_key(a) == probe_mod.board_key(b) == "https://www.4a-games.com.mt/"


def test_group_jobs_keeps_different_boards_apart() -> None:
    grouped = probe_mod.group_jobs(
        [
            {"sourceUrl": "https://jobs.smartrecruiters.com/CDPROJEKTRED/1", "title": "a"},
            {"sourceUrl": "https://jobs.smartrecruiters.com/YggdrasilSandbox/2", "title": "b"},
            {"sourceUrl": "https://jobs.smartrecruiters.com/CDPROJEKTRED/3", "title": "c"},
        ]
    )
    assert len(grouped) == 2
    assert sorted(len(v) for v in grouped.values()) == [1, 2]


def test_jobs_without_a_url_are_skipped() -> None:
    assert probe_mod.group_jobs([{"sourceUrl": "", "title": "x"}]) == {}


# --- List API derivation --------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (
            "https://11bitstudios.recruitee.com/o/x/c/new",
            "https://11bitstudios.recruitee.com/api/offers/",
        ),
        (
            "https://job-boards.greenhouse.io/2kczech/jobs/1",
            "https://boards-api.greenhouse.io/v1/boards/2kczech/jobs?content=true",
        ),
        ("https://jobs.lever.co/larian/abc", "https://api.lever.co/v0/postings/larian?mode=json"),
        (
            "https://jobs.ashbyhq.com/voodoo/13968523",
            "https://api.ashbyhq.com/posting-api/job-board/voodoo",
        ),
        (
            "https://jobs.smartrecruiters.com/YggdrasilSandbox/744",
            "https://api.smartrecruiters.com/v1/companies/YggdrasilSandbox/postings?limit=100",
        ),
        (
            "https://hologate-gmbh.jobs.personio.com/job/2705979",
            "https://hologate-gmbh.jobs.personio.com/xml",
        ),
    ],
)
def test_known_vendors_expose_a_list_api(url: str, expected: str) -> None:
    assert expected in probe_mod.list_api_candidates(url)


def test_unknown_vendor_has_no_list_api() -> None:
    assert (
        probe_mod.list_api_candidates("https://www.4a-games.com.mt/senior-technical-artist") == []
    )


def test_board_root_strips_the_job_path() -> None:
    assert (
        probe_mod.board_root("https://www.playtika.com/position/12345")
        == "https://www.playtika.com/"
    )


# --- Title matching -------------------------------------------------------


def test_full_title_variant_is_always_offered() -> None:
    assert probe_mod.normalize_token("Senior Technical Artist") in probe_mod.title_variants(
        "Senior Technical Artist"
    )


def test_qualifier_suffix_falls_back_to_the_distinctive_head() -> None:
    # Boards append noise; a whole-title compare alone would call these delisted.
    variants = probe_mod.title_variants("Technical Artist - Shaders")
    assert probe_mod.normalize_token("technicalartistshaders") in variants


def test_short_heads_are_rejected_as_too_generic() -> None:
    # "Senior - something" must not reduce to the token "senior".
    assert probe_mod.normalize_token("senior") not in probe_mod.title_variants("Senior - Something")


# --- Verdicts -------------------------------------------------------------


def _job(**over: object) -> dict[str, object]:
    row: dict[str, object] = {
        "title": "Technical Artist",
        "company": "Example",
        "ats": "greenhouse",
        "bucket": "studio_covered_role_absent",
        "sourceUrl": "https://job-boards.greenhouse.io/example/jobs/1",
    }
    row.update(over)
    return row


def _board(**over: object) -> dict[str, object]:
    board: dict[str, object] = {
        "listing_ok": True,
        "listing_text": "technicalartist otherrole",
        "detail_text": "technicalartist",
    }
    board.update(over)
    return board


def test_role_on_the_listing_is_live() -> None:
    result = probe_mod.probe_job(_job(), _board())
    assert result["verdict"] == probe_mod.VERDICT_LIVE


def test_role_absent_from_a_readable_listing_is_delisted() -> None:
    result = probe_mod.probe_job(
        _job(), _board(listing_text="soundsomethingelse", detail_text="gone")
    )
    assert result["verdict"] == probe_mod.VERDICT_DELISTED


def test_detail_page_alone_is_never_live() -> None:
    """The trap this whole tool exists to avoid.

    The role is still on the (fetchable) detail page but gone from the listing,
    which is what a closed posting looks like.
    """
    result = probe_mod.probe_job(
        _job(), _board(listing_text="soundsomethingelse", detail_text="technicalartist")
    )
    assert result["verdict"] == probe_mod.VERDICT_INCONCLUSIVE
    assert "outlive" in result["reason"]


def test_unreadable_board_is_inconclusive_not_delisted() -> None:
    """No listing means absence proves nothing, so never call it delisted."""
    result = probe_mod.probe_job(_job(), _board(listing_ok=False, listing_text=""))
    assert result["verdict"] == probe_mod.VERDICT_INCONCLUSIVE
    assert result["verdict"] != probe_mod.VERDICT_DELISTED


# --- Run-level guards -----------------------------------------------------


def test_report_aborts_when_the_known_good_control_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe_mod, "run_control", lambda: (False, "control -> HTTP 403"))
    result = probe_mod.probe_report({"misses": [{"sourceUrl": "https://x/", "title": "t"}]})
    assert result["aborted"] is True
    assert result["liveBoards"] == {}


def test_inconclusive_jobs_are_omitted_from_the_booleans_map() -> None:
    """Otherwise coverage_audit reads them as False and calls them delisted."""
    rows = [
        {"sourceUrl": "https://a/1", "verdict": probe_mod.VERDICT_LIVE},
        {"sourceUrl": "https://b/1", "verdict": probe_mod.VERDICT_DELISTED},
        {"sourceUrl": "https://c/1", "verdict": probe_mod.VERDICT_INCONCLUSIVE},
        {"sourceUrl": "https://d/1", "verdict": probe_mod.VERDICT_BOARD_DEAD},
    ]
    keep = {probe_mod.VERDICT_LIVE, probe_mod.VERDICT_DELISTED}
    live_boards = {
        r["sourceUrl"]: r["verdict"] == probe_mod.VERDICT_LIVE for r in rows if r["verdict"] in keep
    }
    assert set(live_boards) == {"https://a/1", "https://b/1"}
    assert live_boards["https://a/1"] is True
    assert live_boards["https://b/1"] is False


def test_client_errors_are_answers_and_are_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 404 is a verdict. Retrying it just wastes requests on every board."""
    calls: list[str] = []

    class _HTTPError(Exception):
        def __init__(self, code: int) -> None:
            self.code = code

    import urllib.error

    def _boom(request: object, timeout: float = 0.0, context: object = None) -> None:
        calls.append("x")
        raise urllib.error.HTTPError("u", 404, "nf", {}, None)  # type: ignore[arg-type]

    monkeypatch.setattr(probe_mod.urllib.request, "urlopen", _boom)
    status, _ = probe_mod.http_get("https://example/", attempts=3)
    assert status == 404
    assert len(calls) == 1


def test_transient_failures_are_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """Board roots flake; without retries the verdicts are not reproducible."""
    calls: list[int] = []

    class _Resp:
        status = 200

        def read(self) -> bytes:
            return b"technicalartist"

        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

    def _urlopen(request: object, timeout: float = 0.0, context: object = None) -> _Resp:
        calls.append(1)
        if len(calls) < 3:
            raise TimeoutError("flaky board root")
        return _Resp()

    monkeypatch.setattr(probe_mod.urllib.request, "urlopen", _urlopen)
    monkeypatch.setattr(probe_mod.time, "sleep", lambda _s: None)

    status, body = probe_mod.http_get("https://example/", attempts=3)
    assert status == 200
    assert "technicalartist" in body
    assert len(calls) == 3, "a transient failure must be retried, not returned"
