"""Regression: the title-hydration prefetch gate must match the row-loop gate.

Background (measured): `prefetch()` ran in a ThreadPoolExecutor, but its
candidate filter required URL title derivation to SUCCEED, while the row loop
calls `resolve_title()` exactly when derivation FAILS
(`canonicalize_google_sheets_title.py`). The two gates were inverted, so
prefetch warmed 0 feeds and every feed was fetched serially inside the row loop.

Measured on 1,500 rows: 142 serial fetches at 744 ms = 105,579 ms, which matched
the serial prediction within 0.1% while the parallel path would take ~6.6 s.
At full scale (30,970 rows) the fix took the source from 1,191,903 ms to
152,894 ms (7.8x) with identical output.

These tests pin the alignment so the inversion cannot silently return.
"""

from __future__ import annotations

from typing import Any

from src import jobs_fetcher as jf
from src.jobs.canonicalize_google_sheets_title import (
    _derive_google_sheets_title_from_url,
)
from src.jobs.canonicalize_redirects import (
    _google_sheet_title_hydration_candidate_link,
)


def _resolver() -> Any:
    return jf.GoogleSheetsProviderTitleResolver(
        fetch_text=lambda _url, _timeout: "{}",
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )


def _candidate_link(raw: dict[str, Any], *, idx: int = 0) -> str:
    return _google_sheet_title_hydration_candidate_link(
        raw=raw,
        idx=idx,
        source="google_sheets",
        resolved_links={},
        title_hydration_resolver=_resolver(),
    )


def test_prefetch_gate_selects_rows_whose_title_cannot_be_derived_from_url() -> None:
    """A row the row loop would hydrate must be prefetched.

    This is the inverted-gate regression: the prefetch gate previously rejected
    exactly these rows.
    """
    # Greenhouse link: a provider title target exists, and the generic
    # "Careers" title cannot be derived from the URL.
    raw = {
        "title": "Careers",
        "company": "Example Games",
        "jobLink": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
    }

    link = _candidate_link(raw)

    assert link == "https://job-boards.greenhouse.io/examplegames/jobs/12345", (
        "rows needing hydration (URL derivation fails) must pass the prefetch gate; "
        "rejecting them makes every feed fetch serial in the row loop"
    )


def test_prefetch_gate_agrees_with_derivation_for_real_job_titles() -> None:
    """For a real job row, derivation fails, so prefetch MUST select the row.

    Measured over 30,970 real sheet rows: URL title derivation succeeded for
    ZERO rows. That is precisely why the old (inverted) gate selected nothing
    and every feed was fetched serially in the row loop.

    The fixtures are real rows from the sheet (provider-backed links with
    ordinary job titles).
    """
    rows = [
        {
            "title": "Lead Gameplay Animator",
            "company": "Pubgsanramon",
            "jobLink": "https://job-boards.greenhouse.io/pubgsanramon/jobs/8466855002",
        },
        {
            "title": "Level Designer",
            "company": "Blackbirdinteractive",
            "jobLink": (
                "https://jobs.lever.co/blackbirdinteractive/4c29f0b7-dbb9-4ff5-92da-05e1b33e8a23"
            ),
        },
    ]

    for raw in rows:
        derived = _derive_google_sheets_title_from_url(
            source="google_sheets",
            title=raw["title"],
            company=raw["company"],
            job_link=raw["jobLink"],
        )
        assert derived == "", f"real job titles are not derivable: {raw['title']!r}"
        assert _candidate_link(raw) != "", (
            f"a row the row loop would hydrate must be prefetched: {raw['title']!r}; "
            "selecting nothing forces every feed fetch to happen serially"
        )


def test_prefetch_gate_skips_non_google_sheets_sources() -> None:
    for source in ("greenhouse", "lever"):
        link = _google_sheet_title_hydration_candidate_link(
            raw={"title": "Careers", "company": "X", "jobLink": "https://jobs.lever.co/x/1"},
            idx=0,
            source=source,
            resolved_links={},
            title_hydration_resolver=_resolver(),
        )
        assert link == "", f"{source} is not a google_sheets source"


def test_prefetch_gate_skips_rows_missing_required_fields() -> None:
    base = {
        "title": "Careers",
        "company": "Example Games",
        "jobLink": "https://job-boards.greenhouse.io/examplegames/jobs/12345",
    }
    for missing in ("title", "company", "jobLink"):
        raw = {k: v for k, v in base.items() if k != missing}
        assert _candidate_link(raw) == "", f"missing {missing} must be rejected"


def test_prefetch_gate_rejects_unsupported_provider_links() -> None:
    """Links with no provider feed target cannot be hydrated."""
    raw = {
        "title": "Careers",
        "company": "Example Games",
        "jobLink": "https://example.invalid/not-a-provider/jobs",
    }

    assert _candidate_link(raw) == ""


def test_prefetch_and_row_loop_gates_agree_on_a_mixed_row_set() -> None:
    """End-to-end alignment check over rows spanning both gate outcomes.

    Every row prefetch selects must also be a row the row loop would ask to
    hydrate (i.e. the resolver supports it and derivation fails), and vice
    versa. A mismatch here reintroduces serial fetches.
    """
    rows = [
        # Provider-backed, non-derivable title -> must be prefetched.
        {
            "title": "Lead Gameplay Animator",
            "company": "Pubgsanramon",
            "jobLink": "https://job-boards.greenhouse.io/pubgsanramon/jobs/8466855002",
        },
        {
            "title": "Level Designer",
            "company": "Blackbirdinteractive",
            "jobLink": "https://jobs.lever.co/blackbirdinteractive/4c29f0b7-dbb9-4ff5-92da-05e1b33e8a23",
        },
        # Non-provider link -> no feed target, never prefetched.
        {
            "title": "Head of VFX",
            "company": "22 dog studios",
            "jobLink": "https://22dogstudio.com/careers/head-of-vfx-toronto-studio",
        },
    ]

    prefetched = [r for r in rows if _candidate_link(r)]

    # Row-loop side: hydration is requested only when the resolver supports the
    # link AND URL derivation yields nothing.
    expected = [
        r
        for r in rows
        if _resolver().supports(r["jobLink"])
        and not _derive_google_sheets_title_from_url(
            source="google_sheets",
            title=r["title"],
            company=r["company"],
            job_link=r["jobLink"],
        )
    ]

    assert {r["jobLink"] for r in prefetched} == {r["jobLink"] for r in expected}, (
        "prefetch gate and row-loop gate disagree; the mismatch becomes serial "
        "network fetches inside the row loop"
    )
