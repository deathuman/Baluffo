"""A multi-tenant platform adapter must not lend game provenance to every tenant.

`has_game_source_provenance` treated a non-static bundle item as a games-industry board on the
strength of the adapter name. For ATS adapters (greenhouse, workday, lever, ...) the adapter IS
employer-specific -- the tenant slug names the company -- but `phenom` is the J2W/Phenom *platform*:
the tenant, not the adapter, is the employer. RTL Enterprises' board returned 25 media/corporate
roles (controlling, editorial, retail, payroll) and every row inherited `sector: Game`, even though
the parser's own `sector: "Game"` hardcode is ignored by `normalize_sector` -- the misclassification
came from this provenance rule, not the parser.

The narrow fix (measured 2026-09-26, ~133 legitimately-game rows depend on the provenance branch and
are all non-phenom): a platform-adapter bundle item counts only when the studio name itself carries
game scope (`GAME_KEYWORDS` / `GAME_EMPLOYER_NAME_HINTS`). Everything else falls through to the
company/title keyword evidence exactly as an unknown-adapter row would. The cost is accepted and
bounded: a future phenom tenant whose name has no game token and whose titles say "Senior Backend
Developer" classifies Tech -- the conservatism direction the 2026-09-06 contamination precedent
already chose.
"""

from __future__ import annotations

from src.jobs.common.heuristics import classify_company_type
from src.jobs.game_detection import has_game_source_provenance, has_positive_game_evidence
from src.jobs.normalizers import normalize_sector

RTL_BUNDLE = [
    {"source": "phenom_sources", "adapter": "phenom", "studio": "RTL Enterprises"},
]

_PROVENANCE_KEYS = ("source", "source_bundle", "company")


def provenance(row: dict) -> bool:
    """has_game_source_provenance accepts a narrower signature than the row dicts."""
    return has_game_source_provenance(**{k: row[k] for k in _PROVENANCE_KEYS if k in row})


def test_a_platform_adapter_alone_is_not_game_provenance() -> None:
    """RTL's real shape: phenom bundle, non-game studio, non-game title."""
    row = {
        "company": "RTL Enterprises",
        "title": "Working Student – Financial Controlling (m/f/d)",
        "source": "phenom_sources",
        "job_link": "https://jobsearch.createyourowncareer.com/RTL/",
        "source_bundle": RTL_BUNDLE,
    }
    assert not provenance(row)
    assert not has_positive_game_evidence(**row)
    assert normalize_sector("Game", **row) == "Tech"
    assert classify_company_type(**row) == "Tech"


def test_the_same_row_classes_game_when_the_parser_value_is_unused() -> None:
    """Before the fix this exact row classified Game; the parser's own sector value played no part.

    Pinning the *incoming* value's irrelevance is what keeps the parser hardcode from being mistaken
    for the cause in the future: `normalize_sector("Game", ...)` and `normalize_sector("Tech", ...)`
    must round-trip to the same verdict.
    """
    row = {
        "company": "RTL Enterprises",
        "title": "Payroll Officer (m/f/d)",
        "source": "phenom_sources",
        "job_link": "https://jobsearch.createyourowncareer.com/RTL/",
        "source_bundle": RTL_BUNDLE,
    }
    for incoming in ("Game", "Tech", ""):
        assert normalize_sector(incoming, **row) == "Tech"


def test_a_platform_adapter_with_a_game_named_studio_still_counts() -> None:
    """A future game-studio tenant must not be misclassified by this fix."""
    row = {
        "company": "BoomBit Games",
        "title": "Senior Backend Developer",
        "source": "phenom_sources",
        "job_link": "https://phenom.example/boombit/jobs/x",
        "source_bundle": [
            {"source": "phenom_sources", "adapter": "phenom", "studio": "BoomBit Games"},
        ],
    }
    assert provenance(row)
    assert normalize_sector("Tech", **row) == "Game"
    assert classify_company_type(**row) == "Game"


def test_employer_hint_tables_rescue_a_platform_tenant_by_name() -> None:
    row = {
        "company": "Metacore",
        "title": "Office Manager",
        "source": "phenom_sources",
        "job_link": "https://phenom.example/metacore/jobs/x",
        "source_bundle": [
            {"source": "phenom_sources", "adapter": "phenom", "studio": "Metacore"},
        ],
    }
    assert provenance(row)
    assert normalize_sector("Tech", **row) == "Game"


def test_ats_adapter_provenance_is_unchanged() -> None:
    """The branch must not tighten for adapters that are employer-specific by construction."""
    row = {
        "company": "Streamline Studios",
        "title": "Accountant",
        "source": "greenhouse_boards",
        "job_link": "https://job-boards.greenhouse.io/streamline/jobs/1",
        "source_bundle": [
            {
                "source": "greenhouse_boards",
                "adapter": "greenhouse",
                "studio": "Streamline Studios",
            },
        ],
    }
    assert provenance(row)
    assert normalize_sector("Tech", **row) == "Game"


def test_a_platform_item_without_a_studio_carries_no_evidence() -> None:
    assert not has_game_source_provenance(
        source="phenom_sources",
        source_bundle=[{"source": "phenom_sources", "adapter": "phenom"}],
        company="RTL Enterprises",
    )


def test_a_non_game_platform_item_cannot_perturb_multi_board_scoping() -> None:
    """The skipped item is neutral, not static: it must not switch consistency checking on.

    With the item counted as static, a static+platform bundle would start demanding employer
    consistency from its provider items; as skipped, a legitimately-inconsistent provider item in the
    same bundle still loses provenance exactly as before this change.
    """
    row = {
        "company": "Acme Aggregated Jobs",
        "title": "Marketing Manager",
        "source": "static_sources",
        "job_link": "https://acme.example/listing/x",
        "source_bundle": [
            {"source": "static_sources", "adapter": "static", "studio": "Acme Aggregated Jobs"},
            {"source": "phenom_sources", "adapter": "phenom", "studio": "RTL Enterprises"},
        ],
    }
    # No provider item counts any more, so provenance is False: the bundle behaves like the
    # static-only case.
    assert not provenance(row)


def test_a_game_platform_item_in_a_mixed_bundle_still_scores_as_provider() -> None:
    row = {
        "company": "Acme Aggregated Jobs",
        "title": "Marketing Manager",
        "source": "static_sources",
        "job_link": "https://acme.example/listing/x",
        "source_bundle": [
            {"source": "static_sources", "adapter": "static", "studio": "Acme Aggregated Jobs"},
            {"source": "phenom_sources", "adapter": "phenom", "studio": "BoomBit Games"},
        ],
    }
    # Game-named platform item: provider side has a conflicting employer, so the multi-board rule
    # binds it and provenance stays off for this row.
    assert not provenance(row)
