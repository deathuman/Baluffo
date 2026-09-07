"""Regression tests for curated employer-name game evidence.

The 2026-09-06 keyword scoping (board-URL keywords no longer carry game
evidence) dropped a tail of TRUE game companies whose rows rode shared
aggregator sources (``google_sheets`` serves every studio's ATS links),
adapterless lever rows, or static boards — no per-employer provenance exists
to certify them, and their names carry no game token ("Electronic Arts",
"Square Enix", "Avalanche"). ``GAME_EMPLOYER_NAME_HINTS`` restores them with
company-field-only name evidence.

Per-source provenance staging was measured impossible for this class: the rows
share sources with every other employer, so no per-employer whitelist can
exist at the source layer.

Evidence: ``docs/snapshots/sector-signal-contamination-2026-09-06.md``
(disposition 5), T13 execution notes in
``docs/plans/jobs-coverage-improvement-plan.md``.
"""

from __future__ import annotations

from src.jobs.common.heuristics import classify_company_type
from src.jobs.game_detection import has_positive_game_evidence
from src.jobs.normalizers import normalize_sector

RECOVERED_ROWS = {
    "electronic_arts": {
        "company": "Electronic Arts",
        "title": "Anti-Cheat Engineer",
        "source": "static_source::static:listing_url:https://careers.ea.com/careers",
        "job_link": "https://jobs.ea.com/en_US/careers/JobDetail/Anti-Cheat-Engineer/212779",
    },
    "ea_sports": {
        "company": "EA Sports",
        "title": "Software Engineer, C++",
        "source": "google_sheets",
        "job_link": "https://jobs.ea.com/en_US/careers/JobDetail/Software-Engineer-C-EA-SPORTS-FC/212300",
    },
    "avalanche": {
        "company": "Avalanchestudios",
        "title": "Total Rewards Specialist",
        "source": "google_sheets",
        "job_link": "https://jobs.lever.co/avalanchestudios/72e4e6a4-f723-48ef-8d83-93e885ecd8a1",
    },
    "metacore": {
        "company": "Metacore",
        "title": "Talent Acquisition Partner",
        "source": "google_sheets",
        "job_link": "https://job-boards.eu.greenhouse.io/metacore/jobs/4793672101",
    },
    "playrix": {
        "company": "playrix",
        "title": "Junior QA Engineer (Manual)",
        "source": "google_sheets",
        "job_link": "https://playrix.com/job/open/qa/junior-qa-engineer-manual",
    },
    "daybreak": {
        "company": "Daybreak",
        "title": "Accountant Intern",
        "source": "google_sheets",
        "job_link": "https://www.daybreakgames.com/careers?job=8468583002",
    },
    "square_enix": {
        "company": "Square Enix",
        "title": "Japan",
        "source": "static_source::static:listing_url:https://www.square-enix-games.com/en_us/careers",
        "job_link": "https://www.jp.square-enix.com/recruit/career",
    },
    "lightbulb_crew": {
        "company": "Lightbulb Crew",
        "title": "Ex Sanguis",
        "source": "static_source::static:listing_url:https://lightbulbcrew.fr",
        "job_link": "https://firesquid.games/games/ex-sanguis",
    },
}


def test_curated_employer_names_recover_url_carried_companies() -> None:
    """Each targeted employer classifies Game through the full signal seams.

    Measured dropped populations on the contamination-sweep baseline (all
    recovered by the hint table, per-company gains exact): EA family 271
    (careers.ea.com/jobs.ea.com statics + sheet rows), Avalanchestudios 17,
    Metacore 11, Playrix 11, Lightbulb Crew 9, Square Enix 7 statics,
    Daybreak 4 (daybreakgames.com links).
    """
    for name, row in RECOVERED_ROWS.items():
        assert has_positive_game_evidence(**row), name
        assert normalize_sector("Tech", **row) == "Game", name
    # Non-keyword titles stay Game through the employer-type contract too.
    assert classify_company_type(**RECOVERED_ROWS["avalanche"]) == "Game"
    assert classify_company_type(**RECOVERED_ROWS["playrix"]) == "Game"


def test_employer_name_hints_are_company_field_only() -> None:
    """Name evidence never leaks from titles, sources, or links."""
    assert not has_positive_game_evidence(
        "Bakery Studio", "Former Metacore producer", "google_sheets", "https://example.com/metacore"
    )


def test_employer_name_hints_do_not_capture_lookalikes() -> None:
    """Short-name trap companies from the dataset stay non-Game.

    A bare ``ea`` hint would capture EACH1 and Eacproductdevelopmentsolutions
    (smartrecruiters healthcare/sales employers) and Eataly; EA is matched via
    multi-token hints only ("electronic arts", "ea sports", "ea create"),
    verified against every distinct company containing each candidate hint in
    the 42,181-row baseline.
    """
    lookalikes = [
        {
            "company": "EACH1",
            "title": "Payroll Officer",
            "source": "google_sheets",
            "job_link": "https://jobs.smartrecruiters.com/EACH1/744000112291310-payroll-officer",
        },
        {
            "company": "Eacproductdevelopmentsolutions",
            "title": "Sales Executive - Service Sales",
            "source": "google_sheets",
            "job_link": "https://jobs.smartrecruiters.com/EACProductDevelopmentSolutions/744000112790825-sales-exec",
        },
        {
            "company": "Eataly",
            "title": "Line Cook",
            "source": "google_sheets",
            "job_link": "https://jobs.eataly.com/line-cook",
        },
        {
            "company": "Easyvista",
            "title": "Account Manager",
            "source": "google_sheets",
            "job_link": "https://jobs.easyvista.com/account-manager",
        },
    ]
    for row in lookalikes:
        assert not has_positive_game_evidence(**row), row["company"]
        assert normalize_sector("Game", **row) == "Tech", row["company"]
