"""Regression tests for the sector signal's evidence-scope fixes.

The former ``role_token`` branch of :func:`has_positive_game_evidence`
classified a row as Game when the title contained a generic role word
(``engineer``/``artist``/...) and the company name appeared in its own
``source``/``job_link`` text. Because ATS hosts embed the company slug for
every employer (``job-boards.greenhouse.io/<company>/...``,
``jobs.lever.co/<company>/...``, ``apply.workable.com/<company>/...``) and
own-domain careers sites embed it too (``jobs.apple.com``,
``nvidia.wd1.myworkdayjobs.com``), that corroboration carried no
employer-specific information: 7,730 rows (47.8% of the Game sector) across
1,049 non-games companies (Apple, NVIDIA, Lockheed Martin, PayPal, ...) were
Game with zero game-keyword evidence.

The keyword branch was then scoped to company+title: board URLs like
``careers.wbd.com/.../wb-games-jobs`` and
``disneycareers.com/en/search-jobs/game/...`` carried a games-flavored path
for whole-company sites and misattributed every employer's rows (WBD, CNN,
HBO Max, Disney, SciGames) as Game — 458 rows in the baseline dataset.

Evidence: ``docs/snapshots/sector-signal-contamination-2026-09-06.md``
(2026-09-06 contamination sweep); T13 in
``docs/plans/jobs-coverage-improvement-plan.md``.
"""

from __future__ import annotations

from src.jobs.common.heuristics import classify_company_type
from src.jobs.game_detection import has_game_source_provenance, has_positive_game_evidence
from src.jobs.normalizers import normalize_sector

APPLE_ROW = {
    "company": "Apple",
    "title": "Wireless SoC Design Engineer",
    "source": "google_sheets",
    "job_link": "https://jobs.apple.com/en-us/details/200559444/wireless-soc-design-engineer",
}

NVIDIA_WORKDAY_ROW = {
    "company": "NVIDIA",
    "title": "Senior Software Engineer, Architecture",
    "source": "workday_sources",
    "job_link": "https://nvidia.wd1.myworkdayjobs.com/en-US/NVIDIAExternalCareersSite/job/Senior-Software-Engineer_R1234567",
}

CYBERARK_LEVER_ROW = {
    "company": "CyberArk",
    "title": "Senior Software Engineer",
    "source": "lever_sources",
    "job_link": "https://jobs.lever.co/cyberark/9f2c1a55-4c1e-4f0e-9d4e-9b1d2f3a4b5c",
}


def test_apple_class_row_reclassifies_to_tech() -> None:
    """The measured top role_token carrier (284 Game rows in the sweep) is Tech."""
    assert not has_positive_game_evidence(**APPLE_ROW)
    assert normalize_sector("Game", **APPLE_ROW) == "Tech"
    assert classify_company_type(**APPLE_ROW) == "Tech"


def test_workday_tenant_and_ats_host_rows_reclassify_to_tech() -> None:
    """Own-domain Workday tenants and ATS-hosted links both embed the company slug."""
    for row in (NVIDIA_WORKDAY_ROW, CYBERARK_LEVER_ROW):
        assert not has_positive_game_evidence(**row)
        assert normalize_sector("Game", **row) == "Tech"
        assert classify_company_type(**row) == "Tech"


def test_board_url_keywords_are_not_game_evidence() -> None:
    """The measured URL-keyword classes from the 2026-09-06 keyword recount.

    Whole-company careers boards (WBD's ``wb-games-jobs`` section, Disney's
    ``search-jobs/game`` search URL, the ``scientificgames`` myworkday tenant,
    ``?q=game`` query strings) must not classify every employer's rows as Game.
    Recount on the contamination-sweep baseline: 458 rows (224 source-carried,
    234 link-carried) dropped, led by Disney 64 / WBD 50 / Sglottery 34 /
    CNN 21 / Sonyglobal 13; zero dropped rows had a keyword in the company
    name. Evidence: ``_out/keyword-scope-recount-20260906/`` and
    ``docs/snapshots/sector-signal-contamination-2026-09-06.md``.
    """
    wbd_board_row = {
        "company": "CNN",
        "title": "Software Engineer II (Site Reliability Engineer)",
        "source": "static_source::static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs",
        "job_link": "https://careers.wbd.com/global/en/job/R000105665/Software-Engineer-II-Site-Reliability-Engineer",
    }
    assert not has_positive_game_evidence(**wbd_board_row)
    assert normalize_sector("Game", **wbd_board_row) == "Tech"
    assert classify_company_type(**wbd_board_row) == "Tech"

    disney_search_row = {
        "company": "Disney Experiences",
        "title": "Senior Core Systems Engineer",
        "source": "static_source::static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1",
        "job_link": "https://www.disneycareers.com/en/job/sweden/senior-core-systems-engineer/391/94802233360",
    }
    assert not has_positive_game_evidence(**disney_search_row)
    assert normalize_sector("Game", **disney_search_row) == "Tech"
    assert classify_company_type(**disney_search_row) == "Tech"

    scigames_row = {
        "company": "Sglottery",
        "title": "Software Engineer II",
        "source": "workday_sources",
        "job_link": "https://scientificgames.wd5.myworkdayjobs.com/en-US/SciPlayCareers/job/Software-Engineer-II_R-24110",
    }
    assert not has_positive_game_evidence(**scigames_row)
    assert normalize_sector("Game", **scigames_row) == "Tech"

    query_row = {
        "company": "Sonyglobal",
        "title": "Principal Technical Program Manager",
        "source": "google_sheets",
        "job_link": "https://sonyglobal.wd1.myworkdayjobs.com/en-US/SonyGlobalCareers/job/Principal-Technical-Program-Manager_JR-118447?q=game",
    }
    assert not has_positive_game_evidence(**query_row)
    assert normalize_sector("Game", **query_row) == "Tech"


def test_company_and_title_keyword_evidence_is_scoped_not_lost() -> None:
    """The post-scope survivors from the recount: 4,632 keyword rows stay Game."""
    # Title carries the keyword (2,018 recount rows).
    assert has_positive_game_evidence(
        "Studio Other",
        "Senior Gameplay Programmer",
        "google_sheets",
        "https://example.com/careers/senior-gameplay-programmer",
    )
    # Company name carries the keyword (2,614 recount rows).
    assert has_positive_game_evidence(
        "Schell Games",
        "Software Engineer",
        "google_sheets",
        "https://www.schellgames.com/careers/software-engineer",
    )
    # A real game row on the WBD whole-company board keeps Game via its title.
    assert has_positive_game_evidence(
        "WB Games",
        "Senior Gameplay Programmer",
        "static_source::static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs",
        "https://careers.wbd.com/global/en/job/R000105670/Senior-Gameplay-Programmer",
    )


def test_dedicated_game_source_family_hints_are_unaffected() -> None:
    """Source-family provenance (gamejobs, gamesindustry, ...) stays the board signal."""
    assert has_positive_game_evidence(
        "Some Studio",
        "Producer",
        "gamejobs",
        "https://gamejobs.co/some-studio-producer",
    )
    assert (
        normalize_sector(
            "Tech", "Some Studio", "Producer", "gamesindustry_biz", "https://example.com/a"
        )
        == "Game"
    )


def test_role_word_in_title_alone_is_not_game_evidence() -> None:
    assert not has_positive_game_evidence("Some Employer", "Backend Engineer")
    assert not has_positive_game_evidence("Some Employer", "UX Designer")
    assert not has_positive_game_evidence("Some Employer", "Concept Artist")


def test_game_keyword_rows_are_unaffected() -> None:
    """Title and company-name keyword evidence carries Game rows without provenance."""
    assert has_positive_game_evidence(
        "Studio Other",
        "Senior Gameplay Programmer",
        "google_sheets",
        "https://example.com/gameplay",
    )
    assert normalize_sector("Tech", "Studio Other", "Senior Gameplay Programmer") == "Game"
    # Game-industry employer name is itself the keyword ("Riot Games" -> "game").
    assert has_positive_game_evidence(
        "Riot Games",
        "Software Engineer",
        "google_sheets",
        "https://www.riotgames.com/en/work-with-us",
    )
    assert normalize_sector("Tech", "Riot Games", "Software Engineer") == "Game"


def test_provenance_rows_are_unaffected() -> None:
    """Keyword-less game employers (Activision class) still classify Game via provenance."""
    activision = {
        "company": "Activision",
        "title": "Software Engineer",
        "source": "workday_sources",
        "job_link": "https://activision.wd1.myworkdayjobs.com/ActivisionCentralTech/jobs/Software-Engineer_R26107",
        "source_bundle": [
            {
                "source": "workday_sources",
                "sourceJobId": "workday:x:1",
                "adapter": "workday_cxs",
                "studio": "Activision",
            }
        ],
    }
    assert has_positive_game_evidence(**activision)
    assert normalize_sector("Tech", **activision) == "Game"

    zynga = {
        "company": "Zynga",
        "title": "Marketing Artist",
        "source": "greenhouse_boards",
        "job_link": "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
        "source_bundle": [
            {
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:zyngacareers:5835998004",
                "adapter": "greenhouse",
                "studio": "Zynga",
            }
        ],
    }
    assert has_positive_game_evidence(**zynga)
    assert normalize_sector("Tech", **zynga) == "Game"


def test_multiboard_static_with_foreign_provider_studio_loses_provenance() -> None:
    """WBD-class shape: static items + a provider item from a different employer.

    A multi-board static row (one careers site aggregating several employers)
    must not have one employer's provider board certify another employer's
    row. Evidence: docs/snapshots/sector-signal-contamination-2026-09-06.md,
    disposition item 2.
    """
    row = {
        "company": "CNN",
        "title": "Software Engineer",
        "source": "static_source::static:listing_url:https://careers.wbd.com/careers",
        "job_link": "https://careers.wbd.com/global/en/job/r0000878",
        "source_bundle": [
            {
                "source": "static_source::static:listing_url:https://careers.wbd.com/careers",
                "adapter": "static",
                "studio": "Warner Bros. Discovery",
            },
            {
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:wbgames:1",
                "adapter": "greenhouse",
                "studio": "WB Games",
            },
        ],
    }
    assert not has_game_source_provenance(row["source"], row["source_bundle"], row["company"])
    assert not has_positive_game_evidence(**row)
    assert normalize_sector("Game", **row) == "Tech"


def test_multiboard_static_with_consistent_provider_studios_keep_provenance() -> None:
    """PlayStation-class mixed bundle: every provider studio matches the employer."""
    row = {
        "company": "PlayStation Global",
        "title": "Software Engineer",
        "source": "greenhouse_boards",
        "job_link": "https://job-boards.greenhouse.io/sonyinteractiveentertainmentglobal/jobs/6178586004",
        "source_bundle": [
            {
                "source": "google_sheets_1er2oaxo",
            },
            {
                "source": "static_source::static:listing_url:https://sonysandiegostudio.example/careers",
                "adapter": "static",
                "studio": "Sony Interactive Entertainment San Diego Studio",
            },
            {
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:sonyinteractiveentertainmentglobal:6178586004",
                "adapter": "greenhouse",
                "studio": "PlayStation Global",
            },
        ],
    }
    assert has_game_source_provenance(row["source"], row["source_bundle"], row["company"])
    assert has_positive_game_evidence(**row)
    assert normalize_sector("Game", **row) == "Game"


def test_pure_provider_bundle_provenance_is_unaffected_by_scoping() -> None:
    """No static items in the bundle means no multi-board scoping applies."""
    row = {
        "company": "Krafton",
        "title": "Backend Engineer",
        "source": "greenhouse_boards",
        "job_link": "https://job-boards.greenhouse.io/krafton/jobs/1",
        "source_bundle": [
            {
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:krafton:1",
                "adapter": "greenhouse",
                "studio": "KRAFTON",
            }
        ],
    }
    assert has_game_source_provenance(row["source"], row["source_bundle"], row["company"])
    assert has_positive_game_evidence(**row)


def test_strict_sector_gate_drops_reclassified_rows_but_keeps_game_rows() -> None:
    """End to end through the BALUFFO_STRICT_GAME_ONLY seam the sweep flagged."""
    import src.jobs.pipeline_finalize as pipeline_finalize

    game_row = {
        "title": "Senior Gameplay Programmer",
        "company": "Studio Other",
        "source": "google_sheets",
        "sector": "Game",
    }
    apple_output_row = {**APPLE_ROW, "sector": normalize_sector("Game", **APPLE_ROW)}
    assert apple_output_row["sector"] == "Tech"

    original = pipeline_finalize.STRICT_GAME_ONLY_ENABLED
    try:
        pipeline_finalize.STRICT_GAME_ONLY_ENABLED = True
        kept, dropped = pipeline_finalize._apply_sector_gate([apple_output_row, dict(game_row)], [])
    finally:
        pipeline_finalize.STRICT_GAME_ONLY_ENABLED = original

    assert dropped == 1
    assert [row["company"] for row in kept] == ["Studio Other"]
