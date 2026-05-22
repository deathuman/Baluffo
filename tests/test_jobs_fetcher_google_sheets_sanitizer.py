import json
from pathlib import Path

from src import jobs_fetcher as jf

_TITLE_SPECIFICITY_CORPUS = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "jobs"
    / "google_sheets_title_specificity_corpus.json"
)


def test_canonicalize_google_sheets_rows_drops_category_labels_with_non_game_evidence() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Sales",
            "company": "Mighty Games",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/KPMGNederland/744000111",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Teaching",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/DominosPizza/744000222",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Senior Programmer",
            "company": "Finitude GmbH",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://finitude.example/jobs/senior-programmer",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Senior Programmer"]
    assert drop_reasons["google_sheets_category_row"] == 2


def test_canonicalize_google_sheets_rows_drops_category_labels_without_game_evidence() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Logistics",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/warehouse-coordinator",
                "sector": "Tech",
            }
        ],
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons["google_sheets_category_row"] == 1


def test_canonicalize_google_sheets_rows_drops_expanded_category_labels() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Account-management",
            "company": "ChainGuard",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://job-boards.greenhouse.io/chainguard/jobs/4658503006",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Quality-assurance",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/qa-analyst",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Full-stack-development",
            "company": "Example Games",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://examplegames.example/jobs/full-stack-engineer",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Full Stack Engineer"]
    assert drop_reasons["google_sheets_category_row"] == 2


def test_google_sheets_url_title_repair_strips_opaque_id_affixes() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Technical-art",
            "company": "Example Games",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://jobs.example.test/p/05e34dd23a3b01-technical-artist",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Product-management",
            "company": "Example Games",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": (
                "https://jobs.example.test/openings/"
                "senior-product-manager-e1c434bd6b7a434a9712312ab6e99bb1"
            ),
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == [
        "Technical Artist",
        "Senior Product Manager",
    ]
    assert not drop_reasons


def test_google_sheets_url_title_repair_skips_terminal_codes_for_title_segments() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Graphic-design",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://jobs.example.test/jobs/example-games/67.001/graphic-designer/0B.56A",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Graphic Designer"]
    assert not drop_reasons


def test_google_sheets_url_title_repair_does_not_use_account_slug_as_title() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Homa Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://apply.example.test/homa-games/j/F424388045",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 1}


def test_google_sheets_url_title_repair_preserves_numeric_roles_and_abbreviations() -> None:
    cases = [
        ("Technical-art", "https://jobs.example.test/jobs/2d-artist", "2D Artist"),
        ("Technical-art", "https://jobs.example.test/jobs/3d-artist", "3D Artist"),
        (
            "Product-management",
            "https://jobs.example.test/jobs/web3-gameplay-engineer",
            "Web3 Gameplay Engineer",
        ),
        ("Technical-art", "https://jobs.example.test/jobs/fx-td", "FX TD"),
        ("Technical-art", "https://jobs.example.test/jobs/cfx-td", "CFX TD"),
        ("Technical-art", "https://jobs.example.test/jobs/art-td", "Art TD"),
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": f"sheet-{index}",
                "title": title,
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": job_link,
                "sector": "Game",
            }
            for index, (title, job_link, _expected) in enumerate(cases, start=1)
        ],
        source="google_sheets",
        fetched_at="2026-05-22T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == [expected for _title, _link, expected in cases]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_drops_link_employer_mismatches() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Administartive",
            "company": "Mighty Games",
            "city": "Rotterdam",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/KPN/744000114033447-senior-administrateur-b2c",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Account-management",
            "company": "Gardens Interactive",
            "city": "Remote",
            "country": "US",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://himalayas.app/companies/canary-technologies/jobs/enterprise-strategic-account-executive",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Account-management",
            "company": "NetApp",
            "city": "Remote",
            "country": "US",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/EthosInteractive/744000108945480-digital-account-manager-",
            "sector": "Tech",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons["google_sheets_category_row"] == 3


def test_canonicalize_google_sheets_rows_drops_additional_link_employer_shapes() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Administartive",
            "company": "Gamecrio Studios Pvt Ltd",
            "city": "Ahmedabad",
            "country": "IN",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": "https://www.shine.com/jobs/admin-assistant-for-ahmedabad-location/zecruiters-jobconnect-private-limited/18730117",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Administartive",
            "company": "iBLOXX Studios DMCC",
            "city": "Abu Dhabi",
            "country": "AE",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": "https://bebee.com/ae/jobs/admin-contracts-securiguard-middle-east-abu-dhabi--theirstack-643514805",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Administartive",
            "company": "Triodoxic Digital Studios",
            "city": "Mollet del Valles",
            "country": "ES",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": "https://bebee.com/es/jobs/administrativo-a-comercial-adecco-mollet-del-valles--oficin-139717532",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons["google_sheets_category_row"] == 3


def test_canonicalize_google_sheets_rows_preserves_game_company_admin_and_account_roles() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Administartive",
            "company": "Sony Interactive Entertainment",
            "city": "San Mateo",
            "country": "US",
            "workType": "Remote",
            "contractType": "Contract",
            "jobLink": "https://careers.playstation.com/executive-assistant-people-and-places-contract/job/5803602004",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Account-management",
            "company": "Push Gaming",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://pushgaming.teamtailor.com/jobs/7202118-account-manager",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Account-management",
            "company": "Example Games",
            "city": "Berlin",
            "country": "DE",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/ExampleGamesGmbH/744000111-account-manager",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == [
        "Executive Assistant People And Places Contract",
        "Account Manager",
        "Account Manager",
    ]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_repairs_category_titles_from_safe_url_slugs() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Product-management",
            "company": "Nike",
            "city": "Hilversum",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://careers.nike.com/senior-professional-producer-brand-creative-emea-production/job/R-81187",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Administartive",
            "company": "People Can Fly",
            "city": "Warsaw",
            "country": "PL",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000115488907-office-specialist",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Technical-art",
            "company": "Gameloft",
            "city": "Paris",
            "country": "FR",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://www.gameloft.com/jobs/Senior%20Office%20Administrator-744000108182315",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-4",
            "title": "Technical-art",
            "company": "Unknown company",
            "city": "Hoevelaken",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://wk.wd3.myworkdayjobs.com/en-US/External/job/NLD---Hoevelaken/Product-Manager-Accounting-Software_R0053364",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-5",
            "title": "Administartive",
            "company": "CD PROJEKT RED",
            "city": "Warsaw",
            "country": "PL",
            "workType": "Onsite",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000108205855-receptionist",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == [
        "Senior Professional Producer Brand Creative Emea Production",
        "Office Specialist",
        "Senior Office Administrator",
        "Product Manager Accounting Software",
        "Receptionist",
    ]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_repairs_broad_animation_titles_from_safe_url_slugs() -> (
    None
):
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Animator",
            "company": "Activision",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://careers.activision.com/job/R026851/Expert-Technical-Animator",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Animation",
            "company": "Activision",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://careers.activision.com/job/R026851/Expert-Technical-Animator",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-3",
            "title": "Technical Animator",
            "company": "Xbox Game Studios",
            "city": "Malmo",
            "country": "SE",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": (
                "https://xboxgaming.wd1.myworkdayjobs.com/en-US/External/job/Malm/"
                "Expert-Technical-Animator_R026851"
            ),
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == [
        "Expert Technical Animator",
        "Expert Technical Animator",
        "Expert Technical Animator",
    ]
    assert not drop_reasons


def test_canonicalize_google_sheets_title_specificity_corpus() -> None:
    cases = json.loads(_TITLE_SPECIFICITY_CORPUS.read_text(encoding="utf-8"))
    for index, case in enumerate(cases, start=1):
        canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
            [
                {
                    "sourceJobId": f"corpus-{index}",
                    "title": case["title"],
                    "company": case["company"],
                    "city": "Remote",
                    "country": "Unknown",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": case["jobLink"],
                    "sector": "Game",
                }
            ],
            source=case.get("source", "google_sheets_1er2oaxo"),
            fetched_at="2026-03-13T00:00:00+00:00",
        )

        assert not drop_reasons, case["name"]
        assert [row.title for row in canonical_rows] == [case["expectedTitle"]], case["name"]


def test_canonicalize_google_sheets_rows_preserves_broad_title_when_url_is_not_stricter() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Animator",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/animator_123",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Animator"]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_preserves_qualified_broad_title_for_lateral_url_slug() -> (
    None
):
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Technical Animator",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/Cinematic-Animator_123",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Technical Animator"]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_strips_short_numeric_ats_suffixes() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/Senior-Product-Manager_123",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert [row.title for row in canonical_rows] == ["Senior Product Manager"]
    assert not drop_reasons


def test_canonicalize_google_sheets_rows_drops_opaque_category_title_urls() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://jobs.ashbyhq.com/sandboxaq/3eabb716-eaef-432e-ad88-f3e16d01e54b",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 1}


def test_canonicalize_google_sheets_rows_drops_repaired_static_non_openings() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Technical-art",
                "company": "Example Games",
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://www.therabytes.de/kopie-von-speculative-application",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons["non_job_static_page"] == 1


def test_canonicalize_google_sheets_rows_drops_unrepaired_ambiguous_category_rows() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Product-management",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://gracklehq.com/rd/123",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Vfx",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/vfx",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons == {"google_sheets_category_row": 2}


def test_canonicalize_google_sheets_rows_drops_ambiguous_category_with_non_game_evidence() -> None:
    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        [
            {
                "sourceJobId": "sheet-1",
                "title": "Product-management",
                "company": jf.UNKNOWN_COMPANY_LABEL,
                "city": "Remote",
                "country": "Unknown",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://careers.walmart.com/jobs/product-manager",
                "sector": "Game",
            }
        ],
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert canonical_rows == []
    assert drop_reasons["google_sheets_category_row"] == 1
