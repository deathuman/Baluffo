from src import jobs_fetcher as jf


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


def test_canonicalize_google_sheets_rows_preserves_opaque_category_title_urls() -> None:
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

    assert [row.title for row in canonical_rows] == ["Product-management"]
    assert not drop_reasons


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


def test_canonicalize_google_sheets_rows_preserves_ambiguous_game_category_rows() -> None:
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

    assert [row.title for row in canonical_rows] == ["Product-management", "Vfx"]
    assert not drop_reasons


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
