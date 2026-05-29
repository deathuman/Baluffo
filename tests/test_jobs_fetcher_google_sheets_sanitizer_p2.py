from __future__ import annotations

from src import jobs_fetcher as jf


def test_canonicalize_google_sheets_rows_drops_category_labels_at_new_non_game_employers() -> None:
    rows = [
        {
            "sourceJobId": "sheet-ebay",
            "title": "Product-management",
            "company": "eBay",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://ebay.wd5.myworkdayjobs.com/apply/job/Austin/Product-Manager",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-blackrock",
            "title": "Software-development-&-engineering",
            "company": "BlackRock",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://blackrock.wd1.myworkdayjobs.com/BlackRock_Professional/job/NY",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-cadence",
            "title": "System-design",
            "company": "Cadence",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://cadence.wd1.myworkdayjobs.com/External_Careers/job/SAN-JOSE/Sr-Principal-Product-Engineer",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-valeo",
            "title": "System-design",
            "company": "Valeo",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://valeo.wd3.myworkdayjobs.com/valeo_jobs/job/Garching/Product-Technical-Manager",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-nasdaq",
            "title": "Software-development-&-engineering",
            "company": "Nasdaq",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://nasdaq.wd1.myworkdayjobs.com/jobs/Nasdaq-Careers/job/Stockholm/Senior-Software-Engineer",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-marvell",
            "title": "System-design",
            "company": "Marvell",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://marvell.wd1.myworkdayjobs.com/MarvellCareers/job/Santa-Clara-CA/System-Validation-Engineer",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-saxobank",
            "title": "Account-management",
            "company": "Saxo Bank",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://saxobank.wd3.myworkdayjobs.com/jobs/Copenhagen/Client-Vigilance-Manager",
            "sector": "Tech",
        },
        {
            "sourceJobId": "sheet-game-company",
            "title": "Product-management",
            "company": "Scopely",
            "city": "Remote",
            "country": "Unknown",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://jobs.smartrecruiters.com/Scopely/7440001234567-senior-product-manager",
            "sector": "Game",
        },
    ]

    canonical_rows, drop_reasons, _stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
    )

    assert drop_reasons["google_sheets_category_row"] == 7
    assert [row.title for row in canonical_rows] == ["Senior Product Manager"]
