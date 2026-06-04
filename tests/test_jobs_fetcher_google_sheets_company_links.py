from __future__ import annotations

from src import jobs_fetcher as jf


def test_parse_google_sheets_csv_infers_company_from_linkedin_and_first_party_urls() -> None:
    csv_text = (
        "Company,Job Title,City,Country,Job Link\n"
        ",Senior Render Artist,Barcelona,ES,https://es.linkedin.com/jobs/view/senior-render-artist-at-scopely-4371673234\n"
        ",Technical Artist,Wroclaw,PL,https://techland.net/job-offers/technical-artist-41\n"
    )

    rows = jf.parse_google_sheets_csv(csv_text)

    assert len(rows) == 2
    assert rows[0]["company"] == "Scopely"
    assert rows[1]["company"] == "Techland"
