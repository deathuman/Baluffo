from __future__ import annotations

from src.jobs.job_link_company import company_from_job_link


class TestCompanyFromJobLink:
    def test_empty_url_returns_empty(self) -> None:
        assert company_from_job_link("") == ""
        assert company_from_job_link(" ") == ""

    def test_malformed_url_returns_empty(self) -> None:
        assert company_from_job_link("not-a-url") == ""

    def test_unknown_host_returns_empty(self) -> None:
        assert company_from_job_link("https://example.com/jobs/123") == ""

    # SmartRecruiters — pattern #1
    def test_smartrecruiters_jobs_subdomain(self) -> None:
        assert (
            company_from_job_link("https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839")
            == "CDPROJEKTRED"
        )

    def test_smartrecruiters_www_subdomain(self) -> None:
        assert (
            company_from_job_link("https://www.smartrecruiters.com/Insomniac-Games/744000999")
            == "Insomniac Games"
        )

    def test_smartrecruiters_with_query_string(self) -> None:
        assert (
            company_from_job_link(
                "https://jobs.smartrecruiters.com/KPMGNederland/744000111?utm_source=linkedin"
            )
            == "Kpmgnederland"
        )

    # Greenhouse — pattern #2
    def test_greenhouse_boards(self) -> None:
        assert (
            company_from_job_link("https://boards.greenhouse.io/hoyoverse/jobs/123") == "Hoyoverse"
        )

    # Greenhouse EU — pattern #3
    def test_greenhouse_eu(self) -> None:
        assert (
            company_from_job_link("https://job-boards.eu.greenhouse.io/guerrilla-games/offer-456")
            == "Guerrilla Games"
        )

    def test_greenhouse_job_boards(self) -> None:
        assert (
            company_from_job_link("https://job-boards.greenhouse.io/ubisoft/jobs/789") == "Ubisoft"
        )

    # Lever — pattern #4
    def test_lever(self) -> None:
        assert company_from_job_link("https://jobs.lever.co/xsolla/58126ec2") == "Xsolla"

    # Lever EU — pattern #5
    def test_lever_eu(self) -> None:
        assert company_from_job_link("https://jobs.eu.lever.co/ubisoft/some-job-id") == "Ubisoft"

    # Workable — pattern #6
    def test_workable(self) -> None:
        assert (
            company_from_job_link("https://apply.workable.com/followloop/j/97BEDE4761/")
            == "Followloop"
        )

    def test_workable_no_j_segment_returns_empty(self) -> None:
        assert company_from_job_link("https://apply.workable.com/followloop/careers") == ""

    # Ashby — pattern #7
    def test_ashby(self) -> None:
        assert (
            company_from_job_link(
                "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679"
            )
            == "Stellarentertainment"
        )

    # Workday — pattern #8
    def test_workday_wd1(self) -> None:
        assert (
            company_from_job_link(
                "https://xboxgaming.wd1.myworkdayjobs.com/en-US/External/job/Malm/"
            )
            == "Xboxgaming"
        )

    def test_workday_wd5(self) -> None:
        assert (
            company_from_job_link(
                "https://ebay.wd5.myworkdayjobs.com/apply/job/Austin/Product-Manager"
            )
            == "Ebay"
        )

    def test_workday_multi_digit_wd_number(self) -> None:
        assert (
            company_from_job_link(
                "https://saxobank.wd3.myworkdayjobs.com/jobs/Copenhagen/Client-Vigilance-Manager"
            )
            == "Saxobank"
        )

    # BambooHR — pattern #9
    def test_bamboohr(self) -> None:
        assert (
            company_from_job_link("https://beamdog.bamboohr.com/jobs/view.php?id=42") == "Beamdog"
        )

    # Breezy — pattern #10
    def test_breezy(self) -> None:
        assert (
            company_from_job_link(
                "https://illfonic.breezy.hr/p/06c96306a484-associate-qa-coordinator"
            )
            == "Illfonic"
        )

    # Teamtailor — pattern #11
    def test_teamtailor(self) -> None:
        assert (
            company_from_job_link("https://pushgaming.teamtailor.com/jobs/12345-senior-engineer")
            == "Pushgaming"
        )

    # Personio — pattern #12
    def test_personio(self) -> None:
        assert (
            company_from_job_link("https://innogames.jobs.personio.de/xml?job_id=999")
            == "Innogames"
        )

    # Himalayas — pattern #13
    def test_himalayas(self) -> None:
        assert (
            company_from_job_link(
                "https://himalayas.app/companies/canary-technologies/jobs/enterprise-strategic-account-executive"
            )
            == "Canary Technologies"
        )

    def test_himalayas_not_companies_path(self) -> None:
        assert company_from_job_link("https://himalayas.app/discover/jobs") == ""

    # Shine — pattern #14
    def test_shine(self) -> None:
        assert (
            company_from_job_link(
                "https://www.shine.com/jobs/admin-assistant-for-ahmedabad-location/zecruiters-jobconnect-private-limited/18730117"
            )
            == "Zecruiters Jobconnect Private Limited"
        )

    # JazzHR — pattern #15
    def test_jazzhr(self) -> None:
        assert (
            company_from_job_link("https://lostboysinteractive.applytojob.com/apply/abc123")
            == "Lostboysinteractive"
        )

    # Recruitee — pattern #16
    def test_recruitee(self) -> None:
        assert (
            company_from_job_link("https://focushomeinteractive.recruitee.com/o/senior-programmer")
            == "Focushomeinteractive"
        )


class TestCompanyFromJobLinkEdgeCases:
    def test_url_with_trailing_slash(self) -> None:
        assert (
            company_from_job_link("https://jobs.smartrecruiters.com/Scopely/7440001/") == "Scopely"
        )

    def test_url_with_fragment(self) -> None:
        assert (
            company_from_job_link("https://jobs.lever.co/riotgames/abc123#description")
            == "Riotgames"
        )

    def test_title_casing(self) -> None:
        assert (
            company_from_job_link("https://boards.greenhouse.io/guerrilla-games/jobs/999")
            == "Guerrilla Games"
        )

    def test_all_caps_preserved(self) -> None:
        assert company_from_job_link("https://jobs.smartrecruiters.com/CDPR/74400012345") == "CDPR"

    def test_single_character_rejected(self) -> None:
        assert company_from_job_link("https://x.wd1.myworkdayjobs.com/jobs") == ""

    def test_uuid_rejected(self) -> None:
        assert (
            company_from_job_link(
                "https://jobs.smartrecruiters.com/8615ea53-9992-489f-b2cd-38ede3434679/12345"
            )
            == ""
        )

    def test_non_ats_url_returns_empty(self) -> None:
        assert (
            company_from_job_link(
                "https://www.linkedin.com/jobs/view/senior-engineer-at-epic-games"
            )
            == ""
        )

    def test_unknown_subdomain_bamboohr_style_returns_empty(self) -> None:
        assert company_from_job_link("https://corp.example.com/careers/jobs/123") == ""

    def test_shine_without_jobs_path_returns_empty(self) -> None:
        assert company_from_job_link("https://www.shine.com/companies/listing") == ""

    def test_short_shine_url_returns_empty(self) -> None:
        assert company_from_job_link("https://www.shine.com/jobs/single-segment") == ""
