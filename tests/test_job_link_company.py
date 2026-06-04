from __future__ import annotations

from src.jobs.job_link_company import company_from_job_link


def test_company_from_job_link_extracts_supported_ats_hosts() -> None:
    cases = [
        (
            "smartrecruiters-jobs-subdomain",
            "https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839",
            "CDPROJEKTRED",
        ),
        (
            "smartrecruiters-www-subdomain",
            "https://www.smartrecruiters.com/Insomniac-Games/744000999",
            "Insomniac Games",
        ),
        (
            "smartrecruiters-query-string",
            "https://jobs.smartrecruiters.com/KPMGNederland/744000111?utm_source=linkedin",
            "Kpmgnederland",
        ),
        ("greenhouse-boards", "https://boards.greenhouse.io/hoyoverse/jobs/123", "Hoyoverse"),
        (
            "greenhouse-eu",
            "https://job-boards.eu.greenhouse.io/guerrilla-games/offer-456",
            "Guerrilla Games",
        ),
        ("greenhouse-job-boards", "https://job-boards.greenhouse.io/ubisoft/jobs/789", "Ubisoft"),
        ("lever", "https://jobs.lever.co/xsolla/58126ec2", "Xsolla"),
        ("lever-eu", "https://jobs.eu.lever.co/ubisoft/some-job-id", "Ubisoft"),
        ("workable", "https://apply.workable.com/followloop/j/97BEDE4761/", "Followloop"),
        (
            "ashby",
            "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679",
            "Stellarentertainment",
        ),
        (
            "workday-wd1",
            "https://xboxgaming.wd1.myworkdayjobs.com/en-US/External/job/Malm/",
            "Xboxgaming",
        ),
        (
            "workday-wd5",
            "https://ebay.wd5.myworkdayjobs.com/apply/job/Austin/Product-Manager",
            "Ebay",
        ),
        (
            "workday-multi-digit",
            "https://saxobank.wd3.myworkdayjobs.com/jobs/Copenhagen/Client-Vigilance-Manager",
            "Saxobank",
        ),
        ("bamboohr", "https://beamdog.bamboohr.com/jobs/view.php?id=42", "Beamdog"),
        (
            "breezy",
            "https://illfonic.breezy.hr/p/06c96306a484-associate-qa-coordinator",
            "Illfonic",
        ),
        (
            "teamtailor",
            "https://pushgaming.teamtailor.com/jobs/12345-senior-engineer",
            "Pushgaming",
        ),
        ("personio", "https://innogames.jobs.personio.de/xml?job_id=999", "Innogames"),
        (
            "himalayas",
            "https://himalayas.app/companies/canary-technologies/jobs/enterprise-strategic-account-executive",
            "Canary Technologies",
        ),
        (
            "shine",
            "https://www.shine.com/jobs/admin-assistant-for-ahmedabad-location/zecruiters-jobconnect-private-limited/18730117",
            "Zecruiters Jobconnect Private Limited",
        ),
        (
            "jazzhr",
            "https://lostboysinteractive.applytojob.com/apply/abc123",
            "Lostboysinteractive",
        ),
        (
            "recruitee",
            "https://focushomeinteractive.recruitee.com/o/senior-programmer",
            "Focushomeinteractive",
        ),
        ("trailing-slash", "https://jobs.smartrecruiters.com/Scopely/7440001/", "Scopely"),
        ("fragment", "https://jobs.lever.co/riotgames/abc123#description", "Riotgames"),
        (
            "title-casing",
            "https://boards.greenhouse.io/guerrilla-games/jobs/999",
            "Guerrilla Games",
        ),
        ("all-caps", "https://jobs.smartrecruiters.com/CDPR/74400012345", "CDPR"),
        (
            "linkedin-detail-company",
            "https://es.linkedin.com/jobs/view/senior-render-artist-at-scopely-4371673234",
            "Scopely",
        ),
        (
            "linkedin-hyphenated-company",
            "https://www.linkedin.com/jobs/view/senior-engineer-at-epic-games-4385809932",
            "Epic Games",
        ),
        (
            "activision-first-party",
            "https://careers.activision.com/job/R026491",
            "Activision",
        ),
        (
            "techland-first-party",
            "https://techland.net/job-offers/technical-artist-41",
            "Techland",
        ),
        (
            "wargaming-first-party",
            "https://wargaming.com/en/careers/vacancy_3389163_warsaw",
            "Wargaming",
        ),
        (
            "rockstar-first-party",
            "https://www.rockstargames.com/careers/openings/position/7488511003",
            "Rockstar Games",
        ),
        ("rovio-first-party", "https://www.rovio.com/open-positions", "Rovio"),
        ("believer-first-party", "https://believer.gg/jobs/7f038142", "Believer"),
        (
            "santa-monica-studio-first-party",
            "https://sms.playstation.com/careers/programming/sr-devops-engineer",
            "Santa Monica Studio",
        ),
    ]

    for case_id, url, expected in cases:
        assert company_from_job_link(url) == expected, case_id


def test_company_from_job_link_rejects_empty_malformed_and_unknown_hosts() -> None:
    cases = [
        ("empty", ""),
        ("blank", " "),
        ("malformed", "not-a-url"),
        ("unknown-host", "https://example.com/jobs/123"),
        ("workable-no-job-segment", "https://apply.workable.com/followloop/careers"),
        ("himalayas-not-companies", "https://himalayas.app/discover/jobs"),
        ("single-character", "https://x.wd1.myworkdayjobs.com/jobs"),
        (
            "uuid-company-token",
            "https://jobs.smartrecruiters.com/8615ea53-9992-489f-b2cd-38ede3434679/12345",
        ),
        ("non-ats-linkedin", "https://www.linkedin.com/jobs/view/senior-engineer-at-epic-games"),
        (
            "linkedin-search-redirect",
            "https://www.linkedin.com/jobs/chief-product-officer-jobs?trk=expired_jd_redirect",
        ),
        ("known-first-party-no-job-path", "https://techland.net/about"),
        ("unknown-bamboohr-style", "https://corp.example.com/careers/jobs/123"),
        ("shine-without-jobs-path", "https://www.shine.com/companies/listing"),
        ("short-shine-url", "https://www.shine.com/jobs/single-segment"),
    ]

    for case_id, url in cases:
        assert company_from_job_link(url) == "", case_id
