from __future__ import annotations

from src import jobs_fetcher as jf


def test_fingerprint_url_matches_greenhouse_public_host_variants() -> None:
    boards = "https://boards.greenhouse.io/wargamingen/jobs/6933589?gh_jid=6933589"
    job_boards = "https://job-boards.greenhouse.io/wargamingen/jobs/6933589"
    other_job = "https://job-boards.greenhouse.io/wargamingen/jobs/7650088"
    assert jf.fingerprint_url(boards) == jf.fingerprint_url(job_boards)
    assert jf.fingerprint_url(boards) != jf.fingerprint_url(other_job)


def test_deduplicate_jobs_merges_smartrecruiters_same_board_title_location_alias() -> None:
    now_iso = jf.now_iso()
    rows, stats = jf.deduplicate_jobs(
        [
            jf.canonicalize_job(
                {
                    "title": "Senior Technical Artist",
                    "company": "People Can Fly",
                    "city": "Montréal",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000082413749",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000082413749",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
            jf.canonicalize_job(
                {
                    "title": "Artiste Technique Senior / Senior Technical Artist",
                    "company": "People can Fly Studio",
                    "city": "Montréal",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000106807696",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000106807696",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
        ]
    )

    assert len(rows) == 1
    assert stats["mergedCount"] == 1
    assert stats["mergedBySecondaryKey"] == 1
    assert rows[0].sourceBundleCount == 2


def test_deduplicate_jobs_merges_static_greenhouse_apply_target_with_provider_row() -> None:
    now_iso = jf.now_iso()
    rows, stats = jf.deduplicate_jobs(
        [
            jf.canonicalize_job(
                {
                    "title": "Render Engineer (Unannounced project)",
                    "company": "Wargaming",
                    "city": "Prague",
                    "country": "Czechia",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://boards.greenhouse.io/wargamingen/jobs/6933589?gh_jid=6933589",
                    "sector": "Game",
                    "sourceJobId": "greenhouse:wargamingen:6933589",
                    "adapter": "greenhouse",
                },
                source="greenhouse_boards",
                fetched_at=now_iso,
            ),
            jf.canonicalize_job(
                {
                    "title": "Render Engineer (Unannounced project)",
                    "company": "Wargaming",
                    "city": "Prague",
                    "country": "Czechia",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://job-boards.greenhouse.io/wargamingen/jobs/6933589",
                    "sector": "Game",
                    "sourceJobId": (
                        "static:static:listing_url:https://wargaming.com/en/careers/:87d0b6e881"
                    ),
                    "adapter": "static",
                },
                source="static_source::static:listing_url:https://wargaming.com/en/careers/",
                fetched_at=now_iso,
            ),
        ]
    )

    assert len(rows) == 1
    assert stats["mergedCount"] == 1
    assert stats["mergedByPrimaryUrl"] == 1
    assert rows[0].sourceBundleCount == 2


def test_deduplicate_jobs_keeps_smartrecruiters_title_alias_with_different_location() -> None:
    now_iso = jf.now_iso()
    rows, stats = jf.deduplicate_jobs(
        [
            jf.canonicalize_job(
                {
                    "title": "Senior Technical Artist",
                    "company": "People Can Fly",
                    "city": "Montréal",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000082413749",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000082413749",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
            jf.canonicalize_job(
                {
                    "title": "Artiste Technique Senior / Senior Technical Artist",
                    "company": "People can Fly Studio",
                    "city": "Vancouver",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000106807696",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000106807696",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
        ]
    )

    assert len(rows) == 2
    assert stats["mergedCount"] == 0


def test_deduplicate_jobs_does_not_alias_plain_smartrecruiters_titles() -> None:
    now_iso = jf.now_iso()
    rows, stats = jf.deduplicate_jobs(
        [
            jf.canonicalize_job(
                {
                    "title": "Senior Technical Artist",
                    "company": "People Can Fly",
                    "city": "Montréal",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000082413749",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000082413749",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
            jf.canonicalize_job(
                {
                    "title": "Senior Technical Artist",
                    "company": "People can Fly Studio",
                    "city": "Montréal",
                    "country": "CA",
                    "workType": "Onsite",
                    "contractType": "Full-time",
                    "jobLink": "https://jobs.smartrecruiters.com/PeopleCanFly/744000106807696",
                    "sector": "Game",
                    "profession": "technical-artist",
                    "sourceJobId": "smartrecruiters:PeopleCanFly:744000106807696",
                },
                source="smartrecruiters_sources",
                fetched_at=now_iso,
            ),
        ]
    )

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
