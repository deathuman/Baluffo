from __future__ import annotations

from src import jobs_fetcher as jf

_GAMESJOBSDIRECT_GUERRILLA_SOURCE = "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-8608_guerrilla-games?page=1"


def _job(*, source: str, title: str, city: str = "Amsterdam", country: str = "NL", link: str):
    return jf.canonicalize_job(
        {
            "title": title,
            "company": "Guerrilla Games",
            "city": city,
            "country": country,
            "locations": [{"city": city, "country": country}],
            "jobLink": link,
            "sector": "Game",
            "sourceJobId": link.rsplit("/", 1)[-1],
        },
        source=source,
        fetched_at="2026-05-03T00:00:00Z",
    )


def test_deduplicate_jobs_classifies_gracklehq_gamesjobsdirect_guerrilla_pair_as_known_mirror_pair() -> (
    None
):
    first = _job(
        source="gracklehq",
        title="Senior Foundational Tools Programmer",
        link="https://gracklehq.com/rd/guerrilla-senior-foundational-tools-programmer",
    )
    second = _job(
        source=_GAMESJOBSDIRECT_GUERRILLA_SOURCE,
        title="Senior Foundational Tools Programmer",
        link="https://www.gamesjobsdirect.com/job/senior-foundational-tools-programmer/12345",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert int(stats.get("mergedByKnownMirrorPair") or 0) == 1
    assert int(stats.get("mergedBySecondaryKey") or 0) == 0
    assert stats["collisionSamples"][0]["reason"] == "known_mirror_pair"
    assert stats["currentRunKnownMirrorPairDedupKeys"] == [rows[0].dedupKey]


def test_deduplicate_jobs_does_not_use_known_mirror_pair_when_title_differs() -> None:
    first = _job(
        source="gracklehq",
        title="Senior Foundational Tools Programmer",
        link="https://gracklehq.com/rd/guerrilla-senior-foundational-tools-programmer",
    )
    second = _job(
        source=_GAMESJOBSDIRECT_GUERRILLA_SOURCE,
        title="Senior Character Animator",
        link="https://www.gamesjobsdirect.com/job/senior-character-animator/12345",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
    assert int(stats.get("mergedByKnownMirrorPair") or 0) == 0
    assert rows[0].dedupKey != rows[1].dedupKey


def test_deduplicate_jobs_does_not_use_known_mirror_pair_when_location_differs() -> None:
    first = _job(
        source="gracklehq",
        title="Senior Foundational Tools Programmer",
        link="https://gracklehq.com/rd/guerrilla-senior-foundational-tools-programmer",
    )
    second = _job(
        source=_GAMESJOBSDIRECT_GUERRILLA_SOURCE,
        title="Senior Foundational Tools Programmer",
        city="Utrecht",
        country="NL",
        link="https://www.gamesjobsdirect.com/job/senior-foundational-tools-programmer/12345",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 2
    assert int(stats["mergedCount"]) == 0
    assert int(stats.get("mergedByKnownMirrorPair") or 0) == 0
    assert rows[0].dedupKey != rows[1].dedupKey


def test_deduplicate_jobs_keeps_other_secondary_key_pairs_on_existing_reason() -> None:
    first = _job(
        source="other-source-a",
        title="Senior Foundational Tools Programmer",
        link="https://jobs.example.com/a/12345",
    )
    second = _job(
        source="other-source-b",
        title="Senior Foundational Tools Programmer",
        link="https://jobs.example.com/b/67890",
    )
    assert first is not None
    assert second is not None

    rows, stats = jf.deduplicate_jobs([first, second])

    assert int(stats["outputCount"]) == 1
    assert int(stats["mergedCount"]) == 1
    assert int(stats.get("mergedByKnownMirrorPair") or 0) == 0
    assert int(stats.get("mergedBySecondaryKey") or 0) == 1
    assert stats["collisionSamples"][0]["reason"] == "secondary_key"
    assert rows[0].sourceBundleCount == 2
