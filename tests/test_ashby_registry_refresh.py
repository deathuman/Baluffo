import json

from src import ashby_registry_refresh as refresh


def _app_data_html(*, organization: str, postings: list[dict]) -> str:
    return (
        "<script>window.__appData = "
        + json.dumps(
            {"organization": {"name": organization}, "jobBoard": {"jobPostings": postings}}
        )
        + ";</script>"
    )


def test_refresh_active_ashby_registry_removes_empty_rows_and_adds_validated_curated_rows(
    tmp_path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "name": "Stale Co (Ashby)",
                    "studio": "Stale Co",
                    "adapter": "ashby",
                    "board_url": "https://jobs.ashbyhq.com/staleco/jobs",
                    "enabledByDefault": True,
                },
                {
                    "name": "Other Provider",
                    "studio": "Other Provider",
                    "adapter": "greenhouse",
                    "slug": "other-provider",
                },
            ]
        ),
        encoding="utf-8",
    )

    responses = {
        "https://jobs.ashbyhq.com/staleco": _app_data_html(organization="", postings=[]),
        "https://jobs.ashbyhq.com/liveco": _app_data_html(
            organization="Live Co",
            postings=[{"id": "abc", "title": "Senior Frontend Engineer"}],
        ),
    }

    def fake_fetch(url: str, timeout_s: int) -> str:
        return responses[url]

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[
            {
                "name": "Live Co (Ashby)",
                "studio": "Live Co",
                "board_url": "https://jobs.ashbyhq.com/liveco/jobs",
                "careersUrl": "https://jobs.ashbyhq.com/liveco",
                "enabledByDefault": True,
            }
        ],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = refresh.load_json_array(active_path, [])
    ashby_rows = [row for row in next_rows if row.get("adapter") == "ashby"]
    assert len(ashby_rows) == 1
    assert ashby_rows[0]["name"] == "Live Co (Ashby)"
    assert ashby_rows[0]["board_url"] == "https://jobs.ashbyhq.com/liveco"
    assert ashby_rows[0]["jobsFound"] == 1
    assert report["removedCount"] == 1
    assert report["addedCount"] == 1


def test_refresh_active_ashby_registry_keeps_live_existing_rows_and_normalizes_urls(
    tmp_path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "name": "thatgamecompany (Ashby)",
                    "studio": "thatgamecompany",
                    "adapter": "ashby",
                    "board_url": "https://jobs.ashbyhq.com/thatgamecompany/jobs",
                    "enabledByDefault": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert url == "https://jobs.ashbyhq.com/thatgamecompany"
        return _app_data_html(
            organization="thatgamecompany",
            postings=[
                {"id": "a", "title": "Senior Frontend Engineer"},
                {"id": "b", "title": "Product Designer"},
            ],
        )

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = refresh.load_json_array(active_path, [])
    assert next_rows[0]["board_url"] == "https://jobs.ashbyhq.com/thatgamecompany"
    assert next_rows[0]["jobsFound"] == 2
    assert report["configuredAfter"] == 1


def test_refresh_active_ashby_registry_rejects_irrelevant_discovery_rows(tmp_path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text("[]", encoding="utf-8")

    responses = {
        "https://jobs.ashbyhq.com/gamechanger": _app_data_html(
            organization="GameChanger",
            postings=[{"id": "a", "title": "Product Manager"}],
        ),
        "https://jobs.ashbyhq.com/level": _app_data_html(
            organization="Level",
            postings=[{"id": "b", "title": "Product Manager"}],
        ),
    }

    def fake_fetch(url: str, timeout_s: int) -> str:
        return responses[url]

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[],
        discovery_rows=[
            {
                "name": "GameChanger (Ashby)",
                "studio": "GameChanger",
                "board_url": "https://jobs.ashbyhq.com/gamechanger",
                "relevanceHint": "sports-tech",
            },
            {
                "name": "Level (Ashby)",
                "studio": "Level",
                "board_url": "https://jobs.ashbyhq.com/level",
            },
        ],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = refresh.load_json_array(active_path, [])
    ashby_rows = [row for row in next_rows if row.get("adapter") == "ashby"]
    assert [row["name"] for row in ashby_rows] == ["GameChanger (Ashby)"]
    assert report["configuredAfter"] == 1
    assert report["rejectedCount"] == 1
    assert report["rejectedCandidates"][0]["name"] == "Level (Ashby)"


def test_refresh_active_ashby_registry_keeps_live_existing_rows_even_if_not_newly_relevant(
    tmp_path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "name": "Improbable (Ashby)",
                    "studio": "Improbable",
                    "adapter": "ashby",
                    "board_url": "https://jobs.ashbyhq.com/improbable",
                    "enabledByDefault": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert url == "https://jobs.ashbyhq.com/improbable"
        return _app_data_html(
            organization="Improbable",
            postings=[{"id": "a", "title": "Treasury Manager"}],
        )

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = json.loads(active_path.read_text(encoding="utf-8"))
    ashby_rows = [row for row in next_rows if row.get("adapter") == "ashby"]
    assert [row["name"] for row in ashby_rows] == ["Improbable (Ashby)"]
    assert report["configuredAfter"] == 1
    assert report["rejectedCount"] == 0
