import json

from src import ashby_registry_refresh as refresh
from src.source_registry_io import load_json_array


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

    next_rows = load_json_array(active_path, [])
    ashby_rows = [row for row in next_rows if row.get("adapter") == "ashby"]
    assert [row["name"] for row in ashby_rows] == ["Improbable (Ashby)"]
    assert report["configuredAfter"] == 1
    assert report["rejectedCount"] == 0


def test_curated_rows_register_voodoo_on_its_ashby_board() -> None:
    """Voodoo moved off Lever; the curated row must point at the Ashby board.

    ``lever:account:voodoo`` 404s on both api.lever.co and jobs.lever.co, so a
    curated row carrying either Lever URL would re-register a dead board.
    """
    voodoo_rows = [
        row for row in refresh.CURATED_ASHBY_ROWS if "voodoo" in str(row.get("name")).lower()
    ]
    assert len(voodoo_rows) == 1, "Voodoo must be curated exactly once"
    row = voodoo_rows[0]
    assert row["board_url"] == "https://jobs.ashbyhq.com/voodoo"
    assert row["careersUrl"] == "https://jobs.ashbyhq.com/voodoo"
    assert "lever.co" not in row["board_url"]
    assert "lever.co" not in row["careersUrl"]
    assert row["studio"] == "Voodoo"


def test_refresh_adds_voodoo_ashby_board_and_leaves_dead_lever_row_alone(tmp_path) -> None:
    """The Ashby board is added; the dead Lever row is preserved, not retired.

    Retiring the dead Lever row is a separate registry-hygiene operation, so the
    refresh must not silently drop it as a side effect of adding the Ashby board.
    """
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "id": "lever:account:voodoo",
                    "name": "Voodoo (Lever)",
                    "studio": "Voodoo",
                    "adapter": "lever",
                    "account": "voodoo",
                    "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json",
                    "enabledByDefault": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert url == "https://jobs.ashbyhq.com/voodoo"
        return _app_data_html(
            organization="Voodoo",
            postings=[
                {"id": "13968523-e0f2-4cdb-81a1-4ac338bd5e0a", "title": "Technical Artist (AI)"},
                {"id": "second", "title": "Game Developer - Puzzle Games"},
            ],
        )

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[
            {
                "name": "Voodoo (Ashby)",
                "studio": "Voodoo",
                "board_url": "https://jobs.ashbyhq.com/voodoo",
                "careersUrl": "https://jobs.ashbyhq.com/voodoo",
                "enabledByDefault": True,
            }
        ],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = load_json_array(active_path, [])
    ids = [row["id"] for row in next_rows]
    assert "ashby:board_url:https://jobs.ashbyhq.com/voodoo" in ids
    assert "lever:account:voodoo" in ids

    voodoo_ashby = next(
        row for row in next_rows if row["id"] == "ashby:board_url:https://jobs.ashbyhq.com/voodoo"
    )
    assert voodoo_ashby["adapter"] == "ashby"
    assert voodoo_ashby["studio"] == "Voodoo"
    assert voodoo_ashby["board_url"] == "https://jobs.ashbyhq.com/voodoo"
    assert voodoo_ashby["jobsFound"] == 2
    assert report["addedCount"] == 1
    assert report["removedCount"] == 0


def test_refresh_keeps_persisted_rows_that_only_carry_the_board_url_in_their_id(
    tmp_path,
) -> None:
    """Persisted Ashby rows keep their board URL in ``id``, not ``board_url``.

    The registry stores a slim row projection, so a row read back from the active
    registry has ``id: ashby:board_url:<url>`` and no ``board_url`` field. Without
    resolving the URL from the id, every such row validates as
    ``invalid``/``missing board_url`` and the refresh silently deletes live boards
    -- observed dropping 9 working boards, including thatgamecompany's 40 jobs.
    """
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "id": "ashby:board_url:https://jobs.ashbyhq.com/thatgamecompany",
                    "name": "thatgamecompany (Ashby)",
                    "adapter": "ashby",
                    "studio": "thatgamecompany",
                    "registryState": "active",
                    "pendingReason": "",
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert url == "https://jobs.ashbyhq.com/thatgamecompany"
        return _app_data_html(
            organization="thatgamecompany",
            postings=[{"id": "a", "title": "3D Character Artist (Mid-Senior)"}],
        )

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = load_json_array(active_path, [])
    assert [row["id"] for row in next_rows] == [
        "ashby:board_url:https://jobs.ashbyhq.com/thatgamecompany"
    ]
    assert next_rows[0]["board_url"] == "https://jobs.ashbyhq.com/thatgamecompany"
    assert next_rows[0]["jobsFound"] == 1
    assert report["removedCount"] == 0
    assert report["addedCount"] == 0
    assert report["configuredAfter"] == 1
