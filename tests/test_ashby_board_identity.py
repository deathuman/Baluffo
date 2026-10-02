"""Board identity for the Ashby registry refresh.

The registry persists a slim row projection, so a row read back from the active
registry carries its board URL only inside ``id``
(``ashby:board_url:<url>``). These tests pin how that URL is recovered, and the
case-insensitive candidate keying that stops one board producing two rows.
"""

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


def test_board_url_is_resolved_from_the_source_id_only_for_ashby_rows() -> None:
    """The id fallback must stay scoped; other adapters resolve to nothing."""
    assert (
        refresh._row_board_url(
            {"id": "ashby:board_url:https://jobs.ashbyhq.com/voodoo", "name": "Voodoo"}
        )
        == "https://jobs.ashbyhq.com/voodoo"
    )
    # A trailing /jobs segment normalizes away, matching an explicit board_url.
    assert (
        refresh._row_board_url({"id": "ashby:board_url:https://jobs.ashbyhq.com/voodoo/jobs"})
        == "https://jobs.ashbyhq.com/voodoo"
    )
    # Explicit fields win over the id.
    assert (
        refresh._row_board_url(
            {
                "id": "ashby:board_url:https://jobs.ashbyhq.com/stale",
                "board_url": "https://jobs.ashbyhq.com/live",
            }
        )
        == "https://jobs.ashbyhq.com/live"
    )
    assert refresh._row_board_url({"id": "greenhouse:slug:guerrillagames"}) == ""
    assert refresh._row_board_url({"id": "lever:account:voodoo"}) == ""
    assert refresh._row_board_url({"name": "Improbable (Ashby)"}) == ""
    assert refresh._row_board_url({}) == ""


def test_board_key_is_case_insensitive_while_the_fetched_url_is_not() -> None:
    """One board, one candidate key -- even when the curated URL capitalises it."""
    mixed = "https://jobs.ashbyhq.com/Joyteractive"
    lower = "https://jobs.ashbyhq.com/joyteractive"
    # The fetch URL keeps the caller's casing.
    assert refresh._normalize_board_url(mixed) == mixed
    # The key does not, so both spellings collapse to one candidate.
    assert refresh._board_key(mixed) == refresh._board_key(lower)
    assert refresh._board_key(mixed + "/jobs") == refresh._board_key(lower)


def test_refresh_does_not_write_two_rows_for_one_board_differing_only_by_case(
    tmp_path,
) -> None:
    """A curated mixed-case URL must merge into the persisted lowercase row.

    ``source_identity`` lowercases when building ``id``, so two rows keyed apart
    only by path case both produced the same ``id`` and double-counted the
    board's jobs.
    """
    active_path = tmp_path / "source-registry-active.json"
    report_path = tmp_path / "ashby-registry-refresh-report.json"
    active_path.write_text(
        json.dumps(
            [
                {
                    "id": "ashby:board_url:https://jobs.ashbyhq.com/joyteractive",
                    "name": "Joyteractive (Ashby)",
                    "adapter": "ashby",
                    "studio": "Joyteractive",
                    "registryState": "active",
                }
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch(url: str, timeout_s: int) -> str:
        assert url.lower() == "https://jobs.ashbyhq.com/joyteractive"
        return _app_data_html(
            organization="Joyteractive",
            postings=[{"id": "a", "title": "Studio Analyst"}],
        )

    report = refresh.refresh_active_ashby_registry(
        active_path=active_path,
        report_path=report_path,
        curated_rows=[
            {
                "name": "Joyteractive (Ashby)",
                "studio": "Joyteractive",
                "board_url": "https://jobs.ashbyhq.com/Joyteractive",
                "careersUrl": "https://jobs.ashbyhq.com/Joyteractive",
                "enabledByDefault": True,
            }
        ],
        discovery_rows=[],
        fetch_text=fake_fetch,
        timeout_s=5,
    )

    next_rows = load_json_array(active_path, [])
    assert len(next_rows) == 1, f"expected one row for the board, got {next_rows}"
    assert next_rows[0]["id"] == "ashby:board_url:https://jobs.ashbyhq.com/joyteractive"
    assert next_rows[0]["jobsFound"] == 1
    assert report["addedCount"] == 0
    assert report["removedCount"] == 0
    assert report["configuredAfter"] == 1
