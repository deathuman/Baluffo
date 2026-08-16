import json
from pathlib import Path
from unittest import mock

from src.source_discovery import config as sd


def test_load_feed_recheck_seeds_reads_queue_rows(tmp_path: Path) -> None:
    queue_path = tmp_path / "discovery-feed-recheck-queue.json"
    queue_path.write_text(
        json.dumps(
            [
                {"studio": "Welevel", "name": "Welevel (Personio)", "feed_url": "https://x"},
                {"studio": "Welevel", "name": "Welevel (Personio)", "feed_url": "https://y"},
                {"studio": "Another Studio", "name": "Another (Personio)", "feed_url": "https://z"},
                "not-a-dict",
            ]
        ),
        encoding="utf-8",
    )
    with mock.patch.object(sd, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", queue_path):
        seeds = sd.load_feed_recheck_seeds()

    assert [seed["studio"] for seed in seeds] == ["Welevel", "Another Studio"]
    assert seeds[0]["nlPriority"] is False


def test_load_feed_recheck_seeds_tolerates_missing_or_invalid_queue(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with mock.patch.object(sd, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", missing):
        assert sd.load_feed_recheck_seeds() == []

    invalid = tmp_path / "invalid.json"
    invalid.write_text("not-json", encoding="utf-8")
    with mock.patch.object(sd, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", invalid):
        assert sd.load_feed_recheck_seeds() == []


def test_studio_seeds_with_feed_recheck_merges_and_dedupes(tmp_path: Path) -> None:
    queue_path = tmp_path / "discovery-feed-recheck-queue.json"
    queue_path.write_text(
        json.dumps([{"studio": "Brand New", "name": "Brand New", "feed_url": "https://x"}]),
        encoding="utf-8",
    )
    base = [{"studio": "Existing", "name": "Existing", "nlPriority": False}]
    with (
        mock.patch.object(sd, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", queue_path),
        mock.patch.object(sd, "STUDIO_SEEDS", base),
    ):
        seeds = sd.studio_seeds_with_feed_recheck()

    assert [seed["studio"] for seed in seeds] == ["Existing", "Brand New"]


def test_studio_seeds_with_feed_recheck_priority_puts_recheck_first(tmp_path: Path) -> None:
    queue_path = tmp_path / "discovery-feed-recheck-queue.json"
    queue_path.write_text(
        json.dumps([{"studio": "Welevel", "name": "Welevel", "feed_url": "https://x"}]),
        encoding="utf-8",
    )
    base = [{"studio": "Existing", "name": "Existing", "nlPriority": False}]
    with (
        mock.patch.object(sd, "DISCOVERY_FEED_RECHECK_QUEUE_PATH", queue_path),
        mock.patch.object(sd, "STUDIO_SEEDS", base),
    ):
        seeds = sd.studio_seeds_with_feed_recheck_priority()

    assert [seed["studio"] for seed in seeds] == ["Welevel", "Existing"]
