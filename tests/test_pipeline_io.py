import gzip
import json
import os
import time

from src.pipeline_io import (
    _STALE_TMP_AGE_SECONDS,
    serialize_rows_for_json,
    write_streamed_text_if_changed,
)


def test_unified_json_serialization_is_compact_and_valid() -> None:
    serialized = serialize_rows_for_json(
        [{"title": "Technical Artist", "company": "Orion Labs"}],
        ["title", "company"],
    )

    assert json.loads(serialized) == [{"title": "Technical Artist", "company": "Orion Labs"}]
    assert "\n" not in serialized
    assert ": " not in serialized


def test_streamed_write_matches_plain_text_and_skips_unchanged(tmp_path) -> None:
    target = tmp_path / "rows.json"

    def stream(handle) -> None:
        handle.write("[")
        for index in range(5):
            if index:
                handle.write(",")
            json.dump({"index": index}, handle, separators=(",", ":"))
        handle.write("]")

    assert write_streamed_text_if_changed(target, stream) is True
    expected = "".join(
        ["["]
        + [
            ("," if index else "") + json.dumps({"index": index}, separators=(",", ":"))
            for index in range(5)
        ]
        + ["]"]
    )
    assert target.read_text(encoding="utf-8") == expected
    assert write_streamed_text_if_changed(target, stream) is False  # unchanged → no rewrite


def test_streamed_write_gzip_backed_target(tmp_path, monkeypatch) -> None:
    # .gz storage mapping (default backend) must gzip-compress like the text writers.
    target = tmp_path / "rows.json.gz"

    def stream(handle) -> None:
        handle.write("[1,2,3]")

    assert write_streamed_text_if_changed(target, stream) is True
    with gzip.open(target, mode="rt", encoding="utf-8") as handle:
        assert handle.read() == "[1,2,3]"
    assert write_streamed_text_if_changed(target, stream) is False  # identical gz bytes → skip


def test_streamed_write_sweeps_stale_tmp_but_keeps_fresh(tmp_path) -> None:
    target = tmp_path / "rows.json.gz"
    stale = tmp_path / f"rows.json.gz.{os.getpid()}.deadbeef.tmp"
    fresh = tmp_path / f"rows.json.gz.{os.getpid()}.alivebeef.tmp"
    stale.write_text("stale", encoding="utf-8")
    fresh.write_text("fresh", encoding="utf-8")
    past = time.time() - (_STALE_TMP_AGE_SECONDS + 60)
    os.utime(stale, (past, past))

    def stream(handle) -> None:
        handle.write("[1,2,3]")

    write_streamed_text_if_changed(target, stream)
    assert not stale.exists()
    assert fresh.exists()
