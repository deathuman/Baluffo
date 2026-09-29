"""Focused tests for bounded bridge log tail routes."""

from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api

_LOG_ROUTE_CASES = [
    ("discovery-tail", "DISCOVERY_LOG_PATH", "/discovery/log"),
    ("fetcher-tail", "FETCHER_LOG_PATH", "/fetcher/log"),
]


def _write_log(api, path_attr: str, content: str) -> Path:
    log_path: Path = getattr(api, path_attr)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(content, encoding="utf-8", newline="\n")
    return log_path


def _read_log(api, route_path: str, query: dict[str, list[str]]) -> dict:
    handler = FakeHandler()
    result = handle_get(handler, api=api, path=route_path, query=query)
    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload: dict = handler.sent[-1]["payload"]
    return payload


def _progress_log(count: int) -> str:
    return (
        "\n".join(
            f"[2026-09-29T17:27:{index:02d}.000000+00:00] "
            f"GameDevMap active dry run careers recovery fetch wave 1: fetched {index * 25}/1515 pages."
            for index in range(count)
        )
        + "\n"
    )


def test_log_routes_support_bounded_tail_view(tmp_path: Path) -> None:
    content = "a" * 512 + "b" * 4096
    cases = [
        ("discovery-tail", "DISCOVERY_LOG_PATH", "/discovery/log"),
        ("fetcher-tail", "FETCHER_LOG_PATH", "/fetcher/log"),
    ]

    for case_id, path_attr, route_path in cases:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        log_path = getattr(api, path_attr)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(content, encoding="utf-8", newline="\n")

        handler = FakeHandler()
        result = handle_get(
            handler,
            api=api,
            path=route_path,
            query={"view": ["tail"], "limitChars": ["4096"]},
        )

        payload = handler.sent[-1]["payload"]
        assert result is True, case_id
        assert handler.sent[-1]["status"] == 200
        assert payload["text"] == "b" * 4096
        assert payload["offset"] == 512
        assert payload["nextOffset"] == len(content)
        assert payload["hasMore"] is True


def test_log_routes_bound_large_offset_reads(tmp_path: Path) -> None:
    content = "a" * (192 * 1024)
    cases = [
        ("discovery-offset", "DISCOVERY_LOG_PATH", "/discovery/log"),
        ("fetcher-offset", "FETCHER_LOG_PATH", "/fetcher/log"),
    ]

    for case_id, path_attr, route_path in cases:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        log_path = getattr(api, path_attr)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(content, encoding="utf-8", newline="\n")

        handler = FakeHandler()
        result = handle_get(
            handler,
            api=api,
            path=route_path,
            query={"offset": ["0"]},
        )

        payload = handler.sent[-1]["payload"]
        assert result is True, case_id
        assert handler.sent[-1]["status"] == 200
        assert len(payload["text"]) == 128 * 1024
        assert payload["offset"] == 0
        assert payload["nextOffset"] == 128 * 1024
        assert payload["hasMore"] is True


def test_log_routes_bound_stale_offset_reads(tmp_path: Path) -> None:
    content = "a" * 4096 + "b" * (192 * 1024)
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.FETCHER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    api.FETCHER_LOG_PATH.write_text(content, encoding="utf-8", newline="\n")

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/fetcher/log",
        query={"offset": ["4096"]},
    )

    payload = handler.sent[-1]["payload"]
    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert payload["text"] == "b" * (128 * 1024)
    assert payload["offset"] == 4096
    assert payload["nextOffset"] == 4096 + (128 * 1024)
    assert payload["hasMore"] is True


def test_log_routes_reject_unknown_view(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    handler = FakeHandler()
    result = handle_get(
        handler,
        api=api,
        path="/fetcher/log",
        query={"view": ["everything"]},
    )

    assert result is True
    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["ok"] is False
    assert handler.sent[-1]["payload"]["error"] == "unsupported log view: everything"


def test_tail_view_keeps_every_line_of_a_log_that_fits_the_window(tmp_path: Path) -> None:
    """Byte 0 is a line boundary, so a small log must not lose its first line."""
    for case_id, path_attr, route_path in _LOG_ROUTE_CASES:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        _write_log(api, path_attr, "line one\nline two\n")

        payload = _read_log(api, route_path, {"view": ["tail"]})

        assert payload["text"].splitlines() == ["line one", "line two"], case_id
        assert payload["offset"] == 0, case_id
        assert payload["nextOffset"] == len("line one\nline two\n"), case_id
        assert payload["hasMore"] is False, case_id


def test_tail_view_never_returns_a_partial_line(tmp_path: Path) -> None:
    """A sliding tail window must not hand the client half a log line.

    The admin page renders every returned line as its own row, so a window that
    starts mid-line produced a truncated row stamped with the browser clock
    instead of the log timestamp.
    """
    for case_id, path_attr, route_path in _LOG_ROUTE_CASES:
        store = FakeDesktopLocalDataStore()
        api = make_stub_bridge_api(tmp_path / case_id, store)
        log_path = _write_log(api, path_attr, _progress_log(120))
        content = log_path.read_text(encoding="utf-8")
        assert len(content.encode("utf-8")) > 4096, case_id

        for tick in range(4):
            with log_path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(
                    f"[2026-09-29T17:28:{tick:02d}.000000+00:00] "
                    f"GameDevMap active dry run careers recovery fetch wave 1: "
                    f"fetched {9000 + tick * 25}/1515 pages.\n"
                )

            payload = _read_log(api, route_path, {"view": ["tail"], "limitChars": ["4096"]})
            assert payload["text"], case_id
            assert payload["text"].endswith("\n"), case_id
            first_line = payload["text"].split("\n", 1)[0]
            assert first_line.startswith("[2026-09-29T17:"), (
                f"{case_id} tick {tick}: {first_line!r}"
            )
            assert "recovery fetch wave 1: fetched" in first_line, case_id
            assert all(
                line.startswith("[2026-09-29T17:") for line in payload["text"].split("\n") if line
            ), case_id


def test_tail_view_withholds_a_line_the_writer_has_not_finished(tmp_path: Path) -> None:
    """An unterminated trailing line is held back until its newline lands."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    log_path = _write_log(api, "DISCOVERY_LOG_PATH", _progress_log(120))

    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write("[2026-09-29T17:29:00.000000+00:00] wave 1: fetched 1515/1515 pa")

    payload = _read_log(api, "/discovery/log", {"view": ["tail"], "limitChars": ["4096"]})
    assert payload["text"].endswith("\n")
    assert "1515/1515 pa" not in payload["text"]
    assert payload["nextOffset"] == log_path.stat().st_size - len(
        "[2026-09-29T17:29:00.000000+00:00] wave 1: fetched 1515/1515 pa"
    )

    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write("ges.\n")

    resumed = _read_log(api, "/discovery/log", {"offset": [str(payload["nextOffset"])]})
    assert resumed["text"] == (
        "[2026-09-29T17:29:00.000000+00:00] wave 1: fetched 1515/1515 pages.\n"
    )
    assert resumed["offset"] == payload["nextOffset"]
    assert resumed["nextOffset"] == log_path.stat().st_size


def test_offset_view_withholds_a_line_the_writer_has_not_finished(tmp_path: Path) -> None:
    """The bounded offset view keeps nextOffset on a line boundary too."""
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    content = _progress_log(120)
    log_path = _write_log(api, "DISCOVERY_LOG_PATH", content)
    raw = content.encode("utf-8")
    cut = raw[:4096]
    assert b"\n" in cut and not cut.endswith(b"\n"), "fixture must cut mid-line"

    payload = _read_log(api, "/discovery/log", {"offset": ["0"], "limitChars": ["4096"]})
    assert payload["offset"] == 0
    assert payload["text"].endswith("\n")
    assert payload["nextOffset"] == cut.rfind(b"\n") + 1
    assert payload["text"] == raw[: payload["nextOffset"]].decode("utf-8")

    resumed = _read_log(api, "/discovery/log", {"offset": [str(payload["nextOffset"])]})
    assert resumed["text"] == raw[payload["nextOffset"] :].decode("utf-8")
    assert resumed["nextOffset"] == log_path.stat().st_size
