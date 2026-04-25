from __future__ import annotations

import json
from pathlib import Path

from src.bridge import diagnostic_events


class _Unserializable:
    pass


def test_build_bridge_event_uses_retained_schema_and_redacts_sensitive_keys() -> None:
    event = diagnostic_events.build_bridge_event(
        "INFO",
        "task_started",
        {
            "runId": "fetch_abc123",
            "owner_token": "owner-secret",
            "nested": {
                "apiKey": "api-secret",
                "desktopSessionId": "session-123",
            },
            "items": [{"password": "hidden"}, {"sessionId": "session-456"}],
            "empty": "",
            "none": None,
            "obj": _Unserializable(),
        },
        "2026-04-25T12:34:56+00:00",
    )

    assert event["schemaVersion"] == 1
    assert event["ts"] == "2026-04-25T12:34:56+00:00"
    assert event["level"] == "info"
    assert event["event"] == "task_started"
    assert event["message"] == "task_started"
    assert event["fields"]["runId"] == "fetch_abc123"
    assert event["fields"]["owner_token"] == "[redacted]"
    assert event["fields"]["nested"]["apiKey"] == "[redacted]"
    assert event["fields"]["nested"]["desktopSessionId"] == "session-123"
    assert event["fields"]["items"][0]["password"] == "[redacted]"
    assert event["fields"]["items"][1]["sessionId"] == "session-456"
    assert "empty" not in event["fields"]
    assert "none" not in event["fields"]
    assert "_Unserializable" in event["fields"]["obj"]


def test_append_and_read_bridge_events_round_trip_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    event = diagnostic_events.build_bridge_event(
        "warn",
        "sync_failed",
        {"runId": "sync_1"},
        "2026-04-25T12:35:00+00:00",
    )

    diagnostic_events.append_bridge_event(path, event)

    rows = diagnostic_events.read_bridge_events(path)
    assert rows == [event]
    assert json.loads(path.read_text(encoding="utf-8").strip()) == event


def test_prune_bridge_events_keeps_newest_valid_rows_and_drops_invalid_lines(
    tmp_path: Path,
) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    path.write_text(
        "\n".join(
            [
                "{not-json",
                json.dumps({"schemaVersion": 1, "event": "old"}),
                json.dumps({"schemaVersion": 1, "event": "middle"}),
                json.dumps({"schemaVersion": 1, "event": "new"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    diagnostic_events.prune_bridge_events(path, max_bytes=1, max_rows=2)

    rows = diagnostic_events.read_bridge_events(path, limit=10)
    assert [row["event"] for row in rows] == ["middle", "new"]
    assert "{not-json" not in path.read_text(encoding="utf-8")


def test_read_bridge_events_applies_limit(tmp_path: Path) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    for index in range(4):
        diagnostic_events.append_bridge_event(path, {"schemaVersion": 1, "event": f"event_{index}"})

    rows = diagnostic_events.read_bridge_events(path, limit=2)

    assert [row["event"] for row in rows] == ["event_2", "event_3"]
