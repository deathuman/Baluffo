from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _call_route(tmp_path: Path, query: dict[str, list[str]] | None = None) -> dict:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    handler = FakeHandler()
    assert handle_get(
        handler,
        api=api,
        path="/ops/discovery-audit-artifacts",
        query=query or {},
    )
    assert handler.sent[-1]["status"] == 200
    return handler.sent[-1]["payload"]


def test_discovery_audit_artifacts_route_reports_known_missing_files(tmp_path: Path) -> None:
    payload = _call_route(tmp_path)

    assert payload["ok"] is True
    assert [row["name"] for row in payload["artifacts"]] == [
        "sheet-directory",
        "web-search",
        "gamedevmap",
        "gameprog",
        "gamesmap",
    ]
    assert all(row["exists"] is False for row in payload["artifacts"])
    assert all(row["warnings"] == ["missing"] for row in payload["artifacts"])
    assert all(str(row["relativePath"]).endswith(".json") for row in payload["artifacts"])


def test_discovery_audit_artifacts_route_summarizes_bounded_json(tmp_path: Path) -> None:
    artifact = tmp_path / "sheet-directory-discovery-audit.json"
    artifact.write_text(
        json.dumps(
            {
                "status": "ok",
                "cacheHit": True,
                "candidates": [{"secret": "do-not-expose"}],
                "failures": [{"url": "https://example.invalid"}],
                "summary": {"candidateCount": 1, "nested": {"raw": "hidden"}},
            }
        ),
        encoding="utf-8",
    )

    payload = _call_route(tmp_path)
    row = payload["artifacts"][0]

    assert row["exists"] is True
    assert row["relativePath"] == "sheet-directory-discovery-audit.json"
    assert row["pathDisplay"] == "data/sheet-directory-discovery-audit.json"
    assert row["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert row["topLevelKeys"] == ["cacheHit", "candidates", "failures", "status", "summary"]
    assert row["summary"]["cacheHit"] is True
    assert row["summary"]["candidatesCount"] == 1
    assert row["summary"]["failuresCount"] == 1
    assert row["summary"]["summary"] == {"candidateCount": 1}
    assert "do-not-expose" not in json.dumps(row)
    assert "https://example.invalid" not in json.dumps(row)


def test_discovery_audit_artifacts_route_cache_returns_copies_and_refreshes(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "sheet-directory-discovery-audit.json"
    artifact.write_text(json.dumps({"status": "ok"}), encoding="utf-8")

    first = _call_route(tmp_path)
    first["artifacts"][0]["summary"]["status"] = "mutated"
    second = _call_route(tmp_path)
    artifact.write_text(
        json.dumps({"status": "failed", "failures": [{"url": "https://hidden.invalid"}]}),
        encoding="utf-8",
    )
    third = _call_route(tmp_path)

    assert second["artifacts"][0]["summary"]["status"] == "ok"
    assert third["artifacts"][0]["summary"]["status"] == "failed"
    assert third["artifacts"][0]["summary"]["failuresCount"] == 1


def test_discovery_audit_artifacts_route_marks_malformed_json(tmp_path: Path) -> None:
    (tmp_path / "web-search-discovery-audit.json").write_text("{not-json", encoding="utf-8")

    payload = _call_route(tmp_path)
    row = payload["artifacts"][1]

    assert row["exists"] is True
    assert row["summary"] == {}
    assert "invalid_json" in row["warnings"]


def test_discovery_audit_artifacts_route_ignores_arbitrary_path_query(tmp_path: Path) -> None:
    secret = tmp_path / "secret.json"
    secret.write_text('{"secret":"should-not-leak"}', encoding="utf-8")

    payload = _call_route(tmp_path, {"path": ["../secret.json"]})

    serialized = json.dumps(payload)
    assert "secret.json" not in serialized
    assert "should-not-leak" not in serialized
    assert len(payload["artifacts"]) == 5
