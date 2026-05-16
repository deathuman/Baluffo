from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _status_payload() -> dict[str, object]:
    return {
        "currentVersion": "0.1.0",
        "latestVersion": "0.2.0",
        "updateAvailable": True,
        "availability": "available",
        "releaseNotesUrl": "https://example.com/releases/v0.2.0",
        "releaseNotesTitle": "Baluffo v0.2.0",
        "releaseNotesBody": "### Fixed\n- Notes",
        "releaseNotesPublishedAt": "2026-04-15T10:00:00Z",
        "releaseNotesHistory": [
            {
                "releaseNotesUrl": "https://example.com/releases/v0.2.0",
                "releaseNotesTitle": "Baluffo v0.2.0",
                "releaseNotesBody": "### Fixed\n- Notes",
                "releaseNotesPublishedAt": "2026-04-15T10:00:00Z",
                "releaseTag": "v0.2.0",
                "releaseVersion": "0.2.0",
            },
            {
                "releaseNotesUrl": "https://example.com/releases/v0.1.33",
                "releaseNotesTitle": "Baluffo v0.1.33",
                "releaseNotesBody": "### Added\n- Previous notes",
                "releaseNotesPublishedAt": "2026-04-14T10:00:00Z",
                "releaseTag": "v0.1.33",
                "releaseVersion": "0.1.33",
            },
        ],
    }


def test_update_status_passes_release_notes_history(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.get_update_status_payload = _status_payload
    handler = FakeHandler()

    assert handle_get(handler, api=api, path="/app/update-status", query={}) is True

    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["releaseNotesHistory"][1]["releaseVersion"] == "0.1.33"


def test_check_for_update_passes_release_notes_history(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.check_for_update = lambda **_kw: _status_payload()
    handler = FakeHandler()

    assert handle_post(handler, api=api, path="/app/check-for-update", payload={"force": True})

    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["releaseNotesHistory"][1]["releaseNotesBody"] == (
        "### Added\n- Previous notes"
    )
