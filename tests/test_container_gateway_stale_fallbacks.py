"""Gateway last-good fallback for diagnostic GET routes.

`/ops/storage-health` used to surface a bridge-side stall as a 504, which the
Admin page then read as "bridge offline". The gateway now serves the last good
reading, marked stale, instead of an error.
"""

from __future__ import annotations

import time

from src import container_gateway


def _reset_last_good() -> None:
    with container_gateway._LAST_GOOD_LOCK:
        container_gateway._LAST_GOOD_BODIES.clear()


def test_storage_health_is_served_stale_when_the_bridge_stalls() -> None:
    assert "/ops/storage-health" in container_gateway._LAST_GOOD_GET_ROUTES


def test_stale_last_good_payload_marks_the_reading_instead_of_failing() -> None:
    _reset_last_good()
    container_gateway._remember_last_good(
        "/ops/storage-health", b'{"ok": true, "storage": {"healthy": true}}'
    )
    try:
        payload = container_gateway._stale_last_good_payload("/ops/storage-health")
    finally:
        _reset_last_good()

    assert payload is not None
    assert payload["ok"] is True
    assert payload["storage"]["healthy"] is True
    assert payload["stale"] is True
    assert payload["gatewayFallback"] is True
    assert payload["staleAgeS"] >= 0


def test_stale_last_good_payload_expires_and_ignores_non_json() -> None:
    _reset_last_good()
    try:
        container_gateway._remember_last_good("/ops/storage-health", b"<html>nope</html>")
        assert container_gateway._stale_last_good_payload("/ops/storage-health") is None

        container_gateway._remember_last_good("/ops/storage-health", b'{"ok": true}')
        with container_gateway._LAST_GOOD_LOCK:
            container_gateway._LAST_GOOD_BODIES["/ops/storage-health"] = (
                time.monotonic() - container_gateway._LAST_GOOD_MAX_AGE_S - 1,
                b'{"ok": true}',
            )
        assert container_gateway._stale_last_good_payload("/ops/storage-health") is None
    finally:
        _reset_last_good()
