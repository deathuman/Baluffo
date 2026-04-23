"""Startup metric helpers behind the packaged smoke root facade."""

from __future__ import annotations

from typing import Any

from src.ship.startup_probe_policy import startup_metric_fields

root: Any | None = None


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.startup_metrics.root is not configured")
    return root


def fetch_startup_metrics(bridge_base_url: str, limit: int = 1000) -> list[dict[str, Any]]:
    deps = _root()
    metrics_payload = deps.fetch_json(
        f"{bridge_base_url}/desktop-local-data/startup-metrics?limit={int(limit)}"
    )
    rows = metrics_payload.get("rows") if isinstance(metrics_payload.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def startup_metric_launch_mode(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        if str(row.get("event") or "").strip() != "desktop_browser_launch_selected":
            continue
        fields = startup_metric_fields(row)
        return str(fields.get("mode") or "").strip().lower()
    return ""


def startup_metric_event_present(
    rows: list[dict[str, Any]],
    event: str,
    **expected_fields: object,
) -> bool:
    expected_event = str(event or "").strip()
    for row in rows:
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return True
    return False


def find_startup_metric_fields(
    rows: list[dict[str, Any]],
    event: str,
    **expected_fields: object,
) -> dict[str, Any] | None:
    expected_event = str(event or "").strip()
    for row in reversed(rows):
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return fields
    return None
