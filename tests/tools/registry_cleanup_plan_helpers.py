"""Shared fixtures for the registry cleanup plan tests.

The registry is matched by host because a registered URL and the URL that 404s are
routinely different paths on the same site; matching on the exact URL would miss every
drifted seed. A dry run must leave the registry byte-identical, and --apply must refuse
without the expected sizes and the probe's control assertion.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

TOOL = Path(__file__).resolve().parents[2] / "tools" / "registry_cleanup_plan.py"

ROWS = [
    {
        "id": "static:name:gone",
        "studio": "Gone Studio",
        "pages": ["https://www.gone.example/old-careers"],
    },
    {
        "id": "static:name:stale",
        "studio": "Stale Studio",
        "pages": ["https://stale.example/jobs"],
    },
    {
        "id": "static:name:moved",
        "studio": "Moved Studio",
        "pages": ["https://moved.example/careers/old"],
        "listing_url": "https://moved.example/careers/old",
        "careersUrl": "https://moved.example/careers/old",
    },
    {
        "id": "static:name:blocked",
        "studio": "Blocked Studio",
        "pages": ["https://blocked.example/careers"],
    },
]


CONTROL = {
    "url": "https://boards-api.greenhouse.io/v1/boards/bandainamco/jobs?content=false",
    "status": 200,
    "ok": True,
    "checkedAt": "2026-11-18T19:27:00+00:00",
}


def _verdict(
    registered: str,
    *,
    failing: str | None = None,
    root_status: int = 200,
    url_status: int = 404,
    verdict: str = "repoint",
    had_success: bool = True,
    control: dict[str, object] | None = CONTROL,
) -> dict[str, object]:
    url = failing or registered
    host = registered.split("//", 1)[-1].split("/", 1)[0]
    payload: dict[str, object] = {
        "name": f"static_source::static:listing_url:{registered}",
        "studio": "",
        "url": url,
        "urls": [url],
        "root": f"https://{host}/",
        "url_status": url_status,
        "root_status": root_status,
        "had_success": had_success,
        "verdict": verdict,
    }
    if control is not None:
        payload["control"] = control
    return payload


VERDICTS = [
    _verdict(
        "https://gone.example/careers",
        root_status=404,
        verdict="retire_dead_host",
    ),
    _verdict(
        "https://stale.example/jobs",
        root_status=200,
        url_status=404,
        verdict="retire_stale_seed",
        had_success=False,
    ),
    _verdict("https://moved.example/careers/old", failing="https://www.moved.example/careers/old;"),
    _verdict(
        "https://blocked.example/careers",
        root_status=403,
        url_status=403,
        verdict="retry",
    ),
]

REPOINT_MAP = {"https://www.moved.example/careers/old;": "https://moved.example/careers"}


def _write_registry(data: Path, rows: list[dict[str, Any]]) -> None:
    data.mkdir(parents=True, exist_ok=True)
    for bucket, bucket_rows in (("active", rows), ("pending", []), ("rejected", [])):
        (data / f"source-registry-{bucket}.json").write_text(
            json.dumps(bucket_rows, indent=2), encoding="utf-8"
        )


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    data = tmp_path / "data"
    _write_registry(data, ROWS)
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(json.dumps(VERDICTS, indent=2), encoding="utf-8")
    repoint_map = tmp_path / "repoint.json"
    repoint_map.write_text(json.dumps(REPOINT_MAP, indent=2), encoding="utf-8")
    return data, verdicts, repoint_map


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(TOOL), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )


def _active_bytes(data: Path) -> bytes:
    return (data / "source-registry-active.json").read_bytes()
