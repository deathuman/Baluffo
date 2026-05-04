from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from src import source_sync as sync

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_source_sync.py"
SCHEMA = ROOT / "schemas" / "source-sync.schema.json"
FIXTURE = ROOT / "tests" / "fixtures" / "source-sync" / "valid-source-sync.json"


def _run_validator(
    snapshot_path: Path, *, schema_path: Path = SCHEMA
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--schema", str(schema_path), str(snapshot_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _base_snapshot() -> dict[str, object]:
    return {
        "schemaVersion": 2,
        "generatedAt": "2026-05-04T10:00:00+00:00",
        "source": {"name": "governance-fixture"},
        "active": [
            {
                "adapter": "static",
                "listing_url": "https://example.com/jobs",
            }
        ],
        "pending": [
            {
                "adapter": "teamtailor",
                "name": "Pending Example",
            }
        ],
    }


def test_schema_matches_current_canonical_shape() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == {
        "schemaVersion",
        "generatedAt",
        "source",
        "active",
        "pending",
    }
    assert schema["properties"]["schemaVersion"]["const"] == 2
    assert set(schema["properties"]["source"]["properties"]) == {"name"}
    assert schema["properties"]["source"]["required"] == ["name"]


def test_validator_accepts_representative_fixture() -> None:
    result = _run_validator(FIXTURE)
    assert result.returncode == 0
    assert f"{FIXTURE}: ok" in result.stdout


def test_github_api_version_constant_is_used_in_headers() -> None:
    headers = sync._github_json_headers("Bearer token")
    assert headers["X-GitHub-Api-Version"] == sync.GITHUB_API_VERSION


@pytest.mark.parametrize(
    ("mutation", "expected_message"),
    [
        (
            lambda snapshot: snapshot.update({"schemaVersion": 1}),
            "schemaVersion must be 2",
        ),
        (
            lambda snapshot: snapshot.update({"unexpected": True}),
            "unknown top-level key(s): unexpected",
        ),
        (
            lambda snapshot: snapshot["pending"].append(
                {
                    "adapter": "static",
                    "listing_url": "https://example.com/jobs",
                }
            ),
            "duplicate canonical source identity across active/pending",
        ),
    ],
)
def test_validator_rejects_invalid_snapshots(
    tmp_path: Path,
    mutation,
    expected_message: str,
) -> None:
    snapshot = _base_snapshot()
    mutation(snapshot)
    snapshot_path = tmp_path / "source-sync.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    result = _run_validator(snapshot_path)

    assert result.returncode == 1
    assert expected_message in result.stderr
