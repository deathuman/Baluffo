from __future__ import annotations

import gzip
import json
from pathlib import Path

from scripts.audit_json_artifacts import audit_roots


def test_json_artifact_audit_reports_plain_policy_backed_json(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / "jobs-unified.json").write_text("[]", encoding="utf-8")
    (root / "jobs-fetch-report.json").write_text("{}", encoding="utf-8")

    report = audit_roots([root])

    assert report["ok"] is False
    assert report["violationCount"] == 1
    assert report["violations"] == [str(root / "jobs-unified.json")]


def test_json_artifact_audit_accepts_gzip_policy_backed_json(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    with gzip.open(root / "jobs-unified.json.gz", mode="wt", encoding="utf-8") as handle:
        json.dump([], handle)
    (root / "jobs-fetch-report.json").write_text("{}", encoding="utf-8")

    report = audit_roots([root])

    assert report["ok"] is True
    assert report["violationCount"] == 0
    assert report["violations"] == []
