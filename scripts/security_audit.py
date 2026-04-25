from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS_LOCK = ROOT / "requirements-lock.txt"
ALLOWLIST_PATH = ROOT / "tools" / "security" / "pip-audit-allowlist.json"
REPORT_PATH = ROOT / ".tmp" / "security" / "pip-audit.json"

REQUIRED_ALLOWLIST_FIELDS = ("id", "package", "reason", "owner", "review_by")


class SecurityAuditConfigError(ValueError):
    pass


def _parse_review_by(raw_value: object, entry_id: str) -> date:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise SecurityAuditConfigError(f"Allowlist entry {entry_id!r} needs review_by date.")
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise SecurityAuditConfigError(
            f"Allowlist entry {entry_id!r} has invalid review_by date {raw_value!r}."
        ) from exc


def _validate_allowlist_entry(raw_entry: object, index: int, today: date) -> str:
    if not isinstance(raw_entry, dict):
        raise SecurityAuditConfigError(f"Allowlist entry #{index} must be an object.")

    for field in REQUIRED_ALLOWLIST_FIELDS:
        value = raw_entry.get(field)
        if not isinstance(value, str) or not value.strip():
            raise SecurityAuditConfigError(f"Allowlist entry #{index} needs non-empty {field!r}.")

    entry_id = raw_entry["id"].strip()
    review_by = _parse_review_by(raw_entry["review_by"], entry_id)
    if review_by < today:
        raise SecurityAuditConfigError(
            f"Allowlist entry {entry_id!r} expired on {review_by.isoformat()}."
        )
    return entry_id


def load_allowlist(path: Path = ALLOWLIST_PATH, today: date | None = None) -> list[str]:
    today = today or datetime.now(UTC).date()
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SecurityAuditConfigError(f"Invalid JSON in {path}.") from exc

    if not isinstance(payload, dict):
        raise SecurityAuditConfigError(f"{path} must contain a JSON object.")

    raw_entries = payload.get("allowlist")
    if not isinstance(raw_entries, list):
        raise SecurityAuditConfigError(f"{path} must contain an allowlist array.")

    advisory_ids = [
        _validate_allowlist_entry(raw_entry, index, today)
        for index, raw_entry in enumerate(raw_entries, start=1)
    ]
    duplicates = sorted(
        {advisory_id for advisory_id in advisory_ids if advisory_ids.count(advisory_id) > 1}
    )
    if duplicates:
        raise SecurityAuditConfigError(
            f"Duplicate pip-audit allowlist advisory ids: {', '.join(duplicates)}."
        )
    return advisory_ids


def build_pip_audit_command(ignored_advisories: list[str]) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "pip_audit",
        "-r",
        str(REQUIREMENTS_LOCK),
        "--format",
        "json",
        "--output",
        str(REPORT_PATH),
    ]
    for advisory_id in sorted(ignored_advisories):
        command.extend(["--ignore-vuln", advisory_id])
    return command


def run_audit(ignored_advisories: list[str]) -> int:
    if not REQUIREMENTS_LOCK.is_file():
        raise SecurityAuditConfigError(f"Missing {REQUIREMENTS_LOCK}.")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    command = build_pip_audit_command(ignored_advisories)
    completed = subprocess.run(command, cwd=ROOT, check=False)  # noqa: S603
    print(f"pip-audit JSON report: {REPORT_PATH.relative_to(ROOT).as_posix()}")
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Baluffo Python dependency security audit.")
    parser.parse_args(argv)

    try:
        ignored_advisories = load_allowlist()
        return run_audit(ignored_advisories)
    except SecurityAuditConfigError as exc:
        print(f"security audit configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
