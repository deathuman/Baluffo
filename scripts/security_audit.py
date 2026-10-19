from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS_LOCK = ROOT / "requirements-lock.txt"
ALLOWLIST_PATH = ROOT / "tools" / "security" / "pip-audit-allowlist.json"
REPORT_PATH = ROOT / ".tmp" / "security" / "pip-audit.json"
RESOLUTION_REPORT_PATH = ROOT / ".tmp" / "security" / "container-resolution.json"
RESOLUTION_TARGET_DIR = ROOT / ".tmp" / "security" / "container-target"

# The audit has to describe the image, not the developer's machine. `Dockerfile`
# builds on `python:3.13-slim` (Debian, glibc) and runs
# `pip install -r requirements-lock.txt` there, so the lock is resolved against
# that target. These constants are pinned against the Dockerfile by
# `test_container_audit_target_matches_the_dockerfile` so a base-image move
# cannot leave the audit measuring a stale target.
CONTAINER_PYTHON_VERSION = "3.13"
CONTAINER_IMPLEMENTATION = "cp"
CONTAINER_ABI = "cp313"

# pip matches wheel tags exactly in cross-platform mode -- it does not expand
# `manylinux2014` to the newer manylinux tags the way an install on the image
# does -- so every tag the image can accept is listed.
CONTAINER_PLATFORMS = (
    "manylinux2014_x86_64",
    "manylinux_2_17_x86_64",
    "manylinux_2_24_x86_64",
    "manylinux_2_28_x86_64",
    "manylinux_2_34_x86_64",
    "manylinux_2_36_x86_64",
    "linux_x86_64",
)

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


def _canonical_name(name: str) -> str:
    """PEP 503 name, so `zope.interface` and `zope-interface` compare equal."""
    return re.sub(r"[-_.]+", "-", name).lower()


def lock_pins() -> dict[str, str]:
    """Return the canonical name -> version pins the lock declares."""
    pins: dict[str, str] = {}
    for line in REQUIREMENTS_LOCK.read_text(encoding="utf-8").splitlines():
        if not line or line[0].isspace() or line.startswith(("#", "-")):
            continue
        name, separator, version = line.partition("==")
        if not separator:
            continue
        pins[_canonical_name(name.strip())] = version.strip()
    return pins


def build_resolution_command() -> list[str]:
    """Resolve the lock for the container image's target instead of this OS."""
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--dry-run",
        "--ignore-installed",
        "--only-binary=:all:",
        "--python-version",
        CONTAINER_PYTHON_VERSION,
        "--implementation",
        CONTAINER_IMPLEMENTATION,
        "--abi",
        CONTAINER_ABI,
    ]
    for platform_tag in CONTAINER_PLATFORMS:
        command.extend(["--platform", platform_tag])
    command.extend(
        [
            "--target",
            str(RESOLUTION_TARGET_DIR),
            "--report",
            str(RESOLUTION_REPORT_PATH),
            "-r",
            str(REQUIREMENTS_LOCK),
        ]
    )
    return command


def _resolved_pins(report_path: Path) -> dict[str, str]:
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        _canonical_name(str(entry["metadata"]["name"])): str(entry["metadata"]["version"])
        for entry in payload.get("install", [])
    }


def verify_container_resolution() -> int:
    """Fail unless every lock pin resolves for the container's CPython/linux target.

    pip still evaluates `sys_platform` markers against the host in cross-platform
    mode, so a Windows-only transitive (pyinstaller's `pefile`) can show up in the
    report. Extras are therefore ignored: the audit reads the lock's own pins, and
    a *missing* pin is the failure that matters -- it names a dependency the image
    cannot install at the pinned version.

    Returns the number of verified pins.
    """
    pins = lock_pins()
    if not pins:
        raise SecurityAuditConfigError(f"{REQUIREMENTS_LOCK.name} declares no pinned dependencies.")

    RESOLUTION_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESOLUTION_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    if RESOLUTION_REPORT_PATH.exists():
        RESOLUTION_REPORT_PATH.unlink()
    completed = subprocess.run(  # noqa: S603
        build_resolution_command(), cwd=ROOT, check=False, capture_output=True, text=True
    )
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout).strip().splitlines()[-3:]
        target = f"CPython {CONTAINER_PYTHON_VERSION}, manylinux x86_64"
        raise SecurityAuditConfigError(
            f"the lock does not resolve for the container target ({target}):\n" + "\n".join(details)
        )

    resolved = _resolved_pins(RESOLUTION_REPORT_PATH)
    missing = sorted(set(pins) - set(resolved))
    shifted = {
        name: (pins[name], resolved[name])
        for name in set(pins) & set(resolved)
        if pins[name] != resolved[name]
    }
    if missing or shifted:
        problems = [
            f"- {name}=={pins[name]} has no distribution the image can install" for name in missing
        ]
        problems.extend(
            f"- {name}=={pinned} resolves as {actual}"
            for name, (pinned, actual) in sorted(shifted.items())
        )
        target = f"CPython {CONTAINER_PYTHON_VERSION}, manylinux x86_64"
        raise SecurityAuditConfigError(
            f"the lock does not resolve for the container target ({target}):\n"
            + "\n".join(problems)
        )
    return len(pins)


def build_pip_audit_command(ignored_advisories: list[str]) -> list[str]:
    # `--no-deps --disable-pip` audits the pins as declared instead of building a
    # venv here. Without them pip-audit installs the lock for the developer's OS
    # and Python -- on Windows that pulls pyinstaller's Windows-only transitive
    # deps into the report, and a machine on a newer interpreter resolves
    # differently again -- so the result would describe this laptop rather than
    # the image. The image is covered by `verify_container_resolution`.
    command = [
        sys.executable,
        "-m",
        "pip_audit",
        "-r",
        str(REQUIREMENTS_LOCK),
        "--no-deps",
        "--disable-pip",
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

    pinned = verify_container_resolution()
    print(
        f"container target: CPython {CONTAINER_PYTHON_VERSION}, manylinux x86_64 "
        f"({pinned} pinned packages resolve)"
    )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    command = build_pip_audit_command(ignored_advisories)
    completed = subprocess.run(command, cwd=ROOT, check=False)  # noqa: S603
    print(f"pip-audit JSON report: {REPORT_PATH.relative_to(ROOT).as_posix()}")
    return completed.returncode


def _ensure_pip_audit_available() -> None:
    """Fail fast with install guidance when pip-audit is missing (e.g. local gate)."""
    if importlib.util.find_spec("pip_audit") is None:
        raise SecurityAuditConfigError(
            "pip-audit is not installed. Install it with "
            "`python -m pip install pip-audit==2.10.0` to run the "
            "dependency security audit."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Baluffo Python dependency security audit.")
    parser.parse_args(argv)

    try:
        _ensure_pip_audit_available()
        ignored_advisories = load_allowlist()
        return run_audit(ignored_advisories)
    except SecurityAuditConfigError as exc:
        print(f"security audit configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
