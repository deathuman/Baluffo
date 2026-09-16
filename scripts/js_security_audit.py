"""Run the Baluffo JavaScript (npm) dependency security audit.

Mirrors ``scripts/security_audit.py`` (the pip lane): ``npm audit --json`` runs
against the committed ``package-lock.json`` (dev transitives included — the
js-yaml/smol-toml/@humanfs advisories all rode tooling chains), findings at or
above the severity floor fail the gate unless their advisory ids are recorded
in the expiry-enforced allowlist (``tools/security/npm-audit-allowlist.json``),
and every run writes a JSON report to ``.tmp/security/npm-audit.json``.

The gate fails closed: a missing npm, an unparsable report, or malformed
advisory data exits 2 rather than passing the lane. Findings exit 1; a clean
tree exits 0. Wired into CI on PRs and main pushes via ``lint.yml``
(``npm run security:js``), and the ``.github/dependabot.yml`` ecosystem
registration makes Dependabot scan PR manifests and open update PRs so new
advisories surface at review time instead of only on release pushes.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    # Direct `python scripts/js_security_audit.py` runs put scripts/ on sys.path,
    # not the repo root; bootstrap it so the pip-lane config reuse imports.
    sys.path.insert(0, str(ROOT))

from scripts.security_audit import SecurityAuditConfigError, load_allowlist  # noqa: E402

LOCKFILE = ROOT / "package-lock.json"
ALLOWLIST_PATH = ROOT / "tools" / "security" / "npm-audit-allowlist.json"
REPORT_PATH = ROOT / ".tmp" / "security" / "npm-audit.json"

_SEVERITY_ORDER = {"low": 0, "moderate": 1, "high": 2, "critical": 3}
DEFAULT_AUDIT_LEVEL = "moderate"


def npm_executable() -> str:
    """Resolve npm for subprocess use (Windows needs the .cmd path)."""
    resolved = shutil.which("npm.cmd") or shutil.which("npm")
    if not resolved:
        raise SecurityAuditConfigError(
            "npm is not on PATH. Install Node.js (pinned at 25.8.0 across CI "
            "workflows) to run the JavaScript dependency security audit."
        )
    return resolved


def build_npm_audit_command(npm: str) -> list[str]:
    return [npm, "audit", "--json"]


def _advisory_ids(via: object) -> list[str]:
    """Advisory ids from an npm-audit ``via`` list (string entries are chain causes).

    npm omits ``id`` on some advisory records and carries the identifier only in
    ``url`` (observed live: cross-spawn 7.0.3 ReDoS reports ``url`` ending in
    ``GHSA-3xgq-45jj-v275`` with no ``id`` key). Without the URL fallback such a
    finding can never be allowlisted by its GHSA id — the documented acceptance
    remedy dead-ends (found by the 2026-09-16 red-team drill).
    """
    ids: list[str] = []
    seen: set[str] = set()
    if isinstance(via, list):
        for entry in via:
            if not isinstance(entry, dict):
                continue
            advisory_id = str(entry.get("id") or "").strip()
            if not advisory_id:
                tail = str(entry.get("url") or "").rstrip("/").rsplit("/", 1)[-1]
                if tail.upper().startswith(("GHSA-", "PYSEC-")):
                    advisory_id = tail
            if advisory_id and advisory_id not in seen:
                seen.add(advisory_id)
                ids.append(advisory_id)
    return ids


def _fix_text(fix_available: object) -> str:
    if fix_available is True:
        return "yes"
    if isinstance(fix_available, dict):
        return f"yes ({fix_available.get('name')}@{fix_available.get('version')})"
    if fix_available is False or fix_available is None:
        return "no"
    return str(fix_available)


def assess_audit_payload(
    payload: object,
    allowlisted: set[str],
    audit_level: str = DEFAULT_AUDIT_LEVEL,
) -> list[str]:
    """Return human-readable findings that survive allowlist and severity floor."""
    if not isinstance(payload, dict):
        raise SecurityAuditConfigError("npm audit JSON output was not an object.")
    vulnerabilities = payload.get("vulnerabilities")
    if vulnerabilities is None:
        raise SecurityAuditConfigError("npm audit JSON output is missing `vulnerabilities`.")

    floor_rank = _SEVERITY_ORDER[audit_level]
    findings: list[str] = []
    for name, record in sorted(vulnerabilities.items()):
        if not isinstance(record, dict):
            raise SecurityAuditConfigError(f"Vulnerability record for {name!r} is malformed.")
        severity = str(record.get("severity") or "")
        rank = _SEVERITY_ORDER.get(severity)
        if rank is not None and rank < floor_rank:
            continue
        advisory_ids = _advisory_ids(record.get("via"))
        surviving = [advisory for advisory in advisory_ids if advisory not in allowlisted]
        if advisory_ids and not surviving:
            continue
        severity_label = severity if severity in _SEVERITY_ORDER else "unknown"
        advisories_label = (
            ", ".join(advisory_ids) if advisory_ids else "(transitive effect — no advisory id)"
        )
        findings.append(
            f"{name} [{severity_label}] advisories: {advisories_label} "
            f"fixAvailable: {_fix_text(record.get('fixAvailable'))}"
        )
    return findings


def run_audit(audit_level: str = DEFAULT_AUDIT_LEVEL) -> int:
    if not LOCKFILE.is_file():
        raise SecurityAuditConfigError(f"Missing {LOCKFILE}.")

    allowlisted = set(load_allowlist(ALLOWLIST_PATH))
    npm = npm_executable()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    command = build_npm_audit_command(npm)
    completed = subprocess.run(  # noqa: S603
        command,
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
    )
    # npm exits nonzero when it finds vulnerabilities; the gate decides from the
    # JSON payload instead so the severity floor and allowlist stay in one place.
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        detail = (completed.stderr or completed.stdout or "").strip()[:400]
        raise SecurityAuditConfigError(
            f"npm audit did not return JSON (exit {completed.returncode}): {detail}"
        ) from exc

    REPORT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    findings = assess_audit_payload(payload, allowlisted, audit_level)
    if findings:
        print(
            f"npm dependency security audit FAILED: {len(findings)} finding(s) "
            f"at or above {audit_level}:"
        )
        for finding in findings:
            print(f"  - {finding}")
        print(f"Full npm audit JSON report: {REPORT_PATH.relative_to(ROOT).as_posix()}")
        print(
            "Accept knowingly via tools/security/npm-audit-allowlist.json "
            "(id/package/reason/owner/review_by)."
        )
        return 1
    print(
        f"npm dependency security audit passed (floor {audit_level}, allowlisted {len(allowlisted)})."
    )
    print(f"npm audit JSON report: {REPORT_PATH.relative_to(ROOT).as_posix()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Baluffo JavaScript (npm) dependency security audit."
    )
    parser.add_argument(
        "--audit-level",
        choices=sorted(_SEVERITY_ORDER),
        default=DEFAULT_AUDIT_LEVEL,
        help="Fail on advisories at or above this severity (default: moderate).",
    )
    args = parser.parse_args(argv)

    try:
        return run_audit(args.audit_level)
    except SecurityAuditConfigError as exc:
        print(f"security audit configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
