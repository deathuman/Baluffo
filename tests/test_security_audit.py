from __future__ import annotations

import json
import re
import subprocess
from datetime import date
from pathlib import Path

from scripts import security_audit


def _write_allowlist(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_malformed_allowlist_entry_fails(tmp_path: Path) -> None:
    allowlist = tmp_path / "pip-audit-allowlist.json"
    _write_allowlist(
        allowlist,
        '{"allowlist": [{"id": "PYSEC-2026-1", "package": "scrapy"}]}',
    )

    try:
        security_audit.load_allowlist(allowlist, today=date(2026, 4, 25))
    except security_audit.SecurityAuditConfigError as exc:
        assert "reason" in str(exc)
    else:
        raise AssertionError("Malformed allowlist entry should fail.")


def test_expired_allowlist_entry_fails(tmp_path: Path) -> None:
    allowlist = tmp_path / "pip-audit-allowlist.json"
    _write_allowlist(
        allowlist,
        """
        {
          "allowlist": [
            {
              "id": "PYSEC-2026-1",
              "package": "scrapy",
              "reason": "Not reachable in Baluffo.",
              "owner": "@deathuman",
              "review_by": "2026-04-24"
            }
          ]
        }
        """,
    )

    try:
        security_audit.load_allowlist(allowlist, today=date(2026, 4, 25))
    except security_audit.SecurityAuditConfigError as exc:
        assert "expired on 2026-04-24" in str(exc)
    else:
        raise AssertionError("Expired allowlist entry should fail.")


def test_valid_allowlist_entries_become_ignore_args(tmp_path: Path, monkeypatch) -> None:
    requirements = tmp_path / "requirements-lock.txt"
    report = tmp_path / ".tmp" / "security" / "pip-audit.json"
    requirements.write_text("scrapy==2.15.0\n", encoding="utf-8")

    monkeypatch.setattr(security_audit, "REQUIREMENTS_LOCK", requirements)
    monkeypatch.setattr(security_audit, "REPORT_PATH", report)

    command = security_audit.build_pip_audit_command(["PYSEC-2026-2", "PYSEC-2026-1"])

    assert command[:5] == [
        security_audit.sys.executable,
        "-m",
        "pip_audit",
        "-r",
        str(requirements),
    ]
    assert command.count("--ignore-vuln") == 2
    assert command[-4:] == [
        "--ignore-vuln",
        "PYSEC-2026-1",
        "--ignore-vuln",
        "PYSEC-2026-2",
    ]
    # The audit must read the declared pins, not resolve the lock for this OS.
    assert "--no-deps" in command
    assert "--disable-pip" in command


def test_resolution_command_targets_the_container_platform() -> None:
    """The lock is resolved for the image's CPython/linux target, not this machine."""
    command = security_audit.build_resolution_command()

    assert command[1:3] == ["-m", "pip"]
    assert command[command.index("--python-version") + 1] == security_audit.CONTAINER_PYTHON_VERSION
    assert command[command.index("--implementation") + 1] == security_audit.CONTAINER_IMPLEMENTATION
    assert command[command.index("--abi") + 1] == security_audit.CONTAINER_ABI
    platforms = [command[index + 1] for index, part in enumerate(command) if part == "--platform"]
    assert platforms == list(security_audit.CONTAINER_PLATFORMS)
    assert "--only-binary=:all:" in command
    assert "--dry-run" in command
    assert str(security_audit.REQUIREMENTS_LOCK) in command


def test_container_audit_target_matches_the_dockerfile(repo_root: Path) -> None:
    """A base-image move has to fail here instead of auditing a stale target."""
    dockerfile = (repo_root / "Dockerfile").read_text(encoding="utf-8")
    base = re.search(r"^FROM python:(\d+\.\d+)-slim", dockerfile, re.MULTILINE)
    assert base, "expected a `FROM python:X.Y-slim` runtime stage in the Dockerfile"
    version = base.group(1)
    assert security_audit.CONTAINER_PYTHON_VERSION == version
    assert security_audit.CONTAINER_IMPLEMENTATION == "cp"
    assert security_audit.CONTAINER_ABI == f"cp{version.replace('.', '')}"


def test_resolution_failure_names_the_pin_the_image_cannot_install(
    tmp_path: Path, monkeypatch
) -> None:
    requirements = tmp_path / "requirements-lock.txt"
    requirements.write_text("scrapy==2.19.0\npsutil==7.2.2\n", encoding="utf-8")
    resolution_report = tmp_path / ".tmp" / "security" / "container-resolution.json"

    def fake_run(command, cwd, check, **kwargs):
        resolution_report.parent.mkdir(parents=True, exist_ok=True)
        resolution_report.write_text(
            json.dumps({"install": [{"metadata": {"name": "scrapy", "version": "2.19.0"}}]}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(security_audit, "REQUIREMENTS_LOCK", requirements)
    monkeypatch.setattr(security_audit, "RESOLUTION_REPORT_PATH", resolution_report)
    monkeypatch.setattr(security_audit, "RESOLUTION_TARGET_DIR", tmp_path / ".tmp" / "target")
    monkeypatch.setattr(security_audit.subprocess, "run", fake_run)

    try:
        security_audit.verify_container_resolution()
    except security_audit.SecurityAuditConfigError as exc:
        assert "psutil==7.2.2" in str(exc)
        assert "manylinux x86_64" in str(exc)
    else:
        raise AssertionError("A pin the image cannot install must fail the audit.")


def test_committed_security_inputs_close_python_security_caveats(repo_root: Path) -> None:
    requirements = (repo_root / "requirements.txt").read_text(encoding="utf-8")
    lock = (repo_root / "requirements-lock.txt").read_text(encoding="utf-8")
    allowlist_path = repo_root / "tools" / "security" / "pip-audit-allowlist.json"
    advisory_ids = security_audit.load_allowlist(allowlist_path, today=date(2026, 10, 17))
    entries = json.loads(allowlist_path.read_text(encoding="utf-8"))["allowlist"]

    assert "Scrapy==2.19.0" in requirements
    assert "Twisted==26.4.0" in requirements
    assert "scrapy==2.19.0" in lock
    assert "twisted==26.4.0" in lock
    # Every allowlisted advisory is a Twisted finding whose affected component
    # (IMAP4 server, conch telnet, words XMPP/IRC, twisted.web, twisted.names) is
    # never instantiated: Twisted is only Scrapy's asyncio reactor here. None can be
    # upgraded away either -- 26.4.0 is PyPI's latest and the flagged version, and
    # the fixes exist only as the 26.10.0rc1/rc2 pre-releases. Pinning the exact id
    # set means any *other* advisory has to be dealt with by an upgrade.
    assert advisory_ids == [
        "CVE-2026-106454",
        "CVE-2026-115778",
        "CVE-2026-115774",
        "CVE-2026-115775",
        "CVE-2026-115776",
        "CVE-2026-115877",
        "CVE-2026-83813",
    ]
    assert [entry["package"] for entry in entries] == ["twisted"] * len(advisory_ids)


def test_missing_pip_audit_fails_with_install_guidance(monkeypatch) -> None:
    def fake_find_spec(name: str) -> None:
        return None

    monkeypatch.setattr(security_audit.importlib.util, "find_spec", fake_find_spec)

    try:
        security_audit._ensure_pip_audit_available()
    except security_audit.SecurityAuditConfigError as exc:
        assert "pip install pip-audit" in str(exc)
    else:
        raise AssertionError("Missing pip-audit should fail with install guidance.")


def test_precommit_config_registers_pip_audit_hook(repo_root: Path) -> None:
    config = (repo_root / ".pre-commit-config.yaml").read_text(encoding="utf-8")

    assert "id: pip-audit" in config
    assert "python scripts/security_audit.py" in config
    assert "requirements-lock" in config
    assert "pip-audit-allowlist" in config


def test_run_audit_propagates_pip_audit_exit_code(tmp_path: Path, monkeypatch) -> None:
    requirements = tmp_path / "requirements-lock.txt"
    report = tmp_path / ".tmp" / "security" / "pip-audit.json"
    resolution_report = tmp_path / ".tmp" / "security" / "container-resolution.json"
    requirements.write_text("scrapy==2.15.0\n", encoding="utf-8")
    captured: list[list[str]] = []

    def fake_run(command, cwd, check, **kwargs):
        captured.append(command)
        if command[1:3] == ["-m", "pip"]:
            resolution_report.parent.mkdir(parents=True, exist_ok=True)
            resolution_report.write_text(
                json.dumps({"install": [{"metadata": {"name": "scrapy", "version": "2.15.0"}}]}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0)
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(security_audit, "ROOT", tmp_path)
    monkeypatch.setattr(security_audit, "REQUIREMENTS_LOCK", requirements)
    monkeypatch.setattr(security_audit, "REPORT_PATH", report)
    monkeypatch.setattr(security_audit, "RESOLUTION_REPORT_PATH", resolution_report)
    monkeypatch.setattr(
        security_audit, "RESOLUTION_TARGET_DIR", tmp_path / ".tmp" / "security" / "container-target"
    )
    monkeypatch.setattr(security_audit.subprocess, "run", fake_run)

    assert security_audit.run_audit(["PYSEC-2026-1"]) == 1
    # Resolution first, so the audit cannot start from this machine's target.
    assert captured[0][1:3] == ["-m", "pip"]
    assert captured[1][1:3] == ["-m", "pip_audit"]
    assert report.parent.is_dir()
