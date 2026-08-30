from __future__ import annotations

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


def test_committed_security_inputs_close_python_security_caveats(repo_root: Path) -> None:
    requirements = (repo_root / "requirements.txt").read_text(encoding="utf-8")
    lock = (repo_root / "requirements-lock.txt").read_text(encoding="utf-8")
    advisory_ids = security_audit.load_allowlist(
        repo_root / "tools" / "security" / "pip-audit-allowlist.json",
        today=date(2026, 5, 21),
    )

    assert "Scrapy==2.16.0" in requirements
    assert "Twisted==26.4.0" in requirements
    assert "scrapy==2.16.0" in lock
    assert "twisted==26.4.0" in lock
    assert advisory_ids == []


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
    requirements.write_text("scrapy==2.15.0\n", encoding="utf-8")
    captured: dict[str, object] = {}

    def fake_run(command: list[str], cwd: Path, check: bool) -> subprocess.CompletedProcess:
        captured["command"] = command
        captured["cwd"] = cwd
        captured["check"] = check
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(security_audit, "ROOT", tmp_path)
    monkeypatch.setattr(security_audit, "REQUIREMENTS_LOCK", requirements)
    monkeypatch.setattr(security_audit, "REPORT_PATH", report)
    monkeypatch.setattr(security_audit.subprocess, "run", fake_run)

    assert security_audit.run_audit(["PYSEC-2026-1"]) == 1
    assert captured["cwd"] == tmp_path
    assert captured["check"] is False
    assert report.parent.is_dir()
