from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts import js_security_audit


def _allowlist(tmp_path: Path, body: str = '{"allowlist": []}') -> Path:
    path = tmp_path / "npm-audit-allowlist.json"
    path.write_text(body, encoding="utf-8")
    return path


def _payload(vulns: dict[str, object]) -> str:
    return json.dumps({"vulnerabilities": vulns, "metadata": {}})


def test_missing_vulnerabilities_block_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(js_security_audit.SecurityAuditConfigError, match="vulnerabilities"):
        js_security_audit.assess_audit_payload({"metadata": {}}, set())


def test_malformed_record_fails_closed() -> None:
    with pytest.raises(js_security_audit.SecurityAuditConfigError, match="malformed"):
        js_security_audit.assess_audit_payload({"vulnerabilities": {"x": ["oops"]}}, set())


def test_severity_floor_skips_below_level() -> None:
    payload = {
        "vulnerabilities": {
            "low-pkg": {"severity": "low", "via": [{"id": "GHSA-low"}]},
        }
    }
    assert js_security_audit.assess_audit_payload(payload, set()) == []


def test_allowlisted_advisory_suppressed() -> None:
    payload = {
        "vulnerabilities": {
            "pkg": {"severity": "high", "via": [{"id": "GHSA-1"}]},
        }
    }
    assert js_security_audit.assess_audit_payload(payload, {"GHSA-1"}) == []


def test_partially_allowlisted_chain_still_fails() -> None:
    """A `via` mixing an accepted id with an unaccepted one must still fail."""
    payload = {
        "vulnerabilities": {
            "pkg": {"severity": "high", "via": [{"id": "GHSA-1"}, {"id": "GHSA-2"}]},
        }
    }
    findings = js_security_audit.assess_audit_payload(payload, {"GHSA-1"})
    assert len(findings) == 1
    assert "GHSA-2" in findings[0]


def test_transitive_effect_without_advisory_id_still_fails() -> None:
    payload = {
        "vulnerabilities": {
            "pkg": {"severity": "moderate", "via": ["chain-cause-pkg"]},
        }
    }
    findings = js_security_audit.assess_audit_payload(payload, set())
    assert findings == [
        "pkg [moderate] advisories: (transitive effect — no advisory id) fixAvailable: no"
    ]


def test_fix_available_rendering_variants() -> None:
    payload = {
        "vulnerabilities": {
            "a": {"severity": "high", "via": [{"id": "GHSA-a"}], "fixAvailable": True},
            "b": {
                "severity": "high",
                "via": [{"id": "GHSA-b"}],
                "fixAvailable": {"name": "knip", "version": "1.9.0"},
            },
            "c": {"severity": "high", "via": [{"id": "GHSA-c"}], "fixAvailable": False},
        }
    }
    findings = js_security_audit.assess_audit_payload(payload, set())
    assert "fixAvailable: yes" in findings[0]
    assert "fixAvailable: yes (knip@1.9.0)" in findings[1]
    assert "fixAvailable: no" in findings[2]


def test_unknown_severity_is_reported_not_dropped() -> None:
    payload = {"vulnerabilities": {"pkg": {"severity": "weird", "via": [{"id": "GHSA-x"}]}}}
    findings = js_security_audit.assess_audit_payload(payload, set())
    assert findings == ["pkg [unknown] advisories: GHSA-x fixAvailable: no"]


def test_run_audit_finds_vulnerability(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / ".tmp" / "security" / "npm-audit.json"
    allowlist = _allowlist(tmp_path)
    monkeypatch.setattr(js_security_audit, "LOCKFILE", tmp_path / "package-lock.json")
    monkeypatch.setattr(js_security_audit, "REPORT_PATH", report)
    monkeypatch.setattr(js_security_audit, "ALLOWLIST_PATH", allowlist)
    monkeypatch.setattr(js_security_audit, "npm_executable", lambda: "npm-fake")
    (tmp_path / "package-lock.json").write_text("{}", encoding="utf-8")

    stdout = _payload({"pkg": {"severity": "high", "via": [{"id": "GHSA-1"}]}})

    def fake_run(command, cwd, capture_output, check, text, encoding):  # noqa: ANN001
        return subprocess.CompletedProcess(command, 1, stdout=stdout, stderr="")

    monkeypatch.setattr(js_security_audit.subprocess, "run", fake_run)

    assert js_security_audit.run_audit() == 1
    assert json.loads(report.read_text(encoding="utf-8"))["vulnerabilities"]


def test_run_audit_clean_tree_exits_zero(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / ".tmp" / "security" / "npm-audit.json"
    allowlist = _allowlist(tmp_path)
    monkeypatch.setattr(js_security_audit, "LOCKFILE", tmp_path / "package-lock.json")
    monkeypatch.setattr(js_security_audit, "REPORT_PATH", report)
    monkeypatch.setattr(js_security_audit, "ALLOWLIST_PATH", allowlist)
    monkeypatch.setattr(js_security_audit, "npm_executable", lambda: "npm-fake")
    (tmp_path / "package-lock.json").write_text("{}", encoding="utf-8")

    def fake_run(command, cwd, capture_output, check, text, encoding):  # noqa: ANN001
        return subprocess.CompletedProcess(command, 0, stdout=_payload({}), stderr="")

    monkeypatch.setattr(js_security_audit.subprocess, "run", fake_run)

    assert js_security_audit.run_audit() == 0
    assert report.is_file()


def test_run_audit_non_json_output_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(js_security_audit, "LOCKFILE", tmp_path / "package-lock.json")
    monkeypatch.setattr(js_security_audit, "ALLOWLIST_PATH", _allowlist(tmp_path))
    monkeypatch.setattr(js_security_audit, "npm_executable", lambda: "npm-fake")
    (tmp_path / "package-lock.json").write_text("{}", encoding="utf-8")

    def fake_run(command, cwd, capture_output, check, text, encoding):  # noqa: ANN001
        return subprocess.CompletedProcess(command, 254, stdout="", stderr="ELOCK lost")

    monkeypatch.setattr(js_security_audit.subprocess, "run", fake_run)

    with pytest.raises(js_security_audit.SecurityAuditConfigError, match="ELOCK"):
        js_security_audit.run_audit()


def test_missing_lockfile_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(js_security_audit, "LOCKFILE", tmp_path / "missing-lock.json")
    with pytest.raises(js_security_audit.SecurityAuditConfigError, match="Missing"):
        js_security_audit.run_audit()


def test_committed_security_inputs_close_the_js_lane(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    dependabot = (repo_root / ".github" / "dependabot.yml").read_text(encoding="utf-8")
    lint_workflow = (repo_root / ".github" / "workflows" / "lint.yml").read_text(encoding="utf-8")

    assert package["scripts"]["security:js"] == "python scripts/js_security_audit.py"
    assert js_security_audit.load_allowlist(js_security_audit.ALLOWLIST_PATH) == []
    assert 'package-ecosystem: "npm"' in dependabot
    assert "npm run security:js" in lint_workflow
    # npm audit exits nonzero on findings; the gate must read the JSON payload
    # rather than trusting the exit code (severity floor + allowlist live here).
    assert "the gate decides from the" in (
        repo_root / "scripts" / "js_security_audit.py"
    ).read_text(encoding="utf-8")
