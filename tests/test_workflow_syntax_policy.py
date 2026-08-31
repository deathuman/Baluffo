from __future__ import annotations

import hashlib
import sys
import zipfile
from pathlib import Path

from tools.repo_health.workflow_syntax_policy import (
    CACHE_SUBDIR,
    _checksum_for,
    _find_workflow_files,
    _install_cached_binary,
    _platform_arch,
    _platform_os,
    _run_actionlint,
    asset_filename,
    check_workflow_syntax,
)


def _fake_actionlint(tmp_path: Path, *, fail: bool) -> Path:
    """A tiny executable that mimics actionlint's exit/output contract."""
    script = tmp_path / "actionlint"
    lines = ["#!/usr/bin/env python3", "import sys"]
    if fail:
        lines.append("print('workflow.yml:3:5: foo is not allowed')")
        lines.append("print('deploy.yml:9:1: missing permissions')")
        lines.append("sys.exit(1)")
    else:
        lines.append("sys.exit(0)")
    script.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script.chmod(0o755)
    return script


def _init_repo_with_workflow(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    workflows = repo / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "build.yml").write_text("name: Build\non: [push]\n", encoding="utf-8")
    return repo


def test_asset_filename_maps_all_supported_platforms() -> None:
    assert asset_filename(os_name="windows", arch="amd64") == "actionlint_1.7.12_windows_amd64.zip"
    assert asset_filename(os_name="linux", arch="amd64") == "actionlint_1.7.12_linux_amd64.tar.gz"
    assert asset_filename(os_name="linux", arch="arm64") == "actionlint_1.7.12_linux_arm64.tar.gz"
    assert asset_filename(os_name="darwin", arch="arm64") == "actionlint_1.7.12_darwin_arm64.tar.gz"


def test_platform_os_maps_darwin_and_arch_aliases() -> None:
    assert _platform_os() in ("windows", "linux", "darwin")
    assert _platform_arch() in ("amd64", "arm64")


def test_checksum_for_parses_published_format() -> None:
    text = (
        "abc123  actionlint_1.7.12_linux_amd64.tar.gz\n"
        "def456  actionlint_1.7.12_windows_amd64.zip\n"
    )
    assert _checksum_for(text, "actionlint_1.7.12_windows_amd64.zip") == "def456"
    assert _checksum_for(text, "actionlint_1.7.12_linux_amd64.tar.gz") == "abc123"


def test_checksum_for_missing_asset_returns_none() -> None:
    assert _checksum_for("abc  other_asset.zip\n", "actionlint_1.7.12_windows_amd64.zip") is None


def test_find_workflow_files_discovers_only_workflow_yml(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    (repo / ".github" / "workflows" / "notes.txt").write_text("x", encoding="utf-8")
    files = _find_workflow_files(repo)
    assert [path.name for path in files] == ["build.yml"]


def test_check_workflow_syntax_fails_when_workflows_dir_missing(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    failures = check_workflow_syntax(repo)
    assert len(failures) == 1
    assert "workflows directory is missing" in failures[0]


def test_check_workflow_syntax_fails_when_no_workflow_files(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / ".github" / "workflows").mkdir(parents=True)
    failures = check_workflow_syntax(repo)
    assert len(failures) == 1
    assert "no workflow files found" in failures[0]


def test_install_cached_binary_accepts_verified_zip(tmp_path: Path) -> None:
    cache_dir = tmp_path / CACHE_SUBDIR
    binary_name = "actionlint.exe"
    archive = tmp_path / "actionlint_1.7.12_windows_amd64.zip"
    payload = b"MZ fake windows binary"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr(binary_name, payload)

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    checksum_text = f"{digest}  actionlint_1.7.12_windows_amd64.zip\n"

    binary, error = _install_cached_binary(
        cache_dir,
        checksum_text,
        archive,
        os_name="windows",
        arch="amd64",
    )
    assert error is None
    assert binary is not None and binary.name == binary_name
    assert binary.read_bytes() == payload


def test_install_cached_binary_rejects_checksum_mismatch(tmp_path: Path) -> None:
    cache_dir = tmp_path / CACHE_SUBDIR
    archive = tmp_path / "actionlint_1.7.12_windows_amd64.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("actionlint.exe", b"tampered")

    binary, error = _install_cached_binary(
        cache_dir,
        "0" * 64 + "  actionlint_1.7.12_windows_amd64.zip\n",
        archive,
        os_name="windows",
        arch="amd64",
    )
    assert binary is None
    assert error is not None and "checksum mismatch" in error


def test_install_cached_binary_rejects_unlisted_asset(tmp_path: Path) -> None:
    cache_dir = tmp_path / CACHE_SUBDIR
    archive = tmp_path / "actionlint_1.7.12_linux_amd64.tar.gz"
    archive.write_bytes(b"not really a tarball")
    binary, error = _install_cached_binary(
        cache_dir,
        "abc  some_other_asset.zip\n",
        archive,
        os_name="linux",
        arch="amd64",
    )
    assert binary is None
    assert error is not None and "does not list" in error


def test_run_actionlint_passes_when_clean(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    fake = _fake_actionlint(tmp_path, fail=False)
    files = _find_workflow_files(repo)
    returncode, output = _run_actionlint([sys.executable, str(fake)], files, repo)
    assert returncode == 0
    assert output == ""


def test_run_actionlint_reports_issues(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    fake = _fake_actionlint(tmp_path, fail=True)
    files = _find_workflow_files(repo)
    returncode, output = _run_actionlint([sys.executable, str(fake)], files, repo)
    assert returncode == 1
    assert "workflow.yml:3:5: foo is not allowed" in output
    assert "deploy.yml:9:1: missing permissions" in output


def test_check_workflow_syntax_passes_with_clean_binary(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    fake = _fake_actionlint(tmp_path, fail=False)
    assert check_workflow_syntax(repo, actionlint_bin=[sys.executable, str(fake)]) == []


def test_check_workflow_syntax_reports_issues_via_binary(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    fake = _fake_actionlint(tmp_path, fail=True)
    failures = check_workflow_syntax(repo, actionlint_bin=[sys.executable, str(fake)])
    assert len(failures) == 1
    assert "2 issue(s)" in failures[0]
    assert "foo is not allowed" in failures[0]


def test_check_workflow_syntax_empty_output_reports_exit_code(tmp_path: Path) -> None:
    repo = _init_repo_with_workflow(tmp_path)
    script = tmp_path / "actionlint"
    script.write_text("#!/usr/bin/env python3\nimport sys\nsys.exit(3)\n", encoding="utf-8")
    script.chmod(0o755)
    failures = check_workflow_syntax(repo, actionlint_bin=[sys.executable, str(script)])
    assert len(failures) == 1
    assert "exited with code 3 without output" in failures[0]
