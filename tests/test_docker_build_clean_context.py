from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from scripts import docker_build_clean_context as clean_context


def _write_archive(path: Path) -> None:
    with tarfile.open(path, "w") as archive:
        data = b"FROM scratch\n"
        info = tarfile.TarInfo("Dockerfile")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))


def test_clean_context_build_archives_head_runs_docker_and_cleans(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    calls: list[list[str]] = []

    def runner(command, cwd):
        calls.append(list(command))
        assert cwd == repo.resolve()
        if command[0] == "git":
            output_arg = next(item for item in command if str(item).startswith("--output="))
            _write_archive(Path(str(output_arg).split("=", 1)[1]))

    context_dir = clean_context.build_from_clean_context(
        repo_root=repo,
        ref="HEAD",
        tag="ghcr.io/deathuman/baluffo:local",
        runner=runner,
    )

    assert calls[0][:3] == ["git", "archive", "--format=tar"]
    assert calls[0][-1] == "HEAD"
    assert calls[1][:4] == ["docker", "build", "--progress=plain", "-t"]
    assert calls[1][4] == "ghcr.io/deathuman/baluffo:local"
    assert not context_dir.exists()
    assert not (repo / ".tmp" / "docker-build-context-HEAD.tar").exists()


def test_clean_context_build_supports_custom_ref_tag_and_keep_context(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def runner(command, _cwd):
        if command[0] == "git":
            output_arg = next(item for item in command if str(item).startswith("--output="))
            _write_archive(Path(str(output_arg).split("=", 1)[1]))

    context_dir = clean_context.build_from_clean_context(
        repo_root=repo,
        ref="feature/test branch",
        tag="baluffo:test",
        keep_context=True,
        runner=runner,
    )

    assert context_dir.name == "docker-build-context-feature-test-branch"
    assert (context_dir / "Dockerfile").is_file()
    assert (repo / ".tmp" / "docker-build-context-feature-test-branch.tar").is_file()


def test_clean_context_cleanup_rejects_paths_outside_tmp(tmp_path: Path) -> None:
    tmp_root = tmp_path / ".tmp"
    tmp_root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("keep", encoding="utf-8")

    with pytest.raises(clean_context.CleanContextError):
        clean_context.remove_context_path(outside, tmp_root)

    assert outside.read_text(encoding="utf-8") == "keep"


def test_clean_context_extraction_rejects_archive_members_outside_context(tmp_path: Path) -> None:
    archive_path = tmp_path / "bad.tar"
    with tarfile.open(archive_path, "w") as archive:
        data = b"bad"
        info = tarfile.TarInfo("../escape.txt")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))

    with pytest.raises(clean_context.CleanContextError):
        clean_context.extract_tar_safely(archive_path, tmp_path / "context")
