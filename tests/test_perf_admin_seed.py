from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from scripts import perf_admin_seed


def _write(path: Path, payload: bytes = b"{}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def test_seed_volume_accepts_plain_and_gz_variants(tmp_path: Path) -> None:
    """A repo checkout stores some artifacts gzip-on-disk; both must land.

    Regression: the whitelist listed ``source-registry-active.json`` only, so a
    repo ``data/`` checkout (which keeps it as ``.json.gz``) silently under-seeded
    the benchmark volume.
    """
    source = tmp_path / "data"
    target = tmp_path / "seed"
    _write(source / "source-registry-active.json.gz", gzip.compress(b'[{"a":1}]'))
    _write(source / "jobs-fetch-report.json", b'{"runId":"x"}')

    result = perf_admin_seed.seed_volume(source, target)

    assert (target / "source-registry-active.json.gz").is_file()
    assert (target / "jobs-fetch-report.json").is_file()
    assert "source-registry-active.json.gz" in result["copied"]
    assert "jobs-fetch-report.json" in result["copied"]


def test_seed_volume_prefers_plain_file_over_gz(tmp_path: Path) -> None:
    source = tmp_path / "data"
    target = tmp_path / "seed"
    _write(source / "jobs-unified.json", b"plain")
    _write(source / "jobs-unified.json.gz", gzip.compress(b"gz"))

    result = perf_admin_seed.seed_volume(source, target)

    assert (target / "jobs-unified.json").read_bytes() == b"plain"
    assert "jobs-unified.json" in result["copied"]


def test_seed_volume_reports_missing_entries(tmp_path: Path) -> None:
    source = tmp_path / "data"
    target = tmp_path / "seed"
    _write(source / "jobs-fetch-report.json", b"{}")

    result = perf_admin_seed.seed_volume(source, target)

    assert result["filesCopied"] == 1
    assert "baluffo-runtime.db" in result["missing"]
    assert "jobs-fetch-report.json" not in result["missing"]


def test_seed_volume_fails_loudly_on_repo_root(tmp_path: Path) -> None:
    """Pointing at a repo root used to copy 0 files and print a success line."""
    repo_root = tmp_path / "repo"
    (repo_root / "data").mkdir(parents=True)
    (repo_root / "package.json").write_text("{}", encoding="utf-8")
    _write(repo_root / "data" / "jobs-fetch-report.json", b"{}")

    with pytest.raises(SystemExit) as excinfo:
        perf_admin_seed.seed_volume(repo_root, tmp_path / "seed")

    message = str(excinfo.value)
    assert "repo checkout root" in message
    assert "data" in message


def test_seed_volume_fails_loudly_when_nothing_matched(tmp_path: Path) -> None:
    source = tmp_path / "empty"
    source.mkdir()

    with pytest.raises(SystemExit) as excinfo:
        perf_admin_seed.seed_volume(source, tmp_path / "seed")

    assert "seeded 0 files" in str(excinfo.value)


def test_seed_volume_missing_source_dir_still_errors(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        perf_admin_seed.seed_volume(tmp_path / "nope", tmp_path / "seed")

    assert "source data dir not found" in str(excinfo.value)


def test_main_reports_count_and_notes_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "data"
    target = tmp_path / "seed"
    _write(source / "jobs-fetch-report.json", b"{}")

    exit_code = perf_admin_seed.main(["--from-volume-path", str(source), "--output", str(target)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "seeded 1 file(s)" in captured.out
    assert "whitelisted artifact(s) were absent" in captured.out


def test_seed_files_whitelist_has_no_explicit_gz_entries() -> None:
    """Plain logical names already try their gzip sibling automatically."""
    assert not [name for name in perf_admin_seed.SEED_FILES if name.endswith(".gz")]


def test_seed_volume_counts_gzip_fallback_once(tmp_path: Path) -> None:
    source = tmp_path / "data"
    target = tmp_path / "seed"
    _write(source / "jobs-unified.json.gz", gzip.compress(b"[]"))

    result = perf_admin_seed.seed_volume(source, target)

    assert result["filesCopied"] == 1
    assert result["copied"] == ["jobs-unified.json.gz"]
    assert [path.name for path in target.iterdir()] == ["jobs-unified.json.gz"]
