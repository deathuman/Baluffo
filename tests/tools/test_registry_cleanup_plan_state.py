"""Registry cleanup must not plan or apply from an unreadable registry."""

from __future__ import annotations

import gzip
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from tests.tools.registry_cleanup_plan_helpers import TOOL, _fixture, _run


def _load_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("registry_cleanup_plan_under_test", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("name", "payload"),
    [
        pytest.param("source-registry-active.json", b"{not json", id="plain-not-json"),
        pytest.param("source-registry-active.json", b"[3]", id="plain-non-row"),
        pytest.param(
            "source-registry-active.json.gz", gzip.compress(b"[]")[:-8], id="gzip-truncated"
        ),
        pytest.param(
            "source-registry-active.json.gz", gzip.compress(b'{"active": []}'), id="gzip-object"
        ),
    ],
)
def test_apply_refuses_an_unreadable_active_registry_without_writing(
    tmp_path: Path, name: str, payload: bytes
) -> None:
    """A corrupt active registry aborts before any backup or write.

    The lean ``.json.gz`` entrypoint is read in preference to the plain ``.json`` one, so
    a corrupt gzip must abort rather than fall through to the readable sibling.
    """
    data, verdicts, repoint_map = _fixture(tmp_path)
    active_path = data / name
    active_path.write_bytes(payload)
    before = active_path.read_bytes()
    backup_dir = tmp_path / "backup"
    module = _load_tool()

    with pytest.raises(SystemExit, match="active registry"):
        module.main(
            [
                "--data-dir",
                str(data),
                "--verdicts",
                str(verdicts),
                "--repoint-map",
                str(repoint_map),
                "--apply",
                "--expect-retire",
                "0",
                "--expect-repoint",
                "0",
                "--backup-dir",
                str(backup_dir),
            ]
        )

    assert active_path.read_bytes() == before
    assert not backup_dir.exists()


def test_a_gzipped_registry_is_planned_not_refused(tmp_path: Path) -> None:
    data, verdicts, _repoint_map = _fixture(tmp_path)
    plain = data / "source-registry-active.json"
    compressed = plain.with_name(plain.name + ".gz")
    compressed.write_bytes(gzip.compress(plain.read_bytes()))
    plain.unlink()

    run = _run("--data-dir", str(data), "--verdicts", str(verdicts))

    assert run.returncode == 0, run.stderr
    assert "json (.json.gz)" in run.stdout
    assert "'active': 4" in run.stdout
