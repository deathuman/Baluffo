"""Basic Memory version-drift rules in ``scripts/ai_env_check.py``.

tools/mcp/BASIC_MEMORY.md requires the two launch shapes to stay on one version: every
first-class client runs ``uvx basic-memory@latest``, while the optional bare-command
examples run whatever ``uv tool install`` last put on PATH. They share one
``memory.db``, so a lagging install dies with ``Can't locate revision`` once the newer
shape migrates it — and each version looks fine when checked alone. These tests pin both
drift directions, the one-sided cases, and the warning-only contract.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import ai_env_check


class _Completed:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _done(stdout: str, returncode: int = 0) -> _Completed:
    return _Completed(stdout, returncode=returncode)


def _which(monkeypatch, *, uvx: bool, on_path: bool) -> None:
    def fake(name: str) -> str | None:
        if name == "uvx":
            return "uvx" if uvx else None
        return "basic-memory.exe" if on_path else None

    monkeypatch.setattr(ai_env_check.shutil, "which", fake)


def _versions(monkeypatch, *, uvx: str | None, on_path: str | None) -> None:
    """Answer the two probes by executable, so a swapped probe order cannot pass."""

    def fake(*args: str, timeout: int = 15):
        assert args[-1] == "--version", args
        resolved = uvx if Path(args[0]).name.lower().startswith("uvx") else on_path
        if resolved is None:
            return _done("", returncode=1)
        return _done(f"Basic Memory version: {resolved}\n")

    monkeypatch.setattr(ai_env_check, "_capture", fake)


def test_reports_ok_when_both_shapes_resolve_the_same_version(monkeypatch) -> None:
    _which(monkeypatch, uvx=True, on_path=True)
    _versions(monkeypatch, uvx="1.0.0", on_path="1.0.0")

    check = ai_env_check._check_basic_memory()

    assert check == ai_env_check.Check(
        "basic_memory", "ok", "1.0.0 via uvx and PATH; both launch shapes agree"
    )


def test_warns_when_the_path_install_lags_uvx(monkeypatch) -> None:
    """The failure the guide describes: uvx migrates, the stale install cannot start."""
    _which(monkeypatch, uvx=True, on_path=True)
    _versions(monkeypatch, uvx="1.0.0", on_path="0.24.1")

    check = ai_env_check._check_basic_memory()

    assert check.status == "warn"
    assert "uvx 1.0.0" in check.detail
    assert "PATH 0.24.1" in check.detail
    assert "memory.db" in check.detail
    assert ai_env_check.BASIC_MEMORY_REFRESH in check.detail


def test_warns_when_uvx_is_ahead_and_the_path_install_is_missing(monkeypatch) -> None:
    _which(monkeypatch, uvx=True, on_path=False)
    _versions(monkeypatch, uvx="1.0.0", on_path=None)

    check = ai_env_check._check_basic_memory()

    assert check.status == "warn"
    assert "1.0.0 via uvx only" in check.detail
    assert ai_env_check.BASIC_MEMORY_REFRESH in check.detail


def test_warns_when_only_the_path_install_resolves(monkeypatch) -> None:
    _which(monkeypatch, uvx=True, on_path=True)
    _versions(monkeypatch, uvx=None, on_path="1.0.0")

    check = ai_env_check._check_basic_memory()

    assert check.status == "warn"
    assert "PATH 1.0.0 only" in check.detail
    assert "did not report a version" in check.detail
    assert ai_env_check.BASIC_MEMORY_REFRESH in check.detail


def test_warns_when_uvx_is_not_on_path(monkeypatch) -> None:
    _which(monkeypatch, uvx=False, on_path=True)
    _versions(monkeypatch, uvx=None, on_path="1.0.0")

    check = ai_env_check._check_basic_memory()

    assert check.status == "warn"
    assert "`uvx` is not on PATH" in check.detail


def test_warns_rather_than_fails_when_nothing_resolves(monkeypatch) -> None:
    """Nothing is installed on a fresh machine; that is a setup gap, not a broken repo."""
    _which(monkeypatch, uvx=False, on_path=False)
    _versions(monkeypatch, uvx=None, on_path=None)

    check = ai_env_check._check_basic_memory()

    assert check.status == "warn"
    assert "neither uvx nor PATH resolves basic-memory" in check.detail
    assert ai_env_check.BASIC_MEMORY_REFRESH in check.detail


def test_trailing_zero_segments_are_not_drift(monkeypatch) -> None:
    _which(monkeypatch, uvx=True, on_path=True)
    _versions(monkeypatch, uvx="1.0.0", on_path="1.0")

    check = ai_env_check._check_basic_memory()

    assert check.status == "ok"


def test_row_is_wired_into_the_default_check_list(monkeypatch) -> None:
    for name in [attr for attr in vars(ai_env_check) if attr.startswith("_check_")]:
        if name != "_check_basic_memory":
            monkeypatch.setattr(
                ai_env_check, name, lambda *args, **kwargs: ai_env_check.Check("stub", "ok", "x")
            )
    _which(monkeypatch, uvx=True, on_path=True)
    _versions(monkeypatch, uvx="1.0.0", on_path="1.0.0")

    names = {check.name for check in ai_env_check._checks(smoke=False, check_updates=False)}

    assert "basic_memory" in names


@pytest.mark.parametrize(
    ("uvx", "on_path"),
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_row_survives_the_json_report_in_every_branch(monkeypatch, uvx, on_path) -> None:
    """--json serialises ``Check.__dict__``, and no branch may escalate to fail."""
    _which(monkeypatch, uvx=uvx, on_path=on_path)
    _versions(
        monkeypatch,
        uvx="1.0.0" if uvx else None,
        on_path="0.24.1" if on_path else None,
    )

    payload = json.loads(json.dumps(ai_env_check._check_basic_memory().__dict__))

    assert payload["name"] == "basic_memory"
    assert payload["status"] in {"ok", "warn"}
