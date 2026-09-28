"""Serena resolution rules in ``scripts/ai_env_check.py``.

The check used to be a bare ``shutil.which("serena")``, which reports a hard failure
for a Serena whose MCP is connected and serving tool calls: both first-class clients
register it through ``uvx`` (tools/mcp/SERENA.md), and uvx resolves the tool per launch
so no ``serena`` executable ever lands on PATH. AGENTS.md requires checking the real
thing rather than a lookup that reports a working tool as missing, so the resolution
order and its failure paths are pinned here.
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


# Point the module at a scratch ROOT so no test ever reads or writes the repo's real
# opencode.json; the config file is the thing under test.
@pytest.fixture
def client_root(tmp_path, monkeypatch):
    monkeypatch.setattr(ai_env_check, "ROOT", tmp_path)
    return tmp_path


def _write_config(root: Path, command: list[str] | None, nested: bool = True) -> None:
    entry = {} if command is None else {"serena": {"command": command}}
    mcp = {"servers": entry} if nested else entry
    (root / ai_env_check.SERENA_CLIENT_CONFIG).write_text(
        json.dumps({"mcp": mcp}), encoding="utf-8"
    )


def test_uvx_prefix_keeps_flag_values_and_drops_subcommand() -> None:
    command = [
        "uvx",
        "-p",
        "3.13",
        "--from",
        "serena-agent@latest",
        "serena",
        "start-mcp-server",
        "--context",
        "ide",
        "--project-from-cwd",
    ]
    assert ai_env_check._uvx_tool_prefix(command) == [
        "uvx",
        "-p",
        "3.13",
        "--from",
        "serena-agent@latest",
        "serena",
    ]


def test_uvx_prefix_rejects_commands_with_no_bare_tool_name() -> None:
    # A bare launcher and a flag-only run both fail to name a tool to execute.
    assert ai_env_check._uvx_tool_prefix(["uvx"]) is None
    assert ai_env_check._uvx_tool_prefix(["uvx", "--quiet"]) is None


def test_uvx_probe_accepts_the_flat_mcp_layout(client_root, monkeypatch) -> None:
    """The committed config and locally normalised copies disagree on the nesting.

    `mcp.<name>` is what HEAD carries; `mcp.servers.<name>` is what a normalised local
    copy carries. Either must resolve, or the check flips between fail and warn purely
    on which copy is checked out.
    """
    _write_config(client_root, ["uvx", "--from", "serena-agent@latest", "serena"], nested=False)
    monkeypatch.setattr(ai_env_check.shutil, "which", lambda name: "uvx" if name == "uvx" else None)
    probe = ai_env_check._serena_uvx_probe()
    assert probe == ["uvx", "--from", "serena-agent@latest", "serena", "--version"]


def test_uvx_probe_is_none_when_config_has_no_serena_registration(client_root) -> None:
    _write_config(client_root, None)
    assert ai_env_check._serena_uvx_probe() is None


def test_uvx_probe_is_none_for_a_non_uvx_registration(client_root) -> None:
    # A PATH-style registration is the other branch's job; do not second-guess it.
    _write_config(client_root, ["serena", "start-mcp-server"])
    assert ai_env_check._serena_uvx_probe() is None


def test_uvx_probe_is_none_when_launcher_is_missing(client_root, monkeypatch) -> None:
    _write_config(client_root, ["uvx", "--from", "serena-agent@latest", "serena"])
    monkeypatch.setattr(ai_env_check.shutil, "which", lambda name: None)
    assert ai_env_check._serena_uvx_probe() is None


def test_check_reports_warn_not_fail_when_only_uvx_resolves(client_root, monkeypatch) -> None:
    """A reachable Serena must never be a hard failure: the MCP may be serving calls."""
    _write_config(
        client_root,
        [
            "uvx",
            "-p",
            "3.13",
            "--from",
            "serena-agent@latest",
            "serena",
            "start-mcp-server",
            "--project-from-cwd",
        ],
    )
    monkeypatch.setattr(ai_env_check.shutil, "which", lambda name: "uvx" if name == "uvx" else None)

    def fake_capture(*args: str, timeout: int = 15):
        # Asking for a version must never actually start the MCP server.
        assert args[-1] == "--version", args

        class Done:
            returncode = 0
            stdout = "Serena 1.7.0\n"
            stderr = ""

        return Done()

    monkeypatch.setattr(ai_env_check, "_capture", fake_capture)
    check = ai_env_check._check_serena(check_updates=False)

    assert check.status == "warn"
    assert "1.7.0" in check.detail
    assert "uvx" in check.detail


def test_check_still_fails_when_serena_is_genuinely_unresolvable(client_root, monkeypatch) -> None:
    """The fix must not soften the real failure: neither PATH nor uvx can find it."""
    _write_config(client_root, None)
    monkeypatch.setattr(ai_env_check.shutil, "which", lambda name: None)
    check = ai_env_check._check_serena(check_updates=False)

    assert check.status == "fail"
    assert "not resolvable" in check.detail
