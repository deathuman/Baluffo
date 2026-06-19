from __future__ import annotations

from pathlib import Path

from tools.repo_health import repo_guardrails


def test_bridge_server_bridge_api_guard_allows_protocol_types(tmp_path: Path, monkeypatch) -> None:
    server_root = tmp_path / "src" / "bridge" / "server"
    server_root.mkdir(parents=True)
    (server_root / "handler.py").write_text(
        "from typing import Protocol\n"
        "class ServerApi(Protocol):\n"
        "    def bridge_log(self, level: str, event: str) -> None: ...\n"
        "def make_handler(api: ServerApi) -> object:\n"
        "    return object()\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_server_bridge_api_imports() == []


def test_bridge_server_bridge_api_guard_rejects_bridge_api_typing(
    tmp_path: Path, monkeypatch
) -> None:
    server_root = tmp_path / "src" / "bridge" / "server"
    server_root.mkdir(parents=True)
    (server_root / "handler.py").write_text(
        "from src.bridge.api import BridgeApi\n"
        "def make_handler(api: BridgeApi) -> object:\n"
        "    return object()\n",
        encoding="utf-8",
    )
    (server_root / "httpd.py").write_text(
        "import src.bridge.api as bridge_api\n"
        "def run_http_server(api: bridge_api.BridgeApi) -> int:\n"
        "    return 0\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_server_bridge_api_imports() == [
        "src/bridge/server/handler.py imports BridgeApi at lines [1]; "
        "bridge server modules must depend on narrow capability protocols.",
        "src/bridge/server/handler.py references BridgeApi at lines [2]; "
        "bridge server modules must type against narrow capability protocols.",
        "src/bridge/server/httpd.py imports BridgeApi at lines [1]; "
        "bridge server modules must depend on narrow capability protocols.",
        "src/bridge/server/httpd.py references BridgeApi at lines [2]; "
        "bridge server modules must type against narrow capability protocols.",
    ]
