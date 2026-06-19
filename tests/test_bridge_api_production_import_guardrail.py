from __future__ import annotations

from pathlib import Path

from tools.repo_health import repo_guardrails


def test_bridge_api_production_import_guard_allows_composition_modules(
    tmp_path: Path, monkeypatch
) -> None:
    bridge_root = tmp_path / "src" / "bridge"
    bridge_root.mkdir(parents=True)
    (bridge_root / "api.py").write_text(
        "class BridgeApi:\n    pass\n",
        encoding="utf-8",
    )
    (bridge_root / "bootstrap.py").write_text(
        "from src.bridge.api import BridgeApi\ndef build() -> BridgeApi:\n    return BridgeApi()\n",
        encoding="utf-8",
    )
    (bridge_root / "admin_entrypoint_api.py").write_text(
        "from src.bridge.api import BridgeApi\n"
        "def build_bridge_api() -> BridgeApi:\n"
        "    return BridgeApi()\n",
        encoding="utf-8",
    )
    (tmp_path / "src" / "admin_bridge.py").write_text(
        "from typing import Any\ndef build_bridge_api() -> Any:\n    return object()\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_production_bridge_api_imports() == []


def test_bridge_api_production_import_guard_rejects_non_composition_imports(
    tmp_path: Path, monkeypatch
) -> None:
    bridge_root = tmp_path / "src" / "bridge"
    bridge_root.mkdir(parents=True)
    (bridge_root / "api.py").write_text(
        "class BridgeApi:\n    pass\n",
        encoding="utf-8",
    )
    (tmp_path / "src" / "admin_bridge.py").write_text(
        "from src.bridge.api import BridgeApi\n"
        "def build_bridge_api() -> BridgeApi:\n"
        "    return BridgeApi()\n",
        encoding="utf-8",
    )
    (bridge_root / "server.py").write_text(
        "import src.bridge.api as bridge_api\n"
        "def make(api: bridge_api.BridgeApi) -> object:\n"
        "    return api\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_production_bridge_api_imports() == [
        "src/admin_bridge.py imports BridgeApi at lines [1]; production modules outside "
        "bridge composition must depend on narrow capability protocols.",
        "src/admin_bridge.py references BridgeApi at lines [2, 3]; production modules outside "
        "bridge composition must type against narrow capability protocols.",
        "src/bridge/server.py imports BridgeApi at lines [1]; production modules outside "
        "bridge composition must depend on narrow capability protocols.",
        "src/bridge/server.py references BridgeApi at lines [2]; production modules outside "
        "bridge composition must type against narrow capability protocols.",
    ]
