from __future__ import annotations

from pathlib import Path

from src.app_version import APP_VERSION

ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_dockerignore_excludes_local_runtime_and_secret_artifacts() -> None:
    content = _read(".dockerignore")

    for pattern in (
        ".git",
        ".github",
        ".tmp",
        "_out",
        "dist",
        "node_modules",
        ".venv/",
        ".venv/**",
        "**/.venv/**",
        "baluffo.config.local.json",
        "packaging/github-app-sync-config.json",
        "packaging/github-app-sync-config.localkey.json",
        "data/*",
        "*.db",
        "*.jsonl",
    ):
        assert pattern in content

    assert "!data/defaults/" in content
    assert "!data/contracts/" in content


def test_ghcr_workflow_builds_multi_arch_image_without_pr_push() -> None:
    content = _read(".github/workflows/build-container.yml")

    assert "IMAGE_NAME: ghcr.io/deathuman/baluffo" in content
    assert "docker/setup-qemu-action@v4" in content
    assert "docker/setup-buildx-action@v4" in content
    assert "docker/metadata-action@v6" in content
    assert "docker/build-push-action@v7" in content
    assert "platforms: linux/amd64,linux/arm64" in content
    assert "push: ${{ github.event_name != 'pull_request' }}" in content
    assert "type=raw,value=latest,enable={{is_default_branch}}" in content


def test_umbrel_metadata_uses_app_proxy_raw_lan_contract() -> None:
    store = _read("umbrel-app-store.yml")
    manifest = _read("deathuman-baluffo/umbrel-app.yml")
    compose = _read("deathuman-baluffo/docker-compose.yml")

    assert 'id: "deathuman"' in store
    assert "id: deathuman-baluffo" in manifest
    assert "manifestVersion: 1" in manifest
    assert "category: development" in manifest
    assert "port: 8877" in manifest
    assert f"image: ghcr.io/deathuman/baluffo:{APP_VERSION}" in compose
    assert "app_proxy:" in compose
    assert "APP_PORT: 8080" in compose
    assert 'PROXY_AUTH_ADD: "false"' in compose
    assert "${APP_DATA_DIR}/data:/data" in compose
