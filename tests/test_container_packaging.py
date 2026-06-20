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
        ".container-frontend",
        "_out",
        "dist",
        "docs/**",
        "tests/**",
        "tools/mcp/**",
        "node_modules",
        ".venv/",
        ".venv/**",
        "**/.venv/**",
        "**/.venv/lib64",
        "**/.venv/lib64/**",
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

    assert "push:" in content
    assert "branches: [main]" in content
    assert 'tags: ["v*"]' in content
    assert "workflow_dispatch:" in content
    assert content.count("paths-ignore:") == 2
    assert (
        "# Branch path filters do not apply to tag pushes. Version tags still publish." in content
    )
    for ignored_path in (
        "docs/**",
        "tests/**",
        "tools/mcp/**",
        "README.md",
        "CONTRIBUTING.md",
        "SECURITY.md",
        "AGENTS.md",
        "LICENSE",
    ):
        assert f'- "{ignored_path}"' in content
    assert "IMAGE_NAME: ghcr.io/deathuman/baluffo" in content
    assert "docker/setup-qemu-action@v4" in content
    assert "docker/setup-buildx-action@v4" in content
    assert "docker/metadata-action@v6" in content
    assert "docker/build-push-action@v7" in content
    assert "platforms: linux/amd64,linux/arm64" in content
    assert "push: ${{ github.event_name != 'pull_request' }}" in content
    assert "type=raw,value=latest,enable={{is_default_branch}}" in content
    assert (
        "BALUFFO_CONTAINER_REQUIRE_SYNC_CONFIG=${{ github.event_name != 'pull_request' }}"
        in content
    )
    assert "Missing BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM secret for container publish." in content
    assert "secret-files:" in content
    assert (
        "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM=${{ env.BALUFFO_CONTAINER_SYNC_PRIVATE_KEY_FILE }}"
        in content
    )
    for secret_name in (
        "BALUFFO_SYNC_BUILD_APP_ID",
        "BALUFFO_SYNC_BUILD_INSTALLATION_ID",
        "BALUFFO_SYNC_BUILD_REPO",
    ):
        assert f"{secret_name}=${{{{ secrets.{secret_name} }}}}" in content


def test_container_frontend_bundle_script_uses_esbuild_dev_dependency() -> None:
    package_json = _read("package.json")
    package_lock = _read("package-lock.json")
    script = _read("scripts/build_container_frontend.mjs")

    assert '"build:container-frontend": "node scripts/build_container_frontend.mjs"' in package_json
    assert '"esbuild": "^0.28.1"' in package_json
    assert '"node_modules/esbuild"' in package_lock
    assert "strip-import-query" in script
    assert "admin.html" in script
    assert "jobs.html" in script
    assert "saved.html" in script


def test_dockerfile_prepares_bind_mount_before_non_root_runtime() -> None:
    content = _read("Dockerfile")

    assert "# syntax=docker/dockerfile:" in content
    assert "FROM node:25.8-slim AS container-frontend" in content
    assert "RUN npm ci --ignore-scripts" in content
    assert "npm run build:container-frontend -- --out-dir /container-frontend" in content
    assert "COPY --from=container-frontend /container-frontend ./.container-frontend" in content
    assert "useradd --uid 1000 --gid baluffo" in content
    assert "src.container_entrypoint" in content
    assert "http://127.0.0.1:8080/app/ready" in content
    assert "http://127.0.0.1:8080/ops/health" not in content
    assert "USER baluffo" not in content
    assert "src.container_server" not in content.split("CMD", 1)[-1]


def test_dockerfile_generates_container_sync_config_from_buildkit_secrets() -> None:
    content = _read("Dockerfile")

    assert "ARG BALUFFO_CONTAINER_REQUIRE_SYNC_CONFIG=false" in content
    assert "scripts/build_container_sync_config.py --require" in content
    assert "COPY packaging/github-app-sync-config.json" not in content
    for secret_name in (
        "BALUFFO_SYNC_BUILD_APP_ID",
        "BALUFFO_SYNC_BUILD_INSTALLATION_ID",
        "BALUFFO_SYNC_BUILD_REPO",
        "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM",
    ):
        assert f"--mount=type=secret,id={secret_name},required=false" in content


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
    assert "ports:" not in compose
    assert '"8877:8080"' not in compose
    assert "${APP_DATA_DIR}/data:/data" in compose
