import json
import shutil
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CODEX_TMP_ROOT = REPO_ROOT / ".codex-tmp-tests"


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture()
def codex_tmp_root() -> Path:
    CODEX_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    return CODEX_TMP_ROOT


@pytest.fixture()
def make_test_root(codex_tmp_root: Path):
    created: list[Path] = []

    def _make(prefix: str) -> Path:
        root = codex_tmp_root / f"{prefix}-{uuid.uuid4().hex}"
        root.mkdir(parents=True, exist_ok=True)
        created.append(root)
        return root

    yield _make

    for root in created:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def source_sync_test_root(make_test_root):
    """Temp root for source_sync tests: clears runtime state, provides config_path and env."""
    from src import source_sync as sync

    root = make_test_root("source-sync")
    config_path = root / "github-app-sync-config.json"
    env = {sync.PACKAGED_SYNC_CONFIG_ENV: str(config_path)}
    sync._clear_runtime_state()  # noqa: SLF001
    with sync._RATE_LIMIT_LOCK:  # noqa: SLF001
        sync._RATE_LIMIT_STATE["calls"] = []  # noqa: SLF001
        sync._RATE_LIMIT_STATE["strike"] = 0  # noqa: SLF001
        sync._RATE_LIMIT_STATE["until"] = None  # noqa: SLF001

    def write_packaged_config(payload: dict | None = None) -> None:
        base = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "owner/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
        }
        if payload:
            base.update(payload)
        config_path.write_text(json.dumps(base), encoding="utf-8")

    class _Root:
        pass

    out = _Root()
    out.root = root
    out.config_path = config_path
    out.env = env
    out.write_packaged_config = write_packaged_config
    yield out

    sync._clear_runtime_state()  # noqa: SLF001
