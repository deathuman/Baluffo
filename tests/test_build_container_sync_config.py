from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.build_container_sync_config import build_container_sync_config_from_secrets
from src import source_sync, source_sync_crypto

_PRIVATE_KEY = "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n"


def _write_secret(secret_dir: Path, name: str, value: str) -> None:
    secret_dir.mkdir(parents=True, exist_ok=True)
    (secret_dir / name).write_text(value, encoding="utf-8")


def test_container_sync_config_skips_when_secrets_missing_and_not_required(tmp_path: Path) -> None:
    output = tmp_path / "packaging" / "github-app-sync-config.json"

    result = build_container_sync_config_from_secrets(
        secret_dir=tmp_path / "secrets",
        output=output,
        require=False,
    )

    assert result is None
    assert not output.exists()


def test_container_sync_config_requires_complete_core_secrets(tmp_path: Path) -> None:
    secret_dir = tmp_path / "secrets"
    _write_secret(secret_dir, "BALUFFO_SYNC_BUILD_APP_ID", "123456")

    with pytest.raises(RuntimeError, match="Incomplete container source sync build secrets"):
        build_container_sync_config_from_secrets(
            secret_dir=secret_dir,
            output=tmp_path / "github-app-sync-config.json",
            require=True,
        )


def test_container_sync_config_require_fails_when_no_secrets_exist(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Missing container source sync build secrets"):
        build_container_sync_config_from_secrets(
            secret_dir=tmp_path / "secrets",
            output=tmp_path / "github-app-sync-config.json",
            require=True,
        )


def test_container_sync_config_writes_portable_embedded_config(tmp_path: Path) -> None:
    secret_dir = tmp_path / "secrets"
    output = tmp_path / "packaging" / "github-app-sync-config.json"
    _write_secret(secret_dir, "BALUFFO_SYNC_BUILD_APP_ID", "123456")
    _write_secret(secret_dir, "BALUFFO_SYNC_BUILD_INSTALLATION_ID", "999999")
    _write_secret(secret_dir, "BALUFFO_SYNC_BUILD_REPO", "owner/repo")
    _write_secret(
        secret_dir, "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM", _PRIVATE_KEY.replace("\n", "\\n")
    )

    result = build_container_sync_config_from_secrets(
        secret_dir=secret_dir,
        output=output,
        require=True,
    )

    assert result == output
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["keyDerivation"] == source_sync.KEY_DERIVATION_EMBEDDED
    assert payload["allowedRepo"] == "owner/repo"
    assert payload["allowedBranch"] == source_sync.DEFAULT_BRANCH
    assert payload["allowedPathPrefix"] == source_sync.DEFAULT_PATH
    assert "privateKeyPem" not in payload
    assert payload["privateKeyPemEnc"].startswith("v2.")
    assert (
        source_sync_crypto.decrypt_private_key_pem_for_embedded(
            payload["privateKeyPemEnc"],
            salt_b64=payload["keySalt"],
            app_id=payload["appId"],
            installation_id=payload["installationId"],
            hint=payload["embeddedKeyHint"],
            version=payload["embeddedKeyVersion"],
        )
        == _PRIVATE_KEY
    )


def test_container_sync_config_cli_runs_from_repo_root_without_secrets(
    repo_root: Path, tmp_path: Path
) -> None:
    output = tmp_path / "packaging" / "github-app-sync-config.json"

    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "scripts/build_container_sync_config.py",
            "--secret-dir",
            str(tmp_path / "secrets"),
            "--output",
            str(output),
        ],
        cwd=repo_root,
        capture_output=True,
        check=True,
        text=True,
    )

    assert "build secrets were not provided" in completed.stdout
    assert not output.exists()
