import json
from pathlib import Path

from src import source_sync, source_sync_crypto
from src.app_version import APP_VERSION


def _write_packaged_sync_bundle_config(
    portable_root: Path, *, key_derivation: str = "embedded"
) -> Path:
    app_dir = portable_root / "ship" / "app"
    version_dir = app_dir / "versions" / APP_VERSION / "packaging"
    version_dir.mkdir(parents=True, exist_ok=True)
    (app_dir / "current.txt").write_text(f"{APP_VERSION}\n", encoding="utf-8")
    private_key_pem = "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n"
    salt_b64 = source_sync._base64url_encode(b"packaged-sync-rehearsal-salt")  # noqa: SLF001
    payload = {
        "schemaVersion": 1,
        "appId": "123456",
        "installationId": "999999",
        "repo": "owner/repo",
        "branch": "main",
        "path": "baluffo/source-sync.json",
        "allowedRepo": "owner/repo",
        "allowedBranch": "main",
        "allowedPathPrefix": "baluffo/source-sync.json",
    }
    if key_derivation == "embedded":
        payload.update(
            {
                "keyDerivation": "embedded",
                "embeddedKeyHint": "sync-smoke-hint",
                "embeddedKeyVersion": "v1",
                "keySalt": salt_b64,
                "privateKeyPemEnc": source_sync_crypto.encrypt_private_key_pem_for_embedded(
                    private_key_pem,
                    salt_b64=salt_b64,
                    app_id="123456",
                    installation_id="999999",
                    hint="sync-smoke-hint",
                    version="v1",
                ),
            }
        )
    else:
        payload.update(
            {
                "keyDerivation": "machine",
                "keySalt": salt_b64,
                "privateKeyPemEnc": source_sync.encrypt_private_key_pem(
                    private_key_pem,
                    salt_b64=salt_b64,
                    app_id="123456",
                    installation_id="999999",
                ),
            }
        )
    config_path = version_dir / "github-app-sync-config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path
