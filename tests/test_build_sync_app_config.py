import pytest

from scripts.build_sync_app_config import (
    build_packaged_sync_payload,
    parse_args,
    write_packaged_sync_config,
)
from src import source_sync, source_sync_crypto

_PRIVATE_KEY = "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n"


def _payload_kwargs() -> dict[str, str]:
    return {
        "app_id": "123456",
        "installation_id": "999999",
        "repo": "owner/repo",
        "private_key_pem": _PRIVATE_KEY,
    }


def test_embedded_packaged_sync_payload_uses_v2_encrypted_key() -> None:
    salt_b64 = source_sync._base64url_encode(b"unit-test-build-salt")  # noqa: SLF001

    payload = build_packaged_sync_payload(
        **_payload_kwargs(),
        salt=salt_b64,
        key_derivation=source_sync.KEY_DERIVATION_EMBEDDED,
        embedded_key_hint="unit-hint",
        embedded_key_version="v1",
    )

    assert payload["keyDerivation"] == source_sync.KEY_DERIVATION_EMBEDDED
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


def test_packaged_sync_payload_defaults_to_embedded_derivation() -> None:
    salt_b64 = source_sync._base64url_encode(b"unit-test-default-salt")  # noqa: SLF001

    payload = build_packaged_sync_payload(
        **_payload_kwargs(),
        salt=salt_b64,
        embedded_key_hint="default-hint",
    )

    assert payload["keyDerivation"] == source_sync.KEY_DERIVATION_EMBEDDED
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


def test_plaintext_packaged_sync_payload_generation_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="Plaintext packaged sync config generation"):
        build_packaged_sync_payload(
            **_payload_kwargs(),
            plaintext=True,
        )

    with pytest.raises(RuntimeError, match="Plaintext packaged sync config generation"):
        build_packaged_sync_payload(
            **_payload_kwargs(),
            key_derivation=source_sync.KEY_DERIVATION_PLAINTEXT,
        )


def test_packaged_sync_writer_rejects_plaintext_private_key(tmp_path) -> None:
    payload = build_packaged_sync_payload(
        **_payload_kwargs(),
        embedded_key_hint="writer-guard",
    )
    payload["privateKeyPem"] = _PRIVATE_KEY

    with pytest.raises(RuntimeError, match="refuses plaintext privateKeyPem"):
        write_packaged_sync_config(tmp_path / "github-app-sync-config.json", payload)


def test_cli_key_derivation_defaults_to_embedded() -> None:
    args = parse_args(
        [
            "--app-id",
            "123456",
            "--installation-id",
            "999999",
            "--repo",
            "owner/repo",
            "--private-key",
            "key.pem",
        ]
    )

    assert args.key_derivation == source_sync.KEY_DERIVATION_EMBEDDED


def test_plaintext_cli_flag_is_not_supported() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--app-id",
                "123456",
                "--installation-id",
                "999999",
                "--repo",
                "owner/repo",
                "--private-key",
                "key.pem",
                "--plaintext",
            ]
        )
