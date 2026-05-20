from src import source_sync as sync
from src import source_sync_crypto
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


def test_legacy_embedded_packaged_key_still_decrypts(source_sync_test_root) -> None:
    private_key = "-----BEGIN RSA PRIVATE KEY-----\nlegacy\n-----END RSA PRIVATE KEY-----"
    private_key_enc = "bKHvpD87qt6f0rZng3KnVyg3N_HB3Q-Fpx4_rn5ftNBNaAQ2mSkyWcW9Gv_xT3YCmC1LQbKtzFIgJZOzsO0-kJpX1p8"
    source_sync_test_root.write_packaged_config(
        {
            "keyDerivation": "embedded",
            "embeddedKeyHint": "legacy-hint",
            "embeddedKeyVersion": "v1",
            "keySalt": "bGVnYWN5LWVtYmVkZGVkLXNhbHQ",
            "privateKeyPemEnc": private_key_enc,
            "privateKeyPem": "",
        }
    )

    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)

    assert cfg.packaged_config is not None
    assert cfg.packaged_config.private_key_pem == private_key
    assert sync.config_status(cfg)["ready"] is True
    assert (
        source_sync_crypto.decrypt_private_key_pem_for_embedded(
            private_key_enc,
            salt_b64="bGVnYWN5LWVtYmVkZGVkLXNhbHQ",
            app_id="123456",
            installation_id="999999",
            hint="legacy-hint",
            version="v1",
        )
        == private_key
    )
