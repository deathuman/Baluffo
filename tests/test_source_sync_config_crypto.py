import pytest

from src import source_sync as sync
from src import source_sync_crypto
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


def _write_embedded_v2_packaged_config(source_sync_test_root) -> str:
    private_key = "-----BEGIN RSA PRIVATE KEY-----\nembedded-v2\n-----END RSA PRIVATE KEY-----"
    salt_b64 = sync._base64url_encode(b"unit-test-salt-embedded-v2")  # noqa: SLF001
    private_key_enc = source_sync_crypto.encrypt_private_key_pem_for_embedded(
        private_key,
        salt_b64=salt_b64,
        app_id="123456",
        installation_id="999999",
        hint="embedded-v2-hint",
        version="v1",
    )
    source_sync_test_root.write_packaged_config(
        {
            "keyDerivation": "embedded",
            "embeddedKeyHint": "embedded-v2-hint",
            "embeddedKeyVersion": "v1",
            "keySalt": salt_b64,
            "privateKeyPemEnc": private_key_enc,
            "privateKeyPem": "",
        }
    )
    return private_key


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


def test_embedded_v2_key_falls_back_after_expected_passphrase_failure(
    source_sync_test_root, monkeypatch
) -> None:
    private_key = _write_embedded_v2_packaged_config(source_sync_test_root)

    def fail_passphrase_decrypt(*args, **kwargs):  # noqa: ANN002, ANN003
        raise ValueError("bad passphrase")

    monkeypatch.setattr(sync, "decrypt_private_key_pem_with_passphrase", fail_passphrase_decrypt)
    env = {**source_sync_test_root.env, sync.PACKAGED_SYNC_PASSPHRASE_ENV: "wrong-passphrase"}

    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=env)

    assert cfg.packaged_config is not None
    assert cfg.packaged_config.private_key_pem == private_key
    assert sync.config_status(cfg)["ready"] is True


def test_embedded_v2_key_does_not_fallback_after_unexpected_passphrase_failure(
    source_sync_test_root, monkeypatch
) -> None:
    _write_embedded_v2_packaged_config(source_sync_test_root)

    def fail_passphrase_decrypt(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("passphrase bug")

    monkeypatch.setattr(sync, "decrypt_private_key_pem_with_passphrase", fail_passphrase_decrypt)
    env = {**source_sync_test_root.env, sync.PACKAGED_SYNC_PASSPHRASE_ENV: "wrong-passphrase"}

    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=env)
    status = sync.config_status(cfg)

    assert status["ready"] is False
    assert status["state"] == "misconfigured"
    assert "passphrase bug" in status["message"]


def test_packaged_key_cache_write_expected_failure_remains_best_effort(
    source_sync_test_root, monkeypatch
) -> None:
    source_sync_test_root.write_packaged_config()
    monkeypatch.setattr(
        sync,
        "_write_local_wrapped_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("cache write failed")),
    )

    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)

    assert cfg.packaged_config is not None
    assert sync.config_status(cfg)["ready"] is True


def test_packaged_key_cache_write_unexpected_failure_propagates(
    source_sync_test_root, monkeypatch
) -> None:
    source_sync_test_root.write_packaged_config()
    monkeypatch.setattr(
        sync,
        "_write_local_wrapped_key",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("cache bug")),
    )

    with pytest.raises(AssertionError, match="cache bug"):
        sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
