import pytest

from src import source_sync as sync
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


def test_machine_bound_packaged_key_does_not_swallow_unexpected_decrypt_bug(
    source_sync_test_root, monkeypatch
):
    source_sync_test_root.write_packaged_config(
        {
            "keyDerivation": sync.KEY_DERIVATION_MACHINE,
            "keySalt": sync._base64url_encode(b"unit-test-salt-999"),  # noqa: SLF001
            "privateKeyPemEnc": "ciphertext",
            "privateKeyPem": "",
        }
    )

    def fail_machine_decrypt(*args, **kwargs):  # noqa: ANN002,ANN003
        raise AttributeError("unexpected decrypt bug")

    monkeypatch.setattr(sync, "decrypt_private_key_pem", fail_machine_decrypt)

    with pytest.raises(AttributeError, match="unexpected decrypt bug"):
        sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
