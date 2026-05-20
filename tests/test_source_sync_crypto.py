# ruff: noqa: SLF001
import base64
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

import src.source_sync_crypto as crypto


def _der_length(length: int) -> bytes:
    if length < 0x80:
        return bytes([length])
    raw = length.to_bytes((length.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(raw), *raw])


def _tlv(tag: int, value: bytes) -> bytes:
    return bytes([tag]) + _der_length(len(value)) + value


def _integer(value: int) -> bytes:
    raw = max(0, value).to_bytes(max(1, (max(0, value).bit_length() + 7) // 8), "big")
    if raw[0] & 0x80:
        raw = b"\x00" + raw
    return _tlv(0x02, raw)


def _sequence(*children: bytes) -> bytes:
    return _tlv(0x30, b"".join(children))


def _octet_string(value: bytes) -> bytes:
    return _tlv(0x04, value)


def _pkcs1_private_key_der(*, n: int = 3233, d: int = 2753) -> bytes:
    return _sequence(
        _integer(0),
        _integer(n),
        _integer(17),
        _integer(d),
        _integer(61),
        _integer(53),
        _integer(53),
        _integer(49),
        _integer(38),
    )


def _pem_from_der(der: bytes) -> str:
    encoded = base64.b64encode(der).decode("ascii")
    return f"-----BEGIN RSA PRIVATE KEY-----\n{encoded}\n-----END RSA PRIVATE KEY-----"


def test_base64url_decode_accepts_unpadded_values_and_stream_requires_key() -> None:
    encoded = crypto.base64url_encode(b"unit-test")

    assert encoded == "dW5pdC10ZXN0"
    assert crypto.base64url_decode(encoded.rstrip("=")) == b"unit-test"
    with pytest.raises(RuntimeError, match="Missing encryption key"):
        crypto.stream_encrypt(b"payload", b"")


def test_key_derivation_encryption_round_trips_and_fingerprint_are_stable() -> None:
    salt_b64 = crypto.base64url_encode(b"unit-test-salt")
    machine_key = crypto.derive_private_key_binding_key(
        salt_b64=salt_b64,
        app_id=" 123 ",
        installation_id=" 456 ",
        machine_fingerprint="machine-a",
    )
    passphrase_key = crypto.derive_passphrase_key(
        salt_b64=salt_b64,
        app_id="123",
        installation_id="456",
        passphrase="secret",
    )

    assert machine_key != passphrase_key
    encrypted = crypto.encrypt_private_key_pem(
        "-----BEGIN KEY-----\nabc\n-----END KEY-----",
        salt_b64=salt_b64,
        app_id="123",
        installation_id="456",
        key=machine_key,
    )
    assert encrypted.startswith(crypto.PRIVATE_KEY_ENCRYPTION_PREFIX_V2)
    assert (
        crypto.decrypt_private_key_pem(encrypted, key=machine_key)
        == "-----BEGIN KEY-----\nabc\n-----END KEY-----"
    )
    assert crypto.build_embedded_passphrase(hint="hint") == crypto.build_embedded_passphrase(
        hint=" hint ", version=""
    )
    assert crypto.local_key_cache_fingerprint(
        {
            "appId": "123",
            "installationId": "456",
            "repo": "Owner/Repo",
            "branch": "main",
            "path": "sync.json",
            "keyDerivation": "Embedded",
            "keySalt": salt_b64,
            "privateKeyPemEnc": encrypted,
            "embeddedKeyHint": "hint",
            "embeddedKeyVersion": "v1",
        }
    ) == crypto.local_key_cache_fingerprint(
        {
            "appId": "123",
            "installationId": "456",
            "repo": "owner/repo",
            "branch": "main",
            "path": "sync.json",
            "keyDerivation": "embedded",
            "keySalt": salt_b64,
            "privateKeyPemEnc": encrypted,
            "embeddedKeyHint": "hint",
            "embeddedKeyVersion": "v1",
        }
    )


def test_legacy_private_key_ciphertexts_remain_decryptable() -> None:
    salt_b64 = "bGVnYWN5LW1hY2hpbmUtc2FsdA"
    legacy_ciphertext = "ja_DzZxWecmT1TkB54TnhJZAnFkpxPgNKZYlY9Ju4rUKVsE7ye09haNBUJzUImHwz22adxX42Z73FHBjugYAkbjDYO4"
    legacy_key = crypto.derive_legacy_private_key_binding_key(
        salt_b64=salt_b64,
        app_id="123456",
        installation_id="999999",
        machine_fingerprint="machine-a",
    )

    assert not crypto.is_v2_encrypted_private_key(legacy_ciphertext)
    assert crypto.decrypt_private_key_pem(legacy_ciphertext, key=legacy_key) == (
        "-----BEGIN RSA PRIVATE KEY-----\nlegacy\n-----END RSA PRIVATE KEY-----"
    )
    assert (
        crypto.build_legacy_embedded_passphrase(hint="legacy-hint", version="v1")
        == "c0758de5f707c4a2a82569b78d7e53216a7fcc40a23c53bb5f97e538"
    )
    assert crypto.build_embedded_passphrase(hint="legacy-hint", version="v1") != (
        "c0758de5f707c4a2a82569b78d7e53216a7fcc40a23c53bb5f97e538"
    )


def test_asn1_helpers_read_children_and_reject_malformed_values() -> None:
    assert crypto._asn1_read_children(_integer(5) + _integer(7)) == [
        (0x02, b"\x05"),
        (0x02, b"\x07"),
    ]
    long_value = b"x" * 130
    assert crypto._asn1_read_tlv(_tlv(0x04, long_value), 0) == (0x04, long_value, 133)

    malformed_cases = [
        (b"", "Unexpected end"),
        (b"\x02", "Missing ASN.1 length"),
        (b"\x02\x80", "Invalid ASN.1 length"),
        (b"\x02\x82\x01", "Invalid ASN.1 length"),
        (b"\x04\x03ab", "exceeds buffer"),
    ]
    for raw, message in malformed_cases:
        with pytest.raises(ValueError, match=message):
            crypto._asn1_read_tlv(raw, 0)

    with pytest.raises(ValueError, match="Missing ASN.1 integer"):
        crypto._asn1_integer(b"")


def test_parse_rsa_private_key_der_supports_pkcs1_and_wrapped_octet_key() -> None:
    pkcs1 = _pkcs1_private_key_der(n=3233, d=2753)
    wrapped = _sequence(_integer(0), _sequence(_tlv(0x06, b"*")), _octet_string(pkcs1))

    assert crypto._parse_rsa_private_key_der(pkcs1) == (3233, 2753)
    assert crypto._parse_rsa_private_key_der(wrapped) == (3233, 2753)

    with pytest.raises(ValueError, match="Invalid RSA private key sequence"):
        crypto._parse_rsa_private_key_der(_integer(1))
    with pytest.raises(ValueError, match="Unsupported RSA private key encoding"):
        crypto._parse_rsa_private_key_der(_sequence(_integer(0), _integer(1)))


def test_rsa_sign_rejects_too_small_key_after_pem_decoding() -> None:
    with pytest.raises(RuntimeError, match="RSA key too small"):
        crypto.rsa_pkcs1_sign_sha256(b"message", _pem_from_der(_pkcs1_private_key_der()))


def test_build_app_jwt_uses_injected_clock_encoder_and_signer() -> None:
    calls: list[bytes] = []

    def fake_sign(message: bytes, private_key_pem: str) -> bytes:
        calls.append(message)
        assert private_key_pem == "pem"
        return b"sig"

    issued_at = datetime(2026, 4, 25, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    token = crypto.build_app_jwt(
        " 123456 ",
        "pem",
        issued_at=issued_at,
        now_utc_fn=lambda: datetime(2000, 1, 1, tzinfo=UTC),
        base64url_encode_fn=crypto.base64url_encode,
        sign_sha256_fn=fake_sign,
        jwt_ttl_seconds=60,
    )

    header_b64, payload_b64, signature_b64 = token.split(".")
    assert json.loads(crypto.base64url_decode(header_b64)) == {"alg": "RS256", "typ": "JWT"}
    assert json.loads(crypto.base64url_decode(payload_b64)) == {
        "exp": 1777111230,
        "iat": 1777111170,
        "iss": "123456",
    }
    assert signature_b64 == crypto.base64url_encode(b"sig")
    assert calls == [f"{header_b64}.{payload_b64}".encode("ascii")]
