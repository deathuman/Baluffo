from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import UTC, datetime

from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

MACHINE_SCOPE = "baluffo-github-app-sync"
KEY_DERIVATION_PASSPHRASE = "passphrase"
KEY_DERIVATION_EMBEDDED = "embedded"
EMBEDDED_KEY_VERSION_DEFAULT = "v1"
JWT_TTL_SECONDS = 9 * 60
SHA256_DIGEST_INFO_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")
PRIVATE_KEY_ENCRYPTION_PREFIX_V2 = "v2."
PRIVATE_KEY_ENCRYPTION_ALGORITHM_V2 = "AES-256-GCM"
PASSPHRASE_KDF_ITERATIONS = 210_000
_AES_GCM_NONCE_BYTES = 12
_PRIVATE_KEY_ENCRYPTION_AAD = b"baluffo:github-app-private-key:v2"
_EMBEDDED_SECRET_PARTS = (
    "bA1uFf0",
    "o.Sync",
    ".Emb3d",
    "ded.KeY",
)


def base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def base64url_decode(text: str) -> bytes:
    padded = str(text or "").strip()
    padded += "=" * ((4 - (len(padded) % 4)) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _hash_sha256(raw: bytes) -> bytes:
    digest = hashes.Hash(hashes.SHA256())
    digest.update(raw)
    return digest.finalize()


def _hmac_sha256(key: bytes, raw: bytes) -> bytes:
    signer = hmac.HMAC(key, hashes.SHA256())
    signer.update(raw)
    return signer.finalize()


def _hkdf_sha256(*, material: bytes, salt: bytes, info: bytes) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=info,
    ).derive(material)


def is_v2_encrypted_private_key(value: str) -> bool:
    return str(value or "").strip().startswith(PRIVATE_KEY_ENCRYPTION_PREFIX_V2)


def stream_encrypt(raw: bytes, key: bytes) -> bytes:
    """Legacy v1 XOR transform retained only to read existing packaged configs."""
    if not key:
        raise RuntimeError("Missing encryption key")
    out = bytearray()
    counter = 0
    while len(out) < len(raw):
        block = _hash_sha256(key + counter.to_bytes(4, "big"))
        out.extend(block)
        counter += 1
    return bytes(a ^ b for a, b in zip(raw, out[: len(raw)], strict=False))


def derive_legacy_private_key_binding_key(
    *, salt_b64: str, app_id: str, installation_id: str, machine_fingerprint: str
) -> bytes:
    salt = base64url_decode(salt_b64)
    material = "|".join(
        [
            machine_fingerprint,
            str(app_id or "").strip(),
            str(installation_id or "").strip(),
            base64url_encode(salt),
        ]
    ).encode("utf-8")
    return _hash_sha256(material)


def derive_private_key_binding_key(
    *, salt_b64: str, app_id: str, installation_id: str, machine_fingerprint: str
) -> bytes:
    salt = base64url_decode(salt_b64)
    material = "|".join(
        [
            MACHINE_SCOPE,
            "private-key",
            "machine",
            machine_fingerprint,
            str(app_id or "").strip(),
            str(installation_id or "").strip(),
        ]
    ).encode("utf-8")
    return _hkdf_sha256(
        material=material,
        salt=salt,
        info=f"{MACHINE_SCOPE}|private-key|machine|v2".encode(),
    )


def derive_embedded_key(
    *, salt_b64: str, app_id: str, installation_id: str, hint: str, version: str
) -> bytes:
    salt = base64url_decode(salt_b64)
    normalized_version = str(version or EMBEDDED_KEY_VERSION_DEFAULT).strip()
    normalized_hint = str(hint or "").strip()
    material = "|".join(
        [
            MACHINE_SCOPE,
            KEY_DERIVATION_EMBEDDED,
            normalized_version or EMBEDDED_KEY_VERSION_DEFAULT,
            normalized_hint,
            str(app_id or "").strip(),
            str(installation_id or "").strip(),
            "".join(_EMBEDDED_SECRET_PARTS),
        ]
    ).encode("utf-8")
    return _hkdf_sha256(
        material=material,
        salt=salt,
        info=f"{MACHINE_SCOPE}|private-key|embedded|v2".encode(),
    )


def encrypt_private_key_pem(
    private_key_pem: str, *, salt_b64: str, app_id: str, installation_id: str, key: bytes
) -> str:
    if not key:
        raise RuntimeError("Missing encryption key")
    nonce = os.urandom(_AES_GCM_NONCE_BYTES)
    ciphertext = AESGCM(key).encrypt(
        nonce,
        str(private_key_pem or "").encode("utf-8"),
        _PRIVATE_KEY_ENCRYPTION_AAD,
    )
    envelope = {
        "alg": PRIVATE_KEY_ENCRYPTION_ALGORITHM_V2,
        "ciphertext": base64url_encode(ciphertext),
        "nonce": base64url_encode(nonce),
    }
    raw = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return PRIVATE_KEY_ENCRYPTION_PREFIX_V2 + base64url_encode(raw)


def decrypt_private_key_pem(private_key_pem_enc: str, *, key: bytes) -> str:
    if not key:
        raise RuntimeError("Missing encryption key")
    value = str(private_key_pem_enc or "").strip()
    if is_v2_encrypted_private_key(value):
        raw = base64url_decode(value[len(PRIVATE_KEY_ENCRYPTION_PREFIX_V2) :])
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Invalid encrypted private key envelope")
        if payload.get("alg") != PRIVATE_KEY_ENCRYPTION_ALGORITHM_V2:
            raise ValueError("Unsupported encrypted private key algorithm")
        nonce = base64url_decode(str(payload.get("nonce") or ""))
        ciphertext = base64url_decode(str(payload.get("ciphertext") or ""))
        decrypted = AESGCM(key).decrypt(nonce, ciphertext, _PRIVATE_KEY_ENCRYPTION_AAD)
    else:
        decrypted = stream_encrypt(base64url_decode(value), key)
    return decrypted.decode("utf-8")


def encrypt_private_key_pem_for_machine(
    private_key_pem: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    machine_fingerprint: str,
) -> str:
    key = derive_private_key_binding_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        machine_fingerprint=machine_fingerprint,
    )
    return encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        key=key,
    )


def decrypt_private_key_pem_for_machine(
    private_key_pem_enc: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    machine_fingerprint: str,
) -> str:
    if is_v2_encrypted_private_key(private_key_pem_enc):
        key = derive_private_key_binding_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            machine_fingerprint=machine_fingerprint,
        )
    else:
        key = derive_legacy_private_key_binding_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            machine_fingerprint=machine_fingerprint,
        )
    return decrypt_private_key_pem(private_key_pem_enc, key=key)


def derive_legacy_passphrase_key(
    *, salt_b64: str, app_id: str, installation_id: str, passphrase: str
) -> bytes:
    salt = base64url_decode(salt_b64)
    material = "|".join(
        [
            MACHINE_SCOPE,
            KEY_DERIVATION_PASSPHRASE,
            str(app_id or "").strip(),
            str(installation_id or "").strip(),
            base64url_encode(salt),
            str(passphrase or ""),
        ]
    ).encode("utf-8")
    return _hash_sha256(material)


def derive_passphrase_key(
    *, salt_b64: str, app_id: str, installation_id: str, passphrase: str
) -> bytes:
    salt = base64url_decode(salt_b64)
    scoped_salt = b"|".join(
        [
            MACHINE_SCOPE.encode("utf-8"),
            KEY_DERIVATION_PASSPHRASE.encode("utf-8"),
            str(app_id or "").strip().encode("utf-8"),
            str(installation_id or "").strip().encode("utf-8"),
            salt,
        ]
    )
    return PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=scoped_salt,
        iterations=PASSPHRASE_KDF_ITERATIONS,
    ).derive(str(passphrase or "").encode("utf-8"))


def encrypt_private_key_pem_for_passphrase(
    private_key_pem: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    passphrase: str,
) -> str:
    key = derive_passphrase_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        passphrase=passphrase,
    )
    return encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        key=key,
    )


def decrypt_private_key_pem_for_passphrase(
    private_key_pem_enc: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    passphrase: str,
) -> str:
    if is_v2_encrypted_private_key(private_key_pem_enc):
        key = derive_passphrase_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            passphrase=passphrase,
        )
    else:
        key = derive_legacy_passphrase_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            passphrase=passphrase,
        )
    return decrypt_private_key_pem(private_key_pem_enc, key=key)


def build_embedded_passphrase(*, hint: str, version: str = EMBEDDED_KEY_VERSION_DEFAULT) -> str:
    normalized_version = str(version or EMBEDDED_KEY_VERSION_DEFAULT).strip()
    normalized_hint = str(hint or "").strip()
    secret = "".join(_EMBEDDED_SECRET_PARTS).encode("utf-8")
    seed = "|".join(
        [
            MACHINE_SCOPE,
            KEY_DERIVATION_EMBEDDED,
            normalized_version or EMBEDDED_KEY_VERSION_DEFAULT,
            normalized_hint,
        ]
    ).encode("utf-8")
    d1 = _hmac_sha256(secret, seed)
    d2 = _hmac_sha256(d1, normalized_hint.encode("utf-8"))
    return base64url_encode(d1 + d2)[:56]


def build_legacy_embedded_passphrase(
    *, hint: str, version: str = EMBEDDED_KEY_VERSION_DEFAULT
) -> str:
    seed = "|".join(
        [
            MACHINE_SCOPE,
            KEY_DERIVATION_EMBEDDED,
            str(version or EMBEDDED_KEY_VERSION_DEFAULT).strip(),
            str(hint or "").strip(),
            "".join(_EMBEDDED_SECRET_PARTS),
        ]
    ).encode("utf-8")
    d1 = _hash_sha256(seed).hex()
    d2 = _hash_sha256((d1 + "|" + str(hint or "").strip()).encode("utf-8")).hex()
    return f"{d1[:24]}{d2[8:40]}"


def encrypt_private_key_pem_for_embedded(
    private_key_pem: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    hint: str,
    version: str,
) -> str:
    key = derive_embedded_key(
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        hint=hint,
        version=version,
    )
    return encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=app_id,
        installation_id=installation_id,
        key=key,
    )


def decrypt_private_key_pem_for_embedded(
    private_key_pem_enc: str,
    *,
    salt_b64: str,
    app_id: str,
    installation_id: str,
    hint: str,
    version: str,
) -> str:
    if is_v2_encrypted_private_key(private_key_pem_enc):
        key = derive_embedded_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            hint=hint,
            version=version,
        )
    else:
        key = derive_legacy_passphrase_key(
            salt_b64=salt_b64,
            app_id=app_id,
            installation_id=installation_id,
            passphrase=build_legacy_embedded_passphrase(hint=hint, version=version),
        )
    return decrypt_private_key_pem(private_key_pem_enc, key=key)


def local_key_cache_fingerprint(normalized: dict[str, str]) -> str:
    material = "|".join(
        [
            str(normalized.get("appId") or "").strip(),
            str(normalized.get("installationId") or "").strip(),
            str(normalized.get("repo") or "").strip().lower(),
            str(normalized.get("branch") or "").strip(),
            str(normalized.get("path") or "").strip(),
            str(normalized.get("keyDerivation") or "").strip().lower(),
            str(normalized.get("keySalt") or "").strip(),
            str(normalized.get("privateKeyPemEnc") or "").strip(),
            str(normalized.get("embeddedKeyHint") or "").strip(),
            str(normalized.get("embeddedKeyVersion") or "").strip(),
        ]
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _asn1_read_tlv(data: bytes, offset: int) -> tuple[int, bytes, int]:
    if offset >= len(data):
        raise ValueError("Unexpected end of ASN.1 data")
    tag = data[offset]
    offset += 1
    if offset >= len(data):
        raise ValueError("Missing ASN.1 length")
    length_byte = data[offset]
    offset += 1
    if length_byte & 0x80:
        length_len = length_byte & 0x7F
        if length_len == 0 or offset + length_len > len(data):
            raise ValueError("Invalid ASN.1 length")
        length = int.from_bytes(data[offset : offset + length_len], "big")
        offset += length_len
    else:
        length = length_byte
    end = offset + length
    if end > len(data):
        raise ValueError("ASN.1 length exceeds buffer")
    return tag, data[offset:end], end


def _asn1_read_children(data: bytes) -> list[tuple[int, bytes]]:
    children: list[tuple[int, bytes]] = []
    offset = 0
    while offset < len(data):
        tag, value, offset = _asn1_read_tlv(data, offset)
        children.append((tag, value))
    return children


def _asn1_integer(data: bytes) -> int:
    if not data:
        raise ValueError("Missing ASN.1 integer data")
    return int.from_bytes(data, "big", signed=False)


def _pem_to_der(private_key_pem: str) -> bytes:
    lines = [
        line.strip()
        for line in str(private_key_pem or "").splitlines()
        if line and not line.startswith("-----")
    ]
    return base64.b64decode("".join(lines).encode("ascii"))


def _parse_rsa_private_key_der(der: bytes) -> tuple[int, int]:
    tag, value, end = _asn1_read_tlv(der, 0)
    if tag != 0x30 or end != len(der):
        raise ValueError("Invalid RSA private key sequence")
    children = _asn1_read_children(value)
    if len(children) >= 9 and all(tag_value[0] == 0x02 for tag_value in children[:9]):
        return _asn1_integer(children[1][1]), _asn1_integer(children[3][1])
    if len(children) >= 3 and children[2][0] == 0x04:
        return _parse_rsa_private_key_der(children[2][1])
    raise ValueError("Unsupported RSA private key encoding")


def rsa_pkcs1_sign_sha256(message: bytes, private_key_pem: str) -> bytes:
    n, d = _parse_rsa_private_key_der(_pem_to_der(private_key_pem))
    digest = hashlib.sha256(message).digest()
    digest_info = SHA256_DIGEST_INFO_PREFIX + digest
    modulus_len = max(1, (n.bit_length() + 7) // 8)
    padding_len = modulus_len - len(digest_info) - 3
    if padding_len < 8:
        raise RuntimeError("RSA key too small for RS256 signing")
    encoded = b"\x00\x01" + (b"\xff" * padding_len) + b"\x00" + digest_info
    signature = pow(int.from_bytes(encoded, "big"), d, n)
    return signature.to_bytes(modulus_len, "big")


def build_app_jwt(
    app_id: str,
    private_key_pem: str,
    *,
    issued_at: datetime | None = None,
    now_utc_fn,
    base64url_encode_fn,
    sign_sha256_fn,
    jwt_ttl_seconds: int = JWT_TTL_SECONDS,
) -> str:
    now = issued_at.astimezone(UTC) if issued_at else now_utc_fn()
    iat = int(now.timestamp()) - 30
    exp = iat + jwt_ttl_seconds
    header = base64url_encode_fn(
        json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
    )
    payload = base64url_encode_fn(
        json.dumps(
            {"iat": iat, "exp": exp, "iss": str(app_id or "").strip()},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    signing_input = f"{header}.{payload}".encode("ascii")
    signature = base64url_encode_fn(sign_sha256_fn(signing_input, private_key_pem))
    return f"{header}.{payload}.{signature}"
