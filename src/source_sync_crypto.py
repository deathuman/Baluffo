from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

MACHINE_SCOPE = "baluffo-github-app-sync"
KEY_DERIVATION_PASSPHRASE = "passphrase"
KEY_DERIVATION_EMBEDDED = "embedded"
EMBEDDED_KEY_VERSION_DEFAULT = "v1"
JWT_TTL_SECONDS = 9 * 60
SHA256_DIGEST_INFO_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")
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


def stream_encrypt(raw: bytes, key: bytes) -> bytes:
    if not key:
        raise RuntimeError("Missing encryption key")
    out = bytearray()
    counter = 0
    while len(out) < len(raw):
        block = __import__("hashlib").sha256(key + counter.to_bytes(4, "big")).digest()
        out.extend(block)
        counter += 1
    return bytes(a ^ b for a, b in zip(raw, out[: len(raw)], strict=False))


def derive_private_key_binding_key(
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
    return __import__("hashlib").sha256(material).digest()


def encrypt_private_key_pem(
    private_key_pem: str, *, salt_b64: str, app_id: str, installation_id: str, key: bytes
) -> str:
    encrypted = stream_encrypt(str(private_key_pem or "").encode("utf-8"), key)
    return base64url_encode(encrypted)


def decrypt_private_key_pem(private_key_pem_enc: str, *, key: bytes) -> str:
    decrypted = stream_encrypt(base64url_decode(private_key_pem_enc), key)
    return decrypted.decode("utf-8")


def derive_passphrase_key(
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
    return __import__("hashlib").sha256(material).digest()


def build_embedded_passphrase(*, hint: str, version: str = EMBEDDED_KEY_VERSION_DEFAULT) -> str:
    seed = "|".join(
        [
            MACHINE_SCOPE,
            KEY_DERIVATION_EMBEDDED,
            str(version or EMBEDDED_KEY_VERSION_DEFAULT).strip(),
            str(hint or "").strip(),
            "".join(_EMBEDDED_SECRET_PARTS),
        ]
    ).encode("utf-8")
    d1 = __import__("hashlib").sha256(seed).hexdigest()
    d2 = (
        __import__("hashlib")
        .sha256((d1 + "|" + str(hint or "").strip()).encode("utf-8"))
        .hexdigest()
    )
    return f"{d1[:24]}{d2[8:40]}"


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
    return __import__("hashlib").sha256(material).hexdigest()


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
    digest = __import__("hashlib").sha256(message).digest()
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
