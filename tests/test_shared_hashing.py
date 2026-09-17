"""Guard the consolidated streaming SHA-256 helper.

Five modules used to carry byte-identical copies of the same digest loop under
four different names, which the duplication gate recorded as one baselined
pattern (``4ee6199f500d``, 5 copies). They now delegate to
``src.shared.hashing.sha256_file``. These tests pin both the shared behaviour and
the delegation, so a future edit cannot silently reintroduce a divergent copy.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from scripts.build_portable_exe import _sha256_bytes, _sha256_file, _sha256_text
from scripts.build_ship_bundle import _hash_file
from src.bridge.discovery_audit_artifacts import _sha256 as audit_sha256
from src.shared.hashing import CHUNK_SIZE, sha256_bytes, sha256_file
from src.ship.desktop_update_shared import compute_sha256 as desktop_shared_sha256
from src.ship.update_manager_validation import compute_sha256 as validation_sha256

DELEGATES = {
    "build_portable_exe._sha256_file": _sha256_file,
    "build_ship_bundle._hash_file": _hash_file,
    "discovery_audit_artifacts._sha256": audit_sha256,
    "desktop_update_shared.compute_sha256": desktop_shared_sha256,
    "update_manager_validation.compute_sha256": validation_sha256,
}


def test_sha256_file_matches_hashlib(tmp_path: Path) -> None:
    target = tmp_path / "payload.bin"
    payload = b"baluffo-hash-parity" * 100
    target.write_bytes(payload)

    assert sha256_file(target) == hashlib.sha256(payload).hexdigest()


def test_sha256_file_streams_across_chunk_boundaries(tmp_path: Path) -> None:
    """A file larger than CHUNK_SIZE must hash identically to a single-shot digest."""
    target = tmp_path / "big.bin"
    payload = b"A" * (CHUNK_SIZE * 2 + 7)
    target.write_bytes(payload)

    assert target.stat().st_size > CHUNK_SIZE
    assert sha256_file(target) == hashlib.sha256(payload).hexdigest()


def test_sha256_file_handles_empty_file(tmp_path: Path) -> None:
    target = tmp_path / "empty.bin"
    target.write_bytes(b"")

    assert sha256_file(target) == hashlib.sha256(b"").hexdigest()


def test_sha256_bytes_matches_hashlib() -> None:
    assert sha256_bytes(b"x") == hashlib.sha256(b"x").hexdigest()


def test_every_former_copy_delegates_to_the_shared_helper(tmp_path: Path) -> None:
    target = tmp_path / "payload.bin"
    payload = b"delegation-parity" * 250
    target.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()

    for label, func in DELEGATES.items():
        assert func(target) == expected, f"{label} diverged from the shared helper"


def test_portable_exe_byte_and_text_helpers_route_through_shared_helper() -> None:
    assert _sha256_bytes(b"payload") == sha256_bytes(b"payload")
    assert _sha256_text("héllo") == hashlib.sha256("héllo".encode()).hexdigest()


def test_the_former_duplicate_pattern_is_not_baselined() -> None:
    """The consolidated digest must not linger in the duplication baseline."""
    from tools.repo_health.duplicate_body_policy import load_baseline

    repo_root = Path(__file__).resolve().parents[1]
    assert "4ee6199f500d" not in load_baseline(repo_root)
