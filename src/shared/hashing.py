"""Shared hashing helpers for file digests.

AI boundary owns: streaming SHA-256 digests of files and byte payloads.
AI boundary implement in: this file for digest computation only; callers own
signature policy, manifest shape, and verification decisions.
AI boundary search before contracts: build scripts, ship update manager
validation, desktop update helpers, and discovery audit artifact writers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused hashing tests.

This exists because five modules had grown byte-identical copies of the same
streaming digest loop under four different names (``_sha256_file``,
``_hash_file``, ``_sha256``, ``compute_sha256``), which the duplication gate
recorded as one baselined pattern.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK_SIZE = 1024 * 1024


def sha256_file(path: Path) -> str:
    """Return the lowercase hex SHA-256 of ``path``, streamed in 1 MiB chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    """Return the lowercase hex SHA-256 of ``payload``."""
    return hashlib.sha256(payload).hexdigest()
