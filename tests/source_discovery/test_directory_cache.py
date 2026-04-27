from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from src.source_discovery import directory_cache

from ._helpers import workspace_tmpdir


def _cache_payload(*, updated_at: str, signature: dict[str, object]) -> dict[str, object]:
    return {
        "updatedAt": updated_at,
        "configSignature": signature,
        "providerCandidates": [
            {"id": "greenhouse:one", "adapter": "greenhouse", "slug": "one"},
            {"id": "greenhouse:one", "adapter": "greenhouse", "slug": "one"},
        ],
        "staticCandidates": [
            {
                "id": "static:listing_url:https://example.com/jobs",
                "adapter": "static",
                "listing_url": "https://example.com/jobs",
            }
        ],
        "failures": [{"stage": "fetch", "error": "boom"}],
    }


def test_directory_cache_loads_fresh_rows_and_dedupes_candidates() -> None:
    with workspace_tmpdir("directory-cache-load") as root:
        cache_path = root / "cache.json"
        signature = {"source": "test"}
        cache_path.write_text(
            json.dumps(
                _cache_payload(
                    updated_at=datetime.now(UTC).isoformat(),
                    signature=signature,
                )
            ),
            encoding="utf-8",
        )

        loaded = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
        )

    assert loaded is not None
    provider_rows, static_rows, failures = loaded
    assert len(provider_rows) == 1
    assert len(static_rows) == 1
    assert failures == [{"stage": "fetch", "error": "boom"}]


def test_directory_cache_rejects_stale_invalid_or_disabled_cache() -> None:
    with workspace_tmpdir("directory-cache-reject") as root:
        cache_path = root / "cache.json"
        signature = {"source": "test"}
        cache_path.write_text(
            json.dumps(
                _cache_payload(
                    updated_at=(datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
                    signature=signature,
                )
            ),
            encoding="utf-8",
        )
        stale = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
        )
        disabled = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
            use_cache=False,
        )

        cache_path.write_text("{", encoding="utf-8")
        bad_json = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
        )

        cache_path.write_text(
            json.dumps(_cache_payload(updated_at=datetime.now(UTC).isoformat(), signature={})),
            encoding="utf-8",
        )
        signature_mismatch = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
        )

        cache_path.write_text(
            json.dumps(
                {
                    "updatedAt": "",
                    "configSignature": signature,
                    "providerCandidates": [],
                    "staticCandidates": [],
                    "failures": [],
                }
            ),
            encoding="utf-8",
        )
        missing_updated_at = directory_cache.load_directory_cache(
            cache_path,
            ttl_minutes=60,
            expected_signature=signature,
        )

    assert stale is None
    assert disabled is None
    assert bad_json is None
    assert signature_mismatch is None
    assert missing_updated_at is None


def test_directory_cache_write_emits_existing_shape() -> None:
    with workspace_tmpdir("directory-cache-write") as root:
        cache_path = root / "nested" / "cache.json"
        directory_cache.write_directory_cache(
            cache_path,
            signature={"source": "test"},
            provider_candidates=[
                {"id": "greenhouse:one", "adapter": "greenhouse", "slug": "one"},
                {"id": "greenhouse:one", "adapter": "greenhouse", "slug": "one"},
            ],
            static_candidates=[
                {
                    "id": "static:listing_url:https://example.com/jobs",
                    "adapter": "static",
                    "listing_url": "https://example.com/jobs",
                }
            ],
            failures=[{"stage": "fetch", "error": "boom"}],
        )
        payload = json.loads(cache_path.read_text(encoding="utf-8"))

    assert set(payload) == {
        "updatedAt",
        "configSignature",
        "providerCandidates",
        "staticCandidates",
        "failures",
    }
    assert payload["configSignature"] == {"source": "test"}
    assert len(payload["providerCandidates"]) == 1
    assert len(payload["staticCandidates"]) == 1
    assert payload["failures"] == [{"stage": "fetch", "error": "boom"}]
