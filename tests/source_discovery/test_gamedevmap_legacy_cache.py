from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.source_discovery import gamedevmap
from src.source_discovery.config import DEFAULT_DISCOVERY_CONFIG

from ._helpers import json, workspace_tmpdir
from .gamedevmap_test_helpers import CSV_URL, INDEX_URL, gamedevmap_fetcher, gamedevmap_payloads


def _legacy_cache_config(cache_path: str | None = None) -> dict[str, object]:
    cfg = dict(DEFAULT_DISCOVERY_CONFIG["gamedevmap"])
    cfg.update(
        {
            "enabled": True,
            "activeAuditEnabled": False,
            "csvUrl": CSV_URL,
            "indexUrl": INDEX_URL,
            "maxRows": 3,
            "maxHomepageFetches": 1,
            "cacheTtlMinutes": 60,
        }
    )
    if cache_path is not None:
        cfg["cachePath"] = cache_path
    return {"gamedevmap": cfg}


def test_gamedevmap_legacy_cache_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gamedevmap-legacy-cache") as root:
        cache_path = root / "gamedevmap-cache.json"
        config = _legacy_cache_config(str(cache_path))
        calls: list[str] = []

        first = gamedevmap.discover_gamedevmap_candidates(
            5,
            config=config,
            fetcher=gamedevmap_fetcher(gamedevmap_payloads(), calls),
        )
        assert cache_path.exists()
        assert calls

        def blocked_fetcher(url: str, _: int) -> str:
            raise AssertionError(f"cache hit should not fetch {url}")

        second = gamedevmap.discover_gamedevmap_candidates(
            5,
            config=config,
            fetcher=blocked_fetcher,
        )
        assert second == first


def test_gamedevmap_legacy_cache_rejects_stale_or_signature_mismatched_payload() -> None:
    with workspace_tmpdir("gamedevmap-legacy-cache-stale") as root:
        cache_path = root / "gamedevmap-cache.json"
        config = _legacy_cache_config(str(cache_path))
        cfg = dict(config["gamedevmap"])  # type: ignore[arg-type]
        payload = {
            "updatedAt": (datetime.now(UTC) - timedelta(days=2)).isoformat().replace("+00:00", "Z"),
            "configSignature": gamedevmap._gamedevmap_cache_signature(cfg),
            "providerCandidates": [],
            "staticCandidates": [],
            "failures": [],
        }
        cache_path.write_text(json.dumps(payload), encoding="utf-8")

        assert (
            gamedevmap._load_gamedevmap_cache(config, cfg, fetcher=gamedevmap_fetcher({})) is None
        )

        payload["updatedAt"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload["configSignature"] = {"csvUrl": "different"}
        cache_path.write_text(json.dumps(payload), encoding="utf-8")

        assert (
            gamedevmap._load_gamedevmap_cache(config, cfg, fetcher=gamedevmap_fetcher({})) is None
        )


def test_gamedevmap_legacy_cache_bypasses_custom_fetcher_without_explicit_cache_path() -> None:
    config = _legacy_cache_config()
    cfg = dict(config["gamedevmap"])  # type: ignore[arg-type]

    assert gamedevmap._load_gamedevmap_cache(config, cfg, fetcher=gamedevmap_fetcher({})) is None


def test_gamedevmap_legacy_cache_write_keeps_existing_shape() -> None:
    with workspace_tmpdir("gamedevmap-legacy-cache-write") as root:
        cache_path = root / "gamedevmap-cache.json"
        config = _legacy_cache_config(str(cache_path))
        cfg = dict(config["gamedevmap"])  # type: ignore[arg-type]
        provider = {
            "name": "Cached Studio",
            "adapter": "greenhouse",
            "slug": "cached",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/cached/jobs?content=true",
        }
        failure = {"name": "Cached Failure", "adapter": "gamedevmap", "stage": "csv_fetch"}

        gamedevmap._write_gamedevmap_cache(
            config,
            cfg,
            provider_candidates=[provider, dict(provider)],
            static_candidates=[],
            failures=[failure],
        )

        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        assert set(payload) == {
            "updatedAt",
            "configSignature",
            "providerCandidates",
            "staticCandidates",
            "failures",
        }
        assert payload["configSignature"] == gamedevmap._gamedevmap_cache_signature(cfg)
        assert len(payload["providerCandidates"]) == 1
        assert payload["providerCandidates"][0]["slug"] == "cached"
        assert payload["providerCandidates"][0]["id"] == "greenhouse:slug:cached"
        assert payload["staticCandidates"] == []
        assert payload["failures"] == [failure]
