from __future__ import annotations

from pathlib import Path

from src.source_discovery import (
    audit_config,
    directory_cache,
    gameprog,
    gamesmap_candidates,
    sheet_directory,
    web_search_candidates,
)


def test_audit_config_section_merges_defaults_and_preserves_flat_fallback() -> None:
    assert audit_config.config_section(
        {"example": {"activeAuditEnabled": False}},
        "example",
        defaults={"activeAuditEnabled": True, "activeAuditTtlMinutes": 360},
    ) == {"activeAuditEnabled": False, "activeAuditTtlMinutes": 360}

    assert audit_config.config_section(
        {"activeAuditEnabled": False},
        "example",
        defaults={"activeAuditEnabled": True},
    ) == {"activeAuditEnabled": False}


def test_audit_config_enabled_path_ttl_and_int_defaults() -> None:
    config = {
        "example": {
            "activeAuditEnabled": False,
            "activeAuditPath": "data/custom-audit.json",
            "activeAuditTtlMinutes": "15",
            "limit": "-3",
        }
    }

    assert audit_config.audit_enabled(config, "example") is False
    assert audit_config.audit_artifact_path(
        config,
        "example",
        default_filename="default-audit.json",
    ) == Path("data/custom-audit.json")
    assert audit_config.audit_ttl_minutes(config, "example", fallback_ttl=60) == 15
    assert audit_config.int_config_value(config, "limit", section_name="example", default=8) == 0


def test_audit_config_ttl_invalid_value_uses_fallback() -> None:
    assert (
        audit_config.audit_ttl_minutes(
            {"example": {"activeAuditTtlMinutes": "not-an-int"}},
            "example",
            fallback_ttl=45,
        )
        == 45
    )


def test_adapter_audit_config_wrappers_preserve_defaults_and_rollbacks() -> None:
    assert gameprog._gameprog_audit_enabled({"gameprog": {"enabled": True}}) is True
    assert (
        gameprog._gameprog_audit_enabled(
            {"gameprog": {"enabled": True, "activeAuditEnabled": False}}
        )
        is False
    )
    assert gamesmap_candidates._gamesmap_audit_enabled({"enabled": True}) is True
    assert (
        gamesmap_candidates._gamesmap_audit_enabled({"enabled": True, "activeAuditEnabled": False})
        is False
    )

    assert (
        sheet_directory._sheet_directory_audit_ttl_minutes(
            {"sheetDirectory": {"activeAuditTtlMinutes": "bad"}}
        )
        == 360
    )
    assert web_search_candidates._web_search_max_queries({"webSearch": {"maxQueries": "bad"}}) == 24


def test_directory_cache_config_helpers_preserve_adapter_paths_and_ttls() -> None:
    assert (
        directory_cache.directory_cache_path(
            None,
            "gameprog",
            default_filename="gameprog-discovery-cache.json",
        ).name
        == "gameprog-discovery-cache.json"
    )
    assert (
        directory_cache.directory_cache_path(
            None,
            "gamesmap",
            default_filename="gamesmap-discovery-cache.json",
        ).name
        == "gamesmap-discovery-cache.json"
    )
    assert (
        directory_cache.directory_cache_path(
            None,
            "gamedevmap",
            default_filename="gamedevmap-discovery-cache.json",
            flat_fallback=False,
        ).name
        == "gamedevmap-discovery-cache.json"
    )
    assert directory_cache.directory_cache_path(
        {"gameprog": {"cachePath": "data/custom.json"}},
        "gameprog",
        default_filename="gameprog-discovery-cache.json",
    ) == Path("data/custom.json")
    assert (
        directory_cache.directory_cache_path(
            {"cachePath": "data/ignored-flat.json"},
            "gamedevmap",
            default_filename="gamedevmap-discovery-cache.json",
            flat_fallback=False,
        ).name
        == "gamedevmap-discovery-cache.json"
    )
    assert gameprog._gameprog_cache_ttl_minutes({"gameprog": {"cacheTtlMinutes": "bad"}}) == 360
    assert (
        directory_cache.directory_cache_ttl_minutes(
            {"gamesmap": {"cacheTtlMinutes": "12"}},
            "gamesmap",
        )
        == 12
    )
    assert (
        directory_cache.directory_cache_ttl_minutes(
            {"gamedevmap": {"cacheTtlMinutes": "-1"}},
            "gamedevmap",
            flat_fallback=False,
        )
        == 0
    )


def test_directory_cache_use_allowed_preserves_custom_fetcher_rule() -> None:
    def default_fetcher() -> None:
        return None

    def custom_fetcher() -> None:
        return None

    assert directory_cache.directory_cache_use_allowed(
        {},
        "gameprog",
        fetcher=default_fetcher,
        default_fetcher=default_fetcher,
    )
    assert not directory_cache.directory_cache_use_allowed(
        {},
        "gameprog",
        fetcher=custom_fetcher,
        default_fetcher=default_fetcher,
    )
    assert directory_cache.directory_cache_use_allowed(
        {"gameprog": {"cachePath": "data/custom.json"}},
        "gameprog",
        fetcher=custom_fetcher,
        default_fetcher=default_fetcher,
    )
