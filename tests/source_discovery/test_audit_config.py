from __future__ import annotations

from pathlib import Path

from src.source_discovery import (
    audit_config,
    sheet_directory,
    web_search_candidates,
)


def test_audit_config_section_merges_defaults_and_preserves_flat_fallback() -> None:
    assert audit_config.config_section(
        {"example": {"activeAuditTtlMinutes": 15}},
        "example",
        defaults={"activeAuditTtlMinutes": 360, "limit": 8},
    ) == {"activeAuditTtlMinutes": 15, "limit": 8}

    assert audit_config.config_section(
        {"activeAuditTtlMinutes": 15},
        "example",
        defaults={"activeAuditTtlMinutes": 360},
    ) == {"activeAuditTtlMinutes": 15}


def test_audit_config_path_ttl_and_int_defaults() -> None:
    config = {
        "example": {
            "activeAuditPath": "data/custom-audit.json",
            "activeAuditTtlMinutes": "15",
            "limit": "-3",
        }
    }

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


def test_adapter_audit_config_defaults_preserve_invalid_ttl_fallbacks() -> None:
    assert (
        audit_config.audit_ttl_minutes(
            sheet_directory._sheet_directory_config_section(
                {"sheetDirectory": {"activeAuditTtlMinutes": "bad"}}
            )
        )
        == 360
    )
    assert web_search_candidates._web_search_max_queries({"webSearch": {"maxQueries": "bad"}}) == 24
