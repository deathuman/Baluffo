from __future__ import annotations

"""Internal queue policy helpers for prevalidated discovery candidates."""

from typing import Any

QUEUE_ADAPTER_CAP_OVERRIDE_FIELD = "queueAdapterCapOverride"
QUEUE_DOMAIN_CAP_OVERRIDE_FIELD = "queueDomainCapOverride"


def positive_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def apply_prevalidated_queue_overrides(
    row: dict[str, Any],
    *,
    adapter_cap: Any = 0,
    domain_cap: Any = 0,
) -> dict[str, Any]:
    updated = dict(row)
    normalized_adapter_cap = positive_int(adapter_cap)
    normalized_domain_cap = positive_int(domain_cap)
    if normalized_adapter_cap > 0:
        updated[QUEUE_ADAPTER_CAP_OVERRIDE_FIELD] = normalized_adapter_cap
    if normalized_domain_cap > 0:
        updated[QUEUE_DOMAIN_CAP_OVERRIDE_FIELD] = normalized_domain_cap
    return updated


def effective_adapter_cap(
    row: dict[str, Any],
    adapter: str,
    effective_adapter_caps: dict[str, int],
    *,
    default: int = 3,
) -> int:
    configured = int(effective_adapter_caps.get(adapter, default) or default)
    override = positive_int(row.get(QUEUE_ADAPTER_CAP_OVERRIDE_FIELD))
    return max(configured, override) if override > 0 else configured


def effective_domain_cap(row: dict[str, Any], configured_domain_cap: int) -> int:
    configured = max(0, int(configured_domain_cap or 0))
    override = positive_int(row.get(QUEUE_DOMAIN_CAP_OVERRIDE_FIELD))
    return max(configured, override) if override > 0 else configured


def strip_internal_queue_fields(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    updated.pop(QUEUE_ADAPTER_CAP_OVERRIDE_FIELD, None)
    updated.pop(QUEUE_DOMAIN_CAP_OVERRIDE_FIELD, None)
    return updated
