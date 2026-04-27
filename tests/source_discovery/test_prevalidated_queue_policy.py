from __future__ import annotations

from src.source_discovery import prevalidated_queue_policy as policy
from src.source_discovery.core_queue import apply_queue_balancing


def test_apply_prevalidated_queue_overrides_applies_only_positive_values() -> None:
    row = {"name": "Validated Studio", "adapter": "static"}

    updated = policy.apply_prevalidated_queue_overrides(
        row,
        adapter_cap="12",
        domain_cap=0,
    )

    assert updated is not row
    assert row == {"name": "Validated Studio", "adapter": "static"}
    assert updated[policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD] == 12
    assert policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD not in updated

    without_overrides = policy.apply_prevalidated_queue_overrides(
        row,
        adapter_cap=-1,
        domain_cap="not an int",
    )

    assert policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD not in without_overrides
    assert policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD not in without_overrides


def test_effective_caps_never_reduce_configured_caps() -> None:
    lower_override = {
        policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD: 2,
        policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD: 1,
    }
    higher_override = {
        policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD: 25,
        policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD: 8,
    }

    assert policy.effective_adapter_cap(lower_override, "static", {"static": 8}) == 8
    assert policy.effective_domain_cap(lower_override, 3) == 3
    assert policy.effective_adapter_cap(higher_override, "static", {"static": 8}) == 25
    assert policy.effective_domain_cap(higher_override, 3) == 8


def test_strip_internal_queue_fields_returns_public_row_copy() -> None:
    row = {
        "name": "Validated Studio",
        "adapter": "static",
        policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD: 25,
        policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD: 8,
    }

    stripped = policy.strip_internal_queue_fields(row)

    assert stripped == {"name": "Validated Studio", "adapter": "static"}
    assert policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD in row
    assert policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD in row


def test_queue_balancing_strips_internal_fields_from_queued_and_deferred_rows() -> None:
    candidates = [
        {
            "name": f"Validated Studio {index}",
            "studio": f"Validated Studio {index}",
            "adapter": "static",
            "score": 50 - index,
            "evidenceScore": 50,
            "jobsFound": 3,
            "pages": [f"https://validated.example.com/jobs/{index}"],
            "careersUrl": f"https://validated.example.com/jobs/{index}",
            policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD: 20,
            policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD: 1,
        }
        for index in range(3)
    ]

    queued, report_rows, stats = apply_queue_balancing(
        candidates,
        top_n=0,
        domain_cap=1,
        adapter_caps={"static": 8},
    )

    assert len(queued) == 1
    assert int((stats.get("deferredReasons") or {}).get("domain_cap") or 0) == 2
    assert all(policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD not in row for row in queued)
    assert all(policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD not in row for row in queued)
    assert all(policy.QUEUE_ADAPTER_CAP_OVERRIDE_FIELD not in row for row in report_rows)
    assert all(policy.QUEUE_DOMAIN_CAP_OVERRIDE_FIELD not in row for row in report_rows)
