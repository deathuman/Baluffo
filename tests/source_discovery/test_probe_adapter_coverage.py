"""An adapter can be declared supported and still be unprobeable.

`SUPPORTED_PROVIDERS` lists 14 adapters. `parse_probe_count` had no branch for
**bamboohr**, **oracle_hcm** or **workday** and raised `ValueError("unsupported
adapter")` for all three -- accounting for 68 of the 71 recorded probe failures across
the curated catalogue.

That failure mode is silent in the worst way. Nothing errors at import, the adapter
appears in every configuration list, and no URL helps: the count function refuses
before reading anything. Workday alone holds 1,065 openings and BambooHR 192, and both
were simply never going to land no matter what board URL they carried.

This file is the guardrail for that class of bug. It is worth more than the three
branches themselves, because the same gap already cost two full measurement cycles:
**Ashby** landed 0 of 39 boards and **Breezy** 0 of 15 until both were added to the
probe's `provider_specs`, while Greenhouse -- which was listed -- landed 40 of 44. Each
took a full drain run to find.
"""

from __future__ import annotations

import pytest

from src.source_discovery.config import SUPPORTED_PROVIDERS
from src.source_discovery.probe import parse_probe_count

# A payload shaped like each provider's listing response. The point is that a branch
# *exists*, not that the payload is realistic -- an unrecognised shape and a missing
# branch both raise, so this catches the gap without pinning vendor payload details
# that legitimately change.
_JSON_PAYLOAD = '{"jobs":[{"id":1}]}'


@pytest.mark.parametrize("adapter", sorted(SUPPORTED_PROVIDERS))
def test_every_supported_adapter_has_a_probe_count_path(adapter: str) -> None:
    """The guardrail.

    Fails loudly at test time rather than silently at fetch time, which is the only
    window where anyone can act on it.
    """
    try:
        parse_probe_count(adapter, _JSON_PAYLOAD)
    except ValueError as exc:  # pragma: no cover - the assertion is the point
        pytest.fail(
            f"{adapter} is declared in SUPPORTED_PROVIDERS but parse_probe_count "
            f"rejects it: {exc}. No board on this adapter can ever be probed, so it "
            f"can never reach the registry."
        )


def test_the_three_known_gaps_are_named_so_a_regression_is_recognisable() -> None:
    """Pinned deliberately: if this fails, a previously-fixed adapter was removed."""
    for adapter in ("bamboohr", "oracle_hcm", "workday"):
        assert adapter in SUPPORTED_PROVIDERS, f"{adapter} left SUPPORTED_PROVIDERS"
        try:
            parse_probe_count(adapter, _JSON_PAYLOAD)
        except ValueError as exc:
            pytest.fail(f"{adapter} lost its probe branch: {exc}")


def test_an_adapter_outside_the_supported_set_still_reports_clearly() -> None:
    """The error must name the adapter, not fail obscurely."""
    with pytest.raises(ValueError, match="unsupported adapter"):
        parse_probe_count("not_a_real_vendor", _JSON_PAYLOAD)


def test_bamboo_hr_counts_rows() -> None:
    """BambooHR's careers page is a JavaScript app, but the probe still has to try."""
    payload = (
        '<div class="opening"><a href="https://x.bamboohr.com/careers/1">One</a></div>'
        '<div class="opening"><a href="https://x.bamboohr.com/careers/2">Two</a></div>'
    )
    assert parse_probe_count("bamboohr", payload) == 2
