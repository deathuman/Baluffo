from __future__ import annotations

from unittest import mock

import pytest

from src.jobs.adapters import provider_personio


def test_personio_expected_source_failure_remains_adapter_error() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]

    with pytest.raises(provider_personio.AdapterValidationError):
        provider_personio.run_personio_sources_source(
            fetch_text=lambda _url, _timeout: (_ for _ in ()).throw(
                RuntimeError("HTTP 429 for https://innogames.jobs.personio.de/xml")
            ),
            timeout_s=5,
            retries=0,
            backoff_s=0,
            registry_entries_fn=lambda _adapter: source_rows,
        )


def test_personio_unexpected_runtime_failure_propagates() -> None:
    source_rows = [
        {
            "name": "InnoGames (Personio)",
            "studio": "InnoGames",
            "adapter": "personio",
            "feed_url": "https://innogames.jobs.personio.de/xml",
            "enabledByDefault": True,
        }
    ]

    with mock.patch.object(
        provider_personio,
        "parse_personio_feed_xml",
        side_effect=RuntimeError("unexpected personio parser bug"),
    ):
        with pytest.raises(RuntimeError, match="unexpected personio parser bug"):
            provider_personio.run_personio_sources_source(
                fetch_text=lambda _url, _timeout: "<workzag-jobs />",
                timeout_s=5,
                retries=0,
                backoff_s=0,
                registry_entries_fn=lambda _adapter: source_rows,
            )
