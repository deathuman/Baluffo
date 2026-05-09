from src.source_discovery.orchestrator_generation import _dedupe_discovered_candidates
from src.source_discovery.orchestrator_runtime import DiscoveryRunState


def test_discovery_dedupe_suppresses_family_scoped_static_url_aliases() -> None:
    state = DiscoveryRunState()
    discovered = [
        {
            "adapter": "static",
            "studio": "Studio",
            "name": "Studio Careers",
            "listing_url": "https://www.studio.example/careers/index.html?page=1#jobs",
        },
        {
            "adapter": "static",
            "studio": "Other Studio",
            "name": "Other Careers",
            "listing_url": "https://www.studio.example/careers/",
        },
    ]

    _dedupe_discovered_candidates(
        state=state,
        discovered=discovered,
        seen_ids=set(),
        seen_domains=set(),
        seen_static_aliases={"studio\thttps://studio.example/careers"},
    )

    assert len(state.filtered) == 1
    assert state.filtered[0]["studio"] == "Other Studio"
    assert state.duplicate_reasons["existing_static_url_alias"] == 1
