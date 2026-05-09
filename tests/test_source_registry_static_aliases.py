from src import source_registry as sr


def test_static_listing_url_alias_collapses_safe_url_variants() -> None:
    first = sr.static_listing_url_alias(
        "HTTP://www.Studio.Example:80/careers/index.html?page=1&utm_source=x#opening"
    )
    second = sr.static_listing_url_alias("https://studio.example/careers/")

    assert first == second == "https://studio.example/careers"


def test_static_listing_url_alias_preserves_semantic_url_parts() -> None:
    filtered = sr.static_listing_url_alias("https://studio.example/careers?department=art")
    paged = sr.static_listing_url_alias("https://studio.example/careers?page=2")
    ported = sr.static_listing_url_alias("https://studio.example:8443/careers")

    assert filtered == "https://studio.example/careers?department=art"
    assert paged == "https://studio.example/careers?page=2"
    assert ported == "https://studio.example:8443/careers"


def test_static_listing_url_aliases_only_apply_to_static_rows() -> None:
    assert sr.static_listing_url_aliases(
        {
            "id": "static:listing_url:https://www.studio.example/careers/#jobs",
            "adapter": "static",
        }
    ) == {"https://studio.example/careers"}
    assert (
        sr.static_listing_url_aliases(
            {"adapter": "greenhouse", "listing_url": "https://www.studio.example/careers/"}
        )
        == set()
    )


def test_static_listing_url_aliases_include_probe_endpoint_urls() -> None:
    assert sr.static_listing_url_aliases(
        {
            "adapter": "static",
            "sourceId": "static:listing_url:https://studio.example/work-with-us/index.html",
            "endpointUrl": "https://studio.example/work-with-us/index.html",
            "finalUrl": "https://www.studio.example/work-with-us/#jobs",
        }
    ) == {"https://studio.example/work-with-us"}
