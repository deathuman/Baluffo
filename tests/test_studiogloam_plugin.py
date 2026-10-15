"""Studio Gloam static plugin: the object-shape read of the Vite bundle.

No network here. The fixtures are small JS snippets resembling the bundle's shape:
the openings array, plus the team-member bio objects that share the page and must
not be mistaken for postings.
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters.plugins.static import studiogloam
from src.jobs.adapters.plugins.types import AdapterPluginContext

# Two real openings, in the bundle's own key order, plus a decoy: the team-member
# objects carry a ``role:`` field and none of title/type/location/contract.
_BUNDLE = """
var Nn=function(e){return e.title};const ar={discipline:"Lighting Art & Tech Art",type:"Open-World Survival"};
const _T=[{name:"Alex Vance",role:"Founder & Lighting Director",bio:"Leads the studio."},
{name:"Sam Ito",role:"Technical Art Lead",bio:"Owns the pipeline."}];
const QN=[{title:"Senior Lighting Artist",type:"Lighting Art",location:"Remote \\u00b7 Europe",contract:"Fixed Term Contract",summary:"Own the look of a world.",responsibilities:["Lighting direction"],requirements:["UE5"]},{title:"Senior Technical Artist",type:"Tech Art",location:"Remote \\u00b7 Europe",contract:"Fixed Term Contract",summary:"Keep productions moving.",responsibilities:["PCG"],requirements:["Houdini"]}];
export{QN as default};
"""

_SHELL = """
<!doctype html><html><head><script type="module" crossorigin src="/assets/index-C6_1yUeX.js"></script></head>
<body><div id="root"></div></body></html>
"""

_ROLES: dict[str, Any] = {}


def _fetch(pages: dict[str, str]):
    def _fetch_text(url: str, _timeout: int) -> str:
        return pages.get(url, "")

    return _fetch_text


def _run(*, shell: str = _SHELL, bundle: str = _BUNDLE, **kwargs: Any):
    pages = {"https://studiogloam.com/": shell}
    if bundle:
        pages["https://studiogloam.com/assets/index-C6_1yUeX.js"] = bundle
    return studiogloam.run(
        fetch_text=_fetch(pages),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://studiogloam.com/"],
        source_row={"name": "Studio Gloam", "id": "studiogloam"},
        **kwargs,
    )


def test_can_handle_accepts_the_studiogloam_identity_and_rejects_an_unrelated_host() -> None:
    assert studiogloam.can_handle(
        AdapterPluginContext(
            family="static", adapter_key="static", source_identity="studiogloam.com"
        )
    )
    assert studiogloam.can_handle(
        AdapterPluginContext(
            family="static", adapter_key="static", source_identity="www.studiogloam.com"
        )
    )
    for other in ("example.com", "studiogloam.com.evil.test", "www.kojimaproductions.jp"):
        assert not studiogloam.can_handle(
            AdapterPluginContext(family="static", adapter_key="static", source_identity=other)
        ), other


def test_the_two_real_openings_are_found_and_the_team_member_decoy_is_ignored() -> None:
    rows = _run()

    assert [row["title"] for row in rows] == ["Senior Lighting Artist", "Senior Technical Artist"]
    assert all("Founder" not in row["title"] for row in rows)
    assert all(row["company"] == "Studio Gloam" for row in rows)


def test_non_opening_name_object_with_opening_metadata_is_ignored() -> None:
    bundle = """
    const data = [
      {name:"Company award",type:"Lighting Art",location:"Remote · Europe",contract:"Fixed Term Contract"},
      {title:"Senior Lighting Artist",type:"Lighting Art",location:"Remote · Europe",contract:"Fixed Term Contract"}
    ];
    """

    rows = _run(bundle=bundle)

    assert [row["title"] for row in rows] == ["Senior Lighting Artist"]


def test_titles_locations_and_contracts_are_the_real_values() -> None:
    rows = _run()

    assert {row["title"] for row in rows} == {"Senior Lighting Artist", "Senior Technical Artist"}
    assert {row["contractType"] for row in rows} == {"Fixed Term Contract"}
    assert {row["locationSummary"] for row in rows} == {"Remote · Europe"}
    assert {row["sector"] for row in rows} == {"Game"}


def test_two_openings_become_two_rows_with_distinct_ids_and_links() -> None:
    rows = _run()

    assert len(rows) == 2
    ids = [row["sourceJobId"] for row in rows]
    links = [row["jobLink"] for row in rows]
    assert len(set(ids)) == 2, ids
    assert len(set(links)) == 2, links


def test_each_link_is_a_per_role_link_on_the_live_host() -> None:
    rows = _run()

    links = sorted(row["jobLink"] for row in rows)
    assert links == [
        "https://studiogloam.com/?careers=senior-lighting-artist",
        "https://studiogloam.com/?careers=senior-technical-artist",
    ]
    assert all(link.startswith("https://studiogloam.com/") for link in links)


def test_the_per_role_links_survive_url_normalization_so_dedup_keeps_both() -> None:
    """The fragment form would collapse both rows; the query form must not."""
    from src.jobs.text_utils import normalize_url

    rows = _run()

    normalized = {normalize_url(row["jobLink"]) for row in rows}
    assert len(normalized) == 2, normalized


def test_each_row_is_stamped_with_the_static_adapter_and_source() -> None:
    rows = _run()

    assert all(row["adapter"] == "static" for row in rows)
    assert all(row["source"] == "Studio Gloam" for row in rows)
    assert all(row["studio"] == "Studio Gloam" for row in rows)


def test_a_bundle_without_the_openings_shape_yields_no_rows_and_does_not_raise() -> None:
    rows = _run(bundle="const other=[{a:1,b:2}];")

    assert rows == []


def test_a_missing_bundle_yields_no_rows_and_does_not_raise() -> None:
    rows = _run(bundle="")

    assert rows == []


def test_a_shell_with_no_entry_script_yields_no_rows_and_does_not_raise() -> None:
    rows = _run(shell="<html><body><div id='root'></div></body></html>")

    assert rows == []


def test_an_empty_parse_is_recorded_on_the_source_row() -> None:
    source_row: dict[str, Any] = {"name": "Studio Gloam", "id": "studiogloam"}
    pages = {"https://studiogloam.com/": _SHELL}
    rows = studiogloam.run(
        fetch_text=_fetch(pages),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        pages=["https://studiogloam.com/"],
        source_row=source_row,
    )

    assert rows == []
    assert isinstance(source_row.get("_staticPluginMeta"), dict)


def test_escaped_quotes_and_backslashes_in_bundle_strings_are_unescaped() -> None:
    bundle = (
        'const QN=[{title:"Senior \\"Lighting\\" Artist",type:"Lighting Art",'
        'location:"Remote \\u00b7 Europe",contract:"Fixed Term Contract"}];'
    )
    rows = _run(bundle=bundle)

    assert [row["title"] for row in rows] == ['Senior "Lighting" Artist']


def test_a_json_rendered_dom_fallback_reads_the_same_object_shape() -> None:
    """When the bundle yields nothing, the rendered HTML is read by the same extractor."""
    rendered = "<html><body><script>" + _BUNDLE + "</script></body></html>"

    rows = _run(
        shell="<html><body><div id='root'></div></body></html>",
        bundle="",
        try_playwright=lambda _url, _timeout: (rendered, ""),
    )

    assert sorted(row["title"] for row in rows) == [
        "Senior Lighting Artist",
        "Senior Technical Artist",
    ]


def test_full_static_source_flow_keeps_both_verified_openings() -> None:
    """The Studio Gloam-only filter exception survives the real static source flow."""
    from src.jobs.adapters import static as static_adapter

    pages = {
        "https://studiogloam.com/": _SHELL,
        "https://studiogloam.com/assets/index-C6_1yUeX.js": _BUNDLE,
    }
    rows = static_adapter.run_static_source_entry_source(
        source_row={
            "name": "Studio Gloam",
            "studio": "Studio Gloam",
            "company": "Studio Gloam",
            "id": "studiogloam",
            "adapter": "static",
            "pages": ["https://studiogloam.com/"],
        },
        diagnostics_name="test_studiogloam_full_static_flow",
        fetch_text=_fetch(pages),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        force_refresh_all=True,
    )

    assert {row["title"] for row in rows} == {
        "Senior Lighting Artist",
        "Senior Technical Artist",
    }


def test_unrelated_static_plugin_still_filters_senior_lighting_artist(
    monkeypatch: Any,
) -> None:
    """The Studio Gloam exception must not broaden the shared role vocabulary."""
    from src.jobs.adapters import static as static_adapter
    from src.jobs.adapters.plugins import default_registry
    from src.jobs.adapters.plugins.types import SimpleAdapterPlugin

    source_host = "unrelated-studio.example"
    unrelated_plugin = SimpleAdapterPlugin(
        name="unrelated_test_plugin",
        family="static",
        priority=1,
        can_handle_fn=lambda ctx: ctx.source_identity == source_host,
        run_fn=lambda **_kwargs: [
            {
                "title": "Senior Lighting Artist",
                "company": "Unrelated Studio",
                "jobLink": f"https://{source_host}/careers/lighting-artist",
                "sourceJobId": "lighting-artist",
            }
        ],
    )
    monkeypatch.setitem(default_registry._families, "static", [unrelated_plugin])

    rows = static_adapter.run_static_source_entry_source(
        source_row={
            "name": "Unrelated Studio",
            "studio": "Unrelated Studio",
            "company": "Unrelated Studio",
            "id": "unrelated-studio",
            "adapter": "static",
            "pages": [f"https://{source_host}/careers"],
        },
        diagnostics_name="test_unrelated_static_plugin_filter_control",
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        force_refresh_all=True,
    )

    assert rows == []
