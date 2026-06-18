from __future__ import annotations

import pytest

from src.source_discovery.directory_index_collection import collect_directory_index_entries


@pytest.mark.parametrize(
    (
        "fetch_payloads",
        "parse_behavior",
        "index_urls",
        "expected_detail_entries",
        "expected_failure",
    ),
    [
        pytest.param(
            {"https://directory.example/two": "two"},
            "success",
            ["https://directory.example/one", "https://directory.example/two"],
            [{"detailUrl": "https://directory.example/detail/two"}],
            {
                "name": "https://directory.example/one",
                "adapter": "example",
                "error": "fetch failed",
                "stage": "directory_index_fetch",
            },
            id="fetch-failure-keeps-scanning",
        ),
        pytest.param(
            None,
            "raise-first",
            ["https://directory.example/one", "https://directory.example/two"],
            [{"detailUrl": "https://directory.example/detail/two"}],
            {
                "name": "https://directory.example/one",
                "adapter": "example",
                "error": "parse failed",
                "stage": "directory_index_parse",
            },
            id="parse-failure-keeps-scanning",
        ),
        pytest.param(
            None,
            "empty",
            ["https://directory.example/one"],
            [],
            {
                "name": "https://directory.example/one",
                "adapter": "example",
                "error": "no entries parsed from index",
                "stage": "directory_index_parse",
            },
            id="empty-parse-reports-failure",
        ),
    ],
)
def test_collect_directory_index_entries_failure_paths(
    fetch_payloads: dict[str, str] | None,
    parse_behavior: str,
    index_urls: list[str],
    expected_detail_entries: list[dict[str, object]],
    expected_failure: dict[str, object],
) -> None:
    def fake_fetch(url: str, _: int) -> str:
        if fetch_payloads is None:
            return url
        if url not in fetch_payloads:
            raise RuntimeError("fetch failed")
        return fetch_payloads[url]

    def parse_index_entries(html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        if parse_behavior == "raise-first" and html.endswith("/one"):
            raise ValueError("parse failed")
        if parse_behavior == "empty":
            return ([], {})
        return ([{"detailUrl": "https://directory.example/detail/two"}], {})

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=index_urls,
        adapter="example",
    )

    assert collected["detailEntries"] == expected_detail_entries
    assert collected["failures"] == [expected_failure]


def test_collect_directory_index_entries_dedupes_and_caps_across_indexes() -> None:
    entries_by_index = {
        "one": [
            {"detailUrl": "https://directory.example/detail/a", "source": "one"},
            {"detailUrl": "https://directory.example/detail/b", "source": "one"},
        ],
        "two": [
            {"detailUrl": "https://directory.example/detail/b", "source": "two"},
            {"detailUrl": "https://directory.example/detail/c", "source": "two"},
            {"detailUrl": "https://directory.example/detail/d", "source": "two"},
        ],
    }

    def fake_fetch(url: str, _: int) -> str:
        return url.rsplit("/", 1)[-1]

    def parse_index_entries(html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        return (entries_by_index[html], {})

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=["https://directory.example/one", "https://directory.example/two"],
        adapter="example",
        max_entries=3,
    )

    assert collected["detailEntries"] == [
        {"detailUrl": "https://directory.example/detail/a", "source": "one"},
        {"detailUrl": "https://directory.example/detail/b", "source": "one"},
        {"detailUrl": "https://directory.example/detail/c", "source": "two"},
    ]
    assert collected["failures"] == []


def test_collect_directory_index_entries_aggregates_unresolved_references() -> None:
    def fake_fetch(url: str, _: int) -> str:
        return url.rsplit("/", 1)[-1]

    def parse_index_entries(
        html: str,
        _base_url: str,
        *,
        prefer_english: bool,
    ) -> tuple[list[dict[str, object]], dict]:
        assert prefer_english is True
        return (
            [{"detailUrl": f"https://directory.example/detail/{html}"}],
            {"unresolvedReferenceCount": 2 if html == "one" else 3},
        )

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=["https://directory.example/one", "https://directory.example/two"],
        adapter="example",
        parse_kwargs={"prefer_english": True},
    )

    assert int(collected["unresolvedReferenceCount"]) == 5
    assert collected["failures"] == []


def test_collect_directory_index_entries_does_not_swallow_unexpected_fetch_failure() -> None:
    def fake_fetch(_url: str, _: int) -> str:
        raise AssertionError("unexpected fetch bug")

    def parse_index_entries(html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        return ([{"detailUrl": html}], {})

    with pytest.raises(AssertionError, match="unexpected fetch bug"):
        collect_directory_index_entries(
            timeout_s=5,
            fetcher=fake_fetch,
            parse_index_entries=parse_index_entries,
            base_url="https://directory.example",
            index_urls=["https://directory.example/one"],
            adapter="example",
        )


def test_collect_directory_index_entries_does_not_swallow_unexpected_parse_failure() -> None:
    def fake_fetch(url: str, _: int) -> str:
        return url

    def parse_index_entries(_html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        raise AssertionError("unexpected parse bug")

    with pytest.raises(AssertionError, match="unexpected parse bug"):
        collect_directory_index_entries(
            timeout_s=5,
            fetcher=fake_fetch,
            parse_index_entries=parse_index_entries,
            base_url="https://directory.example",
            index_urls=["https://directory.example/one"],
            adapter="example",
        )
