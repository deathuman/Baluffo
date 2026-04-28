from __future__ import annotations

from src.source_discovery.directory_index_collection import collect_directory_index_entries


def test_collect_directory_index_entries_keeps_scanning_after_fetch_failure() -> None:
    payloads = {"https://directory.example/two": "two"}

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError("fetch failed")
        return payloads[url]

    def parse_index_entries(_html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        return ([{"detailUrl": "https://directory.example/detail/two"}], {})

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=["https://directory.example/one", "https://directory.example/two"],
        adapter="example",
    )

    assert collected["detailEntries"] == [{"detailUrl": "https://directory.example/detail/two"}]
    assert collected["failures"] == [
        {
            "name": "https://directory.example/one",
            "adapter": "example",
            "error": "fetch failed",
            "stage": "directory_index_fetch",
        }
    ]


def test_collect_directory_index_entries_keeps_scanning_after_parse_failure() -> None:
    def fake_fetch(url: str, _: int) -> str:
        return url

    def parse_index_entries(html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        if html.endswith("/one"):
            raise ValueError("parse failed")
        return ([{"detailUrl": "https://directory.example/detail/two"}], {})

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=["https://directory.example/one", "https://directory.example/two"],
        adapter="example",
    )

    assert collected["detailEntries"] == [{"detailUrl": "https://directory.example/detail/two"}]
    assert collected["failures"] == [
        {
            "name": "https://directory.example/one",
            "adapter": "example",
            "error": "parse failed",
            "stage": "directory_index_parse",
        }
    ]


def test_collect_directory_index_entries_reports_empty_parse_failure() -> None:
    def fake_fetch(url: str, _: int) -> str:
        return url

    def parse_index_entries(_html: str, _base_url: str) -> tuple[list[dict[str, object]], dict]:
        return ([], {})

    collected = collect_directory_index_entries(
        timeout_s=5,
        fetcher=fake_fetch,
        parse_index_entries=parse_index_entries,
        base_url="https://directory.example",
        index_urls=["https://directory.example/one"],
        adapter="example",
    )

    assert collected["detailEntries"] == []
    assert collected["failures"] == [
        {
            "name": "https://directory.example/one",
            "adapter": "example",
            "error": "no entries parsed from index",
            "stage": "directory_index_parse",
        }
    ]


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
