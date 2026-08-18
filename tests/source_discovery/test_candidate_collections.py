from __future__ import annotations

from typing import Any

from src.source_discovery import candidate_collections


def test_candidate_rows_ignores_malformed_values() -> None:
    assert candidate_collections.candidate_rows(None) == []
    assert candidate_collections.candidate_rows({"adapter": "greenhouse"}) == []
    assert candidate_collections.candidate_rows([{"adapter": "greenhouse"}, "bad", None]) == [
        {"adapter": "greenhouse"}
    ]


def test_provider_static_rows_from_payload_dedupes_candidates() -> None:
    provider_rows, static_rows = candidate_collections.provider_static_rows_from_payload(
        {
            "providerCandidates": [
                {"adapter": "greenhouse", "slug": "same"},
                {"adapter": "greenhouse", "slug": "same"},
            ],
            "staticCandidates": [
                {"adapter": "static", "pages": ["https://studio.example/jobs"]},
                {"adapter": "static", "pages": ["https://studio.example/jobs"]},
            ],
        }
    )

    assert provider_rows == [
        {"adapter": "greenhouse", "slug": "same", "id": "greenhouse:slug:same"}
    ]
    assert static_rows == [
        {
            "adapter": "static",
            "pages": ["https://studio.example/jobs"],
            "id": static_rows[0]["id"],
        }
    ]
    assert str(static_rows[0]["id"]).startswith("static:")


def test_split_provider_static_rows_routes_static_and_provider_adapters() -> None:
    provider_rows, static_rows = candidate_collections.split_provider_static_rows(
        [
            {"adapter": "greenhouse", "slug": "studio"},
            {"adapter": "static", "pages": ["https://studio.example/jobs"]},
            {"adapter": ""},
            {"name": "missing adapter"},
        ]
    )

    assert provider_rows == [{"adapter": "greenhouse", "slug": "studio"}]
    assert static_rows == [{"adapter": "static", "pages": ["https://studio.example/jobs"]}]


def test_append_provider_static_rows_preserves_keys_and_dedupes() -> None:
    artifact: dict[str, Any] = {
        "providerCandidates": [{"adapter": "greenhouse", "slug": "same"}],
        "staticCandidates": [{"adapter": "static", "pages": ["https://studio.example/jobs"]}],
        "summary": {"unchanged": True},
    }

    candidate_collections.append_provider_static_rows(
        artifact,
        provider_rows=[
            {"adapter": "greenhouse", "slug": "same"},
            {"adapter": "lever", "account": "other"},
        ],
        static_rows=[
            {"adapter": "static", "pages": ["https://studio.example/jobs"]},
            {"adapter": "static", "pages": ["https://other.example/jobs"]},
        ],
    )

    assert artifact["providerCandidates"] == [
        {"adapter": "greenhouse", "slug": "same", "id": "greenhouse:slug:same"},
        {"adapter": "lever", "account": "other", "id": "lever:account:other"},
    ]
    assert artifact["staticCandidates"] == [
        {
            "adapter": "static",
            "pages": ["https://studio.example/jobs"],
            "id": artifact["staticCandidates"][0]["id"],
        },
        {
            "adapter": "static",
            "pages": ["https://other.example/jobs"],
            "id": artifact["staticCandidates"][1]["id"],
        },
    ]
    assert all(str(row["id"]).startswith("static:") for row in artifact["staticCandidates"])
    assert artifact["summary"] == {"unchanged": True}
