from __future__ import annotations

import pytest

from src.source_discovery import provider_inference


@pytest.mark.parametrize(
    ("url", "adapter", "provider_key", "provider_value"),
    [
        (
            "https://boards.greenhouse.io/example-studio/jobs/123",
            "greenhouse",
            "api_url",
            "https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true",
        ),
        (
            "https://api.lever.co/v0/postings/example-studio?mode=json",
            "lever",
            "api_url",
            "https://api.lever.co/v0/postings/examplestudio?mode=json",
        ),
        (
            "https://api.smartrecruiters.com/v1/companies/ExampleStudio/postings",
            "smartrecruiters",
            "company_id",
            "ExampleStudio",
        ),
        (
            "https://apply.workable.com/example-studio/",
            "workable",
            "api_url",
            "https://apply.workable.com/api/v1/widget/accounts/example-studio?details=true",
        ),
        (
            "https://jagex-limited.workable.com/j/8AFE9A3D16",
            "workable",
            "api_url",
            "https://apply.workable.com/api/v1/widget/accounts/jagex-limited?details=true",
        ),
        (
            "https://example.recruitee.com/o/designer",
            "recruitee",
            "api_url",
            "https://example.recruitee.com/api/offers/",
        ),
        (
            "https://example.pinpointhq.com/postings/123",
            "pinpoint",
            "api_url",
            "https://example.pinpointhq.com/postings.json",
        ),
        (
            "https://example.teamtailor.com/jobs/123-gameplay",
            "teamtailor",
            "listing_url",
            "https://example.teamtailor.com/jobs",
        ),
        (
            "https://jobs.ashbyhq.com/example-studio/job/123",
            "ashby",
            "board_url",
            "https://jobs.ashbyhq.com/examplestudio",
        ),
        (
            "https://beamdog.bamboohr.com/careers",
            "bamboohr",
            "listing_url",
            "https://beamdog.bamboohr.com/careers",
        ),
        (
            "https://example.jobs.personio.de/job/123",
            "personio",
            "feed_url",
            "https://example.jobs.personio.de/xml",
        ),
        (
            "https://tencent.wd1.myworkdayjobs.com/en-US/timi_careers?q=game",
            "workday",
            "listing_url",
            "https://tencent.wd1.myworkdayjobs.com/en-US/timi_careers?q=game",
        ),
    ],
)
def test_shared_provider_inference_preserves_provider_row_shapes(
    url: str, adapter: str, provider_key: str, provider_value: str
) -> None:
    row = provider_inference.infer_web_candidate(
        url,
        "Example Studio",
        nl_priority=True,
        discovery_method="web_search",
    )

    assert row is not None
    assert row["name"] == f"Example Studio ({provider_inference.PROVIDER_DISPLAY_NAMES[adapter]})"
    assert row["studio"] == "Example Studio"
    assert row["adapter"] == adapter
    assert row["nlPriority"] is True
    assert row["discoveryMethod"] == "web_search"
    assert row["discoveryStage"] == "web_provider"
    assert row["careersUrl"] == url
    assert row["evidenceTypes"] == ["web_provider_url"]
    assert row["evidenceSource"] == "url"
    assert row[provider_key] == provider_value


def test_shared_provider_inference_preserves_workable_hyphenated_account() -> None:
    row = provider_inference.infer_web_candidate(
        "https://apply.workable.com/jagex-limited/",
        "Jagex",
        nl_priority=False,
    )

    assert row is not None
    assert row["adapter"] == "workable"
    assert row["account"] == "jagex-limited"
    assert (
        row["api_url"]
        == "https://apply.workable.com/api/v1/widget/accounts/jagex-limited?details=true"
    )


def test_shared_provider_inference_rejects_unknown_and_empty_provider_tokens() -> None:
    assert (
        provider_inference.infer_web_candidate(
            "https://example.com/jobs",
            "Example",
            nl_priority=False,
        )
        is None
    )
    assert (
        provider_inference.infer_web_candidate(
            "https://jobs.ashbyhq.com/",
            "Example",
            nl_priority=False,
        )
        is None
    )
    assert (
        provider_inference.infer_web_candidate(
            "https://tencent.wd1.myworkdayjobs.com/",
            "Example",
            nl_priority=False,
        )
        is None
    )
