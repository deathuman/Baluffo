from typing import cast
from unittest import mock

import pytest

from src.jobs.adapters.social_parser import reddit_parser


def test_reddit_html_parser_returns_empty_for_non_string_input() -> None:
    rows, dropped = reddit_parser.parse_reddit_html_payload(
        cast(str, object()),
        subreddit="gamedev",
        min_confidence=20,
        reject_for_hire_posts=True,
    )

    assert rows == []
    assert dropped == 0


def test_reddit_html_parser_propagates_unexpected_helper_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = """
        <article>
          <h2>We're hiring a Unity Engineer at Nebula Games</h2>
          <a href="https://jobs.nebula.dev/unity">Apply</a>
        </article>
        """
    monkeypatch.setattr(
        reddit_parser,
        "social_extract_apply_url",
        mock.Mock(side_effect=RuntimeError("programmer bug")),
    )

    with pytest.raises(RuntimeError, match="programmer bug"):
        reddit_parser.parse_reddit_html_payload(
            html,
            subreddit="gamedev",
            min_confidence=20,
            reject_for_hire_posts=True,
        )
