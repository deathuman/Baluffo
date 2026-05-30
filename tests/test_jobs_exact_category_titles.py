from __future__ import annotations

import pytest

from src.jobs.common.exact_category_titles import (
    is_exact_category_title,
    looks_like_category_container_url,
)


@pytest.mark.parametrize(
    "title",
    [
        "Art",
        "QA",
        "Marketing",
        "Legal",
        "Community",
        "VFX",
        "Technical art",
        "Research & Development",
        "Tech",
    ],
)
def test_is_exact_category_title_marks_static_category_labels(title: str) -> None:
    assert is_exact_category_title(title)


@pytest.mark.parametrize(
    "title",
    [
        "Animator",
        "Graphics Engineer",
        "Legal Counsel",
        "Community Manager",
        "Marketing Manager",
        "Technical Artist",
    ],
)
def test_is_exact_category_title_allows_real_role_titles(title: str) -> None:
    assert not is_exact_category_title(title)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/department/art",
        "https://example.com/jobs/filter/department-is-qa",
        "https://example.com/job-category/vfx",
        "https://example.com/careers/team/legal",
    ],
)
def test_looks_like_category_container_url_detects_category_paths(url: str) -> None:
    assert looks_like_category_container_url(url)
