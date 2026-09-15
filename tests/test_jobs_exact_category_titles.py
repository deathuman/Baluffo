from __future__ import annotations

import pytest

from src.jobs.common.exact_category_titles import (
    has_static_container_artifact_evidence,
    is_exact_category_title,
    is_static_container_artifact_title,
    looks_like_category_container_url,
    looks_like_static_container_url,
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
        "Game Programmer",
        "Software Engineers",
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


@pytest.mark.parametrize(
    ("title", "url"),
    [
        ("Creative", "https://example.com/careers/creative"),
        ("Analytics", "https://example.com/careers/analytics"),
        ("All", "https://example.com/jobs/function-all"),
        ("3D", "https://example.com/careers/function-3d"),
        ("9", "https://example.com/careers?page-is-9"),
        ("Art : new", "https://example.com/careers?department-is-art"),
        ("한국어 ( Koreanisch )", "https://example.com/careers?lang=ko"),
        ("English ( Inglese )", "https://example.com/careers"),
        ("en", "https://example.com/career"),
        ("All categories : new", "https://example.com/vacancies"),
        ("...", "https://example.com/vacancies/filter/page-is-6/apply"),
        ("Skip to content", "https://example.com/careers"),
        ("Careers!", "https://example.com/careers"),
        ("Jobs", "https://example.com/jobs"),
        ("Finance & Legal", "https://example.com/careers/finance-legal"),
        ("Finance &amp; Legal", "https://example.com/careers/finance-legal"),
        ("Finance Legal", "https://example.com/careers/finance-legal"),
        ("DATA & AI", "https://example.com/data-ai"),
        ("Data Ai", "https://example.com/careers/data-ai"),
        ("Wwh", "https://example.com/career/project-wwh"),
    ],
)
def test_static_container_artifact_evidence_marks_generic_container_rows(
    title: str, url: str
) -> None:
    assert has_static_container_artifact_evidence(title, url)


@pytest.mark.parametrize(
    ("title", "url"),
    [
        ("Creative Producer", "https://example.com/careers/creative-producer"),
        ("Creative Producer (Media Marketing)", "https://example.com/jobs/123"),
        ("Art Director", "https://example.com/careers/art-director"),
        ("Web/UI/UX Designer", "https://example.com/jobs/web-ui-ux-designer"),
        (
            "Technical Artist / Graphic Programmer",
            "https://example.com/jobs/technical-artist-graphic-programmer",
        ),
        ("3D Artist", "https://example.com/careers/3d-artist"),
        ("Game Programmer", "https://example.com/en/game-programmer"),
        ("Software Engineers", "https://example.com/job/tech"),
        ("Role A", "https://example.com/jobs/role-a"),
        ("Support Specialist (German)", "https://example.com/jobs/support-specialist-german"),
        ("Sales Representative", "https://example.com/careers/sales-representative"),
        (
            "Sales Development Agent (German-Speaking)",
            "https://example.com/jobs/sales-development-agent",
        ),
        ("Revenue Accountant", "https://example.com/careers/revenue-accountant"),
        ("Gameplay Animator", "https://example.com/careers/gameplay-animator"),
        ("QA Testers", "https://example.com/careers/qa-testers"),
        ("Creative Grouper", "https://example.com/jobs/creative-grouper?lang=es-mx"),
        (
            "Freelance Japanese-English Interpreter & Translator (m/f/d)",
            "https://example.com/jobs/japanese-english-interpreter-translator",
        ),
        (
            "Senior Client Success Advisor, Risk & Audit (English & French)",
            "https://example.com/jobs/senior-client-success-advisor",
        ),
        (
            "Subtitle Writer & AD/CC (English to Tamil) - Remote -Freelancer",
            "https://example.com/jobs/subtitle-writer-tamil",
        ),
    ],
)
def test_static_container_artifact_evidence_allows_real_role_titles(title: str, url: str) -> None:
    assert not has_static_container_artifact_evidence(title, url)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/careers/filter/department-is-creative",
        "https://example.com/jobs/function-all",
        "https://example.com/careers?page-is-9",
        "https://example.com/careers?lang=ko",
        "https://example.com/career",
        "https://example.com/vacancies",
        "https://example.com/career/project-wwh",
    ],
)
def test_looks_like_static_container_url_detects_filter_and_language_urls(
    url: str,
) -> None:
    assert looks_like_static_container_url(url)


# --- language-switch fallback narrowing (2026-09-14 bandai fix) ---------------


@pytest.mark.parametrize(
    "title",
    [
        # bandai careers shapes (the 4 trace targets + survivors): real CJK job
        # titles with ASCII parentheses — the JP market's common style.
        "家庭用ゲームエンジニア (新規格闘アクション)",
        "テクニカルアーティスト (アーティスト部門)",
        "家庭用ゲームエンジニア (クライアント)",
        "家庭用ゲームエンジニア(テクニカルディレクター)",
        "シニアR&Dエンジニア(機械学習)",
        "ゲームエンジニア (プロジェクトマネジメント)",
        # greenhouse (siei): CJK title, brand qualifier in parens.
        "サーバーサイドエンジニア (PlayStation™Network サーバアプリケーション開発)",
        "プロジェクトマネージャー／スクラムマスター(ITセキュリティ領域)",
        # grackle: language word inside the parens, role-shaped outer.
        "Localization Quality Assurance (Simplified Chinese)",
        "Localization Quality Assurance (Traditional Chinese)",
        # gamejobs: CJK title, English department qualifier.
        "C++游戏开发工程师 (Software Engineering)",
        # generic real-title shapes with ASCII parens.
        "Engine Programmer (Unreal Engine)",
        "Senior Producer (Remote)",
        # role-shaped outer containing a veto word.
        "English Teacher (Tokyo)",
        "Language Model Engineer (NLP)",
    ],
)
def test_language_switch_fallback_spares_real_job_titles(title: str) -> None:
    assert not is_static_container_artifact_title(title)


@pytest.mark.parametrize(
    "title",
    [
        # Outer label IS a language name (the switcher contract).
        "English ( Inglese )",
        "EN (日本語)",
        "Deutsch (German)",
        "(English)",
        "(日本語)",
        "(中文)",
        # Whole-outer switcher labels ("Language (中文)" widgets).
        "Language (中文)",
        "言語 (Language)",
        "Sprache (Language)",
        "Languages (English)",
    ],
)
def test_language_switch_fallback_still_flags_switcher_shapes(title: str) -> None:
    assert is_static_container_artifact_title(title)


def test_language_switch_fallback_pure_parenthetical_requires_language_name() -> None:
    """(Remote) is a job posting; (日本語) is a switcher."""
    assert not is_static_container_artifact_title("(Remote)")
    assert not is_static_container_artifact_title("(一部リモート)")
    assert is_static_container_artifact_title("(日本語)")


def test_language_switch_fallback_bare_cjk_without_parens_unchanged() -> None:
    """No parens → the predicate was always False and stays False (out of
    contract); the bare-CJK language-code pins keep their other coverage."""
    assert not is_static_container_artifact_title("日本語版ジョブ一覧")
