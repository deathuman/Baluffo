"""Real game roles the row filter rejected, and business roles it must keep rejecting.

`looks_like_game_job` had 19 sector-grade keywords and rejected outright 1,226 catalogue
roles missing from boards Baluffo already collects: Ubisoft's "Senior Hard Surface Artist",
CD Projekt Red's "Senior VFX Artist", Marvelous's ゲームデザイナー. Every craft token added
recovered at least one of those and admitted none of the control set below, measured with
`tools/coverage_classifier_candidates.py`.

The control set matters as much as the recovery set. A games studio hires project managers,
producers and finance business partners; widening the filter until those land turns a
games-jobs feed into a jobs board.

The second batch came from the zero-kept board probe: Housemarque lighting artists, KRAFTON /
2kvegas / Scape / JustPlay QA and test roles, and Beamdog's Senior Writer. Writer is covered
by title variants rather than the bare token, so Technical Writer, Content Writer and
Copywriter stay rejected; those controls are pinned below.

The same title-scoped variants also recover legitimate adjacent roles at 2K, Cloud Chamber,
and other game/VFX studios. Those are intentional recoveries, not target-board-only
exceptions; they are pinned in the recovery set below.

**The craft tokens must not reach the sector classifier.** `has_positive_game_evidence`
answers a harder question -- what sector is this employer -- and a bare role word at an
unnamed employer must not imply Game. That is why the craft tokens live in
`GAME_ROLE_KEYWORDS` rather than `GAME_KEYWORDS`; folding them together made "UX Designer" at
an arbitrary employer classify as Game.
"""

from __future__ import annotations

import pytest

from src.jobs.game_detection import (
    GAME_KEYWORDS,
    GAME_ROLE_KEYWORDS,
    has_positive_game_evidence,
    looks_like_game_job,
)

# Every one of these was rejected before the craft tokens were added.
RECOVERED = [
    ("Senior VFX Artist", "CD PROJEKT RED"),
    ("Senior Hard Surface Artist", "AEXLAB"),
    ("VEHICLE ARTIST", "Bugbear Entertainment"),
    ("LEVEL DESIGNER", "Bugbear Entertainment"),
    ("3D Generalist", "Ayelet Studio"),
    ("Principal UI/UX Artist (Dundee)", "4J Studios"),
    ("Technical Director", "Area 35 East"),
    ("Environmental Artist", "Atlas Creative"),
    ("Art Director", "Bluembo"),
    ("SENIOR 3D ARTIST", "Cosmico"),
    ("Senior/Lead 2D Artist", "Bluembo"),
    ("Combat Designer", "Various"),
    ("Sound Designer", "Various"),
    ("QA Analyst", "Various"),
    ("Tools Programmer", "Various"),
    # Zero-kept board probe: craft roles the earlier token set still missed.
    ("Lead Lighting Artist", "Housemarque"),
    ("Principal Lighting Artist", "Housemarque"),
    ("Senior Lighting Artist", "Housemarque"),
    ("Publishing QA", "KRAFTON India"),
    ("Technical Test Analyst II", "2kvegas"),
    ("Technical Test Lead", "2kvegas"),
    ("Junior QA Tester", "Scape"),
    ("Platform QA Chapter Lead", "JustPlay"),
    ("Senior Writer", "Beamdog"),
    ("Narrative Writer", "Various"),
    ("Lead Writer", "Various"),
    ("Story Writer", "Various"),
    # Adjacent recoveries the title-scoped variants intentionally keep.
    ("Senior Writer", "2K"),
    ("Senior Writer", "Cloud Chamber"),
    ("[Bluehole Studio] Lighting Artist (Lead / Senior) - Project V", "Bluehole Inc."),
    ("Lead Lighting Artist", "PlayStation Global"),
    ("Lighting Artist", "KOJIMA PRODUCTIONS"),
    ("Lighting Artist", "Kojimaproductions"),
    ("Senior Lighting Artist (Fixed-Term Contract)", "Fools Theory"),
    ("Senior Lighting Artist", "Torn Banner Studios"),
    ("Senior Environment Lighting Artist (Contingent)", "Naughty Dog"),
    ("Lead Lighting Artist - Raven - Middleton, WI", "Activision"),
    ("Lead Lighting Artist - Raven - Middleton, WI", "Raven Software"),
    ("Cinematic Lighting Artist", "Eidos Montreal"),
    ("Lighting Artist (UE4)", "Ride the Lion Studios"),
    # Non-English listings, which an ASCII substring test cannot see at all.
    ("ゲームデザイナー", "Marvelous Inc. Japan"),
    ("ゲームAIエンジニア", "Bandai Namco Studios"),
    ("yuushoku artifactor アートディレクター", "Various"),
    ("Animateur.trice Cinématique Sénior", "Ubisoft"),
    ("Développeur Unity", "Ubisoft"),
]

# Control set: plainly not games work, at studios included.
STILL_REJECTED = [
    ("Logistics Director", "Atlas Creative"),
    ("Project Manager", "AppQuantum"),
    ("Producer", "Atlas Creative"),
    ("Senior Mobile Data Analyst/Scientist", "Big Red Button Enter"),
    ("Marketing Manager, Europe", "Roblox"),
    ("Junior .NET Developer", "Plarium"),
    ("Principal Security Software Engineer, IAM", "Roblox"),
    ("Senior Software Developer - Web Engineering", "Miniclip"),
    ("Networking Engineer", "Sneakybox"),
    ("Office Coordinator", "Hangar 13"),
    ("Talent Acquisition Partner", "Various"),
    ("Finance Business Partner", "Various"),
    ("Accountant", "Various"),
    ("HR Business Partner", "Various"),
    ("Facilities Manager", "Various"),
    # Targeted writer/QA/lighting tokens must not widen into corporate roles.
    ("Technical Writer", "Various"),
    ("Senior Technical Writer", "Beamdog"),
    ("Content Writer", "Various"),
    ("Copywriter", "Various"),
    ("UX Writer", "Various"),
    ("Marketing Writer", "Various"),
    ("Lighting Designer", "Various"),
    ("Lighting Technician", "Various"),
    ("QA Engineer", "Various"),
    ("QA Manager", "Various"),
    ("Software Test Engineer", "Various"),
    ("Test Analyst", "Various"),
    ("Test Lead", "Various"),
]


@pytest.mark.parametrize(("title", "company"), RECOVERED)
def test_real_game_roles_are_no_longer_rejected(title: str, company: str) -> None:
    assert looks_like_game_job(title, company), f"{title} at {company}"


@pytest.mark.parametrize(("title", "company"), STILL_REJECTED)
def test_business_roles_are_still_rejected(title: str, company: str) -> None:
    assert not looks_like_game_job(title, company), f"{title} at {company}"


def test_the_two_keyword_sets_stay_separate() -> None:
    """A craft term is row evidence, never sector evidence on its own."""
    assert GAME_ROLE_KEYWORDS and GAME_KEYWORDS
    assert not (GAME_ROLE_KEYWORDS & GAME_KEYWORDS)
    for role_only in (
        "ux designer",
        "concept artist",
        "animator",
        "art director",
        "lighting artist",
        "qa tester",
        "senior writer",
    ):
        assert role_only in GAME_ROLE_KEYWORDS, role_only
        assert role_only not in GAME_KEYWORDS, role_only


def test_writer_is_covered_by_title_variants_not_a_bare_token() -> None:
    """A bare `writer` would admit technical, content and marketing writers."""
    assert "writer" not in GAME_ROLE_KEYWORDS
    for variant in (
        "game writer",
        "lead writer",
        "narrative writer",
        "senior writer",
        "story writer",
    ):
        assert variant in GAME_ROLE_KEYWORDS, variant


@pytest.mark.parametrize(
    "title",
    [
        "UX Designer",
        "Concept Artist",
        "Animator",
        "Art Director",
        "Lighting Artist",
        "QA Tester",
        "Senior Writer",
    ],
)
def test_a_role_word_alone_is_not_sector_evidence(title: str) -> None:
    assert not has_positive_game_evidence("Some Employer", title)


def test_sector_keywords_still_classify_on_their_own() -> None:
    assert has_positive_game_evidence("Some Studio", "Senior Gameplay Programmer")
    assert looks_like_game_job("Some Studio", "Senior Gameplay Programmer")


def test_non_game_tokens_were_not_added() -> None:
    """The measured change added craft terms, not whole departments."""
    for absent in (
        "project manager",
        "producer",
        "accountant",
        "marketing",
        "recruiter",
        "business development",
        "data analyst",
        "logistics",
    ):
        assert absent not in GAME_ROLE_KEYWORDS, absent
