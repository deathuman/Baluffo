"""A keyword-headed posting section, and the section tabs that must never be counted as one.

The sibling rule in `_is_same_listing_detail_link` requires a posting to sit under the board's own base
path. That misses two common shapes, both measured live: a landing page that is not a listing path at
all (Jyamma Games serves its master list at `/careersjyamma`, so the base check bails before any link is
examined and the probe reported `no_jobs` at `confidence=high` on a page carrying four real roles), and
a listing section spelled differently from the landing page (`/join-us` boards whose roles live under
`/career/<slug>`).

`_is_keyword_headed_detail_link` admits those. Its whole safety rests on the *last* segment looking like
a posting rather than a section, so these tests pin the refusals as firmly as the acceptances -- a
one-sided test here would let a category page start reporting itself as an opening.

The multi-word-labelled section tab is the case that needs its own fixture. The existing
`listing-navigation-only` case protects Krafton's `/careers/jobs/` and `/careers/people/` only because
their labels are single words; nothing pinned the multi-word form, so a change that admitted on label
alone would have passed that fixture while quietly unpinning the protection. These cases close it, and
they are refused on the **path** -- the label is varied deliberately so the path is what has to carry it.
"""

from __future__ import annotations

from src.source_discovery.probe import (
    _is_keyword_headed_detail_link,
    _static_detail_links,
    static_probe_evidence,
)


def test_role_under_a_section_the_base_path_does_not_name() -> None:
    """Jyamma: the base is not a listing path, so the sibling rule cannot see these at all."""
    assert _is_keyword_headed_detail_link(
        "https://jyammagames.com/careers/vfx-artist/", "1 position available"
    )
    assert _is_keyword_headed_detail_link(
        "https://jyammagames.com/careers/senior-game-programmer/", "1 position available"
    )


def test_an_opaque_id_is_rescued_by_a_multi_word_label() -> None:
    assert _is_keyword_headed_detail_link(
        "https://jyammagames.com/careers/34124/", "1 position available"
    )


def test_listing_section_spelled_differently_from_the_landing_page() -> None:
    """`/join-us` landing pages whose roles live under `/career/<slug>`."""
    assert _is_keyword_headed_detail_link("https://eipix.com/career/data-analyst/", "Data Analyst")
    assert _is_keyword_headed_detail_link(
        "https://www.nordcurrent.com/careers/e-commerce-specialist-2/", "E-Commerce Specialist"
    )


def test_compound_head_spellings_are_recognised() -> None:
    """Boards spell the postings area as job-listing, career_listing, open_positions, jobdetails."""
    for url in (
        "https://www.supersocialinc.com/job-listings/engineer-3e7pm",
        "https://panteon.games/job-listing/2d-game-artist/",
    ):
        assert _is_keyword_headed_detail_link(url, ""), url


def test_a_jobdetails_head_with_a_bare_numeric_leaf_needs_a_label() -> None:
    """Demiurge's `/job-details/` is a bare tab that renders one job -- unconfirmed either way.

    A head that says "job details" plus a database id is not identifiable on shape, so it is admitted
    only when the anchor carries a job-like label. Guessing here would report a section tab as a role.
    """
    url = "https://jyammagames.com/job-details/12345"
    assert not _is_keyword_headed_detail_link(url, "")
    assert _is_keyword_headed_detail_link(url, "Senior Technical Animator")


def test_a_compound_section_name_is_refused_by_its_parts() -> None:
    """Hyphen counting is the wrong test: `/careers/benefits-and-perks` has two hyphens.

    The leaf is split on hyphens and refused when any part is a section word, so compound section names
    cannot buy their way in with a shape that a real posting slug also has.
    """
    for url in (
        "https://example.test/careers/benefits-and-perks",
        "https://example.test/careers/life-and-culture",
        "https://example.test/careers/teams-and-staff",
    ):
        assert not _is_keyword_headed_detail_link(url, ""), url


def test_a_short_hyphenated_leaf_is_admitted() -> None:
    """`vfx-artist` is one hyphen and ten characters, and it is a real role."""
    assert _is_keyword_headed_detail_link("https://jyammagames.com/careers/vfx-artist/", "")


def test_a_bare_numeric_id_needs_a_job_like_label() -> None:
    """Aspyr's `/career_listing/6025591004` is only identifiable from its anchor text.

    The leaf is a database id with no hyphen and no role word, so on shape alone it is indistinguishable
    from a category page -- which is the correct answer for a shape-only rule. Its real anchor label is
    the job title, and that is what admits it.
    """
    assert not _is_keyword_headed_detail_link("https://www.aspyr.com/career_listing/6025591004", "")
    assert _is_keyword_headed_detail_link(
        "https://www.aspyr.com/career_listing/6025591004",
        "engineering manager - technical partner management",
    )


def test_an_extra_slot_segment_does_not_hide_the_role() -> None:
    """GSC: `/careers/v/<id>-<slug>` -- the leaf, not the slot, carries the posting."""
    assert _is_keyword_headed_detail_link(
        "https://gscpeopleforce.io/careers/v/239898-lead-writer", "Lead Writer"
    )


# ── the refusals: everything below is a section, a category, or a document ──────────


def test_multi_word_labelled_section_tabs_are_refused() -> None:
    """The gap the single-word Krafton fixture left open.

    Each of these is a navigation target with a multi-word label, so a rule that admitted on the label
    would pass the existing fixture and still report these as openings.
    """
    for url, label in (
        ("https://www.krafton.com/careers/jobs/", "Jobs"),
        ("https://www.krafton.com/careers/people/", "People"),
        ("https://www.krafton.com/careers/faq/", "Frequently Asked Questions"),
        ("https://www.activision.com/careers/veterans/", "Veterans Program"),
        ("https://www.motivegames.com/careers/students/", "Students and Grads"),
        ("https://fanatee.com/careers/community/", "Life at Fanatee Community"),
    ):
        assert not _is_keyword_headed_detail_link(url, label), url


def test_a_department_index_is_refused_even_with_a_multi_word_label() -> None:
    """Byjus: `/careers/all-openings/job-category/<dept>/` is three segments deep."""
    for dept in ("creative", "engineering", "sales"):
        assert not _is_keyword_headed_detail_link(
            f"https://www.byjus.com/careers/all-openings/job-category/{dept}/",
            f"{dept.title()} Jobs",
        ), dept


def test_a_single_bare_word_segment_is_refused() -> None:
    for url in (
        "https://www.krafton.com/careers/benefits/",
        "https://www.krafton.com/careers/culture/",
        "https://www.krafton.com/careers/teams/",
        "https://www.krafton.com/careers/press/",
    ):
        assert not _is_keyword_headed_detail_link(url, "Read more"), url


def test_documents_and_assets_are_refused() -> None:
    for url in (
        "https://madovergames.com/careers/apply-now.pdf",
        "https://example.test/careers/policy.png",
        "https://example.test/careers/team.jpg",
    ):
        assert not _is_keyword_headed_detail_link(url, "Download the team"), url


def test_template_seams_are_refused() -> None:
    """Imagendry serves `/careers/<?php …` -- the fetched page is not a posting."""
    assert not _is_keyword_headed_detail_link(
        "https://imagendry.com/careers/%3C%3Fphp%20echo%20$slug", "Apply now"
    )


def test_generic_application_links_are_refused() -> None:
    for label in ("Open application", "Submit your application", "Speculative application"):
        assert not _is_keyword_headed_detail_link(
            "https://eipix.com/career/speculative-application-2026/", label
        ), label


def test_a_single_segment_path_is_not_a_posting() -> None:
    assert not _is_keyword_headed_detail_link("https://jyammagames.com/careers", "Careers")


def test_a_non_listing_head_is_refused() -> None:
    for url in (
        "https://example.test/blog/senior-gameplay-programmer",
        "https://example.test/news/vfx-artist-2026",
        "https://example.test/press/senior-backend-developer",
    ):
        assert not _is_keyword_headed_detail_link(url, "Senior Backend Developer"), url


# ── end to end ─────────────────────────────────────────────────────────────────────


def test_jyamma_probe_recovers_its_four_roles() -> None:
    html = """
    <html><body><main>
      <a href="/careers/vfx-artist/">1 position available</a>
      <a href="/careers/senior-game-programmer/">1 position available</a>
      <a href="/careers/ui-ux-artist/">1 position available</a>
      <a href="/careers/34124/">1 position available</a>
      <a href="/about/">About us</a>
      <a href="/games/">Our games</a>
    </main></body></html>
    """
    evidence = static_probe_evidence(html, "https://jyammagames.com/careersjyamma")

    assert evidence.count == 4
    links = _static_detail_links(html, "https://jyammagames.com/careersjyamma")
    assert "https://jyammagames.com/careers/vfx-artist/" in links
    assert "https://jyammagames.com/about/" not in links
    assert "https://jyammagames.com/games/" not in links


def test_a_navigation_only_board_still_reports_no_jobs() -> None:
    """The regression this branch could have caused, stated as the end-to-end behaviour."""
    html = """
    <html><body><main>
      <a href="/careers/jobs/">Jobs</a>
      <a href="/careers/people/">People</a>
      <a href="/careers/faq/">Frequently Asked Questions</a>
      <a href="/careers/benefits/">Benefits</a>
    </main></body></html>
    """
    evidence = static_probe_evidence(html, "https://www.krafton.com/careers/")

    assert evidence.count == 0
    assert evidence.reason == "no_jobs"
    assert _static_detail_links(html, "https://www.krafton.com/careers/") == ()


def test_a_department_index_board_still_reports_no_jobs() -> None:
    html = """
    <html><body><main>
      <a href="/careers/all-openings/job-category/creative/">Creative Jobs</a>
      <a href="/careers/all-openings/job-category/engineering/">Engineering Jobs</a>
    </main></body></html>
    """
    evidence = static_probe_evidence(html, "https://www.byjus.com/careers-at-byjus")

    assert evidence.count == 0
