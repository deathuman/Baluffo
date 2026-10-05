"""A provider board with no curated URL must still be reachable by its collected jobs.

**193 of 695 curated rows carry no `listing_url`** -- greenhouse 52, ashby 39, workable 29,
lever 22, smartrecruiters 16, breezy 15, personio 9, jazzhr 8, recruitee 3. None are Workday;
all 17 Workday rows do carry one. They identify by adapter plus slug / account / company_id,
with `api_url` and sometimes `board_url` alongside.

That made them un-attributable by construction. `_posting_prefix(row["listing_url"])` is `""`
for each of them and `_match_board` skips an empty prefix outright, so 190 of the 267 prefix
entries held nothing usable. Measured on the v7 run, 168 provider boards sat at zero openings
while their adapter's rollup collected 7,956 -- and 194 greenhouse postings went unattributed
for want of a rule.

The fix is a second, independent rule: the tenant **is** the board's first URL path segment
(`boards.greenhouse.io/razorgames/jobs/1` -> `razorgames`), and `registry_identity` already
derives the tenant from exactly that segment, so both sides agree by construction rather than
by a prefix guess. Prefix is tried first because it is stricter where a URL exists.

Two guards keep this from becoming a false green:

- only **unique** tenants are indexed, so two boards sharing a tenant string resolve to
  nothing rather than to whichever registered first;
- a job with no `sourceBundle` resolves to nothing, because it carries no provenance.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_attribution_index_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cd = _load()


# --- a provider board with no curated URL is un-attributable by prefix alone ----------


def _greenhouse_report(*slugs):
    """Provider boards in the shape the curated table actually uses.

    **193 of 695 curated rows carry no `listing_url`** -- greenhouse 52, ashby 39,
    workable 29, lever 22, smartrecruiters 16, breezy 15, personio 9, jazzhr 8, recruitee 3.
    None are Workday; all 17 Workday rows do carry one. They identify by adapter + slug /
    account / company_id, with `api_url` and sometimes `board_url` alongside.
    """
    curated, active = [], []
    for slug in slugs:
        curated.append(
            {
                "adapter": "greenhouse",
                "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                "coverageAuditOpenings": 10,
                "listing_url": "",
                "name": slug,
                "nlPriority": False,
                "slug": slug,
                "studio": slug,
            }
        )
        active.append({"id": f"greenhouse:slug:{slug}", "adapter": "greenhouse"})
    return cd.registration_report(curated=curated, active=active, pending=[])


def _greenhouse_job(slug, job_id="12345"):
    url = f"https://boards.greenhouse.io/{slug}/jobs/{job_id}"
    return {
        "source": "greenhouse_boards",
        "jobLink": url,
        "sourceBundle": [
            {"adapter": "greenhouse", "jobLink": url, "source": "greenhouse_boards", "studio": slug}
        ],
    }


def test_provider_board_without_a_curated_url_has_an_empty_prefix():
    """The defect this fixes, stated as the precondition it needs.

    The curated row carries no `listing_url`, so the prefix index holds `""` for it and
    `_match_board` skips an empty prefix outright. These boards were un-attributable by
    construction: 168 of them sat at zero openings while their adapter's rollup collected
    7,956.
    """
    report = _greenhouse_report("razorgames")
    _by_source, prefix_index, _rollup, _tenants = cd._source_key_index(report)
    assert prefix_index, "the board is registered and must appear in the prefix index"
    assert all(prefix == "" for _host, prefix, _key in prefix_index)


def test_a_provider_posting_resolves_to_its_board_by_tenant_segment():
    report = _greenhouse_report("razorgames", "koeitecmo")
    by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    key = cd._match_bundle(_greenhouse_job("razorgames"), prefix_index, tenant_index)
    assert key is not None, "the posting must reach its board with no URL on the curated row"
    assert key[1] == "razorgames"
    assert by_source == {}, "a provider board has no per-board static source row"


def test_each_tenant_gets_its_own_board_not_the_first_one():
    """Cross-tenant attribution is the same defect as cross-tenant suppression.

    66 candidates once matched one Workday tenant to whichever board registered first. The
    same failure here would credit one studio's board with another's postings.
    """
    report = _greenhouse_report("razorgames", "koeitecmo")
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    razor = cd._match_bundle(_greenhouse_job("razorgames"), prefix_index, tenant_index)
    koei = cd._match_bundle(_greenhouse_job("koeitecmo"), prefix_index, tenant_index)
    assert razor[1] == "razorgames"
    assert koei[1] == "koeitecmo"
    assert razor != koei


def test_the_tenant_segment_is_read_case_insensitively():
    """The registry lowercases path segments; a mixed-case posting URL is the same board."""
    report = _greenhouse_report("RazorGames")
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    job = _greenhouse_job("RazorGames")
    assert cd._match_bundle(job, prefix_index, tenant_index)[1] == "razorgames"


def test_the_bundle_studio_name_is_a_second_tenant_source():
    """A CDN-hosted posting whose URL carries no board segment still names its board.

    `sourceBundle` carries `studio` per contributing source precisely so a job whose link
    lives elsewhere can be attributed to whoever served it.
    """
    report = _greenhouse_report("razorgames")
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    job = _greenhouse_job("razorgames")
    job["sourceBundle"][0]["jobLink"] = "https://cdn.greenhouse.io/apply/12345"
    assert cd._match_bundle(job, prefix_index, tenant_index)[1] == "razorgames"


def test_an_ambiguous_tenant_is_dropped_rather_than_guessed():
    """Two boards sharing a tenant string is unresolvable from a posting URL.

    First-wins would credit one board with the other's openings -- a false green built out
    of the exact mistake this index exists to stop.
    """
    curated = [
        {
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/shared/jobs",
            "coverageAuditOpenings": 5,
            "listing_url": "",
            "name": "a",
            "slug": "shared",
        },
        {
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/shared/jobs",
            "coverageAuditOpenings": 5,
            "listing_url": "",
            "name": "b",
            "slug": "shared",
        },
    ]
    active = [
        {"id": "greenhouse:slug:one", "adapter": "greenhouse"},
        {"id": "greenhouse:slug:two", "adapter": "greenhouse"},
    ]
    report = cd.registration_report(curated=curated, active=active, pending=[])
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    assert "shared" not in tenant_index
    # Unresolvable is the honest outcome; a wrong board is not.
    assert cd._match_bundle(_greenhouse_job("shared"), prefix_index, tenant_index) is None


def test_prefix_still_wins_where_it_applies():
    """Prefix is stricter where a URL exists, so it is tried first.

    A board under `/careers` and another under `/careers/milan`: a posting under the longer
    prefix belongs to Milan, and the tenant rule cannot tell them apart.
    """
    curated = [
        {
            "adapter": "workday",
            "coverageAuditOpenings": 5,
            "listing_url": "https://host.wd5.myworkdayjobs.com/tenant/careers",
            "name": "root",
        }
    ]
    active = [
        {
            "id": "workday:listing_url:https://host.wd5.myworkdayjobs.com/tenant/careers",
            "adapter": "workday",
        }
    ]
    report = cd.registration_report(curated=curated, active=active, pending=[])
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    posting = "https://host.wd5.myworkdayjobs.com/tenant/careers/milan/job/1"
    job = {
        "source": "workday_sources",
        "jobLink": posting,
        "sourceBundle": [
            {
                "adapter": "workday",
                "jobLink": posting,
                "source": "workday_sources",
                "studio": "tenant",
            }
        ],
    }
    assert cd._match_bundle(job, prefix_index, tenant_index) is not None


def test_a_posting_under_no_registered_tenant_matches_nothing():
    report = _greenhouse_report("razorgames")
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    assert cd._match_bundle(_greenhouse_job("someotherstudio"), prefix_index, tenant_index) is None


def test_a_job_with_no_source_bundle_still_matches_nothing():
    """A provider job carrying no provenance is unattributable, and must be counted as such.

    Guessing here is how a bundle row came to be credited to a board that fetched nothing.
    """
    report = _greenhouse_report("razorgames")
    _by_source, prefix_index, _rollup, tenant_index = cd._source_key_index(report)
    assert cd._match_bundle({"source": "greenhouse_boards"}, prefix_index, tenant_index) is None


def test_first_path_segment_reads_the_board_not_the_first_folder():
    assert (
        cd._first_path_segment("https://h.wd5.myworkdayjobs.com/Tencent_Careers/job/Tokyo/9")
        == "tencent_careers"
    )
    assert cd._first_path_segment("https://boards.greenhouse.io/razorgames/jobs/1") == "razorgames"
    assert cd._first_path_segment("https://host/") == ""
    assert cd._first_path_segment("") == ""
