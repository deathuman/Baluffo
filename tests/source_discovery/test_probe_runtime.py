from __future__ import annotations

from src.source_discovery import probe_runtime


def _static_candidate(url: str = "https://studio.example/careers") -> dict[str, object]:
    return {
        "name": "Runtime Studio",
        "studio": "Runtime Studio",
        "adapter": "static",
        "listing_url": url,
        "pages": [url],
        "discoveryMethod": "test",
        "evidenceScore": 40,
    }


def test_rendered_static_probe_result_requires_static_matching_positive_page() -> None:
    candidate = _static_candidate()
    result = probe_runtime.rendered_static_probe_result(
        candidate,
        rendered_url="https://studio.example/careers/",
        rendered_html='<a href="/jobs/rendering-engineer">Rendering Engineer</a>',
    )

    assert result is not None
    assert result[0] == candidate
    assert result[1] is True
    assert result[2] == 1
    assert result[3] == ""
    assert result[4] == 0


def test_rendered_static_probe_result_ignores_non_matching_or_zero_job_pages() -> None:
    assert (
        probe_runtime.rendered_static_probe_result(
            {"adapter": "greenhouse", "slug": "runtime"},
            rendered_url="https://studio.example/careers",
            rendered_html='<a href="/jobs/rendering-engineer">Rendering Engineer</a>',
        )
        is None
    )
    assert (
        probe_runtime.rendered_static_probe_result(
            _static_candidate(),
            rendered_url="https://other.example/careers",
            rendered_html='<a href="/jobs/rendering-engineer">Rendering Engineer</a>',
        )
        is None
    )
    assert (
        probe_runtime.rendered_static_probe_result(
            _static_candidate(),
            rendered_url="https://studio.example/careers",
            rendered_html="<html><body>No roles</body></html>",
        )
        is None
    )


def test_probe_candidates_after_rendered_results_skips_rendered_identity() -> None:
    rendered = _static_candidate("https://studio.example/careers")
    remaining = _static_candidate("https://other.example/jobs")

    selected = probe_runtime.probe_candidates_after_rendered_results(
        [rendered, remaining],
        [(rendered, True, 1, "", 0)],
    )

    assert selected == [remaining]


def test_candidate_with_probe_evidence_can_mark_prevalidated_discovery() -> None:
    base = _static_candidate()

    normal = probe_runtime.candidate_with_probe_evidence(base, 2)
    prevalidated = probe_runtime.candidate_with_probe_evidence(
        base,
        2,
        prevalidated_discovery=True,
    )

    assert normal["probeStatus"] == "ok"
    assert normal["candidateState"] == "validated"
    assert normal["jobsFound"] == 2
    assert "prevalidatedDiscovery" not in normal
    assert prevalidated["prevalidatedDiscovery"] is True
    assert prevalidated["id"] == normal["id"]
