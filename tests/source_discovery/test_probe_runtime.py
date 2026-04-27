from __future__ import annotations

import asyncio

from src.source_discovery import probe as probe_module
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


def test_run_bounded_probe_batch_async_uses_caller_probe_and_kwargs() -> None:
    seen: dict[str, object] = {}

    async def fake_probe(row, timeout_s, *, fetcher, try_playwright, playwright_semaphore):
        seen["row"] = row
        seen["timeout_s"] = timeout_s
        seen["try_playwright"] = try_playwright
        seen["playwright_semaphore"] = playwright_semaphore
        return True, 3, ""

    candidate = _static_candidate()
    sentinel_fetcher = object()
    sentinel_playwright = object()
    sentinel_semaphore = object()

    results = asyncio.run(
        probe_runtime.run_bounded_probe_batch_async(
            [candidate],
            timeout_s=7,
            fetcher=sentinel_fetcher,
            async_probe=fake_probe,
            probe_kwargs={
                "try_playwright": sentinel_playwright,
                "playwright_semaphore": sentinel_semaphore,
            },
        )
    )

    assert results[0][:4] == (candidate, True, 3, "")
    assert seen == {
        "row": candidate,
        "timeout_s": 7,
        "try_playwright": sentinel_playwright,
        "playwright_semaphore": sentinel_semaphore,
    }


def test_run_bounded_probe_batch_async_uses_custom_fetcher_thread_path() -> None:
    calls: list[tuple[str, int]] = []

    def custom_fetcher(url: str, timeout_s: int) -> str:
        calls.append((url, timeout_s))
        return "custom fetch body"

    async def fake_probe(row, timeout_s, *, fetcher):
        body = await fetcher(row["listing_url"], timeout_s)
        return True, len(body), ""

    candidate = _static_candidate()

    results = asyncio.run(
        probe_runtime.run_bounded_probe_batch_async(
            [candidate],
            timeout_s=4,
            fetcher=custom_fetcher,
            async_probe=fake_probe,
        )
    )

    assert calls == [("https://studio.example/careers", 4)]
    assert results[0][:4] == (candidate, True, len("custom fetch body"), "")


def test_probe_candidates_async_keeps_default_probe_wrapper(monkeypatch) -> None:
    async def fake_async_probe_candidate(row, timeout_s, *, fetcher):
        return True, int(timeout_s), ""

    monkeypatch.setattr(probe_module, "async_probe_candidate", fake_async_probe_candidate)

    candidate = _static_candidate()
    results = asyncio.run(
        probe_runtime.probe_candidates_async(
            [candidate],
            timeout_s=6,
            fetcher=lambda _url, _timeout_s: "",
        )
    )

    assert results[0][:4] == (candidate, True, 6, "")
