"""The URL-family memo must not move a single normalised URL, seed, or public-URL verdict.

The pass memoises ``text_utils.normalize_url`` and the two wrappers that sit on top of it in
``jobs.common.url``, keyed on ``clean_text(url)`` -- the value each body already derives before it
does any work. Both are gated by ``text_utils.LEGACY_URL_NORMALIZATION`` --
``BALUFFO_URL_NORMALIZATION=legacy`` -- so the corpus A/B can price the seam as a paired study on
one tree.

That gate is only trustworthy if the cached path returns what the uncached one returns, so these
tests compare the public entry points across both modes on a battery of URL shapes rather than
trusting the reading of the diff. A memo that collapsed two distinct URLs (or resurrected a stale
host verdict) would invalidate the A/B's parity line and every number behind it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from src.jobs import text_utils
from src.jobs.common import url as url_module

SAMPLES: tuple[Any, ...] = (
    "https://example.com/jobs/123",
    "https://example.com/jobs/123/",
    "https://example.com/jobs/123/?utm_source=newsletter&utm_medium=email",
    "https://example.com/jobs/123?b=2&a=1",
    "https://example.com/jobs/123?b=2&a=1&utm_campaign=x&fbclid=y",
    "https://EXAMPLE.com/Jobs/123",
    "HTTPS://Example.com/jobs/123",
    "http://example.com:8080/jobs/123?q=one+two",
    "https://example.com/jobs/123?flag=&other=1",
    "https://example.com/jobs/123#section",
    "https://example.com/jobs/123?a=1&a=2",
    "https://example.com/jobs/%C3%BCber",
    "https://user:pw@example.com/jobs/123",
    "https://münchen.example/jobs/1",
    "https://example.com",
    "https://example.com/",
    "example.com/jobs/123",
    "ftp://example.com/jobs/123",
    "javascript:alert(1)",
    "",
    "   ",
    "not a url at all",
    "https://",
    "https://example.com/jobs/123?utm_source=x",
    "https://boards.greenhouse.io/acme/jobs/4001",
    "https://job-boards.greenhouse.io/acme/jobs/4002?gh_jid=4002",
    "https://jobs.smartrecruiters.com/Acme/744000012345678-role",
    "https://api.smartrecruiters.com/v1/companies/Acme/postings/744000012345678",
    "https://www.personio.de/job/12345?language=en",
    "http://localhost:3000/jobs/1",
    "http://localhost.localdomain/jobs/1",
    "http://printer.local/jobs/1",
    "http://192.168.1.10/jobs/1",
    "http://127.0.0.1/jobs/1",
    "http://10.0.0.5/jobs/1",
    "http://8.8.8.8/jobs/1",
    "https://example.com./jobs/1",
    "https://example.com:443/jobs/1",
    "https://example.com/jobs/1?b=&a=",
    "//example.com/jobs/1",
    "/jobs/1",
    "?utm_source=x",
    None,
    0,
    1,
    123,
    True,
    {"url": "https://example.com/jobs/1"},
    ["https://example.com/jobs/1"],
    b"https://example.com/jobs/1",
    "https://example.com/jobs/" + "9" * 300,
)


def _probe(value: Any) -> tuple[Any, ...]:
    return (
        text_utils.normalize_url(value),
        url_module.canonical_url_fingerprint_seed(value),
        url_module.fingerprint_url(value),
        url_module.is_public_job_url(value),
    )


def _snapshot() -> list[tuple[str, tuple[Any, ...]]]:
    return [(repr(value), _probe(value)) for value in SAMPLES]


@contextmanager
def _url_mode(*, legacy: bool) -> Iterator[None]:
    """Flip the switch in both modules: ``common.url`` binds the flag at import time."""

    previous = (
        text_utils.LEGACY_URL_NORMALIZATION,
        url_module.LEGACY_URL_NORMALIZATION,
    )
    text_utils.LEGACY_URL_NORMALIZATION = legacy
    url_module.LEGACY_URL_NORMALIZATION = legacy
    try:
        yield
    finally:
        (
            text_utils.LEGACY_URL_NORMALIZATION,
            url_module.LEGACY_URL_NORMALIZATION,
        ) = previous


def test_legacy_and_cached_paths_agree_on_every_sample() -> None:
    with _url_mode(legacy=False):
        cached = _snapshot()
    with _url_mode(legacy=True):
        legacy = _snapshot()

    divergences = [
        (label, cached_row, legacy_row)
        for (label, cached_row), (_, legacy_row) in zip(cached, legacy, strict=True)
        if cached_row != legacy_row
    ]
    assert divergences == [], f"URL modes disagree on {len(divergences)} sample(s)"


def test_cached_path_memoises_and_legacy_path_does_not() -> None:
    """The A/B's control arm must actually skip the caches, and the fast arm must use them."""

    caches = {
        "normalize_url": text_utils._normalize_url_cached,
        "canonical_url_fingerprint_seed": url_module._canonical_url_fingerprint_seed_cached,
        "is_public_job_url": url_module._is_public_job_url_cached,
    }
    sample = "https://example.com/jobs/123?utm_source=x"

    for name, cache in caches.items():
        cache.cache_clear()
        with _url_mode(legacy=True):
            text_utils.normalize_url(sample)
            url_module.canonical_url_fingerprint_seed(sample)
            url_module.is_public_job_url(sample)
        assert cache.cache_info().hits == 0, f"legacy mode reached the {name} memo"
        assert cache.cache_info().currsize == 0, f"legacy mode inserted into the {name} memo"

    for name, cache in caches.items():
        cache.cache_clear()
        with _url_mode(legacy=False):
            text_utils.normalize_url(sample)
            text_utils.normalize_url(sample)
            url_module.canonical_url_fingerprint_seed(sample)
            url_module.canonical_url_fingerprint_seed(sample)
            url_module.is_public_job_url(sample)
            url_module.is_public_job_url(sample)
        assert cache.cache_info().hits >= 1, f"the cached path did not reuse a repeat for {name}"


def test_the_key_is_the_cleaned_url_so_equivalent_inputs_share_a_hit() -> None:
    """Every input that cleans to the same string is the same key, exactly as it was the same work."""

    cache = url_module._canonical_url_fingerprint_seed_cached
    cache.cache_clear()
    with _url_mode(legacy=False):
        as_string = url_module.canonical_url_fingerprint_seed("https://example.com/jobs/1")
        as_mapping = url_module.canonical_url_fingerprint_seed({"url": "nope"})
        as_none = url_module.canonical_url_fingerprint_seed(None)
        as_int = url_module.canonical_url_fingerprint_seed(7)

    assert url_module.canonical_url_fingerprint_seed("https://example.com/jobs/1") == as_string
    assert as_mapping == as_none == as_int == ""
    hits = cache.cache_info().hits
    assert hits >= 1, "an equivalent input missed the memo"


def test_repeated_calls_return_equal_values_under_both_modes() -> None:
    """Cached results are shared between callers, so the value itself must be repeatable."""

    for legacy in (True, False):
        with _url_mode(legacy=legacy):
            for sample in SAMPLES:
                first = _probe(sample)
                second = _probe(sample)
                assert first == second, f"unstable result for {sample!r} (legacy={legacy})"


def test_public_url_verdict_matches_the_uncached_body_for_public_and_private_hosts() -> None:
    """The memo must not turn a private/localhost verdict into a public one, or the reverse."""

    private = [
        "http://localhost/jobs/1",
        "http://printer.local/jobs/1",
        "http://192.168.0.1/jobs/1",
        "http://127.0.0.1/jobs/1",
        "https://user:pw@example.com/jobs/1",
    ]
    public = ["https://example.com/jobs/1", "https://boards.greenhouse.io/acme/jobs/1"]
    with _url_mode(legacy=False):
        assert [url_module.is_public_job_url(item) for item in private] == [False] * len(private)
        assert [url_module.is_public_job_url(item) for item in public] == [True] * len(public)
