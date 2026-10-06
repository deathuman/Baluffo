"""The redirect guard must not have changed what it allows.

`_safe_redirect_url` is a security boundary: it refuses to follow a redirect that changes
site, drops below https, carries credentials, or uses a non-http scheme. This change attaches a
*diagnosis* to that refusal so the fetch report can distinguish a board that moved to an
applicant-tracking platform from one whose studio closed.

The risk being pinned here is the obvious one: a classification named `platform_migration`
reads like permission, and treating it as permission would quietly remove a guard that exists
for a reason. So these tests assert on the **refusal**, not on the diagnosis.
"""

from __future__ import annotations

import ast
import inspect

import pytest

from src.jobs.adapters import static_runtime_support as mod

GUARD = "mod._safe_redirect_url"


def _fetcher():
    """A fetcher instance with only the state `_safe_redirect_url` touches."""
    return mod.StaticHtmlFetcher.__new__(mod.StaticHtmlFetcher)


def _refuses(source: str, location: str) -> str:
    with pytest.raises(RuntimeError) as excinfo:
        _fetcher()._safe_redirect_url(source, location)
    return str(excinfo.value)


# --- what the guard must keep refusing --------------------------------------------------


@pytest.mark.parametrize(
    ("source", "location", "why"),
    [
        ("https://careers.acme.com/jobs", "ftp://files.acme.com/jobs", "non-http scheme"),
        (
            "https://careers.acme.com/jobs",
            "https://user:pw@elsewhere.com/jobs",
            "credentials in URL",
        ),
        (
            "https://careers.acme.com/jobs",
            "https://www.linkedin.com/company/acme/jobs",
            "cross-site",
        ),
        ("https://careers.acme.com/jobs", "http://careers.acme.com/jobs", "https downgrade"),
    ],
)
def test_the_guard_still_refuses_each_shape(source, location, why):
    """Every shape the guard existed to refuse must still raise.

    The classification is appended to the message; it must never replace the refusal.
    """
    message = _refuses(source, location)
    assert message.startswith("Unsafe static redirect"), why
    assert " followed" not in message.lower()


def test_a_migration_target_is_still_refused():
    """The case most likely to be got wrong: a *recognised* platform target.

    Reading `platform_migration` as "this one is fine, follow it" would delete the cross-site
    guard for exactly the redirects it most needs to cover.
    """
    message = _refuses("https://www.ubisoft.com/jobs", "https://jobs.smartrecruiters.com/Ubisoft2")
    assert "Unsafe static redirect" in message
    assert "[platform_migration" in message, "the diagnosis is still attached"


# --- what it must keep allowing ---------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "location", "expected"),
    [
        # A query change is followed: pagination is the common legitimate redirect.
        (
            "https://careers.acme.com/jobs",
            "https://careers.acme.com/jobs?page=2",
            "https://careers.acme.com/jobs?page=2",
        ),
        # Trailing-slash normalisation on the same site and scheme is followed.
        (
            "https://careers.acme.com/jobs",
            "https://careers.acme.com/jobs/",
            "https://careers.acme.com/jobs/",
        ),
        # A different subdomain is a different site as far as this comparison is concerned.
        (
            "https://careers.acme.com/jobs",
            "https://www.careers.acme.com/jobs",
            "https://www.careers.acme.com/jobs",
        ),
    ],
)
def test_same_site_redirects_are_still_followed(source, location, expected):
    """The guard was never a blanket ban on redirects -- only on the unsafe shapes."""
    assert _fetcher()._safe_redirect_url(source, location) == expected


def test_a_same_site_downgrade_is_still_refused_even_though_the_site_matches():
    """`www.` is stripped before the site comparison, so the scheme check is what refuses.

    Worth pinning on its own: it is the case where the site check passes and only the scheme
    comparison stands between the guard and a downgrade.
    """
    with pytest.raises(RuntimeError, match="Unsafe static redirect"):
        _fetcher()._safe_redirect_url(
            "https://careers.acme.com/jobs", "http://careers.acme.com/other"
        )


def test_a_redirect_resolving_to_the_same_url_is_a_loop():
    """A relative `Location` that resolves back to the source is a loop, not a migration."""
    with pytest.raises(RuntimeError, match="Static redirect loop"):
        _fetcher()._safe_redirect_url("https://careers.acme.com/jobs", "/jobs")


def test_a_redirect_loop_is_still_its_own_error():
    """Loops are a distinct failure and must not be reclassified as a migration."""
    with pytest.raises(RuntimeError, match="Static redirect loop"):
        _fetcher()._safe_redirect_url(
            "https://careers.acme.com/jobs", "https://careers.acme.com/jobs"
        )


def test_a_missing_location_header_is_still_its_own_error():
    with pytest.raises(RuntimeError, match="missing Location"):
        _fetcher()._safe_redirect_url("https://careers.acme.com/jobs", "")


# --- the guard is not importable-and-silent ---------------------------------------------


def test_the_classifier_is_imported_lazily_not_at_module_scope():
    """Static fetching must not depend on `source_discovery` being importable.

    The classifier lives in a different layer, so importing it at module scope would make
    every static fetch depend on discovery loading. A broken import has to degrade to the old
    bare message, not break fetching.
    """
    tree = ast.parse(inspect.getsource(mod))
    module_level = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and "redirect_classification" in ast.dump(node)
    ]
    assert not module_level, f"module-scope import of the classifier: {module_level}"

    # And the in-function import must be inside the raising helper, not at class scope.
    helper = ast.parse(inspect.getsource(mod._static_redirect_error))
    assert any(
        isinstance(node, (ast.Import, ast.ImportFrom))
        and "redirect_classification" in ast.dump(node)
        for node in ast.walk(helper)
    ), "the classifier should be imported inside _static_redirect_error"


def test_a_broken_classifier_degrades_to_the_bare_message(monkeypatch):
    """The degradation path, exercised rather than assumed."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if "redirect_classification" in name:
            raise ImportError("simulated missing module")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    message = mod._static_redirect_error("https://a.com/x", "https://b.com/y")
    assert message == "Unsafe static redirect from https://a.com/x to https://b.com/y"


# --- the message stays parseable ---------------------------------------------------------


def test_the_appended_classification_does_not_hide_the_target_url():
    """Anyone grepping the old error string must still find it.

    The original message is a substring of the new one, which is what lets existing log
    searches and any tooling keyed on it keep working.
    """
    source, target = "https://www.roblox.com/jobs", "https://corp.roblox.com/careers"
    message = mod._static_redirect_error(source, target)
    assert f"Unsafe static redirect from {source} to {target}" in message


@pytest.mark.parametrize(
    ("source", "target", "expected_tag"),
    [
        (
            "https://www.ubisoft.com/jobs",
            "https://jobs.smartrecruiters.com/Ubisoft2",
            "[platform_migration ",
        ),
        ("https://www.roblox.com/jobs", "https://corp.roblox.com/careers", "[site_gone_or_moved]"),
        (
            "https://www.ninjatheory.com/careers",
            "http://www.ninjatheory.com/careers/",
            "[insecure_downgrade]",
        ),
    ],
)
def test_each_shape_gets_its_own_tag(source, target, expected_tag):
    assert expected_tag in mod._static_redirect_error(source, target)
