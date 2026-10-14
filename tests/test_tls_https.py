"""The shared verified TLS anchor: union, still verified, and wired into the sync fetchers.

The defect these cover (measured on this repo's Windows checkout): a bare `urlopen` -- the OS
store alone -- fails chain building with "certificate has expired" for 506 of 3,000 audited
discovery homepages whose served chain is valid, while the async `httpx` path reads them. The
fix anchors verification on certifi *beside* the OS store; these tests pin the three properties
that make it a repair rather than a verification downgrade:

1. the context is still `CERT_REQUIRED` with the hostname check on;
2. the anchor set is a superset of both the default store and certifi (so nothing that verifies
   today can start failing);
3. each shared sync fetcher actually passes that context to `urlopen`.
"""

from __future__ import annotations

import ssl
from typing import Any

import pytest

import src.jobs.common.http as jobs_http
from src.bridge import source_check_http
from src.shared import github_https, tls_https
from src.source_discovery import web_search_fetch


@pytest.fixture(autouse=True)
def _clear_shared_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test builds the process-wide context itself, so a cached one cannot leak across."""
    monkeypatch.setattr(tls_https, "_SHARED_CONTEXT", None)


def _ca_subjects(context: ssl.SSLContext) -> set[str]:
    return {str(cert.get("subject")) for cert in context.get_ca_certs()}


def test_verified_ssl_context_stays_verified_with_hostname_checking() -> None:
    context = tls_https.verified_ssl_context()

    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_verified_ssl_context_anchors_certifi_beside_the_default_store() -> None:
    """The union claim: certifi's roots are a subset of the context's, not a replacement."""
    assert tls_https.certifi is not None, "certifi is a declared runtime dependency"
    certifi_only = ssl.create_default_context(cafile=str(tls_https.certifi.where()))
    union = tls_https.verified_ssl_context()

    assert _ca_subjects(certifi_only) <= _ca_subjects(union)
    assert union.get_ca_certs(), "the union context must still carry the default store's roots"


def test_verified_ssl_context_is_built_once_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    builds: list[str] = []

    def fake_build(*, ca_bundle_envs: tuple[str, ...] = ()) -> Any:
        builds.append("build")
        return f"context-{len(builds)}"

    monkeypatch.setattr(tls_https, "build_verified_ssl_context", fake_build)

    first = tls_https.verified_ssl_context()
    second = tls_https.verified_ssl_context()

    assert first == second == "context-1"
    assert builds == ["build"]


def test_build_verified_ssl_context_loads_named_bundle_then_certifi(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    cafile = tmp_path / "operator-ca.pem"
    cafile.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    seen: dict[str, Any] = {"default_certs": False, "cafiles": []}

    class FakeContext:
        def load_default_certs(self) -> None:
            seen["default_certs"] = True

        def load_verify_locations(self, *, cafile: str | None = None) -> None:
            seen["cafiles"].append(cafile)

    class FakeCertifi:
        @staticmethod
        def where() -> str:
            return "/tmp/certifi/cacert.pem"

    monkeypatch.setattr(tls_https.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(tls_https, "certifi", FakeCertifi())
    monkeypatch.setenv("BALUFFO_TEST_SYNC_CA_BUNDLE", str(cafile))

    context = tls_https.build_verified_ssl_context(ca_bundle_envs=("BALUFFO_TEST_SYNC_CA_BUNDLE",))

    assert isinstance(context, FakeContext)
    assert seen["default_certs"] is True
    assert seen["cafiles"] == [str(cafile), "/tmp/certifi/cacert.pem"]


def test_build_verified_ssl_context_ignores_an_absent_certifi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing or unreadable certifi leaves today's default context, never a failure."""

    class BrokenCertifi:
        @staticmethod
        def where() -> str:
            raise OSError("certifi bundle unavailable")

    monkeypatch.setattr(tls_https, "certifi", BrokenCertifi())

    context = tls_https.build_verified_ssl_context()

    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.get_ca_certs()


def test_resolve_ca_bundle_override_rejects_a_missing_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALUFFO_TEST_SYNC_CA_BUNDLE", "C:/missing-ca.pem")

    with pytest.raises(RuntimeError, match="CA bundle not found"):
        tls_https.resolve_ca_bundle_override(ca_bundle_envs=("BALUFFO_TEST_SYNC_CA_BUNDLE",))


def test_github_context_and_shared_context_anchor_the_same_way(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`github_https` keeps its naming and env; the anchor itself is the shared one now."""
    monkeypatch.delenv(github_https.GITHUB_CA_BUNDLE_ENV, raising=False)

    assert _ca_subjects(github_https.build_github_ssl_context()) == _ca_subjects(
        tls_https.build_verified_ssl_context()
    )


class _FakeResponse:
    def __init__(
        self, *, url: str = "https://example.com/", body: bytes = b"<html></html>"
    ) -> None:
        self._url = url
        self._body = body
        self.headers = _FakeHeaders()

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self, _amount: int = -1) -> bytes:
        return self._body

    def geturl(self) -> str:
        return self._url


class _FakeHeaders:
    @staticmethod
    def get_content_charset() -> str:
        return "utf-8"

    def items(self) -> list[tuple[str, str]]:
        return []


def _recording_urlopen(seen: list[Any]) -> Any:
    def _urlopen(_request: Any, timeout: float, **kwargs: Any) -> _FakeResponse:
        seen.append(kwargs.get("context"))
        return _FakeResponse()

    return _urlopen


def test_discovery_sync_fetch_passes_the_shared_verified_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[Any] = []
    monkeypatch.setattr(web_search_fetch, "urlopen", _recording_urlopen(seen))

    web_search_fetch.fetch_text("https://example.com/careers", 5)

    assert seen == [tls_https.verified_ssl_context()]
    assert seen[0].verify_mode is ssl.CERT_REQUIRED


def test_jobs_lane_fallback_passes_the_shared_verified_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both `common.http` entry points, since static adapters use them interchangeably."""
    seen: list[Any] = []
    monkeypatch.setattr(jobs_http, "urlopen", _recording_urlopen(seen))

    jobs_http.default_fetch_text("https://example.com/jobs", 5, headers={"User-Agent": "x"})
    jobs_http.default_fetch_text_with_response_headers(
        "https://example.com/jobs", 5, headers={"User-Agent": "x"}
    )

    assert seen == [tls_https.verified_ssl_context(), tls_https.verified_ssl_context()]
    assert all(context.verify_mode is ssl.CERT_REQUIRED for context in seen)


def test_source_check_redirect_discovery_passes_the_shared_verified_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[Any] = []

    def _urlopen(_request: Any, timeout: float, **kwargs: Any) -> _FakeResponse:
        seen.append(kwargs.get("context"))
        return _FakeResponse(url="https://example.com/jobs")

    monkeypatch.setattr(source_check_http, "urlopen", _urlopen)

    source_check_http.discover_redirect_career_candidates("https://example.com/", timeout_s=5)

    # Both the bare and the `www.` root are probed, each with the shared context.
    shared = tls_https.verified_ssl_context()
    assert seen == [shared, shared]
