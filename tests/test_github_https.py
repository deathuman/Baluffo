import ssl
from urllib.error import URLError

import pytest

from src.shared import github_https


def test_build_github_ssl_context_loads_default_certs_and_custom_bundle(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    cafile = tmp_path / "custom-ca.pem"
    cafile.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    seen = {"default_certs": False, "cafiles": []}

    class FakeContext:
        def load_default_certs(self) -> None:
            seen["default_certs"] = True

        def load_verify_locations(self, *, cafile=None) -> None:  # noqa: ANN001
            seen["cafiles"].append(cafile)

    monkeypatch.setattr(github_https.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(github_https, "certifi", None)
    monkeypatch.setenv("BALUFFO_TEST_CA_BUNDLE", str(cafile))

    context = github_https.build_github_ssl_context(ca_bundle_envs=("BALUFFO_TEST_CA_BUNDLE",))

    assert isinstance(context, FakeContext)
    assert seen["default_certs"] is True
    assert seen["cafiles"] == [str(cafile)]


def test_build_github_ssl_context_loads_certifi_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {"cafiles": []}

    class FakeContext:
        def load_default_certs(self) -> None:
            return None

        def load_verify_locations(self, *, cafile=None) -> None:  # noqa: ANN001
            seen["cafiles"].append(cafile)

    class FakeCertifi:
        @staticmethod
        def where() -> str:
            return "/tmp/certifi/cacert.pem"

    monkeypatch.setattr(github_https.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(github_https, "certifi", FakeCertifi())

    context = github_https.build_github_ssl_context()

    assert isinstance(context, FakeContext)
    assert seen["cafiles"] == ["/tmp/certifi/cacert.pem"]


def test_build_github_ssl_context_rejects_missing_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALUFFO_TEST_CA_BUNDLE", "C:/missing-ca.pem")
    monkeypatch.setattr(github_https, "certifi", None)

    with pytest.raises(RuntimeError, match="CA bundle not found"):
        github_https.build_github_ssl_context(ca_bundle_envs=("BALUFFO_TEST_CA_BUNDLE",))


def test_wrap_github_request_error_normalizes_certificate_verify_failures() -> None:
    error = github_https.wrap_github_request_error(
        URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")),
        prefix="Desktop update request failed",
    )

    assert "SSL certificate verification failed while connecting to GitHub" in str(error)
    assert "CERTIFICATE_VERIFY_FAILED" in str(error)
