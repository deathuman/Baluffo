import pytest

from src.bridge import source_check_http


class _Response:
    def __init__(self, *, url: str, body: bytes = b"") -> None:
        self._url = url
        self._body = body
        self.headers = self

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def get_content_charset(self) -> str:
        return "utf-8"

    def read(self) -> bytes:
        return self._body


class _InvalidCharsetResponse(_Response):
    def get_content_charset(self) -> str:
        return "missing-codec"


def test_redirect_career_candidates_ignore_expected_probe_failures(monkeypatch) -> None:
    def fail_urlopen(_request, *, timeout: int):  # noqa: ANN001
        raise OSError("network unavailable")

    monkeypatch.setattr(source_check_http, "urlopen", fail_urlopen)

    assert (
        source_check_http.discover_redirect_career_candidates(
            "https://example.com/careers",
            timeout_s=5,
        )
        == []
    )


def test_redirect_career_candidates_ignore_expected_decode_failures(monkeypatch) -> None:
    def fail_urlopen(_request, *, timeout: int):  # noqa: ANN001
        return _InvalidCharsetResponse(url="https://jobs.example.com/", body=b"x")

    monkeypatch.setattr(source_check_http, "urlopen", fail_urlopen)

    assert (
        source_check_http.discover_redirect_career_candidates(
            "https://example.com/careers",
            timeout_s=5,
        )
        == []
    )


def test_redirect_career_candidates_do_not_hide_unexpected_probe_failures(
    monkeypatch,
) -> None:
    def fail_urlopen(_request, *, timeout: int):  # noqa: ANN001
        raise AssertionError("probe bug")

    monkeypatch.setattr(source_check_http, "urlopen", fail_urlopen)

    with pytest.raises(AssertionError, match="probe bug"):
        source_check_http.discover_redirect_career_candidates(
            "https://example.com/careers",
            timeout_s=5,
        )
