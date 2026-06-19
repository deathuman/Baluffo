import builtins
import sys
import types

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


class _FakePlaywrightError(Exception):
    pass


def _install_fake_playwright(
    monkeypatch: pytest.MonkeyPatch,
    *,
    launch_error: Exception | None = None,
) -> None:
    playwright_pkg = types.ModuleType("playwright")
    sync_api_mod = types.ModuleType("playwright.sync_api")

    class _Chromium:
        def launch(self, *, headless: bool) -> object:
            assert headless is True
            if launch_error is not None:
                raise launch_error
            return object()

    class _Playwright:
        chromium = _Chromium()

    class _SyncPlaywright:
        def __enter__(self) -> _Playwright:
            return _Playwright()

        def __exit__(self, *_args: object) -> None:
            return None

    sync_api_mod.Error = _FakePlaywrightError
    sync_api_mod.sync_playwright = lambda: _SyncPlaywright()
    monkeypatch.setitem(sys.modules, "playwright", playwright_pkg)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_mod)


def test_playwright_import_failure_returns_unavailable(monkeypatch) -> None:
    real_import = builtins.__import__

    def fail_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "playwright.sync_api":
            raise ModuleNotFoundError("No module named 'playwright'")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_import)

    assert source_check_http.try_fetch_with_playwright(
        "https://example.com/careers",
        timeout_s=5,
    ) == ("", "browser fallback unavailable (playwright is not installed)")


def test_playwright_import_does_not_hide_unexpected_failures(monkeypatch) -> None:
    real_import = builtins.__import__

    def fail_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "playwright.sync_api":
            raise RuntimeError("broken import side effect")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_import)

    with pytest.raises(RuntimeError, match="broken import side effect"):
        source_check_http.try_fetch_with_playwright(
            "https://example.com/careers",
            timeout_s=5,
        )


def test_playwright_runtime_failure_returns_error(monkeypatch) -> None:
    _install_fake_playwright(
        monkeypatch,
        launch_error=_FakePlaywrightError("browser launch failed"),
    )

    assert source_check_http.try_fetch_with_playwright(
        "https://example.com/careers",
        timeout_s=5,
    ) == ("", "browser launch failed")


def test_playwright_runtime_does_not_hide_unexpected_failures(monkeypatch) -> None:
    _install_fake_playwright(
        monkeypatch,
        launch_error=AssertionError("browser helper bug"),
    )

    with pytest.raises(AssertionError, match="browser helper bug"):
        source_check_http.try_fetch_with_playwright(
            "https://example.com/careers",
            timeout_s=5,
        )


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
