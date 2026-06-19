from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from src.scrapers import runner


class _Stats:
    def get_stats(self) -> dict[str, object]:
        return {"downloader/response_count": 0}


class _Crawler:
    stats = _Stats()


class _CrawlerProcess:
    error: BaseException | None = None

    def __init__(self, *, settings: object) -> None:
        self.settings = settings

    def create_crawler(self, _spider: object) -> _Crawler:
        return _Crawler()

    def crawl(self, *_args: object, **_kwargs: object) -> None:
        if self.error is not None:
            raise self.error

    def start(self, *, stop_after_crawl: bool) -> None:
        return None


class _Settings(dict[str, object]):
    pass


@contextmanager
def _fake_scrapy(error: BaseException | None) -> Iterator[None]:
    scrapy_mod = types.ModuleType("scrapy")
    crawler_mod = types.ModuleType("scrapy.crawler")
    settings_mod = types.ModuleType("scrapy.settings")
    crawler_cls = type("CrawlerProcess", (_CrawlerProcess,), {"error": error})
    crawler_mod.CrawlerProcess = crawler_cls
    settings_mod.Settings = _Settings
    previous = {
        name: sys.modules.get(name) for name in ("scrapy", "scrapy.crawler", "scrapy.settings")
    }
    sys.modules["scrapy"] = scrapy_mod
    sys.modules["scrapy.crawler"] = crawler_mod
    sys.modules["scrapy.settings"] = settings_mod
    try:
        yield
    finally:
        for name, module in previous.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


def _validated_source() -> dict[str, Any]:
    return {
        "source": {
            "name": "Scrapy Runtime Studio",
            "studio": "Scrapy Runtime Studio",
            "pages": ["https://example.com/jobs"],
        },
        "runtime": {
            "timeout_s": 5,
            "retries": 0,
            "backoff_s": 0.0,
            "download_delay": 0.0,
            "use_browser": False,
        },
    }


def test_scrapy_runner_records_expected_crawl_failure() -> None:
    with _fake_scrapy(RuntimeError("crawler runtime failed")):
        envelope = runner._run_scrapy(_validated_source())

    assert envelope["ok"] is False
    assert envelope["partialErrors"] == [
        "Scrapy Runtime Studio: crawl failed: crawler runtime failed"
    ]
    detail = envelope["details"][0]
    assert detail["status"] == "error"
    assert detail["classification"] == "parse_error"


def test_scrapy_runner_does_not_swallow_unexpected_crawl_bug() -> None:
    with _fake_scrapy(AssertionError("unexpected crawler bug")):
        with pytest.raises(AssertionError, match="unexpected crawler bug"):
            runner._run_scrapy(_validated_source())
