"""Scrapy crawl settings for the Baluffo static career crawler. Runner merges runtime overrides when building Settings."""

# Defaults; runner may override DOWNLOAD_DELAY, DOWNLOAD_TIMEOUT, RETRY_TIMES from runtime config.
SCRAPY_SETTINGS_DEFAULTS = {
    "ROBOTSTXT_OBEY": True,
    "DOWNLOAD_DELAY": 1.0,
    "DOWNLOAD_TIMEOUT": 20,
    "RETRY_TIMES": 2,
    "RETRY_HTTP_CODES": [403, 429, 500, 502, 503],
    "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    "AUTOTHROTTLE_ENABLED": True,
    "AUTOTHROTTLE_START_DELAY": 1.0,
    "AUTOTHROTTLE_MAX_DELAY": 10.0,
    "LOG_LEVEL": "WARNING",
    "TELNETCONSOLE_ENABLED": False,
    "WEBSOCKETS_ENABLED": False,
    "REACTOR_THREADPOOL_MAXSIZE": 10,
    "DEPTH_LIMIT": 1,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

# Scrapy-Playwright: merged when use_browser=True. Requires scrapy-playwright; runner falls back to HTTP-only if missing.
SCRAPY_PLAYWRIGHT_SETTINGS = {
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    },
    "PLAYWRIGHT_BROWSER_TYPE": "chromium",
    "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
}
