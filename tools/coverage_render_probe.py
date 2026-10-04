#!/usr/bin/env python3
"""Does a browser actually render job rows out of a Feishu tenant page?

Feishu's eight tenant boards are the last block of the coverage gap worth engineering
(826 openings). They are classified `unsupported_vendor`, so they are never emitted as
candidates at all -- the same mislabel hrmos carried until it was found to be a static
platform the runtime already collects.

The cheap fix would be to reclassify them as static and register them on their recorded
openings, the way 105 other JS-rendered boards were. That is only justified if the runtime's
own browser path yields rows from the page. This answers that question directly rather than
assuming it, because the answer decides whether the reclassification is evidence-backed or
another recorded zero.

The rendered page is counted with the runtime's own `is_probable_job_detail_url`, for the
same reason the rest of this tooling delegates: a second copy of the row rules answers a
subtly different question while looking authoritative.

Usage:
  python tools/coverage_render_probe.py --url https://kurogame.jobs.feishu.cn/index
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.adapters.static_detail_heuristics_filter import (  # noqa: E402
    _DEFAULT_DETAIL_PATH_TOKENS,
    _DEFAULT_DETAIL_QUERY_KEYS,
    is_probable_job_detail_url,
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


async def render(url: str, *, wait_ms: int, timeout_s: int) -> dict[str, Any]:
    """Load the page in Chromium and count the job links it ends up showing."""
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(args=["--no-sandbox"])
        try:
            page = await browser.new_page(user_agent=_UA)
            seen: list[str] = []
            page.on(
                "response",
                lambda r: (
                    seen.append(f"{r.status} {r.url}")
                    if any(k in r.url for k in ("/api/", "/position", "/job/"))
                    else None
                ),
            )
            await page.goto(url, timeout=timeout_s * 1000, wait_until="domcontentloaded")
            await page.wait_for_timeout(wait_ms)
            html = await page.content()
            links = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            return {"html": html, "links": links, "apiCalls": seen[-25:]}
        finally:
            await browser.close()


def count_detail_links(links: list[str]) -> list[str]:
    """Distinct links the runtime's own rule calls a job detail page."""
    out: list[str] = []
    for href in links:
        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        try:
            if (
                is_probable_job_detail_url(
                    href,
                    {},
                    default_path_tokens=list(_DEFAULT_DETAIL_PATH_TOKENS),
                    default_query_keys=list(_DEFAULT_DETAIL_QUERY_KEYS),
                )
                and href not in out
            ):
                out.append(href)
        except Exception:
            continue
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--wait-ms", type=int, default=9000)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--show-links", type=int, default=8)
    args = parser.parse_args()

    result = asyncio.run(render(args.url, wait_ms=args.wait_ms, timeout_s=args.timeout))
    html = str(result["html"])
    details = count_detail_links([str(x) for x in result["links"]])

    print(f"url            : {args.url}")
    print(f"rendered html  : {len(html):,} bytes")
    print(f"anchors        : {len(result['links'])}")
    print(f"job detail links: {len(details)}")
    print(f"\napi-ish responses seen ({len(result['apiCalls'])}):")
    for call in result["apiCalls"][:12]:
        print(f"   {call[:130]}")
    if details:
        print("\nsample detail links:")
        for href in details[: args.show_links]:
            print(f"   {href[:120]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
