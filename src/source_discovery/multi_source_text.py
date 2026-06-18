from __future__ import annotations

"""Shared ordered text-fetch helpers for source-discovery inputs."""

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from . import audit_ledger

TextFetcher = Callable[[str, int], str]


@dataclass(frozen=True)
class MultiSourceTextResult:
    text: str
    last_error: str
    attempted_urls: list[str]
    selected_url: str
    duration_ms: int


def fetch_first_nonempty_text(
    urls: Iterable[str],
    *,
    timeout_s: int,
    fetcher: TextFetcher,
) -> MultiSourceTextResult:
    text = ""
    last_error = ""
    attempted_urls: list[str] = []
    selected_url = ""
    started = time.perf_counter()
    for url in urls:
        candidate_url = str(url or "").strip()
        if not candidate_url:
            continue
        attempted_urls.append(candidate_url)
        try:
            fetched_text = fetcher(candidate_url, timeout_s)
            text = fetched_text if isinstance(fetched_text, str) else str(fetched_text or "")
            if text.strip():
                selected_url = candidate_url
                break
        except (OSError, RuntimeError, ValueError) as exc:
            last_error = str(exc)
            continue
    return MultiSourceTextResult(
        text=text,
        last_error=last_error,
        attempted_urls=attempted_urls,
        selected_url=selected_url,
        duration_ms=audit_ledger.duration_ms(started),
    )
