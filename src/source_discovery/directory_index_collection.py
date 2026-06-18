from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def _capture_index_call_failure(
    *, name: str, adapter: str, stage: str, call: Any
) -> tuple[Any, Any]:
    try:
        return call(), None
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        return None, {
            "name": name,
            "adapter": adapter,
            "error": str(exc),
            "stage": stage,
        }


def _append_unique_detail_entries(
    *,
    detail_entries: list[dict[str, Any]],
    parsed_entries: Iterable[dict[str, Any]],
    seen_details: set[str],
    detail_url_field: str,
    entry_cap: int,
) -> bool:
    for entry in parsed_entries:
        detail_url = str(entry.get(detail_url_field) or "").strip()
        if not detail_url or detail_url in seen_details:
            continue
        seen_details.add(detail_url)
        detail_entries.append(entry)
        if entry_cap and len(detail_entries) >= entry_cap:
            return True
    return False


def collect_directory_index_entries(
    *,
    timeout_s: int,
    fetcher: Any,
    parse_index_entries: Any,
    base_url: str,
    index_urls: Iterable[str],
    adapter: str,
    parse_kwargs: dict[str, Any] | None = None,
    max_entries: int = 0,
    detail_url_field: str = "detailUrl",
    unresolved_reference_key: str = "unresolvedReferenceCount",
    fetch_failure_stage: str = "directory_index_fetch",
    parse_failure_stage: str = "directory_index_parse",
    empty_parse_error: str = "no entries parsed from index",
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    detail_entries: list[dict[str, Any]] = []
    seen_details = set()
    unresolved_reference_count = 0
    kwargs = dict(parse_kwargs or {})
    entry_cap = max(0, int(max_entries or 0))

    for index_url in index_urls:
        url = str(index_url or "").strip()
        if not url:
            continue
        index_html, failure = _capture_index_call_failure(
            name=url,
            adapter=adapter,
            stage=fetch_failure_stage,
            call=lambda url=url: fetcher(url, timeout_s),
        )
        if failure:
            failures.append(failure)
            continue

        parsed, failure = _capture_index_call_failure(
            name=url,
            adapter=adapter,
            stage=parse_failure_stage,
            call=lambda index_html=index_html: parse_index_entries(index_html, base_url, **kwargs),
        )
        if failure:
            failures.append(failure)
            continue
        parsed_entries, diagnostics = parsed

        if isinstance(diagnostics, dict):
            unresolved_reference_count += int(diagnostics.get(unresolved_reference_key) or 0)
        if not parsed_entries:
            failures.append(
                {
                    "name": url,
                    "adapter": adapter,
                    "error": empty_parse_error,
                    "stage": parse_failure_stage,
                }
            )
            continue

        if _append_unique_detail_entries(
            detail_entries=detail_entries,
            parsed_entries=parsed_entries,
            seen_details=seen_details,
            detail_url_field=detail_url_field,
            entry_cap=entry_cap,
        ):
            break

    return {
        "detailEntries": detail_entries,
        "failures": failures,
        "unresolvedReferenceCount": unresolved_reference_count,
    }
