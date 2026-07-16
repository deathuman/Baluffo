"""Safe, bounded direct-link availability checks.

The validator deliberately returns compact evidence only. It never stores page
content, authenticated state, response headers, or transport exception text.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx

from src.jobs.common.datetime_utils import parse_datetime
from src.shared.utils import now_iso

_REDIRECT_CODES = {301, 302, 303, 307, 308}
_TRANSIENT_CODES = {403, 408, 425, 429}
_ANTI_BOT_MARKERS = (
    "cf-chl-",
    "cloudflare ray id",
    "captcha",
    "verify you are human",
    "access denied",
    "unusual traffic",
)
_CLOSED_MARKERS = (
    "this job is no longer available",
    "this position has been filled",
    "this position is no longer available",
    "job posting has expired",
    "the job you are looking for is no longer open",
)
_AMBIGUOUS_CLOSED_MARKERS = (
    "job not found",
    "position closed",
    "posting removed",
    "opportunity is no longer available",
)
_GENERIC_CAREER_SEGMENTS = {"careers", "jobs", "search", "openings", "opportunities"}
_AUTH_SEGMENTS = {"login", "signin", "sign-in", "auth", "account"}
_APPLY_MARKERS = ("apply now", "apply for this job", "submit application")
_APPLY_TARGET_MARKERS = ("apply", "application")


@dataclass(frozen=True)
class _ApplicationAction:
    tag: str
    target: str
    label: str


class _JobPageParser(HTMLParser):
    """Extract bounded visible text and structured job/apply signals."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._hidden_depth = 0
        self._job_schema_depth = 0
        self._visible_size = 0
        self._schema_size = 0
        self.visible_parts: list[str] = []
        self.application_actions: list[_ApplicationAction] = []
        self.job_schema_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.casefold()
        attributes = {str(key).casefold(): str(value or "") for key, value in attrs}
        if name in {"script", "style", "template", "noscript"}:
            if name == "script" and "ld+json" in attributes.get("type", "").casefold():
                self._job_schema_depth = self._hidden_depth + 1
            self._hidden_depth += 1
            return
        if name in {"a", "form"} and self._hidden_depth == 0:
            target = attributes.get("href" if name == "a" else "action", "")
            label = " ".join(
                (attributes.get("aria-label", ""), attributes.get("title", ""))
            ).casefold()
            target_text = target.casefold()
            if target and (
                any(marker in label for marker in _APPLY_MARKERS)
                or any(marker in target_text for marker in _APPLY_TARGET_MARKERS)
            ):
                self.application_actions.append(
                    _ApplicationAction(tag=name, target=target, label=label)
                )

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() in {"script", "style", "template", "noscript"}:
            if self._job_schema_depth == self._hidden_depth:
                self._job_schema_depth = 0
            self._hidden_depth = max(0, self._hidden_depth - 1)

    def handle_data(self, data: str) -> None:
        if self._job_schema_depth and self._job_schema_depth == self._hidden_depth:
            if self._schema_size < 100_000:
                value = data[: 100_000 - self._schema_size]
                self.job_schema_parts.append(value)
                self._schema_size += len(value)
            return
        if self._hidden_depth == 0 and data.strip():
            if self._visible_size < 200_000:
                value = data.strip()[: 200_000 - self._visible_size]
                self.visible_parts.append(value)
                self._visible_size += len(value)


@dataclass(frozen=True)
class _JobPageSignals:
    visible_text: str
    application_actions: tuple[_ApplicationAction, ...]
    job_postings: tuple[dict[str, Any], ...]


def _job_postings_from_value(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    pending = [value]
    while pending and len(found) < 20:
        current = pending.pop()
        if isinstance(current, list):
            pending.extend(current[:100])
            continue
        if not isinstance(current, dict):
            continue
        raw_type = current.get("@type")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        if any(str(item or "").casefold() == "jobposting" for item in types):
            found.append(dict(current))
        for key in ("@graph", "mainEntity", "itemListElement"):
            nested = current.get(key)
            if isinstance(nested, (dict, list)):
                pending.append(nested)
    return found


def _parsed_job_postings(parts: list[str]) -> tuple[dict[str, Any], ...]:
    found: list[dict[str, Any]] = []
    for raw in parts[:20]:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            continue
        found.extend(_job_postings_from_value(payload))
        if len(found) >= 20:
            break
    return tuple(found[:20])


def _page_signals(value: str) -> _JobPageSignals:
    parser = _JobPageParser()
    try:
        parser.feed(str(value or "")[:500_000])
        parser.close()
    except (AssertionError, ValueError):
        pass
    return _JobPageSignals(
        visible_text=" ".join(parser.visible_parts).casefold(),
        application_actions=tuple(parser.application_actions[:50]),
        job_postings=_parsed_job_postings(parser.job_schema_parts),
    )


@dataclass(frozen=True)
class DirectResponse:
    status: int
    final_url: str
    text: str = ""


def _public_http_url(value: str) -> bool:
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username:
        return False
    host = parsed.hostname.casefold()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        return False
    try:
        addresses = {row[4][0] for row in socket.getaddrinfo(host, parsed.port or 443)}
    except OSError:
        return False
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return False
        if not ip.is_global:
            return False
    return bool(addresses)


def _evidence(
    *, kind: str, confidence: str, source: str, checked_at: str, http_status: int | None = None
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "kind": kind,
        "confidence": confidence,
        "checkedAt": checked_at,
        "source": source,
    }
    if isinstance(http_status, int) and 100 <= http_status <= 599:
        row["httpStatus"] = http_status
    return row


def _is_generic_career_destination(value: str) -> bool:
    path = (urlparse(value).path or "").strip("/").casefold()
    if not path:
        return True
    segments = [segment for segment in path.split("/") if segment]
    return len(segments) == 1 and segments[0] in _GENERIC_CAREER_SEGMENTS


def _is_auth_destination(value: str) -> bool:
    segments = {
        segment.casefold()
        for segment in (urlparse(value).path or "").strip("/").split("/")
        if segment
    }
    return bool(segments & _AUTH_SEGMENTS)


def _provider_detail_family(value: str) -> str:
    parsed = urlparse(value)
    host = (parsed.hostname or "").casefold()
    segments = [segment for segment in (parsed.path or "").strip("/").split("/") if segment]
    if host in {"boards.greenhouse.io", "job-boards.greenhouse.io"}:
        if len(segments) >= 3 and segments[-2].casefold() == "jobs":
            return "greenhouse"
    if host == "jobs.lever.co" and len(segments) >= 2:
        return "lever"
    if host == "jobs.ashbyhq.com" and len(segments) >= 2:
        return "ashby"
    if host.endswith(".myworkdayjobs.com") and any(
        segment.casefold() == "job" for segment in segments
    ):
        return "workday"
    if host == "jobs.smartrecruiters.com" and len(segments) >= 3:
        return "smartrecruiters"
    if (
        host.endswith(".bamboohr.com")
        and len(segments) >= 2
        and segments[-2].casefold() == "careers"
    ):
        return "bamboohr"
    if host.endswith(".breezy.hr") and len(segments) >= 2 and segments[0].casefold() == "p":
        return "breezy"
    return ""


def _canonical_url_identity(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname.casefold()
    port = parsed.port
    netloc = host if port in {None, 80, 443} else f"{host}:{port}"
    path = (parsed.path or "/").rstrip("/") or "/"
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunparse((parsed.scheme.casefold(), netloc, path, "", query, ""))


def _redirect_preserves_posting_identity(original_url: str, final_url: str) -> bool:
    original = _canonical_url_identity(original_url)
    final = _canonical_url_identity(final_url)
    if not original or not final:
        return False
    if original == final:
        return True

    original_parsed = urlparse(original)
    final_parsed = urlparse(final)
    if (
        original_parsed.netloc == final_parsed.netloc
        and original_parsed.path.casefold() == final_parsed.path.casefold()
    ):
        return True

    original_family = _provider_detail_family(original_url)
    final_family = _provider_detail_family(final_url)
    if not original_family or original_family != final_family:
        return False
    if original_family != "greenhouse" and original_parsed.hostname != final_parsed.hostname:
        return False
    original_identity = _provider_posting_identity(original_family, original_url)
    final_identity = _provider_posting_identity(final_family, final_url)
    return bool(original_identity) and original_identity == final_identity


def _provider_has_posting_application_action(
    provider_family: str,
    actions: tuple[_ApplicationAction, ...],
    *,
    checked_url: str,
) -> bool:
    if not provider_family:
        return False
    checked = urlparse(checked_url)
    checked_host = (checked.hostname or "").casefold()
    checked_path = (checked.path or "/").rstrip("/") or "/"
    checked_identity = _provider_posting_identity(provider_family, checked_url)
    if not checked_identity:
        return False
    for action in actions:
        target = urlparse(urljoin(checked_url, action.target))
        if (target.hostname or "").casefold() != checked_host:
            continue
        target_path = (target.path or "/").rstrip("/") or "/"
        target_evidence = " ".join(
            (target_path, target.query, target.fragment, action.label)
        ).casefold()
        has_apply_action = any(marker in target_evidence for marker in _APPLY_TARGET_MARKERS)
        target_identity = _provider_posting_identity(provider_family, target.geturl())
        same_posting = target_path.casefold() == checked_path.casefold() or (
            bool(target_identity) and target_identity == checked_identity
        )
        if has_apply_action and same_posting:
            return True
    return False


def _identity_after_marker(segments: list[str], marker: str) -> tuple[str, ...]:
    if marker not in segments:
        return ()
    index = segments.index(marker)
    return tuple(segments[index + 1 :])


def _greenhouse_posting_identity(segments: list[str]) -> tuple[str, ...]:
    if "jobs" not in segments:
        return ()
    index = segments.index("jobs")
    return (
        (segments[index - 1], segments[index + 1])
        if index > 0 and index + 1 < len(segments)
        else ()
    )


def _provider_posting_identity(provider_family: str, value: str) -> tuple[str, ...]:
    segments = [segment.casefold() for segment in urlparse(value).path.split("/") if segment]
    while segments and segments[-1] in _APPLY_TARGET_MARKERS:
        segments.pop()
    if provider_family == "greenhouse":
        return _greenhouse_posting_identity(segments)
    if provider_family in {"lever", "ashby"} and len(segments) >= 2:
        return tuple(segments[:2])
    if provider_family == "workday":
        return _identity_after_marker(segments, "job")
    if provider_family == "smartrecruiters" and len(segments) >= 2:
        return tuple(segments[:2])
    if provider_family == "bamboohr":
        return _identity_after_marker(segments, "careers")[:1]
    if provider_family == "breezy" and segments[:1] == ["p"] and len(segments) >= 2:
        return (segments[1],)
    return ()


def _structured_job_state(
    postings: tuple[dict[str, Any], ...], *, checked_url: str, checked_at: str
) -> str:
    target = _canonical_url_identity(checked_url)
    checked_dt = parse_datetime(checked_at)
    for posting in postings:
        identities = {
            _canonical_url_identity(str(posting.get(field) or "")) for field in ("url", "@id")
        }
        identities.discard("")
        if not target or target not in identities:
            continue
        valid_through = parse_datetime(posting.get("validThrough"))
        if valid_through and checked_dt:
            return "active" if valid_through >= checked_dt else "expired"
    return "unverified"


def classify_direct_response(
    response: DirectResponse, *, original_url: str, checked_at: str = ""
) -> dict[str, Any]:
    """Classify a response without retaining its body."""

    checked = checked_at or now_iso()
    status = int(response.status or 0)
    destination = response.final_url or original_url
    signals = _page_signals(response.text)
    text = signals.visible_text
    source = urlparse(destination).hostname or "direct_link"
    if status in _TRANSIENT_CODES or status >= 500:
        return _evidence(
            kind="direct_unverified",
            confidence="unknown",
            source=source,
            checked_at=checked,
            http_status=status,
        )
    if destination != original_url and not _redirect_preserves_posting_identity(
        original_url, destination
    ):
        original_family = _provider_detail_family(original_url)
        destination_family = _provider_detail_family(destination)
        is_ambiguous_redirect = bool(original_family and destination_family) or (
            _is_generic_career_destination(destination) or _is_auth_destination(destination)
        )
        return _evidence(
            kind="generic_redirect" if is_ambiguous_redirect else "direct_unverified",
            confidence="ambiguous" if is_ambiguous_redirect else "unknown",
            source=source,
            checked_at=checked,
            http_status=status or None,
        )
    if status in {404, 410}:
        return _evidence(
            kind="direct_closed",
            confidence="definitive",
            source=source,
            checked_at=checked,
            http_status=status,
        )
    if any(marker in text for marker in _ANTI_BOT_MARKERS):
        return _evidence(
            kind="anti_bot",
            confidence="unknown",
            source=source,
            checked_at=checked,
            http_status=status,
        )
    if 200 <= status < 400:
        provider_family = _provider_detail_family(destination)
        closed_marker = any(marker in text for marker in _CLOSED_MARKERS)
        ambiguous_closed_marker = any(marker in text for marker in _AMBIGUOUS_CLOSED_MARKERS)
        has_apply_signal = _provider_has_posting_application_action(
            provider_family,
            signals.application_actions,
            checked_url=destination,
        )
        structured_state = _structured_job_state(
            signals.job_postings,
            checked_url=destination,
            checked_at=checked,
        )
        if closed_marker or ambiguous_closed_marker:
            return _evidence(
                kind="direct_closed",
                confidence="ambiguous",
                source=source,
                checked_at=checked,
                http_status=status,
            )
        if _is_generic_career_destination(destination) or _is_auth_destination(destination):
            return _evidence(
                kind="generic_redirect",
                confidence="ambiguous",
                source=source,
                checked_at=checked,
                http_status=status,
            )
        if provider_family and (has_apply_signal or structured_state == "active"):
            return _evidence(
                kind="direct_live",
                confidence="definitive",
                source=source,
                checked_at=checked,
                http_status=status,
            )
        if provider_family and structured_state == "expired":
            return _evidence(
                kind="direct_closed",
                confidence="ambiguous",
                source=source,
                checked_at=checked,
                http_status=status,
            )
        return _evidence(
            kind="direct_unverified",
            confidence="unknown",
            source=source,
            checked_at=checked,
            http_status=status,
        )
    return _evidence(
        kind="direct_unverified",
        confidence="unknown",
        source=source,
        checked_at=checked,
        http_status=status or None,
    )


class DirectLinkValidator:
    """Direct validator with safe redirects and conservative domain pacing."""

    def __init__(
        self,
        *,
        timeout_s: float = 12.0,
        min_domain_interval_s: float = 1.0,
        max_redirects: int = 4,
        transport: Callable[[str, float, int], DirectResponse] | None = None,
        browser_fallback: Callable[[str], DirectResponse | None] | None = None,
        browser_fallback_limit: int = 1,
    ) -> None:
        self.timeout_s = max(1.0, float(timeout_s))
        self.min_domain_interval_s = max(0.0, float(min_domain_interval_s))
        self.max_redirects = max(0, min(8, int(max_redirects)))
        self.transport = transport or self._fetch
        self.browser_fallback = browser_fallback
        self.browser_fallback_limit = max(0, int(browser_fallback_limit))
        self._browser_uses = 0
        self._last_domain_check: dict[str, float] = {}
        self._lock = threading.Lock()

    def _pace(self, url: str) -> None:
        domain = (urlparse(url).hostname or "").casefold()
        with self._lock:
            previous = self._last_domain_check.get(domain, 0.0)
            delay = self.min_domain_interval_s - (time.monotonic() - previous)
            if delay > 0:
                time.sleep(delay)
            self._last_domain_check[domain] = time.monotonic()

    @staticmethod
    def _fetch(url: str, timeout_s: float, max_redirects: int) -> DirectResponse:
        current = url
        with httpx.Client(follow_redirects=False, timeout=timeout_s) as client:
            for redirect_count in range(max_redirects + 1):
                if not _public_http_url(current):
                    raise ValueError("unsafe_url")
                with client.stream(
                    "GET",
                    current,
                    headers={
                        "User-Agent": "Baluffo-Availability/1.0",
                        "Accept": "text/html,*/*;q=0.8",
                    },
                ) as response:
                    if response.status_code in _REDIRECT_CODES and response.headers.get("Location"):
                        if redirect_count >= max_redirects:
                            raise ValueError("redirect_limit")
                        current = urljoin(current, response.headers["Location"])
                        continue
                    body_bytes = bytearray()
                    for chunk in response.iter_bytes():
                        body_bytes.extend(chunk[: max(0, 500_000 - len(body_bytes))])
                        if len(body_bytes) >= 500_000:
                            break
                    return DirectResponse(
                        int(response.status_code),
                        str(response.url),
                        bytes(body_bytes).decode("utf-8", errors="replace"),
                    )
        raise ValueError("redirect_limit")

    def check(self, url: str, *, checked_at: str = "") -> dict[str, Any]:
        checked = checked_at or now_iso()
        if not _public_http_url(url):
            return _evidence(
                kind="invalid_public_url",
                confidence="unknown",
                source="direct_link",
                checked_at=checked,
            )
        self._pace(url)
        try:
            response = self.transport(url, self.timeout_s, self.max_redirects)
            result = classify_direct_response(response, original_url=url, checked_at=checked)
        except (TimeoutError, OSError, ValueError, httpx.HTTPError):
            result = _evidence(
                kind="direct_unverified",
                confidence="unknown",
                source=urlparse(url).hostname or "direct_link",
                checked_at=checked,
            )
        if (
            result.get("confidence") == "ambiguous"
            and self.browser_fallback
            and self._browser_uses < self.browser_fallback_limit
        ):
            self._browser_uses += 1
            try:
                browser_response = self.browser_fallback(url)
            except (TimeoutError, OSError, ValueError):
                browser_response = None
            if browser_response:
                result = classify_direct_response(
                    browser_response, original_url=url, checked_at=checked
                )
                result["source"] = f"browser:{result.get('source') or 'direct_link'}"
        return result


__all__ = ["DirectLinkValidator", "DirectResponse", "classify_direct_response"]
