"""Browser fallback eligibility policy for static fetch failures.

DNS resolution, TLS validation, and static clock-budget failures are terminal
for the browser lane: a browser cannot resolve a hostname, repair a certificate,
or finish work the source clock already cut short. Keeping the policy here lets
the failure classifier, the queue builder, and the queue reader agree. The
queue allowlist is narrower than "not non-browser-fixable": ``parse_error`` is
a parser finding, not browser-retry evidence, and is excluded unless the static
queue allowlist names it.
"""

from __future__ import annotations

from typing import Any

from src.jobs.common.config import STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE
from src.jobs.text_utils import clean_text

NON_BROWSER_FIXABLE_CLASSIFICATIONS = frozenset({"dns_error", "ssl_certificate_error"})

_NON_BROWSER_FIXABLE_ERROR_MARKERS = (
    "getaddrinfo failed",
    "name or service not known",
    "nodename nor servname provided",
    "temporary failure in name resolution",
    "no address associated with hostname",
    "name resolution",
    "certificate verify failed",
    "certificate_verify_failed",
    "certificate has expired",
    "certificate is not yet valid",
    "hostname mismatch",
    "unable to get local issuer certificate",
    "tlsv1 alert",
    "sslv3_alert",
    "ssl/tls alert",
    "time_budget_exceeded",
    "time budget exceeded",
    "failure budget exceeded",
    "failure_budget_exceeded",
    "request deadline exceeded",
    "total batch deadline exceeded",
)


def is_non_browser_fixable_failure(*, classification: Any = "", error: Any = "") -> bool:
    """True when retrying the URL in a browser cannot fix the failure."""
    if clean_text(classification).lower() in NON_BROWSER_FIXABLE_CLASSIFICATIONS:
        return True
    text = clean_text(error).lower()
    return any(marker in text for marker in _NON_BROWSER_FIXABLE_ERROR_MARKERS)


def is_browser_fallback_queue_eligible(*, classification: Any = "", error: Any = "") -> bool:
    """Whether a failure may be written to or read from the browser queue.

    ``parse_error`` is not a browser-retry classification. It is accepted only
    if the documented static queue allowlist names it; an ordinary parser
    finding must stay a diagnostic instead of entering the next-run queue.
    """
    normalized_classification = clean_text(classification).lower()
    if normalized_classification == "parse_error" and (
        normalized_classification not in STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE
    ):
        return False
    return not is_non_browser_fixable_failure(
        classification=normalized_classification,
        error=error,
    )
