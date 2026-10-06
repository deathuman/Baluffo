"""A board that rebranded is not gone, and the difference is a whole studio's careers page.

Splitting `site_gone_or_moved` on a registrable-domain label match finds the mistake:

    www.bungie.net       -> careers.bungie.com
    www.traviangames.de  -> www.traviangames.com
    www.talespin.company -> www.talespin.com
    gabagoogames.ca      -> www.gabagoogames.com
    starklearning.nl     -> starklearning.eu
    www.softgames.de     -> www.softgames.com

Six studios, each with a board the catalogue already knows, each redirecting to a new domain
that may well serve their jobs. Filed as `site_gone_or_moved`, a live board reads as dead --
which is precisely how a working board gets retired. That is the costliest direction for a
classification error to run in, so the split is worth making.

It is deliberately a **label** match on the registrable domain and nothing more. `exozet` ->
`endava` shares no label, and is correctly left as an acquisition rather than a rebrand;
`amazongames` -> `amazongamestudios` likewise. A looser rule -- "the words look related" --
would swallow acquisitions and hand back wrong boards.

**Nothing here decides a board is alive.** A label match says "same studio, new address", not
"this address lists jobs". Several redirect targets in this data are Steam store pages, which
is why `probe_rebranded_target` exists and why a rebrand is only useful once it has returned
evidence. The classification and the decision are kept apart deliberately: one is cheap and
always available, the other costs a fetch and can come back empty.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

import tldextract

REBRANDED = "site_rebranded"

# Reused across calls: TLDExtract caches its public-suffix list internally, and constructing
# one per redirect would re-read it. A single instance also keeps this free of I/O at import.
_EXTRACT = tldextract.TLDExtract(suffix_list_urls=())


def _registered_domain(host: str) -> str:
    """`bungie.net` from `www.bungie.net`, honouring multi-part suffixes.

    Without the public suffix list, `starklearning.nl` and `seamonster.co.za` are ambiguous:
    a naive last-two-labels split treats `.co.za` as the suffix and invents a registrable
    domain. That is the difference between "these look related" and "these are the same studio".
    """
    extracted = _EXTRACT(host or "")
    if not extracted.domain:
        return ""
    return f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain


def _domain_label(host: str) -> str:
    """The distinctive label of a host: `bungie` from `careers.bungie.com`."""
    return _EXTRACT(host or "").domain or ""


def is_rebrand(source_host: str, target_host: str) -> bool:
    """True when two hosts are the same organisation on different domains.

    Requires a label match on hosts from **different registrable domains**, so it cannot fire
    on a plain subdomain move (``invisiblewalls.co`` -> ``jobs.invisiblewalls.co``), which is
    same-site and not a rebrand at all.
    """
    source_reg = _registered_domain(source_host)
    target_reg = _registered_domain(target_host)
    if not source_reg or not target_reg or source_reg == target_reg:
        return False
    return _domain_label(source_host) == _domain_label(target_host)


def probe_rebranded_target(
    target_url: str, *, detector: Callable[[str], Mapping[str, object]]
) -> RebrandProbe:
    """Decide whether a rebrand's new address actually lists jobs.

    ``detector`` is the runtime's own job-detail-URL detector, injected rather than imported:
    this module lives in ``source_discovery`` and must not reach into an adapter's internals.
    The caller supplies the same predicate the static extractor uses, so "does this page list
    jobs" is answered by the runtime's rule and not by a looser local approximation.

    Returning ``None`` from ``detector`` -- or raising -- is treated as "no evidence", never as
    permission. A rebrand with no evidence stays a rebrand and is not re-pointed.
    """
    if not target_url:
        return RebrandProbe(confirmed=False, reason="no target url")

    # Any failure at all is "no evidence" rather than an error: a probe that times out or
    # raises must never confirm a rebrand, and must never propagate into the caller either.
    try:
        body = detector(target_url)
    except Exception as exc:
        return RebrandProbe(confirmed=False, reason=f"probe failed: {type(exc).__name__}")

    if not body:
        return RebrandProbe(confirmed=False, reason="probe returned nothing")

    try:
        found = bool(body.get("jobLinks"))
    except AttributeError:
        return RebrandProbe(confirmed=False, reason="probe result had no jobLinks")

    if not found:
        return RebrandProbe(confirmed=False, reason="no job links on the new address")
    return RebrandProbe(confirmed=True, reason="job links found on the new address")


@dataclass(frozen=True)
class RebrandProbe:
    """Whether a rebranded board's new address is worth re-pointing at."""

    confirmed: bool
    reason: str = ""
