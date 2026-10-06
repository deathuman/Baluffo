"""Classify a cross-site static redirect as a platform migration, or say why it is not.

A board whose careers page redirects off-site is not a board to patch -- it is a board that
**moved**. `www.ubisoft.com/jobs` answering with a redirect into SmartRecruiters is the same
event as a board being registered as SmartRecruiters in the first place, and it is why
`REDUNDANT_STATIC_IF_PROVIDER` exists at all. Treating the redirect as a URL to follow misses
it; treating it as a migration finds the tenant.

Measured on the live 0.3.008 run, the 135 rows carrying `Unsafe static redirect from A to B`:

| shape | rows |
|---|---:|
| cross-site to a **known ATS host** | **5** |
| cross-site to an unknown host | 118 |
| same site (https downgrade) | 12 |

Only the first group is a migration. The 118 are the far more common shape and none of it is a
platform move: `foxandsheep.com -> linkedin.com` (the studio closed), `roblox.com ->
corp.roblox.com` (rebrand), `exozet.com -> endava.com` and `game-labs.net -> stillfront.com`
(acquired). Calling those migrations would point discovery at `linkedin.com`.

So the classifier is deliberately narrow, and the two other shapes get their own names rather
than being folded in:

- ``platform_migration`` -- target host is a known ATS host. Carries the adapter, because that
  is what tells discovery which loader to register against.
- ``site_gone_or_moved`` -- cross-site to a host we do not recognise. The board left; there is
  nothing to register.
- ``insecure_downgrade`` -- same site, scheme dropped. Not a move at all: `ninjatheory.com`
  answering `http://` for an `https://` page is a server misconfiguration, and the guard is
  right to refuse it.

**This reads the host and never fetches the target.** That is the whole point. The guard in
``_safe_redirect_url`` refuses to follow a cross-site redirect for good reason; this classifier
exists so the refusal can carry a *diagnosis* instead of a bare error string. Feeding the
target back into a fetch would reintroduce exactly what the guard prevents.

**Tenancy is not derived here.** ``target_host`` alone does not identify a board -- Greenhouse
serves every studio from one host. Deriving the tenant is the caller's job, from the path or
from the platform's own API, and a caller that has only a host should treat the tenant as
unknown rather than guess. ``_tenant_from_path`` is offered for the platforms whose board URL
carries the tenant as its first path segment, and returns ``""`` whenever that does not hold
rather than producing a plausible wrong answer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

# Host patterns that identify an applicant-tracking platform. Matched as suffixes, so
# `job-boards.eu.greenhouse.io` is recognised while `notgreenhouse.io` is not.
PLATFORM_HOST_PATTERNS: tuple[tuple[str, str], ...] = (
    ("greenhouse.io", "greenhouse"),
    ("myworkdayjobs.com", "workday"),
    ("myworkday.com", "workday"),
    ("ashbyhq.com", "ashby"),
    ("lever.co", "lever"),
    ("smartrecruiters.com", "smartrecruiters"),
    ("workable.com", "workable"),
    ("teamtailor.com", "teamtailor"),
    ("bamboohr.com", "bamboohr"),
    ("personio.de", "personio"),
    ("personio.com", "personio"),
    ("recruitee.com", "recruitee"),
    ("recruitee.net", "recruitee"),
    ("dayforce.com", "dayforce"),
    ("oraclecloud.com", "oracle_hcm"),
    ("icims.com", "icims"),
    ("applytojob.com", "jazzhr"),
    ("hibob.com", "hibob"),
    ("jobvite.com", "jobvite"),
    ("eightfold.ai", "eightfold"),
    ("pinpointhq.com", "pinpoint"),
    ("comeet.co", "comeet"),
    ("jobs.smartrecruiters.com", "smartrecruiters"),
)

# Platforms whose board URL carries the tenant as its first path segment:
# boards.greenhouse.io/<slug>, <tenant>.myworkdayjobs.com/<tenant>,
# apply.workable.com/<account>, jobs.lever.co/<account>.
_TENANT_IN_FIRST_SEGMENT = frozenset(
    {"greenhouse", "workday", "workable", "lever", "smartrecruiters", "teamtailor", "comeet"}
)

MIGRATION = "platform_migration"
SITE_GONE = "site_gone_or_moved"
INSECURE_DOWNGRADE = "insecure_downgrade"
UNPARSEABLE = "unparseable_redirect"


def _site(host: str) -> str:
    """Host with a leading ``www.`` removed, so a rebrand is not a site change."""
    clean = (host or "").strip().lower()
    return clean[4:] if clean.startswith("www.") else clean


def _platform_for(host: str) -> str:
    clean = (host or "").strip().lower()
    for pattern, adapter in PLATFORM_HOST_PATTERNS:
        if clean == pattern or clean.endswith("." + pattern):
            return adapter
    return ""


def _strip_path_parameters(url: str) -> str:
    """Drop a trailing ``;`` from the end of a URL.

    Servers emit it -- ``https://corp.roblox.com/careers;`` -- and it is a legal path character
    under RFC 3986, so the URL is well formed and the host extracts cleanly either way. The
    problem is only that copying it verbatim into a registration URL leaves a stray character
    on the end for every downstream reader to rediscover.

    ``urlparse`` puts the segment before ``;`` in ``path`` and the rest in ``params``, so the
    semicolon is invisible in ``path`` and stripping it there is a no-op. The character has to
    be removed from the raw string before parsing, which is what this does.
    """
    cleaned = str(url or "").strip()
    if not cleaned.endswith(";"):
        return cleaned
    # Guard against a bare ";" or a ";" that is only the query/fragment separator.
    without_query = cleaned.split("?", 1)[0].split("#", 1)[0]
    if without_query.endswith(";"):
        cleaned = cleaned[:-1]
    return cleaned


def _tenant_from_path(target_url: str, adapter: str) -> str:
    """The tenant, when the platform's board URL carries it as its first path segment.

    Returns ``""`` when it does not, which is the common case and the safe one: a board URL
    that does not name its tenant cannot have its tenant read off the URL, and inventing one
    would register the wrong studio.
    """
    if adapter not in _TENANT_IN_FIRST_SEGMENT:
        return ""
    segments = [s for s in (urlparse(target_url).path or "").split("/") if s]
    if not segments:
        return ""
    candidate = segments[0].strip().lower()
    # Workday puts the tenant on the host and repeats it in the path; Greenhouse puts a slug in
    # the path under a shared host. Both are the first segment, so one rule covers them, but a
    # segment that is obviously not an identifier is not a tenant.
    if not candidate or len(candidate) > 80:
        return ""
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", candidate):
        return ""
    return candidate


@dataclass(frozen=True)
class RedirectClassification:
    """What a refused cross-site redirect turned out to be."""

    kind: str
    adapter: str = ""
    target_host: str = ""
    target_site: str = ""
    target_url: str = ""
    source_site: str = ""
    tenant: str = ""
    reason: str = ""


def classify_cross_site_redirect(source_url: str, target_url: str) -> RedirectClassification:
    """Classify a redirect the runtime refused to follow.

    ``source_url`` and ``target_url`` are read for their **host only**. Nothing here performs
    a fetch, opens a socket, or validates the target beyond pattern-matching its host, so
    calling this on an untrusted ``Location`` header cannot reach anything.
    """
    source = urlparse(str(source_url or ""))
    target = urlparse(_strip_path_parameters(str(target_url or "")))

    source_site = _site(source.hostname or "")
    target_host = (target.hostname or "").strip().lower()
    target_site = _site(target_host)

    if not source_site or not target_site:
        return RedirectClassification(
            kind=UNPARSEABLE,
            target_host=target_host,
            reason="redirect had no usable host on one side",
        )

    if source_site == target_site:
        if source.scheme == "https" and target.scheme != "https":
            return RedirectClassification(
                kind=INSECURE_DOWNGRADE,
                source_site=source_site,
                target_host=target_host,
                target_site=target_site,
                reason="same site, but the scheme dropped below https",
            )
        return RedirectClassification(
            kind=UNPARSEABLE,
            source_site=source_site,
            target_host=target_host,
            target_site=target_site,
            reason="same-site redirect is not a migration",
        )

    adapter = _platform_for(target_host)
    if adapter:
        cleaned = _strip_path_parameters(str(target_url or ""))
        return RedirectClassification(
            kind=MIGRATION,
            adapter=adapter,
            source_site=source_site,
            target_host=target_host,
            target_site=target_site,
            target_url=cleaned,
            tenant=_tenant_from_path(cleaned, adapter),
            reason=f"careers page moved to {adapter}",
        )

    return RedirectClassification(
        kind=SITE_GONE,
        source_site=source_site,
        target_host=target_host,
        target_site=target_site,
        target_url=_strip_path_parameters(str(target_url or "")),
        reason="redirect left the site for a host no platform owns",
    )
