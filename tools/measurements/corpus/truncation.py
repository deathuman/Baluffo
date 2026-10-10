"""Record-time check for the one gap a corpus cannot show about itself: work a phase dropped.

A corpus records the requests a run *made*. A request a phase decided **not** to make leaves no
trace at all -- and there are two measured instances.

The sheet redirect phase. On the full-source epoch of 2026-10-09 it held 465 candidates for one
host, requested 47 of them, and abandoned the host after too many response-less failures
(``PooledRedirectResolver``, ``src/jobs/transport.py``). A replay serves the same bytes instantly,
walks the same candidate list, asks for the links the record abandoned, and dies with a corpus
miss inside that phase. The record already knows the candidate set, because the rows it kept are
the ones whose links the resolver was handed::

    [corpus] WARNING truncation: gracklehq.com - 47 of 443 candidate links requested \\
(396 unrequested)

The second full-source epoch (2026-10-10) showed the join's other error. Its finding read ``0 of
252 candidate links requested``, and all 252 rows came from ``gracklehq`` -- a source the phase is
deliberately never handed (``REDIRECT_RESOLUTION_SKIP_SOURCES`` in
``src/jobs/canonicalize_locations._resolve_job_link``), while the sheet phase it was blamed on had
requested every one of its own 465. A kept row declares a candidate only when its source's
canonicalization path hands the resolver the link, so the candidate rule mirrors that gate the
same way it mirrors the predicate.

Static boards on the per-source clock. The second full-source epoch of 2026-10-09 recorded with
the 90s default clock. Five static sources were stopped by it, and their report text names the URL
each was about to fetch when the clock refused it -- for ``eipix.com/join-us/`` that URL is
``https://www.eipix.com/career/content-creatorcopywriter``. The next replay walked past the
recorded stop (a corpus serves bytes instantly, so the clock never fires) and died on exactly that
URL::

    [corpus] WARNING truncation: static_source::static:listing_url:https://eipix.com/join-us/ \\
- source clock stopped the run; 1 URL the report names has no corpus request

That family needs no new candidate rule: the run's own refusal text is the declaration, and the
corpus confirms the absence, so the finding is both declared and verified. Where a cap drops
candidates without naming them -- ``detail_pages_skipped_by_adaptive_stop``,
``external_detail_links_capped`` -- the finding is the declared count alone, and says so.

Both families are warnings, not failures: a record with a truncated phase is still a usable
coverage record and a usable source for chosen-subset replays. What it must not be is a silent
one -- and what it may not be, unasked, is a **delta referee**. The dividing line is not how much
work was dropped but whether a replay drops the same work: the redirect pool's abandonment and the
source clock are bounds on failure pacing, and replay removes both (bytes arrive instantly and no
request fails), so the replay walks past the recorded stop and misses inside a phase that looks
unrelated to the bound. A record carrying one of those refuses that certification and names the
knob that unbinds it. A declared cap is the exception -- both cap paths are count-driven, so replay
reproduces the drop -- and it is recorded as a note instead.

The bounds are stated in the payload, because each one costs the check range. For the redirect
family the candidate rule is the resolver's own -- an allowlisted host and a ``/rd/`` path
(``SUPPORTED_REDIRECT_HOSTS`` today holds two spellings of one host) -- so it covers exactly the
hosts a resolver would abandon and nothing wider, and candidates read off **kept rows** are a
lower bound on what the phase held (the epoch's own telemetry said 465 where its rows declared
443). For the static family the declaration is the report's text, so a drop the report does not
name or count is invisible here; the report keeps at most the first six partial errors per source
(``pipeline_source_results._apply_partial_errors_and_low_confidence``), which bounds how many
refused URLs one source can offer.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

# Counts read out of reports and stats dicts: the shared coercer, not a local helper, because a bare
# ``except (TypeError, ValueError)`` lets ``int(float("inf"))``'s OverflowError escape (the ratchet
# in ``tests/test_shared_utils_coercion_ratchet.py`` is what caught the local copy).
from src.shared.utils import coerce_non_negative_int

# The predicate the resolver itself uses to decide a link is worth resolving. Imported from the
# product rather than re-implemented: if this tool's idea of a candidate diverges from the
# resolver's, it reports a gap that does not exist, or misses the one that does.
_PREDICATE_MODULE = "src.jobs.transport"
_PREDICATE_NAME = "is_supported_redirect_url"

# The product's own list of sources whose canonicalization path never hands the resolver a link
# (``src/jobs/canonicalize_locations._resolve_job_link``). The candidate rule needs it for the same
# reason it needs the predicate: a kept row from such a source declares a link the phase never
# sees, and counting it reports a gap a replay cannot miss. Measured on the second full-source
# epoch (2026-10-10, pool 64): 252 ``gracklehq`` rows read as unrequested candidates while the
# sheet-redirect phase they were blamed on had requested every one of its own (465).
_SKIP_SOURCES_MODULE = "src.jobs.canonicalize_locations"
_SKIP_SOURCES_NAME = "REDIRECT_RESOLUTION_SKIP_SOURCES"
# The normalisation the gate itself applies to a source before that membership test.
_SOURCE_NORMALIZER_NAME = "norm_text"

_UNIT_SEPARATOR = "; "
# The static lane's own URL normalisation (``src/jobs/text_utils.normalize_url``): trailing-slash
# and tracking-parameter-insensitive, and applied by the lane before it fetches. A named refusal
# compared without it reads as unrequested when the corpus holds the same request spelled longer.
_NORMALIZER_MODULE = "src.jobs.text_utils"
_NORMALIZER_NAME = "normalize_url"

_STATIC_ADAPTER = "static"
_CLOCK_STOP_TOKENS = ("time budget exceeded", "time_budget_exceeded")
# The whole-batch deadline (`src/shared/http_batch.py`) is a different object from the source clock:
# it bounds a *batch* of page fetches by wall clock, so a live record can reach it and a replay
# cannot -- recorded bodies are served instantly, so the batch clock never arrives at its bound.
# Measured on the 2026-10-10 full-source epoch: 16 static sources read `error` with this text, every
# one keeping 0 rows, and the replay clears every one of them (counts and statuses, not jobs).
# Declared rather than refused: the record cannot avoid a wall clock, and the bound has no knob.
_BATCH_DEADLINE_TOKENS = ("total batch deadline exceeded",)
_URL_RE = re.compile(r"https?://\S+")
_MESSAGE_SEPARATOR = ": "

# The detail-work counters a capped static phase books. Both are declarations, not URLs: the
# candidates a cap drops are never written down, so the count is all the record can offer. They are
# also count-driven, which is why they are notes in the referee verdict rather than blockers -- a
# replay of the same bytes stops in the same place.
STATIC_DROP_STATS: tuple[tuple[str, str], ...] = (
    ("detail_pages_skipped_by_adaptive_stop", "detailsSkippedByAdaptiveStop"),
    ("external_detail_links_capped", "externalDetailLinksCapped"),
)

_HASH_PREFIX = "#"

# How many unrequested URLs to name per finding. The count is the finding; the samples are the
# evidence a reader needs to check one by hand.
_SAMPLE_LIMIT = 5

REPORT_NAME = "truncation-report.json"

# The exit code for a record that refuses the certification. It lives beside the verdict because it
# *is* the verdict's exit code: the driver imports it rather than restating the number, so a study
# gate that shares the refusal cannot drift from the record that produced it.
EXIT_REFEREE_REFUSED = 3

# The knob that unbinds each blocking phase, named for the operator rather than described: advice
# an operator cannot paste is not advice. Both names are live surfaces (pinned by tests): the pool
# is a driver flag, because record and replay must use the same one, and the clock is the
# environment variable the lane reads through ``build_static_source_runtime_config``.
KNOB_REDIRECT_POOL = "--google-sheets-redirect-concurrency"
KNOB_STATIC_SOURCE_CLOCK = "BALUFFO_STATIC_SOURCE_TIME_BUDGET_S"
# The one cap with a knob: the external-fanout cap is a domain-profile value (the constant below it
# is the fallback), while the adaptive stop is a policy reaction to recorded failures and has none.
KNOB_EXTERNAL_FANOUT_CAP = "max_external_detail_links (domain profile)"

INTERPRETATION = (
    "Two families, both record-time joins against the corpus. Redirect: a candidate is a link in a "
    "kept row that the redirect resolver treats as a supported redirect URL and whose source's "
    "canonicalization path hands the resolver that link at all -- the product's "
    "REDIRECT_RESOLUTION_SKIP_SOURCES never do, so their rows declare no candidate; an "
    "unrequested "
    "candidate is one the corpus holds no request for, and the shape to expect is a host the "
    "resolver abandoned (it stops requesting a host after max(2, maxConnections * 2) response-less "
    "failures, cleared by any success). Static: a finding is a source whose own report declares "
    "dropped work -- a refusal text naming a URL the source clock cut (reason source_clock), or "
    "counters for detail pages a cap skipped (reason detail_cap), or a whole-batch deadline this "
    "run expired (reason batch_deadline, which names no lost URL: the batch was asked for "
    "everything it was going to ask for). For source_clock the named URLs "
    "are checked against the corpus and the unrequested ones are listed: those are the requests a "
    "replay will make, because replay removes the wall clock that stopped this run. For detail_cap "
    "the counts are declarations only -- nothing records the dropped candidate URLs -- so the "
    "finding reports what the run accounted for and no URLs -- but the drop itself is reproduced "
    "by replay, because both cap paths are count-driven. Replay removes the timing that decided "
    "which candidates were reached, so the redirect-pool and source_clock findings are what refuse "
    "a record's delta-referee certification (see referee_verdict), and each names the knob that "
    "unbinds it. A batch_deadline finding refuses nothing: the bound is a wall clock a replay "
    "cannot reach, so it travels as a note carrying the statuses that will differ. Candidates for "
    "the redirect family count only the links this run's kept rows declare, so that count is a "
    "floor; the static family can see only drops the report names or counts, and the report keeps "
    "at most six partial errors per source."
)

_STATIC_INTERPRETATION_SUFFIX = (
    " Source names are the report's own source names, and a URL is called a page when it matches "
    "one of the source's registered pages, a detail otherwise."
)


def _predicate():
    from importlib import import_module

    module = import_module(_PREDICATE_MODULE)
    return getattr(module, _PREDICATE_NAME)


def _normalizer():
    from importlib import import_module

    module = import_module(_NORMALIZER_MODULE)
    return getattr(module, _NORMALIZER_NAME)


def _source_normalizer():
    from importlib import import_module

    module = import_module(_NORMALIZER_MODULE)
    return getattr(module, _SOURCE_NORMALIZER_NAME)


def _skip_sources() -> set[str]:
    """The product's own skip list, read the way the gate reads it (one import, no copy)."""

    from importlib import import_module

    module = import_module(_SKIP_SOURCES_MODULE)
    return {str(item) for item in getattr(module, _SKIP_SOURCES_NAME)}


def _source_key(value: Any) -> str:
    """A row's source under the gate's own normalisation, so both sides of the test agree."""

    return str(_source_normalizer()(value) or "")


def _canonical(url: Any) -> str:
    """The corpus's own URL normalisation, so the join is on identity rather than spelling.

    A row link and the URL a request was sent to can differ by case, a default port or a
    fragment; comparing them as strings would report a candidate as unrequested when it was
    fetched. Both sides go through the store's normaliser, which is the one the corpus keys on.
    """

    from tools.measurements.corpus.store import canonical_url

    return canonical_url(url)


def _normalized(url: Any) -> str:
    """The static lane's own URL normalisation, for the join that family needs.

    The lane normalises before it fetches, so a refused URL and the request the corpus holds for
    the same page can differ by a trailing slash or a tracking parameter. Applying the lane's own
    normaliser to both sides keeps a fetched URL from being reported as unrequested.
    """

    try:
        return str(_normalizer()(url) or "")
    except Exception:  # pragma: no cover - a broken import degrades this family, not the run
        return ""


def host_of(url: Any) -> str:
    """The host a URL's resolver call would be paced and abandoned by: lowercased, no port."""

    value = str(url or "").strip()
    if "//" not in value:
        return ""
    rest = value.split("//", 1)[1]
    host = rest.split("/", 1)[0].split("?", 1)[0].split(_HASH_PREFIX, 1)[0]
    if "@" in host:
        host = host.rsplit("@", 1)[1]
    return host.split(":", 1)[0].strip().lower()


def candidates_by_host(
    rows: Iterable[Mapping[str, Any]],
    *,
    predicate=None,
    skip_sources: Iterable[Any] | None = None,
) -> dict[str, set[str]]:
    """Redirect candidates per host, read off the links of the rows this run kept.

    A row only declares a candidate when the phase would have been handed its link: sources the
    product's own gate skips are excluded, because their links are never resolved and a replay of
    them makes no request to miss. ``skip_sources`` exists for tests; the default is the product's.
    """

    keep = predicate or _predicate()
    skip = {str(item) for item in skip_sources} if skip_sources is not None else _skip_sources()
    candidates: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if _source_key(row.get("source")) in skip:
            continue
        link = str(row.get("jobLink") or "").strip()
        if not link or not keep(link):
            continue
        host = host_of(link)
        if not host:
            continue
        candidates.setdefault(host, set()).add(link)
    return candidates


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_detail_item(row: Mapping[str, Any]) -> Mapping[str, Any]:
    """The single detail row the pipeline attaches to each source row, when it is there."""

    details = row.get("details")
    if isinstance(details, Sequence) and not isinstance(details, (str, bytes)):
        for item in details:
            if isinstance(item, Mapping):
                return item
    return {}


def clock_refusals(text: Any) -> list[str]:
    """URLs the report says were refused because the source clock had already expired.

    The static lane writes each failure as ``static:{label}:{target_url}: {message}`` and the
    pipeline wraps it as ``{source_name}: {...}``. The label is sometimes a studio and sometimes
    the source name -- which itself ends with the board's URL (``static_source::static:listing_url:
    https://...``) -- so label and target can sit next to each other with no more than a colon
    between them. The URL is therefore read off the segment rather than a parsed field: the text up
    to the last ``": "`` before the clock token, and inside it the LAST URL, which is the fetch
    target wherever the label happens to end.
    """

    refusals: list[str] = []
    for segment in str(text or "").split(_UNIT_SEPARATOR):
        token_index = -1
        for token in _CLOCK_STOP_TOKENS:
            index = segment.find(token)
            if index >= 0 and (token_index < 0 or index < token_index):
                token_index = index
        if token_index < 0:
            continue
        separator = segment.rfind(_MESSAGE_SEPARATOR, 0, token_index)
        if separator < 0:
            continue
        matches = list(_URL_RE.finditer(segment[:separator]))
        if not matches:
            continue
        # A label that ends in the same URL concatenates two of them; the last is the target.
        tail = matches[-1].group(0)
        for marker in ("http://", "https://"):
            last = tail.rfind(marker)
            if last > 0:
                tail = tail[last:]
        if tail:
            refusals.append(tail)
    return refusals


def batch_deadline_segments(text: Any) -> list[str]:
    """Report segments that say the whole-batch deadline expired, not the source clock.

    The two are separate declarations and are read separately: the source clock is a per-source
    budget a replay does not have (its named URLs are checked against the corpus), while the batch
    deadline is a per-batch wall clock a replay cannot reach (there is no URL to check -- the batch
    was already asked for everything it was going to ask for). A segment can carry one, the other,
    or both.
    """

    hits: list[str] = []
    for segment in str(text or "").split(_UNIT_SEPARATOR):
        stripped = segment.strip()
        if stripped and any(token in stripped for token in _BATCH_DEADLINE_TOKENS):
            hits.append(stripped)
    return hits


def _static_source_view(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """What one static source's report row declares it dropped, or ``None`` when it declares none."""

    detail = _first_detail_item(row)
    stats = _as_mapping(detail.get("stats"))
    declared = {
        "candidatesFound": coerce_non_negative_int(stats.get("candidate_links_found")),
        "detailsVisited": coerce_non_negative_int(stats.get("detail_pages_visited")),
    }
    for stat_name, label in STATIC_DROP_STATS:
        declared[label] = coerce_non_negative_int(stats.get(stat_name))
    text = _UNIT_SEPARATOR.join(
        part
        for part in (str(row.get("error") or ""), str(detail.get("error") or ""))
        if str(part).strip()
    )
    refusals = clock_refusals(text)
    deadline = batch_deadline_segments(text)
    dropped = sum(declared[label] for _, label in STATIC_DROP_STATS)
    if not refusals and not dropped and not deadline:
        return None
    return {
        "source": str(row.get("name") or ""),
        "studio": str(row.get("studio") or detail.get("studio") or ""),
        "status": str(row.get("status") or detail.get("status") or ""),
        "keptCount": coerce_non_negative_int(row.get("keptCount")),
        "refusals": refusals,
        "batchDeadline": deadline,
        "declared": declared,
        "pages": [str(page).strip() for page in detail.get("pages") or [] if str(page).strip()],
    }


def _requested_keys(requested_urls: Iterable[Any]) -> set[str]:
    """Both spellings a static join has to accept, for every URL the corpus holds a request for.

    ``canonical_url`` is the form the corpus keys on and the redirect family joins through;
    ``normalize_url`` is the form the static lane fetches with. A set of both means a refusal is
    reported as unrequested only when no request matches it under either rule.
    """

    keys: set[str] = set()
    for url in requested_urls:
        text = str(url or "").strip()
        if not text:
            continue
        canonical = _canonical(text)
        if canonical:
            keys.add(canonical)
        normalized = _normalized(text)
        if normalized:
            keys.add(normalized)
    return keys


def _is_requested(url: str, keys: set[str]) -> bool:
    """True when any request in the corpus matches this URL under either normalisation."""

    for spelling in (_canonical(url), _normalized(url)):
        if spelling and spelling in keys:
            return True
    return False


def _url_kind(url: str, pages: Sequence[str]) -> str:
    """``page`` for a URL that is one of the source's registered listing pages, else ``detail``."""

    for page in pages:
        if _normalized(url) and _normalized(url) == _normalized(page):
            return "page"
    return "detail"


def static_source_findings(
    sources: Iterable[Mapping[str, Any]],
    requested_urls: Iterable[Any],
) -> tuple[list[dict[str, Any]], int]:
    """Per-source static findings, worst first, and how many static sources were read.

    A finding exists when the report declares dropped work: a refusal whose message says the
    source clock expired (``source_clock``), or a non-zero cap counter (``detail_cap``). The
    claimed URLs are the refused ones the corpus holds no request for; a refusal whose URL *is*
    requested is not reported as a lost URL, because the corpus shows the request was made.
    """

    keys = _requested_keys(requested_urls)
    findings: list[dict[str, Any]] = []
    audited = 0
    for row in sources or []:
        if not isinstance(row, Mapping) or str(row.get("adapter") or "") != _STATIC_ADAPTER:
            continue
        audited += 1
        view = _static_source_view(row)
        if view is None:
            continue
        seen: set[str] = set()
        lost: list[dict[str, Any]] = []
        for url in view["refusals"]:
            identity = _canonical(url) or url
            if identity in seen:
                continue
            seen.add(identity)
            if _is_requested(url, keys):
                continue
            lost.append({"url": url, "kind": _url_kind(url, view["pages"])})
        reasons: list[str] = []
        if view["refusals"]:
            reasons.append("source_clock")
        if view["batchDeadline"]:
            reasons.append("batch_deadline")
        if any(view["declared"][label] for _, label in STATIC_DROP_STATS):
            reasons.append("detail_cap")
        findings.append(
            {
                "source": view["source"],
                "studio": view["studio"],
                "status": view["status"],
                "keptCount": view["keptCount"],
                "reasons": reasons,
                # Both of these are bounds a replay does not share: the clock and the batch
                # deadline are wall-clock, so the replay walks past them and the source's own
                # status moves. A cap is count-driven and replay drops the same work.
                "replayRisk": (
                    "diverges"
                    if ("source_clock" in reasons or "batch_deadline" in reasons)
                    else "dropped-work"
                ),
                "unrequestedUrls": lost,
                "unrequestedSamples": [item["url"] for item in lost[:_SAMPLE_LIMIT]],
                "batchDeadlineSamples": view["batchDeadline"][:_SAMPLE_LIMIT],
                "declared": view["declared"],
            }
        )
    findings.sort(
        key=lambda item: (
            -len(item["unrequestedUrls"]),
            -sum(int(item["declared"].get(label) or 0) for _, label in STATIC_DROP_STATS),
            str(item["source"]),
        )
    )
    return findings, audited


def audit(
    *,
    rows: Iterable[Mapping[str, Any]],
    requested_urls: Iterable[Any],
    pool: int = 0,
    scope: str = "",
    sources: Iterable[Mapping[str, Any]] = (),
    predicate=None,
    skip_sources: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """The record-time truncation report: candidates the run knew, minus requests it made.

    ``pool`` is the redirect pool in force (the report's ``runtime.googleSheetsRedirectConcurrency``
    is authoritative; 0 means it was not stated). It is reported as context, not used as a gate:
    the abandonment point depends on how failures interleave with successes, so a threshold that
    can be computed here would be a guess dressed as a rule.

    ``sources`` is the run's per-source report rows (``report["sources"]``). Only rows whose
    adapter is ``static`` are read, and only for work their own accounting says was dropped.
    """

    requested = tuple(str(url) for url in requested_urls if str(url).strip())
    requested_redirect = {_canonical(url) for url in requested}
    candidates = candidates_by_host(rows, predicate=predicate, skip_sources=skip_sources)

    findings: list[dict[str, Any]] = []
    candidate_total = 0
    requested_total = 0
    for host, links in sorted(candidates.items()):
        candidate_total += len(links)
        missing = sorted(link for link in links if _canonical(link) not in requested_redirect)
        requested_total += len(links) - len(missing)
        if not missing:
            continue
        findings.append(
            {
                "host": host,
                "candidates": len(links),
                "requested": len(links) - len(missing),
                "unrequested": len(missing),
                "unrequestedSamples": missing[:_SAMPLE_LIMIT],
            }
        )
    findings.sort(key=lambda item: (-int(item["unrequested"]), str(item["host"])))

    static_findings, static_audited = static_source_findings(sources, requested)
    static_lost = sum(len(finding["unrequestedUrls"]) for finding in static_findings)
    static_dropped = sum(
        int(finding["declared"].get(label) or 0)
        for finding in static_findings
        for _, label in STATIC_DROP_STATS
    )

    effective_pool = int(pool or 0)
    return {
        "schemaVersion": 2,
        "scope": str(scope),
        "pool": effective_pool,
        # The resolver's abandonment bound, for a reader: max(2, connections * 2). Not a gate.
        "abandonmentThresholdAtMost": max(2, effective_pool * 2) if effective_pool else 0,
        "candidateHosts": len(candidates),
        "candidates": candidate_total,
        "requestedOfCandidates": requested_total,
        "unrequestedCandidates": candidate_total - requested_total,
        "hostsWithUnrequestedCandidates": len(findings),
        "hosts": findings,
        "staticSourcesAudited": static_audited,
        "staticSourcesFlagged": len(static_findings),
        "staticBatchDeadlineSources": sum(
            1 for finding in static_findings if "batch_deadline" in finding["reasons"]
        ),
        "staticSourcesWithUnrequestedUrls": sum(
            1 for finding in static_findings if finding["unrequestedUrls"]
        ),
        "staticUnrequestedUrls": static_lost,
        "staticDeclaredDroppedDetails": static_dropped,
        "staticSources": static_findings,
        "status": "flagged" if findings or static_findings else "clean",
        "interpretation": INTERPRETATION + _STATIC_INTERPRETATION_SUFFIX,
    }


def audit_epoch(epoch_dir: Path, run_dir: Path, *, scope: str = "") -> dict[str, Any]:
    """Re-run the record-time audit over a finished epoch, for a reader who was not there.

    The check is a join between artifacts the epoch already holds: the candidates its kept rows
    declare, the requests its corpus holds, and its own per-source accounting (plus the redirected
    pool the run resolved, which is a *context* field rather than a gate). An epoch recorded before
    the check existed has no ``deltaReferee`` block -- and that is exactly the epoch an operator
    choosing a referee needs an answer about, so the join is re-run instead of being declared
    unknowable. It is the same join, not a second implementation: the same :func:`audit`, the same
    three inputs, read from disk rather than from the recorder's memory.

    What comes back is a **recomputation**, and the caller must label it as one. A verdict in
    ``EPOCH.json`` is what that record decided under the code of its day; this is what today's
    audit says about the same artifacts, and where the two disagree the recorded one is the
    epoch's own statement.
    """

    from tools.measurements.corpus import harness
    from tools.measurements.corpus.store import CorpusStore

    report = harness.run_report(run_dir)
    runtime = _as_mapping(report.get("runtime"))
    return audit(
        rows=harness.run_rows(run_dir),
        requested_urls=CorpusStore(epoch_dir).iter_requested_urls(),
        pool=coerce_non_negative_int(runtime.get("googleSheetsRedirectConcurrency")),
        scope=str(scope),
        sources=report.get("sources") or [],
    )


def summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """The compact form that belongs in a manifest, without the per-host evidence."""

    return {
        "status": str(report.get("status") or ""),
        "candidates": int(report.get("candidates") or 0),
        "unrequestedCandidates": int(report.get("unrequestedCandidates") or 0),
        "hostsWithUnrequestedCandidates": int(report.get("hostsWithUnrequestedCandidates") or 0),
        "staticSourcesAudited": int(report.get("staticSourcesAudited") or 0),
        "staticSourcesFlagged": int(report.get("staticSourcesFlagged") or 0),
        "staticBatchDeadlineSources": int(report.get("staticBatchDeadlineSources") or 0),
        "staticUnrequestedUrls": int(report.get("staticUnrequestedUrls") or 0),
        "staticDeclaredDroppedDetails": int(report.get("staticDeclaredDroppedDetails") or 0),
        "report": REPORT_NAME,
    }


def _referee_blockers(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The findings that stop a record substantiating a replay delta, each with the knob that unbinds it.

    The dividing line is not how much work was dropped but whether a replay drops the same work.
    The resolver's abandonment and the source clock bound *failure pacing*, and replay removes both:
    bytes arrive instantly and no request fails, so the replay walks into links the record abandoned
    and dies on a corpus miss inside a phase that looks unrelated to the bound. Those two block. The
    caps are count-driven (an adaptive stop after two off-domain failures or two redirect loops, and
    a pure host-count fanout cap), so a replay of the same bytes stops in the same place.
    """

    blockers: list[dict[str, Any]] = []
    hosts = [item for item in report.get("hosts") or [] if isinstance(item, Mapping)]
    if hosts:
        unrequested = sum(coerce_non_negative_int(item.get("unrequested")) for item in hosts)
        blockers.append(
            {
                "reason": "redirect_pool",
                "hosts": [str(item.get("host") or "") for item in hosts],
                "unrequestedCandidates": unrequested,
                "knob": KNOB_REDIRECT_POOL,
                "detail": (
                    f"the resolver abandoned {len(hosts)} host(s) after too many response-less "
                    f"failures, so {unrequested} candidate link(s) were never requested; set the "
                    "pool in record and replay alike so the bound cannot bind"
                ),
            }
        )
    clock = [
        item
        for item in report.get("staticSources") or []
        if isinstance(item, Mapping) and "source_clock" in list(item.get("reasons") or [])
    ]
    if clock:
        named = sum(len(item.get("unrequestedUrls") or []) for item in clock)
        blockers.append(
            {
                "reason": "source_clock",
                "sources": [str(item.get("source") or "") for item in clock],
                "unrequestedUrls": named,
                "knob": KNOB_STATIC_SOURCE_CLOCK,
                "detail": (
                    f"the source clock stopped {len(clock)} static source(s) with work left; "
                    f"{named} URL(s) the report names have no corpus request and a replay, which "
                    "has no clock, will request them"
                ),
            }
        )
    return blockers


def _referee_notes(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Findings a referee may carry: work dropped *reproducibly*, with whatever knob exists.

    A cap finding is not nothing -- the record says work was dropped and how much -- but it is a
    policy drop a replay repeats, so it is recorded beside the verdict rather than deciding it. The
    adaptive stop has no knob at all (it fires on two recorded detail failures), and saying so is
    part of the finding: a remedy that does not exist is worse than none.

    The batch deadline is the second shape a declaration takes here, and the opposite one: nothing
    was dropped, but the bound the record ran under expires a status the replay will not reproduce.
    A study measuring on this epoch has to know those sources flip, and it can only know it from
    the record -- which is why the count travels with the verdict rather than in a log line.
    """

    notes: list[dict[str, Any]] = []
    capped = [
        item
        for item in report.get("staticSources") or []
        if isinstance(item, Mapping) and "detail_cap" in list(item.get("reasons") or [])
    ]
    if capped:
        declared = [_as_mapping(item.get("declared")) for item in capped]
        stopped = sum(
            coerce_non_negative_int(item.get("detailsSkippedByAdaptiveStop")) for item in declared
        )
        external = sum(
            coerce_non_negative_int(item.get("externalDetailLinksCapped")) for item in declared
        )
        knobs = [KNOB_EXTERNAL_FANOUT_CAP] if external else []
        detail = (
            f"detail work dropped by policy: {stopped} page(s) skipped by an adaptive stop, "
            f"{external} capped from external fanout. Neither refuses the certification, because "
            "both are count-driven and a replay of the same bytes drops the same work"
        )
        if not knobs:
            detail += (
                "; the adaptive stop has no knob -- it is a policy reaction to recorded failures"
            )
        notes.append(
            {
                "reason": "detail_cap",
                "sources": [str(item.get("source") or "") for item in capped],
                "declaredDroppedDetails": stopped + external,
                "knobs": knobs,
                "detail": detail,
            }
        )
    deadline = [
        item
        for item in report.get("staticSources") or []
        if isinstance(item, Mapping) and "batch_deadline" in list(item.get("reasons") or [])
    ]
    if deadline:
        notes.append(
            {
                "reason": "batch_deadline",
                "sources": [str(item.get("source") or "") for item in deadline],
                "notesKeptRows": sum(
                    coerce_non_negative_int(item.get("keptCount")) for item in deadline
                ),
                "detail": (
                    f"the whole-batch deadline expired for {len(deadline)} static source(s) in the "
                    "record and cannot expire in a replay: recorded bodies are served instantly, so "
                    "the batch clock never arrives at its bound and every one of these sources "
                    "reports a different status than the record shows. A count and status flip, not "
                    "a lost job -- all of them kept 0 rows. The bound has no knob: it is arithmetic "
                    "over the per-row timeout and the concurrency the batch ran with, so the only "
                    "honest place for it is beside the verdict"
                ),
            }
        )
    return notes


def referee_verdict(report: Mapping[str, Any], *, accepted: bool = False) -> dict[str, Any]:
    """Whether this record may be replayed against as a delta referee, and what stops it.

    ``accepted`` is the operator's explicit override: it is carried in the verdict so the manifest
    says "a known, deliberate exception" rather than "clean". A certification nothing can
    distinguish from an honest record certifies nothing, so the flag travels with the epoch instead
    of being left in the shell history of whoever recorded it.
    """

    blockers = _referee_blockers(report)
    return {
        "eligible": not blockers,
        # Only meaningful as an override: an eligible record was never excepted from anything.
        "accepted": bool(accepted) and bool(blockers),
        "blocking": blockers,
        "knobs": sorted({str(item["knob"]) for item in blockers}),
        "notes": _referee_notes(report),
    }


def referee_lines(report: Mapping[str, Any], *, accepted: bool = False) -> list[str]:
    """The referee verdict in the driver's own voice: the finding, the knob, and the two ways forward.

    Silent on a record that may be replayed against -- the same rule ``warning_lines`` follows, so an
    ordinary record prints nothing. On one that may not, each blocker is one line naming its knob,
    followed by the decision: refuse the certification (the default) or accept it explicitly.
    """

    verdict = referee_verdict(report, accepted=accepted)
    blockers = verdict["blocking"]
    if not blockers:
        return []
    lines = [
        "[corpus] not a delta referee: {reason} - {detail}; raise {knob}".format(
            reason=item["reason"], detail=item["detail"], knob=item["knob"]
        )
        for item in blockers
    ]
    if verdict["accepted"]:
        lines.append(
            "[corpus] accepted anyway by --accept-truncated-referee: EPOCH.json records this epoch "
            "as accepted-but-truncated, so a study pinned to it can see the bound it ran under"
        )
    else:
        lines.append(
            "[corpus] refusing the certification: the epoch is written and remains a coverage "
            "record; re-record with the knob raised, or pass --accept-truncated-referee to record "
            "it as an accepted-but-truncated referee"
        )
    return lines


def warning_lines(report: Mapping[str, Any]) -> list[str]:
    """One line per flagged host or source, in the driver's own voice, plus closing pointers."""

    lines: list[str] = []
    for finding in report.get("hosts") or []:
        if not isinstance(finding, Mapping):
            continue
        lines.append(
            "[corpus] WARNING truncation: {host} - {requested} of {candidates} candidate links "
            "requested ({unrequested} unrequested)".format(
                host=finding.get("host"),
                requested=finding.get("requested"),
                candidates=finding.get("candidates"),
                unrequested=finding.get("unrequested"),
            )
        )
    if lines:
        lines.append(
            "[corpus] a record with unrequested candidates is not replayable for that phase; "
            "that is what refuses its delta-referee certification (see referee_lines)"
        )
    static_lines: list[str] = []
    clock_stopped = False
    deadline_sources = 0
    for finding in report.get("staticSources") or []:
        if not isinstance(finding, Mapping):
            continue
        source = str(finding.get("source") or "")
        reasons = [str(reason) for reason in finding.get("reasons") or []]
        declared = _as_mapping(finding.get("declared"))
        if "batch_deadline" in reasons:
            deadline_sources += 1
        if "source_clock" in reasons:
            clock_stopped = True
            lost = len(finding.get("unrequestedUrls") or [])
            detail = (
                f"{lost} URL(s) the report names have no corpus request"
                if lost
                else "no unrequested URL was named, so the record's gap is unknown"
            )
            static_lines.append(
                f"[corpus] WARNING truncation: {source} - source clock stopped the run; {detail}"
            )
        if "detail_cap" in reasons:
            static_lines.append(
                "[corpus] WARNING truncation: {source} - detail work dropped: {skipped} skipped by "
                "an adaptive stop, {capped} capped from external fanout (recorded {visited} of "
                "{candidates} declared candidate(s))".format(
                    source=source,
                    skipped=declared.get("detailsSkippedByAdaptiveStop"),
                    capped=declared.get("externalDetailLinksCapped"),
                    visited=declared.get("detailsVisited"),
                    candidates=declared.get("candidatesFound"),
                )
            )
    if static_lines:
        lines.extend(static_lines)
        if clock_stopped:
            lines.append(
                "[corpus] a static source the clock stopped re-runs without the clock in replay "
                "and will request beyond this record; that is what refuses its delta-referee "
                "certification"
            )
        else:
            lines.append(
                "[corpus] a static cap dropped detail work; both cap paths are count-driven, so a "
                "replay drops the same work and the delta-referee certification stands "
                "(see referee_verdict's notes)"
            )
    if deadline_sources:
        lines.append(
            f"[corpus] {deadline_sources} static source(s) expired the whole-batch deadline, which "
            "a replay cannot reach (recorded bodies are served instantly): the record reads "
            "`error` where the replay reads `ok`, with 0 rows either way. Declared rather than "
            "refused, and named in the delta-referee notes"
        )
    return lines


def write_report(report: Mapping[str, Any], path: Path) -> Path:
    # The parent is created here rather than left to the corpus store: an epoch whose every
    # static source was refused before its first fetch has no entries yet, so the directory the
    # store would create does not exist at the moment this report is written beside it.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_report(directory: Path) -> dict[str, Any]:
    """Read a written report, for tests and for a replay that wants to state the bound it ran under."""

    return json.loads((Path(directory) / REPORT_NAME).read_text(encoding="utf-8"))


def candidate_sample_urls(report: Mapping[str, Any]) -> Sequence[str]:
    """Every named sample URL across both families, in report order."""

    urls: list[str] = []
    for finding in report.get("hosts") or []:
        if isinstance(finding, Mapping):
            urls.extend(str(item) for item in finding.get("unrequestedSamples") or [])
    for finding in report.get("staticSources") or []:
        if isinstance(finding, Mapping):
            urls.extend(str(item) for item in finding.get("unrequestedSamples") or [])
    return urls
