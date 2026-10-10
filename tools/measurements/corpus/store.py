"""Content-addressed HTTP corpus for record and replay (S3 of the fetch perf plan).

A corpus is a *capture*, not a cache: deleting it cannot be undone by regenerating it,
because a fresh live run produces a different capture from changed boards. Nothing here
prunes; retention is a deliberate operator decision (one reference corpus per epoch).

What a corpus holds
-------------------

* ``entries/<aa>/<key>.json`` -- one record per logical request, keyed by
  :func:`request_key` (method + canonical URL + the request headers that change a
  response). The record carries the outcome, status, response headers, body hash and
  its failure detail when there is one.
* ``bodies/<aa>/<sha256>.gz`` -- gzipped response bodies, addressed by content, so the
  same body served to two URLs is stored once.
* ``coverage-report.json`` / ``size-report.json`` -- written by :meth:`CorpusStore.write_reports`.

Failures are first-class records, not absences. 257 of 1,450 registered sources error,
and a corpus of successes alone would replay a fictional green run: every measurement
taken on it would be meaningless. A replayed failure is raised as the recorded
exception, never returned as an empty body -- see ``capture.raise_for_entry``.

Fidelity, stated rather than implied
-----------------------------------

At the ``fetch_text`` seam the pipeline hands back **decoded text**, not wire bytes, so
the body stored here is that text re-encoded as UTF-8 and the byte figure is a local
size, not a wire size. Sizes are therefore labelled ``bodyBytes``/``decodedChars`` and
never described as response bytes on the wire.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import threading
import time
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

# The request headers that change a response, per the plan. User-Agent is deliberately
# excluded: the recorded response stands for the pinned user agent, and both arms of a
# replay A/B use the same profile, so folding it into the key would only split entries
# that replayed identically.
KEY_HEADER_SUBSET: tuple[str, ...] = (
    "accept",
    "cookie",
    "authorization",
    "if-none-match",
    "if-modified-since",
)

DEFAULT_METHOD = "GET"
_ENTRY_SUFFIX = ".json"
_BODY_SUFFIX = ".gz"
# The replacement log: one JSON line per re-fetch that overwrote an existing key's stored
# response. It exists because the store is the only witness -- after the write, the previous
# entry is gone and the response it pointed at is an orphaned body, so nothing reading the
# finished corpus can recover which host the lost response came from.
REPLACEMENTS_FILE = "replacements.jsonl"
# Coverage reports name what went wrong rather than only counting it, but a run that
# misses thousands of entries must not write a thousand names into one file.
_NAMED_LIMIT = 200


class CorpusMiss(LookupError):
    """A request that the corpus was asked for and does not hold.

    A miss is loud by design. Returning an empty body instead would make "the corpus
    could not answer" indistinguishable from "the board served nothing", which is the
    failure this whole slice exists to prevent.
    """

    def __init__(self, url: str, key: str, *, surface: str = "") -> None:
        self.url = str(url)
        self.key = str(key)
        self.surface = str(surface)
        detail = f"corpus miss for {self.url!r} (key {self.key}"
        detail += f", surface {self.surface!r})" if self.surface else ")"
        super().__init__(detail)


def canonical_url(url: Any) -> str:
    """Canonical form of a URL for keying.

    Deliberately local rather than reusing ``src.jobs.text_utils.normalize_url``: that
    helper encodes job-listing normalisation policy and may change for reasons that have
    nothing to do with this corpus, which would silently invalidate recorded keys.

    Rules: lowercase scheme and host, drop a default port, drop the fragment, keep the
    path and query exactly as sent (query order is semantic, and a board may care).
    """

    text = str(url or "").strip()
    if not text:
        return ""
    fragment_split = text.split("#", 1)
    without_fragment = fragment_split[0]
    scheme, separator, remainder = without_fragment.partition("://")
    if not separator:
        return without_fragment
    scheme = scheme.lower()
    host_part, slash, path_part = remainder.partition("/")
    # Userinfo is optional and only present when there is an "@"; partitioning without
    # checking keeps the third element empty and silently drops the host.
    userinfo, at, host_port = "", "", host_part
    if "@" in host_part:
        userinfo, _, host_port = host_part.partition("@")
        at = "@"
    # A bracketed IPv6 literal contains colons of its own, so its port split is separate.
    host, colon, port = host_port.partition(":")
    host = host.lower()
    if host_port.startswith("[") and "]" in host_port:
        host = host_port.split("]", 1)[0] + "]"
        # partition yields (before, separator, after): the separator is the colon.
        _, colon, port = host_port.split("]", 1)[1].partition(":")
    if (
        colon
        and port
        and not ((scheme == "http" and port == "80") or (scheme == "https" and port == "443"))
    ):
        host = f"{host}:{port}"
    authority = f"{userinfo}@{host}" if at else host
    return f"{scheme}://{authority}{slash}{path_part}"


def normalize_request_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """The keyed subset of request headers, lowercased and sorted."""

    keyed: dict[str, str] = {}
    for raw_name, raw_value in (headers or {}).items():
        name = str(raw_name or "").strip().lower()
        if not name or name not in KEY_HEADER_SUBSET:
            continue
        value = str(raw_value if raw_value is not None else "").strip()
        if value:
            keyed[name] = value
    return dict(sorted(keyed.items()))


def request_key(
    *,
    method: str = DEFAULT_METHOD,
    url: Any,
    request_headers: Mapping[str, Any] | None = None,
) -> str:
    """Stable key for one logical request: method + canonical URL + keyed headers."""

    method_part = str(method or DEFAULT_METHOD).strip().upper() or DEFAULT_METHOD
    header_part = json.dumps(
        normalize_request_headers(request_headers), ensure_ascii=False, sort_keys=True
    )
    material = f"{method_part}\n{canonical_url(url)}\n{header_part}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def normalize_response_headers(headers: Any) -> dict[str, list[str]]:
    """Response headers as lowercased name -> list of values (multi-valued safe).

    Accepts a mapping or a sequence of ``(name, value)`` pairs, because HTTP layers hand
    back pairs (repeated ``Set-Cookie`` included) while callers that build a record by hand
    pass a mapping.
    """

    if headers is None:
        return {}
    items = headers.items() if isinstance(headers, Mapping) else headers
    normalized: dict[str, list[str]] = {}
    for raw_name, raw_value in items:
        name = str(raw_name or "").strip().lower()
        if not name:
            continue
        values = raw_value if isinstance(raw_value, (list, tuple)) else [raw_value]
        bucket = normalized.setdefault(name, [])
        for value in values:
            bucket.append(str(value if value is not None else ""))
    return dict(sorted(normalized.items()))


def error_record(exc: BaseException | Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Failure detail for an exception, or an already-built record."""

    if exc is None:
        return None
    if isinstance(exc, Mapping):
        return {
            "class": str(exc.get("class") or "RuntimeError"),
            "message": str(exc.get("message") or ""),
            "code": int(exc.get("code") or 0),
        }
    code = 0
    raw_code = getattr(exc, "code", None)
    if isinstance(raw_code, int):
        code = int(raw_code)
    else:
        message = str(exc)
        for token in ("HTTP ", "HTTP Error "):
            if token in message:
                tail = message.split(token, 1)[1].strip().split(" ", 1)[0]
                if tail.isdigit():
                    code = int(tail)
                    break
    return {"class": type(exc).__name__, "message": str(exc), "code": code}


def _shard(key: str) -> str:
    return (str(key) or "00")[:2]


def _replacement_kind(previous: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    """Which stored response a re-fetch replaced, or "" when nothing observable was.

    "Observable" is exact: a key fetched twice with identical bytes has replaced nothing a
    replay could tell apart, and a failure re-recorded with the same error is the same record
    twice. What is not that is the class a replay cannot reproduce from one stored response --
    two different bodies, a body that became a failure, a failure that became a body -- plus the
    failure whose own error or status changed.
    """

    previous_outcome = str(previous.get("outcome") or "")
    outcome = str(entry.get("outcome") or "")
    previous_body = str(previous.get("bodySha256") or "")
    body = str(entry.get("bodySha256") or "")
    if previous_outcome == "ok" and outcome == "ok":
        return "body-changed" if previous_body != body else ""
    if previous_outcome == "ok":
        return "response-lost"
    if outcome == "ok":
        return "response-gained"
    if previous.get("error") != entry.get("error") or int(previous.get("statusCode") or 0) != int(
        entry.get("statusCode") or 0
    ):
        return "failure-changed"
    return ""


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class RequestLedger:
    """Counts what was offered, recorded, hit and missed, and names the exceptions.

    In-memory on purpose: one process records or replays one run, and the report is
    written once at the end. It is still lock-guarded, because one process means many
    threads: a recorded run fetches sources on the pipeline's worker pool, and an
    unguarded ``self.offered += 1`` from two workers loses an increment. That would make
    the coverage report's ``offered == recorded`` claim false rather than merely noisy,
    and coverage is the whole point of the ledger.
    """

    def __init__(self, *, named_limit: int = _NAMED_LIMIT) -> None:
        self._named_limit = max(0, int(named_limit))
        self._lock = threading.Lock()
        self.offered = 0
        self.recorded = 0
        self.hits = 0
        self.misses = 0
        self.by_surface: dict[str, int] = {}
        # Offered and resolved counts are kept per URL so the report can *name* what was
        # never attributed, rather than only reporting a shortfall total. A run offers a
        # few thousand requests, so the dict is cheap; naming is what makes the
        # difference between "we replayed the fetch lane" and "we replayed the part of
        # it we happened to capture".
        self._offered: dict[str, int] = {}
        self._resolved: dict[str, int] = {}
        self._misses: list[dict[str, Any]] = []

    def note_offer(self, url: str, *, surface: str = "") -> None:
        label = str(url)
        surface_label = str(surface or "unknown")
        with self._lock:
            self.offered += 1
            self._offered[label] = int(self._offered.get(label, 0)) + 1
            self.by_surface[surface_label] = int(self.by_surface.get(surface_label, 0)) + 1

    def _note_resolved(self, url: str) -> None:
        """Caller holds the lock: recorded/hit/miss all bump one URL's resolved count."""

        label = str(url)
        self._resolved[label] = int(self._resolved.get(label, 0)) + 1

    def note_recorded(self, url: str) -> None:
        with self._lock:
            self.recorded += 1
            self._note_resolved(url)

    def note_hit(self, url: str) -> None:
        with self._lock:
            self.hits += 1
            self._note_resolved(url)

    def note_miss(self, url: str, key: str, *, reason: str = "", surface: str = "") -> None:
        with self._lock:
            self.misses += 1
            self._note_resolved(url)
            if len(self._misses) < self._named_limit:
                self._misses.append(
                    {
                        "url": str(url),
                        "key": str(key),
                        "reason": str(reason),
                        "surface": str(surface),
                    }
                )

    def _unattributed(self) -> tuple[int, list[str]]:
        """Caller holds the lock: reads the per-URL dicts while writers are stopped."""

        shortfall = 0
        named: list[str] = []
        for url, offered in self._offered.items():
            missing = offered - int(self._resolved.get(url, 0))
            if missing <= 0:
                continue
            shortfall += missing
            if len(named) < self._named_limit:
                named.append(url)
        return shortfall, named

    def as_report(self) -> dict[str, Any]:
        with self._lock:
            shortfall, named = self._unattributed()
            return {
                "requestsOffered": self.offered,
                "requestsRecorded": self.recorded,
                "requestsHit": self.hits,
                "requestsMissed": self.misses,
                "unattributedCount": shortfall,
                "unattributedUrls": named,
                "misses": list(self._misses),
                "offersBySurface": dict(sorted(self.by_surface.items())),
                "namedLimit": self._named_limit,
            }

    def attribution_is_complete(self) -> bool:
        """True when every offer became exactly one record, hit or named miss."""

        with self._lock:
            return self._unattributed()[0] == 0


class CorpusStore:
    """Reads and writes one corpus directory.

    Writes are lock-guarded. ``record`` is called from the pipeline's worker threads, and
    two unguarded writers would race in two ways that both corrupt the corpus rather
    than merely print a wrong number: the same body could be gzip-written twice
    concurrently (interleaved output, unreadable on replay) and the dedupe check could
    lose the race. Reading has no lock because replay is single-run and reads after all
    writes are done.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._lock = threading.Lock()

    # ── paths ───────────────────────────────────────────────────────────────

    @property
    def entries_dir(self) -> Path:
        return self.root / "entries"

    @property
    def bodies_dir(self) -> Path:
        return self.root / "bodies"

    def entry_path(self, key: str) -> Path:
        return self.entries_dir / _shard(key) / f"{key}{_ENTRY_SUFFIX}"

    def body_path(self, body_sha: str) -> Path:
        return self.bodies_dir / _shard(body_sha) / f"{body_sha}{_BODY_SUFFIX}"

    @property
    def coverage_report_path(self) -> Path:
        return self.root / "coverage-report.json"

    @property
    def size_report_path(self) -> Path:
        return self.root / "size-report.json"

    @property
    def replacements_path(self) -> Path:
        return self.root / REPLACEMENTS_FILE

    # ── writing ─────────────────────────────────────────────────────────────

    def record(
        self,
        *,
        url: str,
        method: str = DEFAULT_METHOD,
        request_headers: Mapping[str, Any] | None = None,
        outcome: str = "ok",
        text: str | None = None,
        charset: str = "utf-8",
        status_code: int = 0,
        response_headers: Mapping[str, Any] | None = None,
        redirect_chain: Iterable[str] = (),
        error: BaseException | Mapping[str, Any] | None = None,
        surface: str = "",
        ledger: RequestLedger | None = None,
    ) -> dict[str, Any]:
        """Write one request's outcome and return the stored entry.

        ``outcome="failure"`` records the exception instead of a body. Both outcomes are
        written through this one method so a failure cannot be skipped by accident.
        """

        key = request_key(method=method, url=url, request_headers=request_headers)
        normalized_outcome = "failure" if str(outcome) == "failure" else "ok"
        # Validated before anything is written, so a contradictory call cannot leave an
        # orphan body behind on its way to raising.
        if normalized_outcome == "ok" and error is not None:
            raise ValueError("an ok entry cannot carry an error record")
        if normalized_outcome == "failure" and error is None:
            raise ValueError("a failure entry must carry an error record")
        body_sha = ""
        body_bytes = 0
        decoded_chars = 0
        raw = b""
        if normalized_outcome == "ok":
            raw = ("" if text is None else str(text)).encode("utf-8", "replace")
            body_sha = hashlib.sha256(raw).hexdigest()
            body_bytes = len(raw)
            decoded_chars = len("" if text is None else str(text))
        entry: dict[str, Any] = {
            "key": key,
            "method": str(method or DEFAULT_METHOD).strip().upper() or DEFAULT_METHOD,
            "url": str(url),
            "canonicalUrl": canonical_url(url),
            "requestHeaders": normalize_request_headers(request_headers),
            "outcome": normalized_outcome,
            "statusCode": int(status_code or 0),
            "responseHeaders": normalize_response_headers(response_headers),
            "bodySha256": body_sha,
            "bodyBytes": body_bytes,
            "decodedChars": decoded_chars,
            "charset": str(charset or "utf-8"),
            "redirectChain": [str(item) for item in redirect_chain],
            "error": error_record(error),
            "surface": str(surface or ""),
            "recordedAt": _now_iso(),
        }
        path = self.entry_path(key)
        with self._lock:
            previous = self._read_entry(path)
            if normalized_outcome == "ok":
                self._write_body(body_sha, raw)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(entry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            # After this write the previous response is an orphaned body that no entry points at,
            # and its host is unrecoverable from the corpus. The writer is the last place that can
            # still name it, so a replacement is written down here or nowhere.
            if previous is not None:
                self._note_replacement(previous, entry)
        if ledger is not None:
            ledger.note_recorded(str(url))
        return entry

    def _read_entry(self, path: Path) -> Mapping[str, Any] | None:
        """The entry already stored at this path, or ``None``. A malformed file reads as none.

        Only :meth:`record` writes entries, so a file that does not parse is a corrupted corpus;
        inventing a previous state from it would write a conflict line about a response nobody
        can identify. The corpus's own readers surface the corruption where it matters -- a
        lookup of a malformed entry raises :class:`CorpusMiss`.
        """

        if not path.is_file():
            return None
        try:
            stored = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return stored if isinstance(stored, Mapping) else None

    def _note_replacement(self, previous: Mapping[str, Any], entry: Mapping[str, Any]) -> None:
        """Write down a re-fetch that replaced this key's stored response. Caller holds the lock.

        One line per replacement, in the order they happened, so the log is the run's own account
        of every request key it answered twice with different results. ``previousBodySha256`` is
        the join an audit needs: the body file it names is the orphan the overwrite left behind.
        """

        kind = _replacement_kind(previous, entry)
        if not kind:
            return
        line = {
            "key": str(entry.get("key") or ""),
            "url": str(entry.get("url") or ""),
            "method": str(entry.get("method") or DEFAULT_METHOD),
            "surface": str(entry.get("surface") or ""),
            "kind": kind,
            "previousOutcome": str(previous.get("outcome") or ""),
            "previousBodySha256": str(previous.get("bodySha256") or ""),
            "previousStatusCode": int(previous.get("statusCode") or 0),
            "previousError": previous.get("error"),
            "previousRecordedAt": str(previous.get("recordedAt") or ""),
            "outcome": str(entry.get("outcome") or ""),
            "bodySha256": str(entry.get("bodySha256") or ""),
            "statusCode": int(entry.get("statusCode") or 0),
            "recordedAt": str(entry.get("recordedAt") or ""),
        }
        with self.replacements_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(line, sort_keys=True) + "\n")

    def _write_body(self, body_sha: str, raw: bytes) -> None:
        path = self.body_path(body_sha)
        if path.is_file():
            return  # content-addressed: identical bodies are stored once
        path.parent.mkdir(parents=True, exist_ok=True)
        # mtime=0 keeps the gzip header deterministic, so two corpora recorded from the
        # same bytes are byte-identical on disk and can be compared by hash.
        with gzip.GzipFile(filename="", mode="wb", fileobj=path.open("wb"), mtime=0) as handle:
            handle.write(raw)

    # ── reading ─────────────────────────────────────────────────────────────

    def iter_requested_urls(self) -> Iterator[str]:
        """Every URL the corpus holds a request for, both as sent and as redirected to.

        Streaming and read-only: used by record-time audits that have to join the record
        against what the run *meant* to fetch. A redirected request's entry carries the URL it
        was sent to and the one it ended at; a candidate compared against only one of them
        reads as unrequested when it was fetched, so both are yielded.
        """

        for path in self.entries_dir.rglob(f"*{_ENTRY_SUFFIX}"):
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(entry, Mapping):
                continue
            for field in ("url", "canonicalUrl"):
                value = str(entry.get(field) or "").strip()
                if value:
                    yield value

    def replacement_records(self) -> list[dict[str, Any]]:
        """The replacement log's lines, oldest first. An absent log reads as no lines.

        Tolerant on read for the same reason :meth:`iter_requested_urls` is: the audit runs after
        the run, over a file a killed process may have left half-written, and one truncated line
        must not make the whole corpus unauditable.
        """

        path = self.replacements_path
        if not path.is_file():
            return []
        records: list[dict[str, Any]] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict):
                records.append(record)
        return records

    def response_conflicts(self) -> dict[str, Any]:
        """Bodies no entry references, and the counts around them.

        Orphaned bodies are the store's own evidence that a key was served more than one
        response: the entry a second fetch overwrote is gone, and the response it pointed at is
        left with nothing referencing it. Counting them needs no log -- it is a join between the
        entries and the bodies on disk, so it works over any finished epoch. Naming *which host*
        a lost response belonged to does need the log, because the overwritten entry is gone by
        the time anything reads the corpus.
        """

        referenced: set[str] = set()
        entries = 0
        if self.entries_dir.is_dir():
            for path in self.entries_dir.rglob(f"*{_ENTRY_SUFFIX}"):
                try:
                    entry = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if not isinstance(entry, Mapping):
                    continue
                entries += 1
                body_sha = str(entry.get("bodySha256") or "")
                if body_sha:
                    referenced.add(body_sha)
        bodies: set[str] = set()
        if self.bodies_dir.is_dir():
            suffix_len = len(_BODY_SUFFIX)
            for path in self.bodies_dir.rglob(f"*{_BODY_SUFFIX}"):
                bodies.add(path.name[:-suffix_len])
        return {
            "entries": entries,
            "bodies": len(bodies),
            "referencedBodies": len(referenced & bodies),
            "orphanedBodies": sorted(bodies - referenced),
        }

    def has_entry(self, key: str) -> bool:
        return self.entry_path(key).is_file()

    def lookup(
        self,
        *,
        url: str,
        method: str = DEFAULT_METHOD,
        request_headers: Mapping[str, Any] | None = None,
        surface: str = "",
        ledger: RequestLedger | None = None,
    ) -> dict[str, Any]:
        """The stored entry for a request, or :class:`CorpusMiss`."""

        key = request_key(method=method, url=url, request_headers=request_headers)
        path = self.entry_path(key)
        if not path.is_file():
            if ledger is not None:
                ledger.note_miss(str(url), key, reason="no_entry", surface=surface)
            raise CorpusMiss(str(url), key, surface=surface)
        entry = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(entry, dict):
            if ledger is not None:
                ledger.note_miss(str(url), key, reason="malformed_entry", surface=surface)
            raise CorpusMiss(str(url), key, surface=surface)
        if ledger is not None:
            ledger.note_hit(str(url))
        return entry

    def body_text(self, entry: Mapping[str, Any]) -> str:
        """Decoded body for an ``ok`` entry. Raises for a failure entry or a lost body."""

        if str(entry.get("outcome") or "") != "ok":
            raise ValueError("body_text called for a non-ok entry")
        body_sha = str(entry.get("bodySha256") or "")
        if not body_sha:
            raise ValueError("ok entry without a body hash")
        path = self.body_path(body_sha)
        if not path.is_file():
            raise CorpusMiss(str(entry.get("url") or ""), str(entry.get("key") or ""))
        raw = gzip.decompress(path.read_bytes())
        if hashlib.sha256(raw).hexdigest() != body_sha:
            raise ValueError(f"body hash mismatch for {body_sha}")
        return raw.decode(str(entry.get("charset") or "utf-8"), errors="replace")

    # ── accounting ──────────────────────────────────────────────────────────

    def counts(self) -> dict[str, Any]:
        """Entry and byte counts on disk, with the dedupe saving made explicit."""

        entries = (
            list(self.entries_dir.rglob(f"*{_ENTRY_SUFFIX}")) if self.entries_dir.is_dir() else []
        )
        bodies = list(self.bodies_dir.rglob(f"*{_BODY_SUFFIX}")) if self.bodies_dir.is_dir() else []
        outcomes: dict[str, int] = {}
        naive_uncompressed = 0
        for path in entries:
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(entry, dict):
                continue
            outcome = str(entry.get("outcome") or "unknown")
            outcomes[outcome] = int(outcomes.get(outcome, 0)) + 1
            naive_uncompressed += int(entry.get("bodyBytes") or 0)
        compressed_bytes = sum(path.stat().st_size for path in bodies)
        unique_uncompressed = 0
        for path in bodies:
            try:
                unique_uncompressed += len(gzip.decompress(path.read_bytes()))
            except (OSError, EOFError):
                continue
        return {
            "entries": len(entries),
            "outcomes": dict(sorted(outcomes.items())),
            "uniqueBodies": len(bodies),
            "uniqueBodyBytes": unique_uncompressed,
            "compressedBodyBytes": compressed_bytes,
            "naiveBodyBytes": naive_uncompressed,
            "dedupeSavedBytes": max(0, naive_uncompressed - unique_uncompressed),
            "dedupeRatio": (
                round(1 - (unique_uncompressed / naive_uncompressed), 4)
                if naive_uncompressed
                else 0.0
            ),
            "gzipRatio": (
                round(compressed_bytes / unique_uncompressed, 4) if unique_uncompressed else 0.0
            ),
        }

    def coverage_report(
        self,
        *,
        ledger: RequestLedger | None = None,
        scope: str = "",
        mode: str = "",
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Coverage: what was offered, what the corpus could answer, and what it could not."""

        report: dict[str, Any] = {
            "scope": str(scope),
            "mode": str(mode),
            "generatedAt": _now_iso(),
            "corpusRoot": str(self.root),
            "counts": self.counts(),
        }
        if ledger is not None:
            report.update(ledger.as_report())
        if extra:
            report.update({str(key): value for key, value in extra.items()})
        return report

    def size_report(self, *, scope: str = "") -> dict[str, Any]:
        counts = self.counts()
        return {
            "scope": str(scope),
            "generatedAt": _now_iso(),
            "corpusRoot": str(self.root),
            "entries": counts["entries"],
            "uniqueBodies": counts["uniqueBodies"],
            "compressedBodyBytes": counts["compressedBodyBytes"],
            "uniqueBodyBytes": counts["uniqueBodyBytes"],
            "naiveBodyBytes": counts["naiveBodyBytes"],
            "dedupeSavedBytes": counts["dedupeSavedBytes"],
            "dedupeRatio": counts["dedupeRatio"],
            "gzipRatio": counts["gzipRatio"],
            "totalBytes": sum(
                path.stat().st_size
                for path in (
                    list(self.entries_dir.rglob(f"*{_ENTRY_SUFFIX}"))
                    if self.entries_dir.is_dir()
                    else []
                )
                + (
                    list(self.bodies_dir.rglob(f"*{_BODY_SUFFIX}"))
                    if self.bodies_dir.is_dir()
                    else []
                )
            ),
        }

    def write_reports(
        self,
        *,
        coverage: Mapping[str, Any] | None = None,
        ledger: RequestLedger | None = None,
        scope: str = "",
        mode: str = "",
        extra: Mapping[str, Any] | None = None,
    ) -> tuple[Path, Path]:
        """Write ``coverage-report.json`` and ``size-report.json``; returns both paths."""

        self.root.mkdir(parents=True, exist_ok=True)
        payload = (
            dict(coverage)
            if coverage is not None
            else self.coverage_report(ledger=ledger, scope=scope, mode=mode, extra=extra)
        )
        self.coverage_report_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        size = self.size_report(scope=scope)
        self.size_report_path.write_text(
            json.dumps(size, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return self.coverage_report_path, self.size_report_path
