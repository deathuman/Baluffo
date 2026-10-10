"""Record-time check for the one gap a corpus cannot show about itself: one key, many responses.

The truncation audit reads requests a run decided *not* to make. This one reads the opposite: a
request key the run made more than once and was served a different response each time. The store
keeps one entry per key (method + canonical URL + the response-affecting headers), so a second
fetch overwrites the first entry and leaves the first response as a body no entry points at -- an
orphan. The orphans are the fingerprint, and they are measurable: 780-790 of the ~6.5k bodies in
each 2026-10-10 full-source epoch.

The measured case is LinkedIn's company pages. The static lane fetches one shared browse page once
per consuming source, and LinkedIn renders per-fetch ``refId``/``trackingId`` tokens into the job
card links. One request key held four different responses, and the record's rows carried each
consumer's own render, so the tokens became ``dedupKey``s and merge keys. A replay can hold only
one of the responses; the rows built from the others cannot be reproduced. The choice is not "keep
the right one": keep-first is as wrong as keep-last when four callers saw four renders.

Declared, not refused. Every full-source epoch so far carries this class, and refusing the
certification for it would refuse every reference epoch forever -- the dead end the 2026-10-09
redirect pool produced from the other side. The count travels beside the verdict as a declared
note instead, attributed per host, so a study gate can bind a divergence whose whole residue is
this class. The record cannot remove the class and has no knob that could: the differing responses
are what the board served, not what the run chose.

What the record can and cannot see. A replacement the store's writer saw lands in
``replacements.jsonl`` with the URL and both body hashes, which is what makes the per-host split
possible -- by the time anything reads the corpus, the overwritten entry is gone and its host is
unrecoverable. An epoch recorded before this audit existed has no log: its orphan count is still
measured from disk, and the per-host split is reported as absent rather than guessed. Two fetches
of one key served *identical* bytes leave no orphan and no line, and cannot change a row, so the
count is a lower bound on "fetched more than once" and an exact count of "stored response
replaced".
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from tools.measurements.corpus.store import CorpusStore
from tools.measurements.corpus.truncation import host_of

REPORT_NAME = "conflict-report.json"
# The reason this class travels under in the delta-referee notes. One word for what the corpus
# holds: a key whose stored answer is one of several the live run was given.
NOTE_REASON = "key_conflict"

# How many URLs to name per host: the count is the finding, the samples are what a reader checks.
_SAMPLE_LIMIT = 5
# How many hosts the *note* names. The report keeps every host; the note is the compact form, and
# a verdict that names three hundred hosts is a verdict nobody reads.
_NOTE_HOST_LIMIT = 5
# The replacement kinds that leave the response they overwrote unreferenced. ``response-gained``
# and ``failure-changed`` replace a record, not a body, so they cannot orphan anything.
_ORPHANING_KINDS = ("body-changed", "response-lost")

INTERPRETATION = (
    "One family, read from the corpus itself rather than from the run's report: a request key "
    "whose stored response was replaced. The store keeps one entry per (method, url, "
    "response-affecting headers), so a second fetch overwrites the first entry and leaves the "
    "first response as a body no entry references -- the orphan count is that fingerprint, and "
    "the store's own replacement log supplies the per-host split. The replacement kinds: "
    "body-changed (a stored body replaced by a different one), response-lost (a stored body "
    "replaced by a failure), response-gained (a failure replaced by a body), failure-changed (a "
    "failure whose error or status changed). Only the first two can orphan a body, and only when "
    "no other entry references it. Identical bytes for one key leave nothing, so the count is a "
    "lower bound on 'fetched more than once' and an exact count of 'stored response replaced'. "
    "The class cannot be removed by ordering or store policy -- keep-first is as wrong as "
    "keep-last when several callers saw several renders -- so it is declared beside the verdict, "
    "attributed per host, rather than refusing it."
)


def audit(*, corpus_dir: Path, scope: str = "") -> dict[str, Any]:
    """The record-time conflict report for one corpus directory.

    Three inputs, all read from the epoch: the entries and bodies on disk (the orphan join, which
    works over any finished epoch) and the store's replacement log (the per-host attribution,
    which only an epoch recorded with this audit can have). ``orphanedBodies`` is the disk count
    and ``attributedOrphans`` the part the log explains; ``unattributedOrphans`` is the difference
    and is zero for an epoch the store wrote with its log intact.
    """

    store = CorpusStore(corpus_dir)
    evidence = store.response_conflicts()
    records = store.replacement_records()
    orphans = {str(item) for item in evidence.get("orphanedBodies") or []}

    by_host: dict[str, dict[str, Any]] = {}
    orphan_owner: dict[str, str] = {}
    kinds: dict[str, int] = {}
    replaced_keys: set[str] = set()
    orphaning = 0
    for record in records:
        if not isinstance(record, Mapping):
            continue
        key = str(record.get("key") or "")
        url = str(record.get("url") or "")
        kind = str(record.get("kind") or "")
        host = host_of(url) or "(no host)"
        kinds[kind] = kinds.get(kind, 0) + 1
        if key:
            replaced_keys.add(key)
        finding = by_host.setdefault(
            host,
            {
                "host": host,
                "replacedKeys": set(),
                "orphaningReplacements": 0,
                "orphanedBodies": 0,
                "kinds": {},
                "samples": [],
            },
        )
        if key:
            finding["replacedKeys"].add(key)
        finding["kinds"][kind] = int(finding["kinds"].get(kind, 0)) + 1
        if kind in _ORPHANING_KINDS:
            previous_sha = str(record.get("previousBodySha256") or "")
            if previous_sha and previous_sha in orphans:
                orphaning += 1
                finding["orphaningReplacements"] = int(finding["orphaningReplacements"]) + 1
                # One body, one owner: a body served to two hosts is attributed to the first
                # replacement seen for it, so the per-host column sums to the attributed total.
                if previous_sha not in orphan_owner:
                    orphan_owner[previous_sha] = host
                    finding["orphanedBodies"] = int(finding["orphanedBodies"]) + 1
        if url and url not in finding["samples"] and len(finding["samples"]) < _SAMPLE_LIMIT:
            finding["samples"].append(url)

    hosts = [
        {
            "host": host,
            "replacedKeys": len(finding["replacedKeys"]),
            "orphaningReplacements": int(finding["orphaningReplacements"]),
            "orphanedBodies": int(finding["orphanedBodies"]),
            "kinds": dict(sorted(finding["kinds"].items())),
            "samples": list(finding["samples"]),
        }
        for host, finding in by_host.items()
    ]
    hosts.sort(key=lambda item: (-item["orphanedBodies"], -item["replacedKeys"], item["host"]))

    replaced = len(replaced_keys)
    total_orphans = len(orphans)
    attributed = len(orphan_owner)
    return {
        "schemaVersion": 1,
        "scope": str(scope),
        "corpusDir": str(corpus_dir),
        "entries": int(evidence.get("entries") or 0),
        "bodies": int(evidence.get("bodies") or 0),
        "replacedKeys": replaced,
        "replacements": len(records),
        "replacementsByKind": dict(sorted(kinds.items())),
        "orphaningReplacements": orphaning,
        "orphanedBodies": total_orphans,
        "attributedOrphans": attributed,
        "unattributedOrphans": total_orphans - attributed,
        "replacementLog": "present" if store.replacements_path.is_file() else "absent",
        "hostCount": len(hosts),
        "hosts": hosts,
        "status": "flagged" if (replaced or total_orphans) else "clean",
        "interpretation": INTERPRETATION,
    }


def summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """The compact form that belongs in a manifest, without the per-host evidence."""

    return {
        "status": str(report.get("status") or ""),
        "replacedKeys": int(report.get("replacedKeys") or 0),
        "orphanedBodies": int(report.get("orphanedBodies") or 0),
        "unattributedOrphans": int(report.get("unattributedOrphans") or 0),
        "hostCount": int(report.get("hostCount") or 0),
        "replacementLog": str(report.get("replacementLog") or ""),
        "report": REPORT_NAME,
    }


def referee_note(report: Mapping[str, Any]) -> dict[str, Any] | None:
    """The declaration this audit contributes to a record's certification, or ``None`` when clean.

    A note, never a blocker, and that is the load-bearing decision: the class is a property of what
    the boards served, so a record carrying it is still the best account of the run -- what it
    cannot do is leave a study to discover the divergence without the record's own explanation of
    where it comes from. Only the top hosts are named; the report keeps them all.
    """

    replaced = int(report.get("replacedKeys") or 0)
    orphans = int(report.get("orphanedBodies") or 0)
    if not replaced and not orphans:
        return None
    hosts = [
        str(item.get("host") or "")
        for item in report.get("hosts") or []
        if isinstance(item, Mapping)
    ]
    host_count = int(report.get("hostCount") or len(hosts))
    if replaced:
        count_text = (
            f"{replaced} request key(s) were fetched again with a different stored response, "
            f"leaving {orphans} body(ies) no entry references"
        )
        scope_text = (
            f", across {host_count} host(s)"
            if host_count
            else ", with no per-host split (recorded before the replacement log existed)"
        )
    else:
        count_text = (
            f"{orphans} body(ies) are referenced by no entry, and this epoch predates the "
            "replacement log, so the keys behind them are unknown"
        )
        scope_text = ""
    note: dict[str, Any] = {
        "reason": NOTE_REASON,
        "hosts": hosts[:_NOTE_HOST_LIMIT],
        "hostCount": host_count,
        "replacedKeys": replaced,
        "orphanedBodies": orphans,
        "detail": (
            f"{count_text}{scope_text}. A replay can hold only one response per key, so the rows "
            "the board rendered for the others cannot be reproduced -- keep-first is as wrong as "
            "keep-last when several callers saw several renders. No knob removes this: the "
            "differing responses are what the board served, not what the run chose. Declared "
            "rather than refused, per host, so a study can bind a divergence whose whole residue "
            "is this class"
        ),
    }
    if report.get("unattributedOrphans"):
        note["unattributedOrphans"] = int(report.get("unattributedOrphans") or 0)
    if str(report.get("replacementLog") or "") != "present":
        note["replacementLog"] = str(report.get("replacementLog") or "")
    return note


def carry(verdict: Mapping[str, Any], report: Mapping[str, Any]) -> dict[str, Any]:
    """The record's certification with this audit's declaration appended, when there is one.

    The verdict's structure belongs to ``truncation.referee_verdict``; this audit owns one class
    and one note, and never a blocker. Keeping the merge here rather than inside the verdict keeps
    one owner per class: a caller that reads ``blocking`` sees only what refuses replay, and this
    class never does.
    """

    carried = dict(verdict)
    note = referee_note(report)
    if note is not None:
        carried["notes"] = [*(carried.get("notes") or []), note]
    return carried


def warning_lines(report: Mapping[str, Any]) -> list[str]:
    """One line per host with a replaced response, plus the closing pointer, or nothing when clean."""

    lines: list[str] = []
    for finding in report.get("hosts") or []:
        if not isinstance(finding, Mapping):
            continue
        lines.append(
            "[corpus] WARNING conflict: {host} - {keys} request key(s) served more than one "
            "stored response; {orphans} body(ies) no entry references".format(
                host=finding.get("host"),
                keys=finding.get("replacedKeys"),
                orphans=finding.get("orphanedBodies"),
            )
        )
    if lines:
        lines.append(
            "[corpus] a key the record answers with one response and the live run answered with "
            "another cannot be reproduced by replay; the count is declared in the delta-referee "
            "notes, per host, rather than refusing the certification"
        )
    orphans = int(report.get("orphanedBodies") or 0)
    if orphans and str(report.get("replacementLog") or "") != "present":
        lines.append(
            f"[corpus] {orphans} orphaned body(ies) exist but this epoch has no replacement log, "
            "so the per-host split is unavailable -- the count is declared, the hosts are not"
        )
    return lines


def write_report(report: Mapping[str, Any], path: Path) -> Path:
    # The parent is created here rather than left to the corpus store: the audit runs over a
    # corpus whose entries may all be failures, and the directory the store would create is not
    # guaranteed to exist at the moment this report is written beside it.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_report(directory: Path) -> dict[str, Any]:
    """Read a written report, for tests and for a reader who was not at the record."""

    return json.loads((Path(directory) / REPORT_NAME).read_text(encoding="utf-8"))


def host_sample_urls(report: Mapping[str, Any]) -> Sequence[str]:
    """Every named sample URL across hosts, in report order."""

    urls: list[str] = []
    for finding in report.get("hosts") or []:
        if isinstance(finding, Mapping):
            urls.extend(str(item) for item in finding.get("samples") or [])
    return urls


__all__ = [
    "REPORT_NAME",
    "NOTE_REASON",
    "INTERPRETATION",
    "audit",
    "summary",
    "referee_note",
    "carry",
    "warning_lines",
    "write_report",
    "load_report",
    "host_sample_urls",
]
