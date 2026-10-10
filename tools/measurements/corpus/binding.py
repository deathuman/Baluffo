"""Bind a replay divergence to the conflict class the record declared, per host.

``conflicts.py`` measures the one gap a corpus cannot close about itself: a request key the live run
was served more than one response for, which a replay can only half-reproduce because the store
keeps one response per key. That audit *declares* the class beside the record's certification
rather than refusing it, and it declares it per host, because the store's replacement log names the
URL that was replaced.

Declaring is not the same as being able to use it. ``replay_check`` grades a **birth replay** on
whether it reproduced the record, and an epoch carrying this class cannot reproduce it: on the
measured subset (2026-10-10) the record kept 2,973 rows and the replay 2,493 -- 721 record-only
identities, 241 replay-only, and 352 shared rows whose winning source, company and ``sourceJobId``
moved -- and every one of those rows belonged to ``www.linkedin.com``, one of the five hosts that
epoch had declared. Those differing rows are what the board served, not what the run chose, so
refusing the epoch for them refuses the class *every* full-source epoch carries: the dead end the
declaration was written to avoid.

This module closes the loop. It reads the residue -- which jobs each side kept, and which shared
jobs moved a field a consumer reads -- attributes every differing row to the host of the source
that carried it, and asks the record's own declaration whether it explains **all** of it. The
answer is ``bound``, or it is a refusal that names why:

* the record declares no conflict host at all (a clean audit), so there is nothing to bind to;
* the record's own rows are not on disk, so the residue cannot be read -- silence is not evidence;
* a differing row names no source host, which puts it outside the declared set by construction;
* a differing row belongs to a host the record never declared.

The bind is derived from artifacts, never inferred from a verdict: it reads the epoch's
``conflict-report.json`` (falling back to the ``key_conflict`` note in the manifest, labelled
because that list is the top five only), the record's rows through ``recordedRunDir``, and the
replay's own rows -- the same readers the parity comparison uses, so the residue cannot come from a
different view of the run than the digests it is explaining.

**The binding key is the source, not the job link.** A source names itself by its listing URL
(``static_source::static:listing_url:https://www.linkedin.com/company/x/jobs/``), and the declared
class is a property of the request a source made, so a row is attributed to the hosts of its
``source`` and of every ``sourceBundle`` entry that carried it -- a row carried by any source
outside the declared set is refused rather than bound to its winning source. A row with no source
host at all falls back to its job link, and a row with neither is unattributable.

Two limits, stated rather than hidden. The binding is **per host**, so a divergence inside a
declared host for an unrelated reason is bound along with the class -- the block names the fields
that moved so a reader can see what was accepted. And it needs the record's rows: an epoch whose
record run directory has been cleaned away cannot be bound, and says so instead of reading as
clean.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from tools.measurements.corpus import conflicts, harness
from tools.measurements.corpus import epoch as epoch_mod
from tools.measurements.corpus.run_fetch import STABLE_ROW_FIELDS, job_identity
from tools.measurements.corpus.truncation import host_of

# The same spelling ``truncation`` uses to name a URL in a message, so one URL is one URL wherever
# it is read out of a string.
_URL_RE = re.compile(r"https?://\S+")
# How many hosts a compact form names before counting the rest. A block that names three hundred
# hosts is a block nobody reads; the receipt keeps the whole list.
_HOST_LIMIT = 5

INTERPRETATION = (
    "A replay cannot reproduce a request key the live run was served more than one response for, "
    "and the record declares those keys per host. This block answers whether the record's own "
    "declaration explains a replay's differing rows: every row present on one side only, and every "
    "shared row whose title, company, location, link, sector, profession, source, sourceJobId, "
    "status or postedAt moved, is attributed to the host of the source that carried it, and the "
    "residue is bound only when every one of those hosts is one the record declared. A single "
    "differing row on a host the record did not declare refuses the binding, as does a differing "
    "row that names no source host, an epoch whose record rows are no longer on disk, and an epoch "
    "that declares nothing at all. The binding is host-level: a divergence inside a declared host "
    "for another reason is bound too, which is why the fields that moved are counted here."
)


def source_hosts(row: Mapping[str, Any]) -> tuple[str, ...]:
    """The hosts of every source that carried ``row``, in first-seen order.

    ``source`` is the winning bundle after dedupe and ``sourceBundle`` lists every source that
    carried the job, so both are read: a row the merge took from one source but that another source
    also published can differ because of either. A row naming no source at all falls back to its
    job link -- the row's own page is still evidence of which board served it -- and a row with
    neither returns nothing, which the caller treats as outside the declared set rather than as
    silence.
    """

    hosts: list[str] = []

    def _collect(value: Any) -> None:
        for url in _URL_RE.findall(str(value or "")):
            host = host_of(url)
            if host and host not in hosts:
                hosts.append(host)

    _collect(row.get("source"))
    bundle = row.get("sourceBundle")
    for item in bundle if isinstance(bundle, Sequence) and not isinstance(bundle, str) else []:
        if isinstance(item, Mapping):
            _collect(item.get("source"))
    if not hosts:
        _collect(row.get("jobLink") or row.get("link") or row.get("url"))
    return tuple(hosts)


def declared(epoch_dir: Path | str) -> dict[str, Any]:
    """The hosts an epoch declared as key conflicts, and which document said so.

    The ``conflict-report.json`` beside the epoch is the complete list and the first choice. An
    epoch whose report is missing falls back to the ``key_conflict`` note in its manifest, which is
    labelled ``complete: false`` because that note names only the top five hosts -- a residue on one
    of the unnamed hosts is then refused, which is the conservative direction. No declaration at all
    (a clean audit, or an epoch recorded before the audit existed) returns an empty list, and the
    caller refuses rather than binding to nothing.
    """

    path = Path(epoch_dir) / conflicts.REPORT_NAME
    if path.is_file():
        try:
            report = harness.read_json_artifact(Path(epoch_dir), conflicts.REPORT_NAME)
        except (OSError, ValueError):
            report = None
        hosts = [
            str(item.get("host"))
            for item in (report or {}).get("hosts") or []
            if isinstance(item, Mapping) and str(item.get("host") or "")
        ]
        if hosts:
            return {
                "hosts": hosts,
                "hostCount": len(hosts),
                "source": "report",
                "document": conflicts.REPORT_NAME,
                "complete": True,
            }
    manifest = harness.read_json_artifact(Path(epoch_dir), epoch_mod.EPOCH_FILE)
    notes = (
        (manifest or {}).get("deltaReferee", {}).get("notes")
        if isinstance(manifest, Mapping)
        else None
    )
    for note in notes or []:
        if not isinstance(note, Mapping) or str(note.get("reason") or "") != conflicts.NOTE_REASON:
            continue
        hosts = [str(host) for host in note.get("hosts") or [] if str(host)]
        if hosts:
            return {
                "hosts": hosts,
                "hostCount": int(note.get("hostCount") or len(hosts)),
                "source": "note",
                "document": epoch_mod.EPOCH_FILE,
                "complete": False,
            }
    return {"hosts": [], "hostCount": 0, "source": "none", "document": "", "complete": False}


def _by_identity(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Mapping[str, Any]], int]:
    """Rows keyed by job identity, plus how many identities were not unique.

    Duplicates are counted rather than collapsed quietly: a run that kept two rows for one job makes
    the residue an under-count in a way nothing else in the block would show, and an under-count is
    exactly where a divergence on an undeclared host could hide.
    """

    keyed: dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = job_identity(row)
        if key in keyed:
            duplicates += 1
            continue
        keyed[key] = row
    return keyed, duplicates


def residue(
    record_rows: Sequence[Mapping[str, Any]],
    replay_rows: Sequence[Mapping[str, Any]],
    *,
    declared_hosts: Sequence[str],
    fields: Sequence[str] = STABLE_ROW_FIELDS,
) -> dict[str, Any]:
    """The rows the two sides do not share, attributed to the source that carried each one.

    Three shapes, because they are three different findings: an identity only the record has (a job
    the replay could not build), one only the replay has, and a job both kept whose consumer-facing
    fields moved. ``outside`` is the part of the residue on hosts the record did not declare -- the
    finding that refuses the binding -- and ``fields`` counts what moved, so a reader can see what
    was accepted rather than only where.
    """

    declared_set = {str(host).strip().lower() for host in declared_hosts}
    record_map, record_duplicates = _by_identity(record_rows)
    replay_map, replay_duplicates = _by_identity(replay_rows)

    record_only = [record_map[key] for key in record_map.keys() - replay_map.keys()]
    replay_only = [replay_map[key] for key in replay_map.keys() - record_map.keys()]
    drifted: list[Mapping[str, Any]] = []
    fields_moved: dict[str, int] = {}
    for key in record_map.keys() & replay_map.keys():
        before, after = record_map[key], replay_map[key]
        moved = [str(name) for name in fields if before.get(name, "") != after.get(name, "")]
        if not moved:
            continue
        drifted.append(before)
        for name in moved:
            fields_moved[name] = fields_moved.get(name, 0) + 1

    per_host: dict[str, dict[str, int]] = {}
    unattributed = 0

    def _attribute(rows: Sequence[Mapping[str, Any]], bucket: str) -> None:
        nonlocal unattributed
        for row in rows:
            hosts = source_hosts(row)
            if not hosts:
                unattributed += 1
                continue
            for host in hosts:
                entry = per_host.setdefault(host, {"recordOnly": 0, "replayOnly": 0, "drifted": 0})
                entry[bucket] = int(entry[bucket]) + 1

    _attribute(record_only, "recordOnly")
    _attribute(replay_only, "replayOnly")
    _attribute(drifted, "drifted")

    def _total(counts: Mapping[str, int]) -> int:
        return (
            int(counts.get("recordOnly", 0))
            + int(counts.get("replayOnly", 0))
            + int(counts.get("drifted", 0))
        )

    # Two counts on purpose: ``rows`` is the residue (every differing row identified, attributed or
    # not) and ``attributedRows`` is how much of it landed on a named host -- a row carried by three
    # sources counts three there and once here, and a row with no host counts only in ``rows``, which
    # is what makes the unattributed case visible rather than an empty residue.
    attributed = {
        key: sum(int(counts.get(key, 0)) for counts in per_host.values())
        for key in ("recordOnly", "replayOnly", "drifted")
    }
    hosts = [
        {"host": host, **counts}
        for host, counts in sorted(per_host.items(), key=lambda item: (-_total(item[1]), item[0]))
    ]
    return {
        "recordRows": len(record_rows),
        "replayRows": len(replay_rows),
        "recordOnly": len(record_only),
        "replayOnly": len(replay_only),
        "drifted": len(drifted),
        "rows": len(record_only) + len(replay_only) + len(drifted),
        "attributedRows": _total(attributed),
        "duplicateIdentities": record_duplicates + replay_duplicates,
        "unattributed": unattributed,
        "fields": dict(sorted(fields_moved.items())),
        "hosts": hosts,
        "outside": [
            {"host": item["host"], "rows": _total(item)}
            for item in hosts
            if item["host"] not in declared_set
        ],
    }


def _recorded_run_dir(epoch_dir: Path, manifest: Mapping[str, Any]) -> Path | None:
    """Where the record's own run wrote its rows, or ``None`` when it is no longer on disk.

    The path is recorded relative to the repository on the machine that recorded it, so both
    separator spellings and the repository root are tried: a corpus read on another platform is
    still the same corpus, and a path that cannot be resolved refuses the binding rather than
    silently comparing nothing.
    """

    raw = str(manifest.get("recordedRunDir") or "").strip()
    if not raw:
        return None
    candidates = [Path(raw), Path(raw.replace("\\", "/"))]
    root = Path(__file__).resolve().parents[3]
    candidates.append(root / raw.replace("\\", "/"))
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def _rows_of(run_dir: Path) -> list[dict[str, Any]] | None:
    """A run's rows, or ``None`` when it wrote no rows file at all.

    ``None`` and ``[]`` are different answers: a run that kept zero rows is still a run whose
    outcome can be compared, while a run directory whose rows file is gone cannot be compared and
    must not read as an empty residue.
    """

    if harness.first_artifact(run_dir, harness.ROWS_FILE_CANDIDATES) is None:
        return None
    return harness.run_rows(run_dir)


def bind(*, epoch_dir: Path | str | None, run_dir: Path | str) -> dict[str, Any]:
    """Whether the record's own declaration explains a replay's differing rows, and how.

    ``bound`` is true only when there was a residue to explain and every one of its rows belongs to
    a host the record declared. Every other outcome is a refusal carrying a reason a reader can act
    on. Nothing here raises for a missing artifact -- an unreadable corpus is a refusal, not a crash
    in the middle of a verdict -- and nothing here is inferred from a verdict: the declaration and
    the rows are both read from disk.
    """

    block: dict[str, Any] = {
        "schemaVersion": 1,
        "epochDir": str(epoch_dir or ""),
        "runDir": str(run_dir),
        "declared": {
            "hosts": [],
            "hostCount": 0,
            "source": "none",
            "document": "",
            "complete": False,
        },
        "residue": {},
        "bound": False,
        "reason": "",
        "interpretation": INTERPRETATION,
    }
    if epoch_dir is None:
        block["reason"] = "no epoch was named, so the record's declaration could not be read"
        return block
    epoch_path = Path(str(epoch_dir))
    declaration = declared(epoch_path)
    block["declared"] = declaration
    if not declaration["hosts"]:
        block["reason"] = (
            "the record declares no conflict host (its conflict audit is clean, or it predates the "
            "audit), so there is nothing a residue could be bound to"
        )
        return block
    manifest = harness.read_json_artifact(epoch_path, epoch_mod.EPOCH_FILE)
    if not isinstance(manifest, Mapping):
        block["reason"] = f"no readable {epoch_mod.EPOCH_FILE} under {epoch_path}"
        return block
    recorded_run = _recorded_run_dir(epoch_path, manifest)
    if recorded_run is None:
        block["reason"] = (
            "the record's own run directory is no longer on disk, so its rows cannot be compared "
            "with the replay's -- an unreadable residue is not an empty one"
        )
        return block
    try:
        record_rows = _rows_of(recorded_run)
        replay_rows = _rows_of(Path(run_dir))
    except (OSError, ValueError) as exc:
        block["reason"] = f"the rows of one side could not be read: {exc}"
        return block
    if record_rows is None or replay_rows is None:
        missing = "the record" if record_rows is None else "the replay"
        block["reason"] = f"{missing} wrote no rows file, so the residue cannot be read"
        return block
    analysis = residue(record_rows, replay_rows, declared_hosts=declaration["hosts"])
    block["residue"] = analysis
    # Duplicates first: they are *why* a row count can differ with no residue to show for it, and
    # saying "no differing row" there would be true and useless.
    if analysis["duplicateIdentities"]:
        block["reason"] = (
            f"{analysis['duplicateIdentities']} row(s) share a job identity, so the residue is an "
            "under-count and a divergence on an undeclared host could hide inside it"
        )
        return block
    if not analysis["rows"]:
        block["reason"] = (
            "the parity units differ, but no differing row could be identified in the two row "
            "sets, so there is no attributable residue"
        )
        return block
    if analysis["unattributed"]:
        block["reason"] = (
            f"{analysis['unattributed']} differing row(s) name no source host, which puts them "
            "outside the declared set by construction"
        )
        return block
    if analysis["outside"]:
        named = ", ".join(str(item["host"]) for item in analysis["outside"][:_HOST_LIMIT])
        block["reason"] = (
            f"differing rows belong to {len(analysis['outside'])} host(s) the record did not "
            f"declare: {named}"
        )
        return block
    block["bound"] = True
    block["reason"] = (
        f"every one of the {analysis['rows']} differing row(s) belongs to a host the record "
        "declared as a key conflict: "
        + ", ".join(str(item["host"]) for item in analysis["hosts"][:_HOST_LIMIT])
    )
    return block


def summary(block: Mapping[str, Any]) -> dict[str, Any]:
    """The compact form that belongs beside a verdict, without the per-host evidence.

    Counts, the decision, and bounded host names -- what a listing reader needs to see that a
    divergence was bound and to what. The full block, with every host's per-class counts and the
    fields that moved, stays in the receipt.
    """

    analysis = block.get("residue") or {}
    declaration = block.get("declared") or {}
    residue_hosts = [
        str(item.get("host") or "")
        for item in analysis.get("hosts") or []
        if isinstance(item, Mapping) and str(item.get("host") or "")
    ]
    outside_hosts = [
        str(item.get("host") or "")
        for item in analysis.get("outside") or []
        if isinstance(item, Mapping) and str(item.get("host") or "")
    ]
    return {
        "bound": bool(block.get("bound")),
        "reason": str(block.get("reason") or ""),
        "declaredHostsSource": str(declaration.get("source") or ""),
        "declaredHostCount": int(declaration.get("hostCount") or 0),
        "declaredHosts": [str(host) for host in declaration.get("hosts") or []][:_HOST_LIMIT],
        "residueHosts": residue_hosts[:_HOST_LIMIT],
        "residueHostCount": len(residue_hosts),
        "outsideHosts": outside_hosts[:_HOST_LIMIT],
        "recordOnly": int(analysis.get("recordOnly") or 0),
        "replayOnly": int(analysis.get("replayOnly") or 0),
        "drifted": int(analysis.get("drifted") or 0),
        "residueRows": int(analysis.get("rows") or 0),
        "unattributed": int(analysis.get("unattributed") or 0),
    }


__all__ = [
    "INTERPRETATION",
    "bind",
    "declared",
    "residue",
    "source_hosts",
    "summary",
]
