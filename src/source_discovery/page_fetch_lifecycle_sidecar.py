from __future__ import annotations

"""Per-source publication of page-fetch lifecycle receipts beside a discovery artifact.

The audit artifact itself carries the authoritative `pageFetchLifecycle` list, but it can run to
tens of MB, so a reader that wants run-aware counters on a poll cannot parse it. This module
publishes a small, bounded sidecar next to the artifact -- one fixed filename, one envelope with a
per-source entry -- and a reader that never opens the artifact.

Publisher contract (P1c):

* `publish_page_fetch_lifecycle_sidecar` is called with the finished artifact and the artifact's
  output path. It writes nothing when the artifact carries no `pageFetchLifecycle` list.
* The sidecar is bounded: at most `MAX_SIDECAR_RECEIPT_RECORDS` receipts in total and at most
  `MAX_SIDECAR_BYTES` bytes on disk for the whole envelope. Three fixed audits share the one
  filename inside `data/`, so both bounds are enforced on the merged envelope, never per source.
  `MAX_SIDECAR_BYTES` counts the writer's own serialization (`separators=(",", ":")` plus its
  trailing newline), so the reader's stat check can never reject a file this module wrote.
* On overflow the affected source entries are marked `coverage=partial`, `truncated=true`, their
  `omittedCount` grows, and their `state` is never `clean` (truncation cannot be read as
  cleanliness). Absence of a sidecar never means clean.
* Replace-vs-merge is decided by `runId`: a sidecar from a different run is fully replaced; a
  sidecar from the same run is merged per-source while preserving other sources' entries. The
  audit artifact has no top-level runId, so the runId is derived from the receipts themselves; if
  the receipts disagree on runId the envelope is not published (a mixed-run publish would corrupt
  the replace-vs-merge invariant).
* The write is atomic and best-effort: an `OSError` from the underlying atomic writer must never
  fail the audit it accompanies.

The reader (`read_page_fetch_lifecycle_sidecar`) is a stat-then-read; oversized, malformed or
missing files return None without ever parsing the adjacent artifact.
"""

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, cast

from src import source_registry as source_registry_module
from src.shared.page_fetch_lifecycle import PageFetchLifecycle, rollup_page_fetch_lifecycles
from src.shared.utils import now_iso

PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME = "page-fetch-lifecycle.v1.json"
PAGE_FETCH_LIFECYCLE_SIDECAR_SCHEMA_VERSION = 1
PAGE_FETCH_LIFECYCLE_SIDECAR_RECEIPT_TYPE = "pageFetchLifecycleSidecar"

MAX_SIDECAR_RECEIPT_RECORDS = 512  # total receipts across all sources, per the P0 contract.
MAX_SIDECAR_BYTES = 262_144  # 256 KiB on disk, matches the P0 contract limit.
# The atomic writer writes with `write_text(..., encoding="utf-8")`, whose default newline
# translation turns the trailing "\n" into "\r\n" on Windows, so the file is one byte larger there
# than an LF model predicts. Enforce a one-byte guard margin so the reader's stat check can never
# reject a sidecar this module wrote on any platform.
_WRITE_MARGIN_BYTES = 1


def sidecar_path_for_artifact(output_path: Path) -> Path:
    """Sidecar lives beside the artifact with the fixed envelope filename.

    Three fixed audits (gameprog/gamesmap/web-search) share the default `data/` directory, so this
    single path holds one envelope with a per-source entry instead of clobbering files.
    """
    return Path(output_path).with_name(PAGE_FETCH_LIFECYCLE_SIDECAR_FILENAME)


def receipt_run_id_or_none(receipts: list[dict[str, Any]]) -> str | None:
    """The single run identity the receipts agree on, or None when it is absent or ambiguous.

    Public so the web-search browser-recovery guard can ask the same question the publisher asks
    instead of deriving run identity a second time and drifting from it.

    Every receipt counts here, not only the ones carrying a batchId: a receipt that names a run
    disagrees with the others regardless of whether its batch is identified, and ignoring such a
    receipt would let a mixed-run input publish under the wrong single runId.

    An empty/absent runId is treated as no identity rather than as the identity `""`. Two genuinely
    different unknown runs would otherwise share the `""` key and merge instead of replacing each
    other, and the replace-vs-merge rule is only meaningful for a real run identity.
    """
    run_ids = {
        str(receipt.get("runId") or "").strip() for receipt in receipts if isinstance(receipt, dict)
    }
    run_ids.discard("")
    return next(iter(run_ids)) if len(run_ids) == 1 else None


_ROLLUP_COUNT_FIELDS = (
    "submitted",
    "started",
    "finished",
    "failed",
    "cancelledBeforeStart",
    "queuedNow",
    "outstandingNow",
    "asyncCancellationRequested",
    "asyncCleanupFinished",
)


class _LifecycleSnapshotProxy:
    """Presentation shim so the shared rollup can aggregate the artifact's plain snapshot dicts.

    The shared rollup indexes snapshot keys directly, so missing keys on a malformed receipt would
    raise on the audit path. Defaults here keep a malformed receipt from ever crashing a publish.
    """

    def __init__(self, snapshot: dict[str, Any]) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> dict[str, Any]:
        merged = {
            "batchId": "",
            "source": "",
            "runId": "",
            "phase": "",
            "deadlineExit": "",
            **{field: 0 for field in _ROLLUP_COUNT_FIELDS},
            **self._snapshot,
        }
        return merged


def _rollup_payload(receipts: list[dict[str, Any]]) -> dict[str, Any]:
    """Delegate aggregation semantics to the shared rollup; no duplicated count-field drift.

    The shared rollup is typed for live lifecycle objects; the proxy presents the artifact's frozen
    snapshot dicts through the same `.snapshot()` shape, so the aggregation semantics stay in one
    place rather than being reimplemented here.
    """
    proxies = cast(
        "Iterable[PageFetchLifecycle]",
        (_LifecycleSnapshotProxy(r) for r in receipts),
    )
    return dict(rollup_page_fetch_lifecycles(proxies))


def _source_entry(
    *,
    source: str,
    run_id: str,
    artifact_name: str,
    receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    rollup = _rollup_payload(receipts)
    return {
        "source": source,
        "runId": run_id,
        "artifactName": artifact_name,
        "state": str(rollup.get("state") or "clean"),
        "coverage": "full",
        "truncated": False,
        "omittedCount": 0,
        "receiptCount": len(receipts),
        "rollup": rollup,
        "receipts": receipts,
    }


def _serialized_envelope_bytes(envelope: dict[str, Any]) -> int:
    """Exact LF-model on-disk size, matching `save_json_atomic`'s serialization.

    `_WRITE_MARGIN_BYTES` is *not* included here: this is the byte model used to decide when the
    envelope fits, and the cap comparison adds the margin separately.
    """
    return len(
        (json.dumps(envelope, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    )


def _fits_on_disk(envelope: dict[str, Any]) -> bool:
    """True when the envelope fits the cap once the platform's write margin is accounted for."""
    return _serialized_envelope_bytes(envelope) + _WRITE_MARGIN_BYTES <= MAX_SIDECAR_BYTES


def _receipt_array_bytes(receipts: list[dict[str, Any]]) -> int:
    """Size of these receipts as a serialized JSON array, matching the writer's separators.

    Used to decide how much must be dropped without re-serializing the whole envelope per drop.
    """
    if not receipts:
        return len(b"[]")
    body = sum(
        len(json.dumps(receipt, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        for receipt in receipts
    )
    return body + (len(receipts) - 1) + 2  # array commas plus the enclosing brackets


def _enforce_bounds(envelope: dict[str, Any]) -> dict[str, Any] | None:
    """Enforce the 512-receipt and 262,144-byte bounds on the whole (merged) envelope.

    The caps are properties of the *file*, which carries several sources when the fixed audits
    share a directory, so this operates on the merged `sources` mapping. Receipts are dropped from
    the source currently holding the most, earliest first, and every affected entry is marked
    partial, truncated, and never clean.

    Both loops are linear in the number of dropped receipts and the per-source rollups are rebuilt
    once afterwards, rather than per drop. Recomputing a rollup inside the drop loop re-iterates the
    remaining receipts each time, which made the whole function quadratic in the drop count.

    Returns None in the pathological case where the envelope's own overhead exceeds the cap even
    with no receipts left: an absent sidecar is honest, an unreadable one is not.
    """
    sources = envelope.setdefault("sources", {})
    if not sources:
        finalized = _finalize_bounds(envelope)
        return finalized if _fits_on_disk(finalized) else None

    # Overhead of the envelope with every per-source receipts array emptied.
    envelopes_without_receipts = {
        source: {**entry, "receipts": []} for source, entry in sources.items()
    }
    probe = {**envelope, "sources": envelopes_without_receipts}
    overhead = _serialized_envelope_bytes(probe)
    # Per-source array cost, recomputed only when that source is trimmed.
    array_bytes = {
        source: _receipt_array_bytes(list(entry.get("receipts") or []))
        for source, entry in sources.items()
    }
    total_receipts = sum(len(entry.get("receipts") or []) for entry in sources.values())
    total_bytes = overhead + sum(array_bytes.values())

    while (
        total_receipts > MAX_SIDECAR_RECEIPT_RECORDS
        or total_bytes + _WRITE_MARGIN_BYTES > MAX_SIDECAR_BYTES
    ):
        candidates = [source for source, entry in sources.items() if entry.get("receipts")]
        if not candidates:
            break  # nothing left to drop; handled below if the envelope still cannot fit.
        largest_source = max(candidates, key=lambda name: len(sources[name].get("receipts") or []))
        entry = sources[largest_source]
        receipts = list(entry.get("receipts") or [])
        dropped = receipts.pop()
        dropped_bytes = len(
            json.dumps(dropped, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
        entry["receipts"] = receipts
        entry["receiptCount"] = len(receipts)
        entry["omittedCount"] = int(entry.get("omittedCount") or 0) + 1
        entry["truncated"] = True
        entry["coverage"] = "partial"
        entry["state"] = "pending"  # never clean while the sidecar omits receipts
        # Array cost shrinks by the receipt plus one comma (and both brackets when it empties).
        if receipts:
            array_bytes[largest_source] = array_bytes[largest_source] - dropped_bytes - 1
        else:
            array_bytes[largest_source] = _receipt_array_bytes([])
        total_receipts -= 1
        total_bytes = overhead + sum(array_bytes.values())

    # The cheap estimate can drift by a few bytes because a trimmed entry's rollup changes size.
    # Verify against the real serializer and keep trimming if the estimate was optimistic, so the
    # reader's stat check can never reject a file this module wrote. These loops deliberately do
    # NOT recompute each trimmed entry's rollup: doing that per drop re-iterates the whole
    # remaining receipt list, which is quadratic in the drop count. The rollups are rebuilt once
    # below, after the receipt set has settled.
    while not _fits_on_disk(envelope):
        candidates = [source for source, entry in sources.items() if entry.get("receipts")]
        if not candidates:
            break
        largest_source = max(candidates, key=lambda name: len(sources[name]["receipts"]))
        entry = sources[largest_source]
        receipts = list(entry["receipts"])
        receipts.pop()
        entry["receipts"] = receipts
        entry["receiptCount"] = len(receipts)
        entry["omittedCount"] = int(entry.get("omittedCount") or 0) + 1
        entry["truncated"] = True
        entry["coverage"] = "partial"
        entry["state"] = "pending"

    # Rebuild each trimmed entry's rollup exactly once, from the receipts it ends up carrying.
    for entry in sources.values():
        receipts = list(entry.get("receipts") or [])
        rollup = _rollup_payload(receipts)
        if entry.get("truncated") and rollup.get("state") == "clean":
            # The counters honestly describe the receipts that survived, but a truncated view must
            # not report the shared rollup's "clean": a reader that trusts `rollup.state` would read
            # "everything settled" about a dataset the sidecar knowingly omits receipts from.
            rollup["state"] = "pending"
        entry["rollup"] = rollup

    envelope = _finalize_bounds(envelope)
    # Last resort for the pathological case where the envelope overhead alone exceeds the cap
    # (requires on the order of a thousand empty source entries). Returning an over-cap file would
    # break the writer-side invariant that the reader can always accept what the writer wrote, so
    # such a file is not published at all: an absent sidecar is honest, an unreadable one is not.
    if not _fits_on_disk(envelope):
        return None
    return envelope


def _finalize_bounds(envelope: dict[str, Any]) -> dict[str, Any]:
    """Recompute the envelope-level bound flags from the per-source entries."""
    sources = envelope.get("sources") or {}
    envelope["truncated"] = any(bool(entry.get("truncated")) for entry in sources.values())
    envelope["omittedCount"] = sum(
        int(entry.get("omittedCount") or 0) for entry in sources.values()
    )
    envelope["coverage"] = "partial" if envelope["truncated"] else "full"
    return envelope


def build_page_fetch_lifecycle_sidecar(
    artifact: dict[str, Any],
    output_path: Path,
) -> dict[str, Any] | None:
    """Build the per-source envelope for one artifact.

    Returns None when the artifact carries no receipts or when the receipts disagree on runId (a
    mixed-run publish would corrupt replace-vs-merge semantics).
    """
    receipts = [r for r in list(artifact.get("pageFetchLifecycle") or []) if isinstance(r, dict)]
    if not receipts:
        return None
    run_id = receipt_run_id_or_none(receipts)
    if run_id is None:
        return None

    # First-wins dedupe by batch identity, mirroring _merge_page_fetch_lifecycle_snapshots.
    deduped: dict[tuple[str, str | None, str | None, str | None], dict[str, Any]] = {}
    for receipt in receipts:
        batch_id = str(receipt.get("batchId") or "").strip()
        if not batch_id:
            continue
        source = receipt.get("source") if isinstance(receipt.get("source"), str) else None
        phase = receipt.get("phase") if isinstance(receipt.get("phase"), str) else None
        run = receipt.get("runId") if isinstance(receipt.get("runId"), str) else None
        deduped.setdefault((batch_id, source, run, phase), receipt)

    # Group by source, preserving first-seen order.
    limited: dict[str, list[dict[str, Any]]] = {}
    for identity, receipt in deduped.items():
        source = identity[1] or ""
        limited.setdefault(source, []).append(receipt)
    sources: list[str] = []
    for identity in deduped:
        source = identity[1] or ""
        if source not in sources:
            sources.append(source)

    per_source: dict[str, dict[str, Any]] = {}
    for source in sources:
        per_source[source] = _source_entry(
            source=source,
            run_id=run_id,
            artifact_name=Path(output_path).name,
            receipts=limited.get(source) or [],
        )

    envelope = {
        "schemaVersion": PAGE_FETCH_LIFECYCLE_SIDECAR_SCHEMA_VERSION,
        "receiptType": PAGE_FETCH_LIFECYCLE_SIDECAR_RECEIPT_TYPE,
        "generatedAt": now_iso(),
        "runId": run_id,
        "artifactName": Path(output_path).name,
        "coverage": "full",
        "truncated": False,
        "omittedCount": 0,
        "sources": per_source,
    }
    return _enforce_bounds(envelope)


def _merge_same_run(
    existing: dict[str, Any],
    fresh: dict[str, Any],
) -> dict[str, Any]:
    """Merge a same-run envelope into the existing one, preserving other sources' entries."""
    merged_sources = dict(existing.get("sources") or {})
    merged_sources.update(dict(fresh.get("sources") or {}))
    return {
        "schemaVersion": PAGE_FETCH_LIFECYCLE_SIDECAR_SCHEMA_VERSION,
        "receiptType": PAGE_FETCH_LIFECYCLE_SIDECAR_RECEIPT_TYPE,
        "generatedAt": fresh.get("generatedAt") or now_iso(),
        "runId": fresh.get("runId"),
        "artifactName": fresh.get("artifactName"),
        "coverage": fresh.get("coverage"),
        "truncated": fresh.get("truncated"),
        "omittedCount": fresh.get("omittedCount"),
        "sources": merged_sources,
    }


def publish_page_fetch_lifecycle_sidecar(
    *,
    artifact: dict[str, Any],
    output_path: Path,
    emit_log: Callable[[str], None] | None = None,
) -> dict[str, Any] | None:
    """Publish (or merge) the sidecar for `artifact`. Best-effort: never raises on the audit path.

    Returns the envelope now on disk, or None in any of these cases, which callers treat alike
    because none of them is a failure the audit should react to:

    * the artifact carried no receipts, or no single run identity (nothing to publish);
    * the merged envelope cannot be bounded, so the existing valid sidecar was left intact;
    * the atomic write raised OSError (disk full, permissions).

    The three cases are distinguished only by `emit_log`; the second and third log, the first is
    intentionally silent because publishing nothing is the normal outcome for a receipt-less run.
    """
    envelope = build_page_fetch_lifecycle_sidecar(artifact, output_path)
    if envelope is None:
        return None
    path = sidecar_path_for_artifact(output_path)
    existing = read_page_fetch_lifecycle_sidecar(path)
    if existing is not None and str(existing.get("runId") or "") == str(
        envelope.get("runId") or ""
    ):
        merged = _merge_same_run(existing, envelope)
        bounded = _enforce_bounds(merged)
        if bounded is None:
            # The merged envelope cannot be represented within the cap; keep the existing, valid
            # sidecar rather than overwriting it with something the reader would reject.
            if emit_log is not None:
                emit_log("pageFetchLifecycle sidecar publish skipped: merged envelope exceeds cap.")
            return None
        envelope = bounded
    try:
        source_registry_module.save_json_atomic(path, envelope)
    except OSError as exc:
        if emit_log is not None:
            emit_log(f"pageFetchLifecycle sidecar publish skipped: {exc}")
        return None
    return envelope


def read_page_fetch_lifecycle_sidecar(path: Path) -> dict[str, Any] | None:
    """Stat-then-read: oversized, malformed or missing files return None (absent != clean)."""
    try:
        stat = Path(path).stat()
    except OSError:
        return None
    if stat.st_size <= 0 or stat.st_size > MAX_SIDECAR_BYTES:
        return None
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
