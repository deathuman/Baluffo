"""Shared per-run evidence slots for single-slot run reports.

AI boundary owns: the per-run history-slot invariant for single-slot run
reports (currently ``jobs-fetch-report.json`` and ``source-discovery-report.json``)
and the slot-naming/dedup/retention mechanics.
AI boundary implement in: this leaf only; call sites are the terminal write seams
(``pipeline_io.py`` writers and the runtime-evidence branch of ``save_json_atomic``).

Invariant: after any changed write, the history dir holds the newest terminal
payload of every run that wrote one — a later run can never displace another
run's snapshot, and a run's own final terminal rewrite keeps its slot current.
Progress shells never enter history on their own; they only back up a *terminal*
existing payload before clobbering it.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

FETCH_REPORT_HISTORY_DIR_NAME = "fetch-report-history"
DISCOVERY_REPORT_HISTORY_DIR_NAME = "discovery-report-history"
HISTORY_KEEP = 48
_LOCK = threading.Lock()

_TERMINAL_MARKER_KEYS = ("finishedAt",)


def looks_like_terminal_report_payload(payload: Any) -> bool:
    """Terminal reports carry a truthy terminal marker (e.g. finishedAt)."""
    if not isinstance(payload, dict):
        return False
    return any(bool(payload.get(key)) for key in _TERMINAL_MARKER_KEYS)


def looks_like_terminal_report_text(text: str) -> bool:
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return False
    return looks_like_terminal_report_payload(payload)


def _run_slug(payload: dict[str, Any]) -> str:
    raw = str(payload.get("runId") or "")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("-.")[:48]
    return slug


def _slot_run_component(payload: dict[str, Any], stamp: str) -> str:
    """Slot filename component derived from the report's runId.

    The component is a hex digest of the runId, never the runId text (CodeQL
    py/path-injection #122/#125): digest output cannot carry separators or
    traversal fragments, so the filename is structurally safe no matter what a
    hostile report claims as its runId. The runId itself stays verbatim inside
    the stored payload, and per-run dedup keys on the payload content, so the
    mapping stays deterministic and one-to-one.
    """
    run_id = str(payload.get("runId") or "").strip()
    if run_id:
        return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    return f"run-{stamp}"


def _upsert_history_slot(
    text: str,
    *,
    history_dir: Path,
    file_stem: str,
) -> None:
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            payload = parsed
    except (json.JSONDecodeError, ValueError):
        pass
    run_component = _slot_run_component(payload, stamp)
    history_dir.mkdir(parents=True, exist_ok=True)
    slot_path = history_dir / f"{file_stem}-{run_component}-{stamp}.json.gz"
    if slot_path.exists():
        # Same-second rewrite of the same run — uniquify; dedup below collapses it.
        # The ``_`` separator is deliberate: dedup tiebreaks on (mtime, name) and
        # ``-`` (0x2D) sorts before ``.`` (0x2E), so a ``-``-suffixed uniquified
        # slot lost an mtime tie against the plain slot it superseded — keeping
        # the stale payload and deleting the fresh one. ``_`` (0x5F) sorts after
        # ``.`` so the later (fresher) write always wins the tie.
        slot_path = (
            history_dir / f"{file_stem}-{run_component}-{stamp}_{uuid.uuid4().hex[:6]}.json.gz"
        )
    with gzip.open(slot_path, mode="wt", encoding="utf-8") as handle:
        handle.write(text)

    # Dedup: one history file per runId (newest wins), including the slot just
    # written so an older same-run slot can never outlive the newest snapshot.
    parsed_entries: list[tuple[float, str, Path, str]] = []
    for entry in history_dir.glob(f"{file_stem}-*.json.gz"):
        try:
            mtime = entry.stat().st_mtime
            with gzip.open(entry, mode="rt", encoding="utf-8") as handle:
                entry_payload = json.load(handle)
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        if not isinstance(entry_payload, dict):
            continue
        entry_slug = _run_slug(entry_payload)
        if entry_slug:
            parsed_entries.append((mtime, entry.name, entry, entry_slug))
    newest_by_run: dict[str, tuple[float, str, Path]] = {}
    for mtime, name, entry, slug in parsed_entries:
        known = newest_by_run.get(slug)
        if known is None or (mtime, name) > (known[0], known[1]):
            newest_by_run[slug] = (mtime, name, entry)
    for _mtime, _name, entry, slug in parsed_entries:
        if entry != newest_by_run[slug][2]:
            try:
                entry.unlink()
            except OSError:
                pass

    # Retention: keep only the newest HISTORY_KEEP snapshots.
    survivors: list[tuple[float, str, Path]] = []
    for entry in history_dir.glob(f"{file_stem}-*.json.gz"):
        try:
            survivors.append((entry.stat().st_mtime, entry.name, entry))
        except OSError:
            continue
    for _, _, entry in sorted(survivors)[:-HISTORY_KEEP]:
        try:
            entry.unlink()
        except OSError:
            pass


def upsert_history_slot(text: str, *, history_dir: Path, file_stem: str) -> None:
    """Write one terminal payload into its per-run slot (dedup + retention).

    Public single-snapshot entrypoint for call sites that have already decided
    the payload belongs in history (e.g. the pipeline_io writer seams).
    """
    with _LOCK:
        _upsert_history_slot(text, history_dir=Path(history_dir), file_stem=file_stem)


def _decide_terminal_upserts(existing_payload: Any, incoming_payload: Any) -> list[Any]:
    """Which payloads must be snapshotted for this write (terminal-marker rules)."""
    incoming_terminal = looks_like_terminal_report_payload(incoming_payload)
    existing_terminal = looks_like_terminal_report_payload(existing_payload)
    if not (incoming_terminal or existing_terminal):
        return []
    snapshots: list[Any] = []
    if incoming_terminal:
        snapshots.append(incoming_payload)
    if existing_terminal:
        # The existing terminal payload is about to vanish; finalize its run slot
        # too — unless the incoming terminal is the same run superseding it.
        same_run = (
            incoming_terminal
            and isinstance(existing_payload, dict)
            and isinstance(incoming_payload, dict)
            and _run_slug(existing_payload) == _run_slug(incoming_payload)
        )
        if not same_run:
            snapshots.insert(0, existing_payload)
    return snapshots


def upsert_terminal_report_history_slots(
    *,
    path: Path,
    existing_text: str,
    incoming_text: str,
    report_names: set[str],
    history_dir_name: str,
    file_stem: str,
) -> None:
    """Upsert terminal-report evidence into per-run history slots (best-effort).

    Called after a *changed* report write. Terminal payloads (existing, incoming,
    or both) are snapshotted into their run's slot;    different runs are finalized independently, same-run terminal rewrites refresh
    their own slot in place, and non-terminal writes only back up a terminal
    existing payload.
    """
    if Path(path).name not in report_names:
        return

    # Parse each side independently: a corrupt existing payload must not stop
    # an incoming terminal payload from being snapshotted (and vice versa).
    try:
        existing_payload: Any = json.loads(existing_text)
    except (json.JSONDecodeError, ValueError):
        existing_payload = None
    try:
        incoming_payload: Any = json.loads(incoming_text)
    except (json.JSONDecodeError, ValueError):
        incoming_payload = None
    if existing_payload is None and incoming_payload is None:
        return

    def _upsert(payload: Any) -> None:
        try:
            text = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        except (TypeError, ValueError):
            return
        try:
            _upsert_history_slot(
                text,
                history_dir=Path(path).parent / history_dir_name,
                file_stem=file_stem,
            )
        except OSError:
            pass

    with _LOCK:
        for payload in _decide_terminal_upserts(existing_payload, incoming_payload):
            _upsert(payload)


def upsert_terminal_report_history_payloads(
    *,
    path: Path,
    existing_payload: Any,
    incoming_payload: Any,
    report_names: set[str],
    history_dir_name: str,
    file_stem: str,
) -> None:
    """Payload-level twin of ``upsert_terminal_report_history_slots``.

    For call sites that hold parsed payloads (e.g. ``save_json_atomic``) and do
    not want to re-read the existing file as text.
    """
    if Path(path).name not in report_names:
        return

    def _upsert(payload: Any) -> None:
        try:
            text = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        except (TypeError, ValueError):
            return
        try:
            _upsert_history_slot(
                text,
                history_dir=Path(path).parent / history_dir_name,
                file_stem=file_stem,
            )
        except OSError:
            pass

    with _LOCK:
        for payload in _decide_terminal_upserts(existing_payload, incoming_payload):
            _upsert(payload)
