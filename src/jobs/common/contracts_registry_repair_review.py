"""Registry repair review state: the human-approval gate for registry repairs.

Every other registry mutation in Baluffo is either a sanctioned transition
(``transition_registry_to_pending`` and friends, reached through the Admin
approve/demote routes) or an explicit operator action. This module is the
*approval* half of that story and deliberately not the mutation half: it records
what a human decided about a registry-hygiene finding, and nothing here writes,
demotes, deletes, or repoints a registry row. Applying an approved repair stays
a separate operator action through the existing sanctioned registry paths.

It follows the same shape as the dedup pair review
(``contracts_source_policy_review_state``): a total normalizer that survives
junk, a bounded disposition vocabulary, bounded actor/note fields, and a
missing/malformed warning code rather than an exception.

The safety property that makes this useful rather than decorative is the
**evidence fingerprint**. A recorded decision is only honored while the finding
it was made about still describes the same defect. Bind an approval to a
volatile counter and a source that has simply failed one more time silently
loses its review; bind it to the failing URL and error class, and a domain that
starts failing differently, or a row repointed elsewhere, correctly requires a
human to decide again.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_list, as_json_object
from src.shared.utils import parse_iso

REGISTRY_REPAIR_REVIEW_SCHEMA_VERSION = "1.0"

# "new" is the implicit state of a finding with no recorded decision.
REGISTRY_REPAIR_REVIEW_DECISIONS = frozenset({"new", "acknowledged", "repair_approved", "snoozed"})
# Recorded intent only. Nothing in this module acts on these; naming a repair
# here is a decision record, not an execution request.
REGISTRY_REPAIR_ACTIONS = frozenset({"repoint", "retire", "reclassify"})

_KEY_SEPARATOR = "||"
_NOTES_LIMIT = 500
_ACTOR_LIMIT = 80
_TARGET_LIMIT = 500
_KEY_SEPARATOR_RATIO = 64

# Fields that describe *which* defect a decision is about. A change to any of
# them means the decision no longer describes the finding on screen.
_FINGERPRINT_FIELDS = ("sourceId", "findingKind", "registryState", "unreachableEvidence")


def registry_repair_review_key(*, source_id: str, finding_kind: str) -> str:
    """Stable per-finding review key.

    Keyed on source id *and* finding kind because one row can carry several
    independent findings, and approving a dead-domain repair says nothing about
    a duplicate-URL collision on the same row.
    """
    source_key = norm_text(source_id)
    finding_key = norm_text(finding_kind)
    if not source_key or not finding_key:
        return ""
    return f"{source_key}{_KEY_SEPARATOR}{finding_key}"


def registry_finding_fingerprint(
    *,
    source_id: str,
    finding_kind: str,
    registry_state: str = "",
    unreachable_evidence: str = "",
    affected_urls: Any = (),
) -> str:
    """Fingerprint the *stable* identity of a hygiene finding.

    Deliberately excludes the volatile counters (``consecutiveFailures``,
    ``outageDays``, ``observationAgeDays``). Those are expected to keep climbing
    on a source that stays broken, and folding them in would silently invalidate
    a standing review after one more run -- training the operator to re-approve
    forever, which is worse than no review at all.

    Included instead is what the decision is *about*: the row, the kind of
    finding, the registry state, the classified failure mode, and the URLs
    involved. A ``repoint`` approved for an HTTP 404 at one URL must not carry
    over to a TLS failure at a different URL.
    """
    parts = [
        norm_text(source_id),
        norm_text(finding_kind),
        norm_text(registry_state),
        norm_text(unreachable_evidence),
    ]
    urls = sorted(
        {
            norm_text(url)
            for url in as_json_list(affected_urls)
            if isinstance(url, str) and norm_text(url)
        }
    )
    digest = hashlib.sha1("|".join(parts).encode("utf-8"))
    digest.update(b"||urls")
    for url in urls:
        digest.update(b"\x1f")
        digest.update(url.encode("utf-8"))
    return digest.hexdigest()[:_KEY_SEPARATOR_RATIO]


def _decision(value: Any) -> str:
    state = norm_text(value)
    return state if state in REGISTRY_REPAIR_REVIEW_DECISIONS else "new"


def _action(value: Any) -> str:
    action = norm_text(value)
    return action if action in REGISTRY_REPAIR_ACTIONS else ""


def _parseable_iso(value: Any) -> str:
    text = clean_text(value)
    return text if parse_iso(text) is not None else ""


def normalize_registry_repair_review_row(payload: Any) -> dict[str, Any]:
    """Total normalization: junk in yields a valid ``new`` row, never an error."""
    src = as_json_object(payload)
    decision = _decision(src.get("decision"))
    # An approved action is only meaningful for an approval; a decision of
    # "acknowledged" that still carries an action is a recording mistake, and
    # dropping the action keeps the two fields from disagreeing.
    action = _action(src.get("approvedAction")) if decision == "repair_approved" else ""
    return {
        "sourceId": clean_text(src.get("sourceId"))[:_ACTOR_LIMIT],
        "findingKind": norm_text(src.get("findingKind"))[:_ACTOR_LIMIT],
        "registryState": norm_text(src.get("registryState"))[:_ACTOR_LIMIT],
        "unreachableEvidence": norm_text(src.get("unreachableEvidence"))[:_ACTOR_LIMIT],
        "evidenceFingerprint": norm_text(src.get("evidenceFingerprint"))[:_KEY_SEPARATOR_RATIO],
        "decision": decision,
        "approvedAction": action,
        "approvedTarget": clean_text(src.get("approvedTarget"))[:_TARGET_LIMIT] if action else "",
        "decisionAt": _parseable_iso(src.get("decisionAt")),
        "decidedBy": clean_text(src.get("decidedBy"))[:_ACTOR_LIMIT],
        "note": clean_text(src.get("note"))[:_NOTES_LIMIT],
        "snoozedUntil": _parseable_iso(src.get("snoozedUntil")),
    }


def _summary_from_rows(rows: dict[str, dict[str, Any]]) -> dict[str, int]:
    summary = {decision: 0 for decision in sorted(REGISTRY_REPAIR_REVIEW_DECISIONS - {"new"})}
    for row in rows.values():
        decision = row["decision"]
        if decision in summary:
            summary[decision] += 1
    return summary


def normalize_registry_repair_review_artifact(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    rows: dict[str, dict[str, Any]] = {}
    raw_rows = as_json_object(src.get("rows"))
    for raw_key, raw_row in raw_rows.items():
        row = normalize_registry_repair_review_row(raw_row)
        key = (
            registry_repair_review_key(source_id=row["sourceId"], finding_kind=row["findingKind"])
            or clean_text(raw_key)[: _KEY_SEPARATOR_RATIO * 2]
        )
        if key and row["decision"] != "new":
            rows[key] = row
    return {
        "schemaVersion": clean_text(src.get("schemaVersion"))
        or REGISTRY_REPAIR_REVIEW_SCHEMA_VERSION,
        "updatedAt": clean_text(src.get("updatedAt")),
        "summary": _summary_from_rows(rows),
        "rows": rows,
    }


def read_registry_repair_review_artifact(path: Path) -> tuple[dict[str, Any], str]:
    """Load the review artifact, or an empty one plus a warning code.

    A missing or malformed artifact degrades to "nothing has been reviewed"
    rather than raising, matching the source-policy review reader. That
    direction is the safe one: a corrupt gate must not look like an approval,
    and must not take the pipeline down.
    """
    artifact_path = Path(path)
    if not artifact_path.exists():
        return normalize_registry_repair_review_artifact({}), "missing_registry_repair_review"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return normalize_registry_repair_review_artifact({}), "malformed_registry_repair_review"
    if not isinstance(payload, dict):
        return normalize_registry_repair_review_artifact({}), "malformed_registry_repair_review"
    return normalize_registry_repair_review_artifact(payload), ""


def _recorded_row(review_state: Any, *, source_id: str, finding_kind: str) -> dict[str, Any]:
    src = normalize_registry_repair_review_artifact(review_state)
    key = registry_repair_review_key(source_id=source_id, finding_kind=finding_kind)
    row = src["rows"].get(key)
    return normalize_registry_repair_review_row(row) if isinstance(row, dict) else {}


def _fingerprint_matches(recorded: Any, observed: Any) -> bool:
    """A decision binds only when both sides carry the same fingerprint.

    An unfingerprinted decision never matches: an approval that cannot be
    traced to a specific defect must not be honored, because there is no way to
    tell whether it still describes what is on screen.
    """
    recorded_text = norm_text(recorded)
    observed_text = norm_text(observed)
    return bool(recorded_text) and recorded_text == observed_text


def registry_repair_review_status(
    review_state: Any,
    *,
    source_id: str,
    finding_kind: str,
    evidence_fingerprint: str = "",
) -> dict[str, Any]:
    """Describe the review state of one finding, including staleness.

    The single place that decides whether a recorded decision still applies, so
    a caller cannot disagree with itself about the match. A decision is honored
    only when its recorded fingerprint matches the fingerprint of the finding as
    observed now; that is what makes the gate mean anything, because the review
    tracks the defect rather than the row's continued existence.

    ``isStale`` means a decision exists but was made about different evidence:
    the operator needs to see that the finding has returned to their queue,
    which is different from both "reviewed" and "never looked at".
    """
    row = _recorded_row(review_state, source_id=source_id, finding_kind=finding_kind)
    empty = normalize_registry_repair_review_row({})
    if not row or row["decision"] == "new":
        return {**empty, "isStale": False, "hasDecision": False}
    if not _fingerprint_matches(row.get("evidenceFingerprint"), evidence_fingerprint):
        return {**empty, "isStale": True, "hasDecision": True}
    return {**row, "isStale": False, "hasDecision": True}


def apply_registry_repair_review_action(
    *,
    prior_artifact: Any,
    action_payload: Any,
    updated_at: str,
    default_decided_by: str = "admin",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Record one human decision, returning the new artifact and the row.

    Validates the decision against the bounded vocabulary and raises
    ``ValueError`` on anything else, so an unrecognized action is a loud
    failure at the boundary rather than a silently stored no-op.

    Recording a decision is not applying a repair. Even ``repair_approved``
    only stores intent; the caller still has to go through the sanctioned
    registry transition path separately.
    """
    artifact = normalize_registry_repair_review_artifact(prior_artifact)
    payload = as_json_object(action_payload)
    decision = _decision(payload.get("decision"))
    if decision == "new":
        raise ValueError("invalid registry repair decision")
    source_id = clean_text(payload.get("sourceId"))
    finding_kind = norm_text(payload.get("findingKind"))
    key = registry_repair_review_key(source_id=source_id, finding_kind=finding_kind)
    if not key:
        raise ValueError("registry repair decision requires sourceId and findingKind")

    previous = artifact["rows"].get(key) or {}
    row = normalize_registry_repair_review_row(
        {
            "sourceId": source_id,
            "findingKind": finding_kind,
            "registryState": norm_text(payload.get("registryState")),
            "unreachableEvidence": norm_text(payload.get("unreachableEvidence")),
            "evidenceFingerprint": payload.get("evidenceFingerprint"),
            "decision": decision,
            "approvedAction": payload.get("approvedAction"),
            "approvedTarget": payload.get("approvedTarget"),
            "decisionAt": payload.get("decisionAt") or clean_text(updated_at),
            "decidedBy": payload.get("decidedBy") or default_decided_by,
            "note": payload.get("note"),
            "snoozedUntil": payload.get("snoozedUntil"),
        }
    )
    if not row["evidenceFingerprint"]:
        row["evidenceFingerprint"] = previous.get("evidenceFingerprint", "")

    rows = dict(artifact["rows"])
    rows[key] = row
    return (
        normalize_registry_repair_review_artifact({"rows": rows, "updatedAt": updated_at}),
        row,
    )
