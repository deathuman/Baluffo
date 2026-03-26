# Milestone 3 - Discovery Promotion Pipeline

## Summary

- Build M3 as a promotion-layer milestone on top of the existing discovery stack, not a rewrite.
- Keep the current scored discovery pipeline, dedupe, probing, queue balancing, deferred-by-cap handling, registry buckets, and the real saved setting `autoApproveHealthyPendingOnComplete`.
- Make candidate promotion explicit, rankable, reviewable, and persistently revisit-able, especially for `domain_cap` backlog.
- Treat this as a contract-aware milestone: all new lifecycle and ranking fields are additive, but `data/source-discovery-candidates.json` intentionally expands from a queued-only file into a queued-plus-deferred review queue.

## Primary Implementation Surfaces

- `src/source_discovery/orchestrator.py`
  - Primary M3 integration point for ranking, lifecycle stamping, deferred persistence, and the changed write semantics of `data/source-discovery-candidates.json`.
- `src/bridge/discovery_service.py`
  - Primary M3 integration point for auto-approve behavior, lifecycle stamping during automatic promotion, and preservation of the existing saved setting/default behavior.

## Key Changes

### 3.1 Candidate lifecycle model

- Introduce additive lifecycle metadata on discovery candidate rows and registry rows:
  - `candidateState`
  - `rankScore`
  - `rankReasons`
  - `promotionLane`
  - `approvedAt`
  - `approvedBy`
  - `liveAt`
  - `quarantinedAt`
  - `quarantineReason`
  - `deferCount`
  - `firstDeferredAt`
  - `lastDeferredAt`
- Use this fixed lifecycle vocabulary:
  - `discovered`
  - `probed`
  - `validated`
  - `approved`
  - `live`
  - `quarantined`
- Keep `active`, `pending`, and `rejected` as the storage buckets, but map them explicitly:
  - `pending` contains `validated` or `approved`
  - `active` contains `live`
  - `rejected` contains `quarantined`

### 3.2 Explicit acknowledgment of current behavior change

- Today, `DiscoveryService._auto_approve_healthy_pending_sources(...)` moves qualifying rows directly from `pending` to `active` and only stamps `enabledByDefault`.
- M3 changes that behavior semantically by making the implicit jump explicit in metadata:
  - auto-approve becomes `validated -> approved -> live`
  - manual approve becomes `validated -> approved -> live`
  - reject becomes `validated|approved -> quarantined`
  - rollback becomes `live -> validated`
  - restore-rejected becomes `quarantined -> validated`
- This is a behavioral change to the current approval flow, not just a reporting tweak, so all approval/reject/rollback paths must be updated together.

### 3.3 Ranking and review lanes

- Add a ranking pass after `normalize_candidate(...)` and before `apply_queue_balancing(...)`.
- Rank inputs are limited to existing repo signals:
  - current `score`
  - `confidence`
  - `jobsFound`
  - `evidenceScore`
  - adapter family
  - `nlPriority`
  - discovery stage
  - novelty vs existing `active` and `pending` registry rows
- Emit one `promotionLane` per candidate:
  - `structured_batch`
  - `manual_review`
  - `domain_cap_review`
- Initial lane rules:
  - `greenhouse`, `lever`, and `ashby` with non-low confidence and no defer reason go to `structured_batch`
  - static, weak-signal, or manual-only rows go to `manual_review`
  - rows deferred for `domain_cap` go to `domain_cap_review`

### 3.4 Deferred backlog persistence

- Change discovery so deferred `domain_cap` rows are not only visible in the full report; they are also persisted in `data/source-discovery-candidates.json`.
- This is a deliberate semantic expansion:
  - today `data/source-discovery-candidates.json` is effectively “queued candidates only”
  - after M3 it becomes “queued candidates plus deferred review queue”
- Keep `deferred` and `deferReason` as the compatibility markers that distinguish queued rows from review-only rows.
- Add `deferCount`, `firstDeferredAt`, and `lastDeferredAt` so previously blocked candidates can be revisited intentionally on later runs.

### 3.5 Admin and auto-approve workflow

- Keep `autoApproveHealthyPendingOnComplete` as a real saved setting and keep its default behavior enabled.
- Update `src/bridge/discovery_service.py` so auto-approved rows receive:
  - `candidateState="live"`
  - `approvedAt`
  - `approvedBy="discovery_auto_approve"`
  - `liveAt`
- Keep existing registry routes as the M3 workflow surface.
- The exact route handler file has been verified: approve/reject/rollback/restore-rejected live in `src/bridge/routes/post_routes.py`.
- Update those route handlers so lifecycle metadata is stamped consistently during:
  - `/registry/approve`
  - `/registry/reject`
  - `/registry/rollback`
  - `/registry/restore-rejected`

## Contract and API Notes

- `data/source-discovery-report.json` and `data/source-discovery-candidates.json` remain stable contracts and must stay shape-compatible.
- Lifecycle, ranking, and defer-tracking fields are additive only.
- Additive summary fields may include:
  - `validatedCandidateCount`
  - `approvedCandidateCount`
  - `liveCandidateCount`
  - `quarantinedCandidateCount`
- The main contract risk is not summary compatibility; it is the changed meaning of `data/source-discovery-candidates.json` and any admin/frontend assumptions that it contains only queued rows.
- Required coordinated updates:
  - `docs/DATA_CONTRACT.md`
  - `tests/fixtures/source_discovery_report_snapshot.json`
  - focused discovery tests
  - focused bridge/admin tests
  - frontend/admin tests that assume queued-only candidate persistence
- `src/source_discovery/schemas.py` may need no code change at all unless we want the new summary fields explicitly modeled there, because additive fields already pass validation today.

## Files Touched

- Primary:
  - `src/source_discovery/orchestrator.py`
  - `src/bridge/discovery_service.py`
- Supporting backend:
  - `src/source_discovery/core.py`
  - `src/source_discovery/reporting.py`
  - `src/source_discovery/schemas.py` only if explicit additive summary modeling is desired
  - `src/bridge/routes/post_routes.py`
  - `src/bridge/registry_service.py`
- Supporting frontend/docs/tests:
  - `frontend/admin/app/discovery.js`
  - `frontend/admin/app/registry.js`
  - `frontend/admin/domain.js`
  - `docs/DATA_CONTRACT.md`
  - `docs/milestone-3-plan.md`
  - `tests/test_source_discovery.py`
  - `tests/bridge/test_discovery_service.py`
  - `tests/bridge/test_routes_post.py`
  - `tests/frontend/unit/admin-domain.test.mjs`
  - `tests/frontend/unit/admin-controllers.test.mjs`
  - `tests/fixtures/source_discovery_report_snapshot.json`

## Test Plan

- Discovery tests:
  - candidates receive `rankScore`, `rankReasons`, `promotionLane`, and `candidateState`
  - deferred `domain_cap` rows are persisted in `data/source-discovery-candidates.json`
  - queued counts still count only non-deferred rows
  - older deferred rows gain review priority on later runs
- Bridge tests:
  - auto-approve stamps lifecycle metadata while preserving current default enablement
  - `/registry/approve`, `/registry/reject`, `/registry/rollback`, and `/registry/restore-rejected` update lifecycle metadata consistently
- Frontend/admin tests:
  - UI distinguishes queued rows from deferred review rows
  - summary/render logic does not inflate queued counts when deferred rows are persisted
  - structured batch candidates can still be approved through existing registry actions
- Acceptance scenario:
  - one discovery run yields persisted queued and deferred candidates, preserves contract compatibility, and keeps admin review/approval flows coherent end to end

## Assumptions and defaults

- M3 does not replace `active/pending/rejected`; it layers explicit lifecycle metadata on top of them.
- `greenhouse`, `lever`, and `ashby` are the only initial `structured_batch` families in M3.
- No new bridge endpoints are required for M3 v1.
- All new lifecycle, ranking, and defer-tracking fields remain additive.
- The semantic expansion of `data/source-discovery-candidates.json` is intentional and must be documented before implementation.
