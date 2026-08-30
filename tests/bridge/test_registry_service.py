from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from src import source_registry as sr
from src.bridge import registry_service as registry_service_module
from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.storage_health import (
    close_storage_stores,
    get_storage_health_payload,
    get_storage_store,
)
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_registry_service_auto_heals_duplicate_active_pending_source_id(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    duplicate_id = "greenhouse:slug:guerrilla-games"
    active_row = {
        "id": duplicate_id,
        "name": "Guerrilla Games",
        "adapter": "greenhouse",
        "slug": "guerrilla-games",
        "registryState": "active",
        "stateChangedAt": "2026-05-01T00:00:00+00:00",
        "stateChangedBy": "test",
    }
    pending_row = {
        **active_row,
        "registryState": "pending",
        "pendingReason": "discovery_candidate",
        "stateChangedAt": "2026-05-02T00:00:00+00:00",
    }
    _write_json(active_path, [active_row])
    _write_json(pending_path, [pending_row])
    _write_json(rejected_path, [])
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )

    state = service.load_state()
    report = service.get_auto_heal_report()

    assert [row["id"] for row in state["active"]] == [duplicate_id]
    assert state["pending"] == []
    assert sr.load_json_array(active_path, [])[0]["registryState"] == "active"
    assert sr.load_json_array(pending_path, []) == []
    assert report["autoHealed"] is True
    assert report["duplicateSourceIdCount"] == 1
    assert report["duplicates"] == [
        {
            "sourceId": duplicate_id,
            "keptBucket": "active",
            "removedBucket": "pending",
            "keptName": "Guerrilla Games",
            "removedName": "Guerrilla Games",
            "mergedFields": [],
        }
    ]


def test_registry_service_auto_demotes_safe_static_url_alias_on_load(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://studio.example/careers"
    loser_id = "static:listing_url:https://www.studio.example/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 40,
                "score": 20,
            },
            {
                "id": loser_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 20,
                "score": 10,
            },
        ],
    )
    _write_json(pending_path, [])
    _write_json(rejected_path, [])
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )

    state = service.load_state()
    report = service.get_auto_heal_report()

    assert [row["id"] for row in state["active"]] == [winner_id]
    assert [row["id"] for row in state["pending"]] == [loser_id]
    assert state["pending"][0]["pendingReason"] == "registry_conflict_safe_auto_demote"
    assert state["pending"][0]["stateChangedBy"] == "registry_conflict_safe_auto_demote"
    assert [row["id"] for row in sr.load_json_array(active_path, [])] == [winner_id]
    assert [row["id"] for row in sr.load_json_array(pending_path, [])] == [loser_id]
    assert report["autoHealed"] is True
    assert report["duplicateSourceIdCount"] == 0
    assert report["safeAutomation"]["autoDemoted"] is True
    assert report["safeAutomation"]["demoted"] == 1
    assert report["safeAutomation"]["applied"] == [
        {
            "id": loser_id,
            "familyKey": "static studio",
            "action": "auto_demote_static_normalized_url_alias",
        }
    ]


# ── shared base + journal-overlay scenarios ───────────────────────────────────
# Each scenario is a fresh URL twin delivered the way live discovery lands a
# new row: a seed base holding only the winner, then a jsonl delta appended to
# the journal. One parametrized test drives the full ``RegistryService`` load
# path for every analyzer shape, so future analyzer paths get the same
# base+journal coverage by adding one scenario row.

_OVERLAY_SCENARIOS = (
    "cross-family-url-twin",
    "protected-cross-family-url-twin",
    "same-family-static-alias",
    "protected-discovery-approve",
)

_OVERLAY_EXPECT: dict[str, dict[str, Any]] = {
    # Cross-family twins raise a canonicalize_careers_url ``url-twin:`` card.
    "cross-family-url-twin": {
        "demote": True,
        "family_prefix": "url-twin:",
        "clear_allowlist": True,
    },
    # Discovery-auto-approved wins over the url-twin demote path too.
    "protected-cross-family-url-twin": {
        "demote": False,
        "family_prefix": "url-twin:",
        "clear_allowlist": True,
    },
    # Same-family www/apex aliases heal via the normalized-alias analyzer.
    "same-family-static-alias": {
        "demote": True,
        "family_prefix": "",
        "clear_allowlist": False,
    },
    # Discovery-auto-approved rows are protected from load-time auto-demotion.
    "protected-discovery-approve": {
        "demote": False,
        "family_prefix": "",
        "clear_allowlist": False,
    },
}


@pytest.mark.parametrize("scenario_id", _OVERLAY_SCENARIOS)
def test_journal_overlay_twin_heals_on_load(
    tmp_path: Path, monkeypatch: Any, scenario_id: str
) -> None:
    """End-to-end: a plain registry load over a seed base + jsonl delta overlay
    resolves whichever twin shape discovery delivered and heals it with no
    operator action -- cross-family ``url-twin:`` pairs and same-family
    normalized-alias pairs are auto-demoted, while discovery-auto-approved
    twins are kept untouched (even when the demote path would otherwise fire,
    proving the protected_ids guard wins over url-twin automation).
    """
    winner, twin = _overlay_twin_rows(scenario_id)
    expected = _OVERLAY_EXPECT[scenario_id]
    if expected.get("clear_allowlist"):
        # The canonical-URL allowlist must not suppress this fresh collision.
        monkeypatch.setattr("src.source_registry_policy.known_twin_career_urls", lambda: set())
    active_path, pending_path, rejected_path = _registry_with_journal_overlay(
        tmp_path, winner, twin
    )
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )
    state = service.load_state()
    report = service.get_auto_heal_report()

    if expected["demote"]:
        assert [row["id"] for row in state["active"]] == [winner["id"]]
        assert [row["id"] for row in state["pending"]] == [twin["id"]]
        assert state["pending"][0]["pendingReason"] == "registry_conflict_safe_auto_demote"
        assert state["pending"][0]["stateChangedBy"] == "registry_conflict_safe_auto_demote"
        assert report["safeAutomation"]["autoDemoted"] is True
        applied = report["safeAutomation"]["applied"]
        assert applied and applied[0]["id"] == twin["id"]
        assert applied[0]["action"] == "auto_demote_static_normalized_url_alias"
        family_key = str(applied[0].get("familyKey") or "")
        if expected["family_prefix"]:
            assert family_key.startswith(expected["family_prefix"])
        else:
            assert not family_key.startswith("url-twin:")
        # The persisted store converged too -- only the ordinary load acted.
        assert [row["id"] for row in sr.load_json_array(active_path, [])] == [winner["id"]]
        assert [row["id"] for row in sr.load_json_array(pending_path, [])] == [twin["id"]]
    else:
        # Protected: the discovery-auto-approved twin survives the load.
        assert [row["id"] for row in state["active"]] == [winner["id"], twin["id"]]
        assert state["pending"] == []
        assert report["safeAutomation"]["autoDemoted"] is False
        assert report["safeAutomation"]["skippedRows"] == [
            {"id": twin["id"], "reason": "protected_from_load_time_safe_auto_demote"}
        ]


def test_overlay_auto_demote_compacts_journal_to_single_record(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """After an overlay auto-demote, long-running registries must not grow
    unbounded: repeated converged writes with a compact threshold that a single
    record fits under keep each journal to exactly one folded record (never an
    accumulating delta tail), and the folded base stays loadable with the
    auto-demoted state preserved.
    """
    from src.source_registry_io_journal import _registry_journal_record_text
    from src.source_registry_io_load import _load_json_array_from_storage

    winner, twin = _overlay_twin_rows("same-family-static-alias")
    active_path, pending_path, rejected_path = _registry_with_journal_overlay(
        tmp_path, winner, twin
    )
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )
    service.load_state()  # converge: auto-demote the twin

    active_journal = active_path.with_suffix(".jsonl")
    pending_journal = pending_path.with_suffix(".jsonl")
    active_image = len(_registry_journal_record_text(active_path, [winner]).encode("utf-8"))
    pending_image = len(_registry_journal_record_text(pending_path, [twin]).encode("utf-8"))
    compact_threshold = max(active_image, pending_image) + 1
    monkeypatch.setattr("src.source_registry_io._JSON_JOURNAL_COMPACT_MAX_BYTES", compact_threshold)
    converged = {"active": [winner], "pending": [twin], "rejected": []}
    for _ in range(5):
        sr.save_registry_state_atomic(active_path, pending_path, rejected_path, converged)

    for journal in (active_journal, pending_journal):
        lines = journal.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1, f"journal accumulated {len(lines)} records"
        record = json.loads(lines[0])
        assert record["kind"] == "array_delta"  # single folded snapshot record
        assert journal.stat().st_size <= compact_threshold

    # The compacted journal + folded base still load the converged state, and
    # the folded base file itself parses as a valid row array.
    assert [row["id"] for row in sr.load_json_array(active_path, [])] == [winner["id"]]
    assert [row["id"] for row in sr.load_json_array(pending_path, [])] == [twin["id"]]
    base_active = _load_json_array_from_storage(active_path, [])
    assert base_active is not None
    assert [row["id"] for row in base_active] == [winner["id"]]


def _static_overlay_row(
    row_id: str,
    *,
    studio: str,
    rank_score: int,
    protected: bool = False,
    listing_url: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": row_id,
        "name": studio,
        "studio": studio,
        "adapter": "static",
        "registryState": "active",
        "jobsFound": 3,
        "rankScore": rank_score,
        "score": rank_score // 2,
    }
    if listing_url is not None:
        row["listing_url"] = listing_url
    if protected:
        row["stateChangedBy"] = "discovery_auto_approve"
        row["approvedBy"] = "registry_migration_v2"
    return row


def _overlay_twin_rows(scenario_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    winner_id = "static:listing_url:https://studio.example/careers"
    twin_id = "static:listing_url:https://www.studio.example/careers"
    if scenario_id == "cross-family-url-twin":
        winner = _static_overlay_row(
            winner_id,
            studio="Studio Example",
            rank_score=40,
            listing_url="https://studio.example/careers",
        )
        twin = _static_overlay_row(
            twin_id,
            studio="Studio Example Twin",
            rank_score=20,
            listing_url="https://www.studio.example/careers",
        )
    elif scenario_id == "protected-cross-family-url-twin":
        winner = _static_overlay_row(
            winner_id,
            studio="Studio Example",
            rank_score=40,
            listing_url="https://studio.example/careers",
        )
        twin = _static_overlay_row(
            twin_id,
            studio="Studio Example Twin",
            rank_score=20,
            listing_url="https://www.studio.example/careers",
            protected=True,
        )
    elif scenario_id == "protected-discovery-approve":
        winner = _static_overlay_row(winner_id, studio="Static Studio", rank_score=40)
        twin = _static_overlay_row(twin_id, studio="Static Studio", rank_score=20, protected=True)
    else:  # same-family-static-alias
        winner = _static_overlay_row(winner_id, studio="Static Studio", rank_score=40)
        twin = _static_overlay_row(twin_id, studio="Static Studio", rank_score=20)
    return winner, twin


def _registry_with_journal_overlay(
    tmp_path: Path, winner: dict[str, Any], twin: dict[str, Any]
) -> tuple[Path, Path, Path]:
    """Seed base with only the winner, then the twin appended via the jsonl
    delta overlay (exactly how live discovery lands a new row); asserts the
    overlay is live before returning the registry paths."""
    from src import source_registry_io as _srio

    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_registry_state_atomic(
        active_path,
        pending_path,
        rejected_path,
        {"active": [winner], "pending": [], "rejected": []},
    )
    assert [row["id"] for row in sr.load_json_array(active_path, [])] == [winner["id"]]
    _srio._append_json_journal_record(active_path, [winner, twin])
    assert [row["id"] for row in sr.load_json_array(active_path, [])] == [
        winner["id"],
        twin["id"],
    ]
    return active_path, pending_path, rejected_path


def test_registry_service_load_does_not_auto_demote_discovery_auto_approved_row(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://studio.example/careers"
    loser_id = "static:listing_url:https://www.studio.example/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 3,
                "rankScore": 40,
                "score": 20,
            },
            {
                "id": loser_id,
                "name": "Static Studio",
                "studio": "Static Studio",
                "adapter": "static",
                "registryState": "active",
                "stateChangedBy": "discovery_auto_approve",
                "approvedBy": "registry_migration_v2",
                "jobsFound": 3,
                "rankScore": 20,
                "score": 10,
            },
        ],
    )
    _write_json(pending_path, [])
    _write_json(rejected_path, [])
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )

    state = service.load_state()
    report = service.get_auto_heal_report()

    assert [row["id"] for row in state["active"]] == [winner_id, loser_id]
    assert state["pending"] == []
    assert report["safeAutomation"]["autoDemoted"] is False
    assert report["safeAutomation"]["demoted"] == 0
    assert report["safeAutomation"]["skippedRows"] == [
        {
            "id": loser_id,
            "reason": "protected_from_load_time_safe_auto_demote",
        }
    ]


def test_registry_service_auto_demotes_safe_static_listing_variant_on_load(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    winner_id = "static:listing_url:https://www.rockstargames.com/careers/openings"
    loser_id = "static:listing_url:https://www.rockstargames.com/careers"
    _write_json(
        active_path,
        [
            {
                "id": winner_id,
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 13,
                "rankScore": 60,
                "score": 27,
            },
            {
                "id": loser_id,
                "name": "Rockstar Games",
                "studio": "Rockstar Games",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 1,
                "rankScore": 10,
                "score": 7,
            },
        ],
    )
    _write_json(pending_path, [])
    _write_json(rejected_path, [])
    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
    )

    state = service.load_state()
    report = service.get_auto_heal_report()

    assert [row["id"] for row in state["active"]] == [winner_id]
    assert [row["id"] for row in state["pending"]] == [loser_id]
    assert report["safeAutomation"]["demoted"] == 1
    assert report["safeAutomation"]["applied"] == [
        {
            "id": loser_id,
            "familyKey": "rockstar games",
            "action": "auto_demote_static_same_host_listing_variant",
        }
    ]


def test_registry_service_shadow_writes_sqlite_projection(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    state = {
        "active": [{"id": "active", "name": "Active", "adapter": "static"}],
        "pending": [{"id": "pending", "name": "Pending", "adapter": "lever"}],
        "rejected": [],
    }
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "shadow", reason="test-shadow")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        normalized = service.persist_state(state)
        runtime = SourceRegistryRuntimeStore(store)
        health = get_storage_health_payload(tmp_path)["storage"]

        assert store.get_authority_modes()["sourceRegistry"] == "shadow"
        assert runtime.current_state() == normalized
        assert [
            row["code"] for row in health["diagnostics"] if row["surface"] == "sourceRegistry"
        ] == ["source_registry_projection_match"]
    finally:
        close_storage_stores()


def test_registry_service_shadow_projection_mismatch_rolls_back_to_json(tmp_path: Path) -> None:
    class _FakeAuthorityStore:
        def __init__(self) -> None:
            self.mode = "shadow"

        def get_authority_modes(self) -> dict[str, str]:
            return {"sourceRegistry": self.mode}

        def set_authority_mode(self, surface: str, mode: str, *, reason: str = "") -> None:
            assert surface == "sourceRegistry"
            assert reason == "source_registry_projection_mismatch"
            self.mode = mode

    class _FakeRuntimeStore:
        def __init__(self) -> None:
            self.store = _FakeAuthorityStore()

        def replace_state(self, **_: Any) -> Any:
            return type(
                "Summary",
                (),
                {
                    "generation": "bad",
                    "to_dict": lambda _self: {"generation": "bad"},
                },
            )()

        def parity_hash(self) -> dict[str, str]:
            return {"stateHash": "bad", "tombstoneHash": "bad"}

        def cleanup_old_generations(self) -> int:
            return 0

    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    fake_runtime = _FakeRuntimeStore()
    diagnostics: list[dict[str, Any]] = []

    def _record_diagnostic(_data_dir: Path, **kwargs: Any) -> None:
        diagnostics.append(dict(kwargs))

    service = RegistryService(
        paths=RegistryPaths(
            active=active_path,
            pending=pending_path,
            rejected=rejected_path,
        ),
        default_active=[],
        normalize_manual_static=lambda row: row,
        runtime_store_factory=lambda: cast(SourceRegistryRuntimeStore, fake_runtime),
        record_storage_diagnostic=_record_diagnostic,
    )

    service.persist_state({"active": [{"id": "a", "name": "A"}], "pending": [], "rejected": []})

    assert fake_runtime.store.mode == "json"
    assert diagnostics[-1]["code"] == "source_registry_projection_mismatch"
    assert diagnostics[-1]["ok"] is False


def test_registry_service_sqlite_authority_seeds_and_exports_json(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    _write_json(active_path, [{"id": "a", "name": "Active", "adapter": "static"}])
    _write_json(pending_path, [])
    _write_json(rejected_path, [])
    try:
        store = get_storage_store(tmp_path)
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        state = service.load_state()
        runtime = SourceRegistryRuntimeStore(store)

        assert store.get_authority_modes()["sourceRegistry"] == "sqlite"
        assert runtime.current_state() == state
        assert [row["id"] for row in sr.load_json_array(active_path, [])] == ["a"]
        assert any(
            row["code"] == "source_registry_seeded_from_json"
            for row in get_storage_health_payload(tmp_path)["storage"]["diagnostics"]
        )
    finally:
        close_storage_stores()


def test_registry_service_sqlite_tombstones_update_generation_and_export_json(
    tmp_path: Path,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    try:
        store = get_storage_store(tmp_path)
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[{"id": "a", "name": "Active", "adapter": "static"}],
            normalize_manual_static=lambda row: row,
        )
        service.load_state()

        saved = service.save_tombstones(
            {"a": {"sourceId": "a", "reason": "manual", "bucket": "active"}}
        )

        runtime = SourceRegistryRuntimeStore(store)
        assert runtime.current_tombstones() == saved
        assert sr.load_json_object(tmp_path / "source-registry-tombstones.json", {}) == saved
    finally:
        close_storage_stores()


def test_registry_service_sqlite_json_drift_rolls_back_to_json(tmp_path: Path) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    try:
        store = get_storage_store(tmp_path)
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[{"id": "a", "name": "Active", "adapter": "static"}],
            normalize_manual_static=lambda row: row,
        )
        service.load_state()
        sr.save_json_atomic(
            active_path,
            [{"id": "json-only", "name": "JSON", "adapter": "static"}],
        )

        state = service.load_state()

        assert store.get_authority_modes()["sourceRegistry"] == "json"
        assert [row["id"] for row in state["active"]] == ["json-only"]
        assert any(
            row["code"] == "source_registry_json_sqlite_mismatch"
            for row in get_storage_health_payload(tmp_path)["storage"]["diagnostics"]
        )
    finally:
        close_storage_stores()


def test_registry_service_json_authority_summary_avoids_full_normalization(
    tmp_path: Path, monkeypatch: Any
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(active_path, [{"id": "active-1", "name": "Active", "adapter": "static"}])
    sr.save_json_atomic(
        pending_path,
        [
            {"id": "pending-1", "name": "Pending", "adapter": "greenhouse"},
            {"id": "pending-2", "name": "Pending 2", "adapter": "lever"},
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    def fail_safe_demotions(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("summary computed full conflict demotions")

    monkeypatch.setattr(
        registry_service_module,
        "apply_registry_conflict_safe_demotions",
        fail_safe_demotions,
    )
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-authority")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        summary = service.get_summary_payload()

        assert summary["generation"] == ""
        assert summary["reason"] == "json_summary"
        assert summary["summaryExact"] is False
        assert summary["activeCount"] == 1
        assert summary["pendingCount"] == 2
        assert summary["rejectedCount"] == 1
        assert summary["countBasis"] == "storage"
        assert summary["stateFingerprint"]
    finally:
        close_storage_stores()


def test_registry_service_exact_summary_uses_normalized_rows_without_saving(
    tmp_path: Path, monkeypatch: Any
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(
        active_path,
        [{"id": "active-1", "name": "Active", "adapter": "static"}],
    )
    sr.save_json_atomic(
        pending_path,
        [
            {
                "id": "pending-hidden",
                "name": "Hidden",
                "adapter": "greenhouse",
                "hiddenFromDefault": True,
                "deferReason": "cap",
            },
            {
                "id": "pending-duplicate",
                "name": "Duplicate",
                "adapter": "lever",
                "pendingReason": "duplicate source",
            },
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    def fail_save(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("exact summary must not persist registry state")

    monkeypatch.setattr(registry_service_module, "save_json_atomic", fail_save)
    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-authority")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        summary = service.get_exact_summary_payload()

        assert summary["summaryExact"] is True
        assert summary["countBasis"] == "normalized"
        assert summary["activeCount"] == 1
        assert summary["pendingCount"] == 2
        assert summary["rejectedCount"] == 1
        assert summary["hiddenPendingCount"] == 1
        assert summary["deferredPendingCount"] == 1
        assert summary["duplicatePendingCount"] == 1
        assert summary["invalidRowsCount"] == 0
        assert summary["stateHash"]
        assert summary["stateFingerprint"] == summary["stateHash"]
        assert "storage" not in summary
    finally:
        close_storage_stores()


def test_registry_service_json_authority_compact_table_serves_rows(tmp_path: Path) -> None:
    """JSON authority must serve real compact rows, not a degraded-empty stub.

    The Admin startup lane always requests /registry/sources?detail=summary,
    which routes to get_compact_table_payload; the old json-mode stub returned
    degraded + zero rows, leaving source tables stuck on "refreshing" forever.
    """
    active_path = tmp_path / "source-registry-active.json"
    pending_path = tmp_path / "source-registry-pending.json"
    rejected_path = tmp_path / "source-registry-rejected.json"
    sr.save_json_atomic(
        active_path,
        [{"id": f"active-{i}", "name": f"Active {i}", "adapter": "static"} for i in range(3)],
    )
    sr.save_json_atomic(
        pending_path,
        [
            {"id": "pending-1", "name": "Pending", "adapter": "greenhouse"},
            {
                "id": "pending-hidden",
                "name": "Hidden",
                "adapter": "lever",
                "hiddenFromDefault": True,
            },
        ],
    )
    sr.save_json_atomic(rejected_path, [{"id": "rejected-1", "name": "Rejected"}])

    try:
        store = get_storage_store(tmp_path)
        store.set_authority_mode("sourceRegistry", "json", reason="test-json-compact")
        service = RegistryService(
            paths=RegistryPaths(
                active=active_path,
                pending=pending_path,
                rejected=rejected_path,
            ),
            default_active=[],
            normalize_manual_static=lambda row: row,
        )

        payload = service.get_compact_table_payload(
            buckets=["pending", "active", "rejected"],
            limit_per_bucket=2,
            include_hidden_pending=False,
        )

        assert payload["ok"] is True
        assert "degraded" not in payload
        assert payload["source"] == "registry-json-table"
        # Hidden pending filtered by default; active limited to the bucket cap.
        assert [row["id"] for row in payload["sources"]["pending"]] == ["pending-1"]
        assert len(payload["sources"]["active"]) == 2
        assert len(payload["sources"]["rejected"]) == 1
        # Summary counts stay exact regardless of the per-bucket row limit.
        assert payload["summary"]["activeCount"] == 3
        assert payload["summary"]["pendingCount"] == 2
        assert payload["summary"]["rejectedCount"] == 1
        assert payload["summary"]["countBasis"] == "normalized"
        assert payload["summary"]["tableLimitPerBucket"] == 2

        hidden_payload = service.get_compact_table_payload(
            buckets=["pending"],
            limit_per_bucket=5,
            include_hidden_pending=True,
        )
        assert len(hidden_payload["sources"]["pending"]) == 2
    finally:
        close_storage_stores()
