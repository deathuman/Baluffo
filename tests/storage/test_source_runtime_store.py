from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from src.storage import BaluffoStore, EvidenceArchiveStore, SourceRuntimeStore
from src.storage import evidence_archive as evidence_archive_mod
from tests.helpers.temp_paths import workspace_tmpdir


def test_source_runtime_store_bulk_upserts_and_queries_source_runs() -> None:
    with workspace_tmpdir("source-runtime-store") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T12:00:00+00:00",
                batch_size=1,
            )

            count = runtime.upsert_source_runs(
                run_id="fetch_1",
                rows=[
                    {
                        "name": "Studio A",
                        "status": "ok",
                        "adapter": "static",
                        "fetchStrategy": "http",
                        "studio": "Studio A",
                        "fetchedCount": 3,
                        "keptCount": 2,
                        "durationMs": 120,
                        "details": [{"url": "https://example.com/job/1"}],
                    },
                    {
                        "name": "Studio B",
                        "status": "error",
                        "adapter": "provider",
                        "error": "boom",
                    },
                ],
                evidence_ref={"path": "artifacts/fetch/fetch_1/source-details.json.gz"},
            )

            rows = runtime.source_runs(run_id="fetch_1")
            summary = runtime.source_run_summary(run_id="fetch_1")

            assert count == 2
            assert [row["name"] for row in rows] == ["Studio A", "Studio B"]
            assert rows[0]["details"] == [{"url": "https://example.com/job/1"}]
            assert rows[0]["evidenceRefs"]["path"].endswith("source-details.json.gz")
            assert rows[1]["failedCount"] == 1
            assert summary["rowCount"] == 2
            assert summary["successfulSources"] == 1
            assert summary["failedSources"] == 1


def test_evidence_archive_store_writes_manifest_and_enforces_budget() -> None:
    with workspace_tmpdir("evidence-archive-store") as data_dir:
        archive = EvidenceArchiveStore(
            data_dir,
            now_iso=lambda: "2026-05-12T12:00:00+00:00",
            total_budget_bytes=170,
            per_run_warning_bytes=1,
            retention_days=90,
        )

        first = archive.write_archive(
            run_id="fetch_1",
            kind="source-details",
            payload={"rows": [{"name": "Studio A"}]},
        )
        second = archive.write_archive(
            run_id="fetch_2",
            kind="source-details",
            payload={"rows": [{"name": "Studio B"}]},
        )
        manifest = archive.load_manifest()

        assert first["warning"] == "archive_size_warning"
        assert second["path"] == "artifacts/fetch/fetch_2/source-details.json.gz"
        assert len(manifest["archives"]) <= 2
        retained_paths = {row["path"] for row in manifest["archives"]}
        assert second["path"] in retained_paths
        retained_path = data_dir / second["path"]
        with gzip.open(retained_path, "rt", encoding="utf-8") as handle:
            assert json.load(handle)["rows"][0]["name"] == "Studio B"


def test_evidence_archive_atomic_write_retries_permission_error(monkeypatch) -> None:
    with workspace_tmpdir("evidence-archive-retry") as data_dir:
        archive = EvidenceArchiveStore(
            data_dir,
            now_iso=lambda: "2026-05-12T12:00:00+00:00",
        )
        real_replace = evidence_archive_mod.os.replace
        calls = {"count": 0}

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError("locked")
            return real_replace(src, dst)

        monkeypatch.setattr(evidence_archive_mod.os, "replace", flaky_replace)

        entry = archive.write_archive(
            run_id="fetch_1",
            kind="source-details",
            payload={"rows": [{"name": "Studio A"}]},
        )

        assert calls["count"] >= 2
        assert (data_dir / entry["path"]).exists()
        assert archive.manifest_path.exists()
        assert list(data_dir.glob("*.tmp")) == []


def test_source_runtime_store_validation_fallbacks_filters_and_offsets() -> None:
    with workspace_tmpdir("source-runtime-store-edges") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = SourceRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T12:00:00+00:00",
                row_limit=2,
            )

            with pytest.raises(ValueError, match="source runs require runId"):
                runtime.upsert_source_runs(run_id="", rows=[])

            count = runtime.upsert_source_runs(
                run_id="fetch_edges",
                rows=[
                    {"name": "Studio A", "status": "ok", "keptCount": "2"},
                    {"id": "Source Id", "status": "excluded", "lowConfidenceDropped": "3"},
                    {"status": "", "error": "missing name"},
                    object(),
                ],
                evidence_ref={"path": "artifact.json"},
            )

            assert count == 3
            first_page = runtime.source_runs(run_id="fetch_edges", limit=2)
            second_page = runtime.source_runs(run_id="fetch_edges", offset=2)
            assert [row["sourceKey"] for row in first_page] == ["studio_a", "source_id"]
            assert second_page[0]["sourceKey"] == "source_3"
            assert second_page[0]["name"] == "source_3"
            assert second_page[0]["status"] == "error"
            assert second_page[0]["failedCount"] == 1
            assert (
                runtime.source_runs(run_id="fetch_edges", status="excluded")[0][
                    "lowConfidenceDropped"
                ]
                == 3
            )
            assert runtime.source_run_summary(run_id="fetch_edges")["statusCounts"] == {
                "error": 1,
                "excluded": 1,
                "ok": 1,
            }


def test_evidence_archive_manifest_fallback_active_retention_and_path_safety() -> None:
    with workspace_tmpdir("evidence-archive-edges") as data_dir:
        archive = EvidenceArchiveStore(
            data_dir,
            now_iso=lambda: "2026-05-12T12:00:00+00:00",
            total_budget_bytes=1,
            retention_days=1,
        )
        archive.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        archive.manifest_path.write_text("{not-json", encoding="utf-8")

        assert archive.load_manifest() == {
            "schemaVersion": 1,
            "updatedAt": "",
            "archives": [],
        }

        active = archive.write_archive(
            run_id="fetch/active",
            kind="source details",
            payload={"rows": ["active"]},
            active_run_ids=("fetch/active",),
        )
        pinned = archive.write_archive(
            run_id="fetch_old",
            kind="source-details",
            payload={"rows": ["pinned"]},
            pinned=True,
        )
        archive.manifest_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "archives": [
                        {
                            **active,
                            "createdAt": "2026-05-12T12:00:00+00:00",
                        },
                        {
                            **pinned,
                            "createdAt": "2026-05-10T12:00:00+00:00",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

        result = archive.enforce_retention(active_run_ids=("fetch/active",))
        retained = {row["runId"] for row in archive.load_manifest()["archives"]}
        assert result["deletedCount"] == 0
        assert retained == {"fetch/active", "fetch_old"}
        assert active["path"] == "artifacts/fetch/fetch_active/source_details.json.gz"

        with pytest.raises(ValueError, match="must be relative"):
            archive._resolve_archive_path(Path(data_dir / "absolute.json.gz"))
        with pytest.raises(ValueError, match="escapes data dir"):
            archive._resolve_archive_path(Path("..") / "escape.json.gz")
