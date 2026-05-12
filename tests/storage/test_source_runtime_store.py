from __future__ import annotations

import gzip
import json

from src.storage import BaluffoStore, EvidenceArchiveStore, SourceRuntimeStore
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
