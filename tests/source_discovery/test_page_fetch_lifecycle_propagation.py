from __future__ import annotations

import json
import threading
import time

from src.shared.page_fetch_lifecycle import PageFetchLifecycle
from src.source_discovery import gamedevmap_artifact_store
from src.source_discovery.directory_fetch import fetch_directory_pages
from src.source_discovery.directory_page_recovery import (
    apply_recovery_to_scan_result,
    merge_scan_result_payloads,
)

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config,
    gamedevmap_fetcher,
    gamedevmap_payloads,
)


def test_gamedevmap_page_fetch_receipt_round_trip() -> None:
    with workspace_tmpdir("gamedevmap-page-fetch-receipt") as root:
        output_path = root / "dry-run.json"
        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=gamedevmap_config(activeAuditRecoveryEscalationEnabled=False),
            fetcher=gamedevmap_fetcher(gamedevmap_payloads(), []),
            output_path=output_path,
            batch_size=2,
            max_batches=1,
            reset=True,
        )

        saved = json.loads(output_path.read_text(encoding="utf-8"))
        assert output == saved
        receipts = saved["summary"]["pageFetchLifecycle"]
        assert receipts
        assert all(isinstance(receipt, dict) and receipt for receipt in receipts)

        gamedevmap_artifact_store._summarize_artifact(
            output,
            parsed_rows=[],
            representative_rows=[],
            completed_urls=set(),
            page_fetch_lifecycles=[],
        )
        assert output["summary"]["pageFetchLifecycle"] == receipts


def test_legacy_recovery_without_lifecycle_attribute_does_not_add_receipt() -> None:
    legacy_recovery = type("LegacyRecovery", (), {})()

    result = apply_recovery_to_scan_result({"untouched": True}, legacy_recovery)

    assert result["untouched"] is True
    assert not any("pagefetchlifecycle" in key.lower().replace("_", "") for key in result)


def test_scan_result_merge_omits_empty_page_fetch_receipts() -> None:
    inputs: list[list[dict[str, object]]] = [[{}], [{"pageFetchLifecycle": []}], []]
    for results in inputs:
        assert "pageFetchLifecycle" not in merge_scan_result_payloads(results)


def test_directory_fetch_lifecycle_keeps_abandoned_sync_worker_visible() -> None:
    worker_started = threading.Event()
    worker_gate = threading.Event()
    lifecycle = PageFetchLifecycle(batch_id="directory-abandoned", source="directory-test")

    def blocked_fetcher(_url: str, _timeout_s: float) -> str:
        worker_started.set()
        worker_gate.wait(timeout=5)
        return "<html></html>"

    try:
        fetch_directory_pages(
            1,
            [{"url": "https://directory.example.test/jobs"}],
            total_concurrency=1,
            per_host_concurrency=1,
            progress_label="test",
            emit_progress_log=False,
            fetcher=blocked_fetcher,
            page_fetch_lifecycle=lifecycle,
        )
        assert worker_started.wait(timeout=1)
        assert lifecycle.snapshot()["outstandingNow"] == 1
    finally:
        worker_gate.set()
        settle_by = time.monotonic() + 2
        while lifecycle.snapshot()["outstandingNow"] and time.monotonic() < settle_by:
            time.sleep(0.01)


def test_scan_result_merge_preserves_deduplicated_page_fetch_receipt_identities() -> None:
    first = {"schemaVersion": 1, "receiptType": "pageFetchLifecycle", "batchId": "batch-a"}
    second = {"schemaVersion": 1, "receiptType": "pageFetchLifecycle", "batchId": "batch-b"}

    merged = merge_scan_result_payloads(
        [
            {"pageFetchLifecycle": [first], "batchTiming": {"fetchMs": 3}},
            {"pageFetchLifecycle": [second, first], "batchTiming": {"fetchMs": 5}},
        ]
    )

    assert merged["pageFetchLifecycle"] == [first, second]
    assert all(isinstance(value, int) for value in merged["batchTiming"].values())
