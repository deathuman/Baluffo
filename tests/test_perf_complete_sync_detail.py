from __future__ import annotations

from scripts import perf_complete


def test_sync_detail_summary_extracts_nested_push_stages() -> None:
    summary = {
        "reportPath": "sync-report.json",
        "pushTiming": {
            "totalDurationMs": 111,
            "stageTotalsMs": {"pushRemote": 100},
            "detailTiming": {
                "stageTotalsMs": {"writeShardedSnapshot": 80, "readRemoteSnapshot": 20},
            },
            "remoteTiming": {
                "requestCount": 3,
                "operationTotalsMs": {"pushShard": 40, "pushManifest": 30},
                "slowestRequests": [
                    {
                        "operation": "pushShard",
                        "method": "PUT",
                        "path": "baluffo/source-sync/shards/active/a/hash.json.gz",
                        "durationMs": 40,
                    }
                ],
            },
        },
    }

    detail = perf_complete.build_sync_detail_summary(summary)

    assert detail["available"] is True
    assert detail["stageTop"][0] == {"stage": "writeShardedSnapshot", "durationMs": 80}
    assert detail["stageTotalsMs"]["readRemoteSnapshot"] == 20
    assert detail["remoteTimingAvailable"] is True
    assert detail["remoteRequestCount"] == 3
    assert detail["remoteOperationTop"][0] == {"operation": "pushShard", "durationMs": 40}
    assert detail["remoteSlowestRequests"][0]["method"] == "PUT"
