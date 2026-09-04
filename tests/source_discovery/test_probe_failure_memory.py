"""Tests for the bounded probe-failure memory store."""

# ruff: noqa: F401
import json
from pathlib import Path
from unittest import mock

from src.source_discovery import probe_failure_memory as pfm

from ._helpers import workspace_tmpdir


def test_classify_probe_failure_class_buckets() -> None:
    assert pfm.classify_probe_failure_class("[Errno -2] Name or service not known") == "dns"
    assert pfm.classify_probe_failure_class("Name or service not known in resolve") == "dns"
    assert pfm.classify_probe_failure_class("certificate verify failed: self-signed") == "ssl"
    assert pfm.classify_probe_failure_class("sslv3 alert handshake failure") == "ssl"
    assert pfm.classify_probe_failure_class("sslv3_alert handshake failure") == "ssl"
    assert pfm.classify_probe_failure_class("Client error '404 Not Found' for url") == "4xx"
    assert pfm.classify_probe_failure_class("HTTP Error 404: Not Found") == "4xx"
    assert pfm.classify_probe_failure_class("Connection reset by peer") == "other"
    assert pfm.classify_probe_failure_class("read operation timed out") == "other"
    assert pfm.classify_probe_failure_class("") == "other"


def test_is_quarantine_class_only_allows_dns_and_ssl() -> None:
    assert pfm.is_quarantine_class("dns")
    assert pfm.is_quarantine_class("ssl")
    assert not pfm.is_quarantine_class("4xx")
    assert not pfm.is_quarantine_class("other")
    assert not pfm.is_quarantine_class("")


def test_record_failure_counts_consecutive_and_resets_on_class_change() -> None:
    with workspace_tmpdir("pfm-counter") as root:
        memory = pfm.ProbeFailureMemory(root / "store.json")

        assert memory.record_failure(identity="a", error="Name or service not known") is None
        assert memory.record_failure(identity="a", error="Name or service not known") is None
        # Class change resets the consecutive counter.
        assert memory.record_failure(identity="a", error="HTTP Error 500: boom") is None

        memory.flush()
        store = json.loads((root / "store.json").read_text(encoding="utf-8"))
        record = store["a"]
        assert record["failureClass"] == "other"
        assert record["consecutiveCount"] == 1
        assert "quarantinedUntil" not in record
        assert record["lastError"] == "HTTP Error 500: boom"


def test_quarantine_starts_after_threshold_and_gates_next_run() -> None:
    with workspace_tmpdir("pfm-quarantine") as root:
        memory = pfm.ProbeFailureMemory(root / "store.json")

        assert memory.record_failure(identity="a", error="Name or service not known") is None
        assert memory.record_failure(identity="a", error="Name or service not known") is None
        started = memory.record_failure(identity="a", error="Name or service not known")
        assert started is not None
        assert started["consecutiveCount"] == 3
        assert started["failureClass"] == "dns"
        assert started.get("quarantinedUntil")

        memory.flush()

        # A fresh run's memory (loaded from the flushed store) gates the identity.
        next_run = pfm.ProbeFailureMemory(root / "store.json")
        index = next_run.quarantine_index()
        assert set(index) == {"a"}
        assert index["a"]["failureClass"] == "dns"

        # Transient classes never quarantine even at high counts.
        for _ in range(5):
            assert next_run.record_failure(identity="b", error="Connection reset") is None
        assert "b" not in next_run.quarantine_index()


def test_expired_quarantine_reactivates_on_next_failure() -> None:
    with workspace_tmpdir("pfm-expiry") as root:
        store_path = root / "store.json"
        store_path.write_text(
            json.dumps(
                {
                    "stale": {
                        "failureClass": "dns",
                        "consecutiveCount": 3,
                        "lastError": "Name or service not known",
                        "lastFailureAt": "2020-01-01T00:00:00+00:00",
                        "quarantinedUntil": "2020-01-05T00:00:00+00:00",
                    },
                    "recent": {
                        "failureClass": "ssl",
                        "consecutiveCount": 3,
                        "lastError": "certificate verify failed",
                        "lastFailureAt": pfm.now_iso(),
                        "quarantinedUntil": "2000-01-01T00:00:00+00:00",
                    },
                }
            ),
            encoding="utf-8",
        )

        # The stale record is pruned outright; the recent one survives with an
        # expired quarantine, so one more failure re-quarantines it.
        memory = pfm.ProbeFailureMemory(store_path)
        assert "stale" not in memory.quarantine_index()
        assert "recent" not in memory.quarantine_index()

        restarted = memory.record_failure(identity="recent", error="certificate verify failed")
        assert restarted is not None
        assert restarted["consecutiveCount"] == 4
        assert restarted["quarantinedUntil"] > "2000-01-01T00:00:00+00:00"


def test_flush_prunes_malformed_and_bounded_entries() -> None:
    with workspace_tmpdir("pfm-flush") as root:
        memory = pfm.ProbeFailureMemory(root / "store.json")
        memory.record_failure(identity="keep-newest", error="Name or service not known")
        memory.record_failure(identity="keep-older", error="certificate verify failed")
        memory._records["malformed"] = "not-a-dict"  # type: ignore[assignment]
        memory._records["classless"] = {"consecutiveCount": 2}

        stats = memory.flush()

        assert stats["entries"] == 2
        assert stats["droppedOverQuota"] == 0
        store = json.loads((root / "store.json").read_text(encoding="utf-8"))
        assert set(store) == {"keep-newest", "keep-older"}


def test_success_clears_failure_record() -> None:
    with workspace_tmpdir("pfm-clear") as root:
        memory = pfm.ProbeFailureMemory(root / "store.json")
        memory.record_failure(identity="a", error="Name or service not known")
        memory.clear_identity("a")
        memory.record_failure(identity="b", error="certificate verify failed")
        memory.flush()

        store = json.loads((root / "store.json").read_text(encoding="utf-8"))
        assert "a" not in store
        assert "b" in store


def test_config_backed_threshold_and_retention_are_positive() -> None:
    assert pfm.quarantine_threshold() >= 1
    assert pfm.memory_retention_days() >= 1

    from src.source_discovery import config as discovery_config_module

    with mock.patch.object(discovery_config_module, "PROBE_FAILURE_QUARANTINE_THRESHOLD", 7):
        assert pfm.quarantine_threshold() == 7
    with mock.patch.object(discovery_config_module, "PROBE_FAILURE_MEMORY_RETENTION_DAYS", 9):
        assert pfm.memory_retention_days() == 9
