"""End-to-end discovery-run tests for probe-failure quarantine gating."""

# ruff: noqa: F401
import json

from src.url_hosts import url_host

from ._helpers import (
    GENERATOR_DISABLED_DISCOVERY_CONFIG,
    override_discovery_runtime,
    sd,
    workspace_tmpdir,
)


def _static_candidate(name: str, url: str, *, score: int = 60) -> dict:
    return {
        "name": name,
        "studio": name,
        "adapter": "static",
        "score": score,
        "evidenceScore": 40,
        "listing_url": url,
        "pages": [url],
    }


def _run_discovery(root, fetcher, *, candidates: list[dict]) -> dict:
    with override_discovery_runtime(
        root,
        studio_seeds=[],
        static_candidates=candidates,
        extra_config_overrides={"DISCOVERY_CONFIG_PATH": root / "missing-config.json"},
    ):
        return sd.run_discovery(
            timeout_s=5,
            top_n=0,
            mode="dynamic",
            include_web_search=False,
            discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
            fetcher=fetcher,
        )


def test_repeated_dns_failure_quarantines_candidate_on_next_run() -> None:
    dns_error = "[Errno -2] Name or service not known"

    def failing_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(dns_error)

    with workspace_tmpdir("pfm-e2e") as root:
        candidates = [_static_candidate("Dead Guess", "https://careers.dead-guess.example/jobs")]

        first = _run_discovery(root, failing_fetch, candidates=candidates)
        assert first["summary"].get("failedProbeCount") == 1
        assert int((first.get("suppressionSummary") or {}).get("probeQuarantinedCount") or 0) == 0
        # Threshold is 3: no quarantine starts on the first failure.
        assert (
            int((first.get("suppressionSummary") or {}).get("probeQuarantineStartedCount") or 0)
            == 0
        )

        # Quarantine starts only at threshold: the store holds the record now.
        store = json.loads((root / "source-discovery-probe-failures.json").read_text("utf-8"))
        identity = next(iter(store))
        assert store[identity]["consecutiveCount"] == 1
        assert "quarantinedUntil" not in store[identity]

        # Runs 2 and 3 re-probe; run 3 reaches the threshold and starts the
        # quarantine (recorded in the next run's suppression summary).
        second = _run_discovery(root, failing_fetch, candidates=candidates)
        assert second["summary"].get("failedProbeCount") == 1
        third = _run_discovery(root, failing_fetch, candidates=candidates)
        assert third["summary"].get("failedProbeCount") == 1
        assert (
            int((third.get("suppressionSummary") or {}).get("probeQuarantineStartedCount") or 0)
            == 1
        )

        # Run 4 hits the active quarantine: no probe, a probe_quarantined row.
        fourth = _run_discovery(root, failing_fetch, candidates=candidates)
        assert fourth["summary"].get("failedProbeCount") == 0
        suppressed = int((fourth.get("suppressionSummary") or {}).get("probeQuarantinedCount") or 0)
        assert suppressed == 1
        quarantined_rows = [
            row for row in fourth.get("failures") or [] if row.get("stage") == "probe_quarantined"
        ]
        assert len(quarantined_rows) == 1
        assert quarantined_rows[0]["dropReason"] == "dns"
        assert "probe quarantine active" in str(quarantined_rows[0].get("error"))
        assert fourth["summary"].get("probedCandidateCount") == 0


def test_quarantine_expires_and_probe_resumes() -> None:
    from unittest import mock

    from src.source_discovery import config as discovery_config_module

    failing = "[Errno -2] Name or service not known"

    def failing_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(failing)

    with workspace_tmpdir("pfm-expiry-e2e") as root:
        candidates = [_static_candidate("Dead Guess", "https://careers.dead-guess.example/jobs")]

        for _ in range(3):
            _run_discovery(root, failing_fetch, candidates=candidates)

        with mock.patch.object(discovery_config_module, "PROBE_FAILURE_MEMORY_RETENTION_DAYS", 1):
            # Backdate the record past its retention window; the next run must
            # prune it and probe again instead of quarantining.
            store_path = root / "source-discovery-probe-failures.json"
            store = json.loads(store_path.read_text("utf-8"))
            for record in store.values():
                record["lastFailureAt"] = "2020-01-01T00:00:00+00:00"
                record["quarantinedUntil"] = "2020-01-05T00:00:00+00:00"
            store_path.write_text(json.dumps(store), "utf-8")

            report = _run_discovery(root, failing_fetch, candidates=candidates)
            assert report["summary"].get("failedProbeCount") == 1
            assert (
                int((report.get("suppressionSummary") or {}).get("probeQuarantinedCount") or 0) == 0
            )
