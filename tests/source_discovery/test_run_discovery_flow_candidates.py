"""Tests for source discovery candidate queue behavior."""

# ruff: noqa: F401
from src.url_hosts import url_host

from ._helpers import (
    FIXTURES_DIR,
    GENERATOR_DISABLED_DISCOVERY_CONFIG,
    DiscoveryReportSummarySchema,
    Path,
    _directory_audit_result,
    _fixture_json,
    _fixture_text,
    _gamesmap_next_payload_html,
    discovery_config_module,
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    discovery_url_patches,
    json,
    mock,
    override_discovery_config,
    override_discovery_runtime,
    patch_empty_generator_stages,
    sd,
    sr,
    workspace_tmpdir,
)


def test_apply_sheet_directory_static_probe_cap_limits_overproducing_sheet_static_rows() -> None:
    candidates = [
        {
            "name": "Sheet Static Productive",
            "studio": "Productive",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://productive.example/jobs"],
        },
        {
            "name": "Sheet Static B",
            "studio": "B",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://b.example/jobs"],
        },
        {
            "name": "Sheet Static C",
            "studio": "C",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://c.example/jobs"],
        },
        {
            "name": "Sheet Static D",
            "studio": "D",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://d.example/jobs"],
        },
        {
            "name": "Sheet Static E",
            "studio": "E",
            "adapter": "static",
            "discoveryStage": "sheet_directory",
            "evidenceScore": 46,
            "jobsFound": 0,
            "pages": ["https://e.example/jobs"],
        },
        {
            "name": "Greenhouse A",
            "studio": "Greenhouse A",
            "adapter": "greenhouse",
            "discoveryStage": "provider_pattern",
            "evidenceScore": 70,
            "jobsFound": 0,
            "api_url": "https://boards-api.greenhouse.io/v1/boards/a/jobs?content=true",
        },
    ]
    kept, suppressed = sd.apply_sheet_directory_static_probe_cap(
        candidates,
        top_n=4,
        source_state_rows={
            "Sheet Static Productive": {
                "lastKeptCount": 3,
                "lastJobsFound": 5,
                "lastDurationMs": 1200,
            }
        },
    )
    assert len([row for row in kept if str(row.get("adapter")) == "static"]) == 4
    assert len(suppressed) == 1
    assert any(str(row.get("name")) == "Sheet Static Productive" for row in kept)


def test_discovery_report_snapshot_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(
            root,
            studio_seeds=[],
            static_candidates=[
                {
                    "name": "Demo Lever",
                    "studio": "Demo",
                    "adapter": "lever",
                    "account": "demo",
                    "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                },
                {
                    "name": "Demo Greenhouse",
                    "studio": "Demo",
                    "adapter": "greenhouse",
                    "slug": "demo",
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                },
            ],
        ):

            def fake_fetch(url: str, _: int) -> str:
                if url_host(url) == "api.lever.co":
                    return json.dumps([{"id": 1}, {"id": 2}])
                if url_host(url) == "boards-api.greenhouse.io":
                    return json.dumps({"jobs": [{}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                fetcher=fake_fetch,
            )
            DiscoveryReportSummarySchema.model_validate(report["summary"])
            snapshot = {
                "schemaVersion": report.get("schemaVersion"),
                "mode": str(report.get("mode")),
                "summary": {
                    "foundEndpointCount": int(report["summary"].get("foundEndpointCount") or 0),
                    "probedCandidateCount": int(report["summary"].get("probedCandidateCount") or 0),
                    "queuedCandidateCount": int(report["summary"].get("queuedCandidateCount") or 0),
                    "discoverableButDeferredCount": int(
                        report["summary"].get("discoverableButDeferredCount") or 0
                    ),
                    "failedProbeCount": int(report["summary"].get("failedProbeCount") or 0),
                },
                "counts": {
                    "candidates": len(report.get("candidates") or []),
                    "failures": len(report.get("failures") or []),
                },
                "adapterCounts": report["summary"].get("adapterCounts") or {},
                "methodCounts": report["summary"].get("methodCounts") or {},
                "generatedCountByStage": report["summary"].get("generatedCountByStage") or {},
            }
            assert snapshot == _fixture_json("source_discovery_report_snapshot.json")


def test_run_discovery_applies_existing_url_patches_before_probe() -> None:
    with workspace_tmpdir("source-discovery") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            discovery_url_patches.save_url_patch_manifest(
                {"https://old.example/jobs": "https://new.example/jobs"},
                path=paths.url_patch_manifest_path,
                added=1,
                updated=0,
                reprobed=0,
            )
            with override_discovery_config(
                studio_seeds=[],
                static_candidates=[
                    {
                        "name": "Patched Static",
                        "studio": "Patched Static",
                        "adapter": "static",
                        "listing_url": "https://old.example/jobs",
                        "pages": ["https://old.example/jobs"],
                        "evidenceScore": 52,
                        "evidenceTypes": ["seed_curated"],
                    }
                ],
            ):
                seen_urls = []

                def fake_fetch(url: str, _timeout: int) -> str:
                    seen_urls.append(url)
                    if url == "https://new.example/jobs":
                        return '<a href="https://new.example/jobs/role-1">Role</a>'
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=GENERATOR_DISABLED_DISCOVERY_CONFIG,
                    fetcher=fake_fetch,
                )
            assert "https://old.example/jobs" not in seen_urls
            assert report["summary"]["queuedCandidateCount"] == 1
            assert report["runtime"]["urlPatchStats"]["loaded"] == 1
            queued = json.loads(paths.discovery_candidates_path.read_text(encoding="utf-8"))
            assert queued[0]["listing_url"] == "https://new.example/jobs"


def test_run_discovery_balances_queue_with_deferrals() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_caps = dict(discovery_config_module.ADAPTER_QUEUE_CAPS)
        try:
            with override_discovery_runtime(
                root,
                studio_seeds=[],
                static_candidates=[
                    {
                        "name": "Demo Lever A",
                        "studio": "Demo A",
                        "adapter": "lever",
                        "account": "demoa",
                        "api_url": "https://api.lever.co/v0/postings/demoa?mode=json",
                    },
                    {
                        "name": "Demo Lever B",
                        "studio": "Demo B",
                        "adapter": "lever",
                        "account": "demob",
                        "api_url": "https://api.lever.co/v0/postings/demob?mode=json",
                    },
                ],
            ):
                discovery_config_module.ADAPTER_QUEUE_CAPS["lever"] = 1
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    fetcher=lambda *_: json.dumps([{"id": 1}, {"id": 2}]),
                )
                assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
                assert int(report["summary"].get("discoverableButDeferredCount") or 0) == 1
                assert (
                    int((report["summary"].get("lossAccounting") or {}).get("deferredByCap") or 0)
                    == 1
                )
                deferred = [
                    row for row in (report.get("candidates") or []) if bool(row.get("deferred"))
                ]
                assert len(deferred) == 1
                assert str(deferred[0].get("deferReason") or "") == "adapter_cap"
                assert str(deferred[0].get("dropStage") or "") == "deferred_by_cap"
                assert str(deferred[0].get("dropReason") or "") == "adapter_cap"
        finally:
            discovery_config_module.ADAPTER_QUEUE_CAPS.clear()
            discovery_config_module.ADAPTER_QUEUE_CAPS.update(prev_caps)
