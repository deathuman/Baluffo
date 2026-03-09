import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path

from scripts import source_discovery as sd


class SourceDiscoveryTests(unittest.TestCase):
    @contextmanager
    def workspace_tmpdir(self):
        root = Path(__file__).resolve().parents[1] / ".codex-test-tmp" / f"source-discovery-{uuid.uuid4().hex}"
        root.mkdir(parents=True, exist_ok=True)
        try:
            yield root
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def fixture_json(self, name: str):
        path = Path(__file__).parent / "fixtures" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_build_pattern_candidates_respects_likely_providers(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": True,
                "remoteFriendly": True,
                "likelyProviders": ["greenhouse", "teamtailor"],
            }
        ]
        try:
            rows = sd.build_pattern_candidates()
        finally:
            sd.STUDIO_SEEDS = previous

        adapters = {str(row.get("adapter")) for row in rows}
        self.assertEqual(adapters, {"greenhouse", "teamtailor"})

    def test_build_pattern_candidates_adds_reinforcement_for_provider_matching_careers_url(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "remoteFriendly": True,
                "likelyProviders": ["greenhouse"],
                "careersUrl": "https://boards.greenhouse.io/example-studio",
            }
        ]
        try:
            rows = sd.build_pattern_candidates()
        finally:
            sd.STUDIO_SEEDS = previous
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(all(int(row.get("evidenceScore") or 0) >= 42 for row in rows))
        self.assertTrue(all("provider_reinforced" in (row.get("evidenceTypes") or []) for row in rows))

    def test_probe_candidate_maps_jobs_found_for_greenhouse_and_teamtailor(self) -> None:
        greenhouse = {
            "adapter": "greenhouse",
            "slug": "example",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
        }
        ok, count, error = sd.probe_candidate(greenhouse, timeout_s=5, fetcher=lambda *_: json.dumps({"jobs": [{}, {}]}))
        self.assertTrue(ok)
        self.assertEqual(count, 2)
        self.assertEqual(error, "")

        teamtailor = {"adapter": "teamtailor", "listing_url": "https://example.teamtailor.com/jobs"}
        html = """
        <a href="https://example.teamtailor.com/jobs/123-role-a">A</a>
        <a href="https://example.teamtailor.com/jobs/456-role-b">B</a>
        """
        ok, count, error = sd.probe_candidate(teamtailor, timeout_s=5, fetcher=lambda *_: html)
        self.assertTrue(ok)
        self.assertEqual(count, 2)
        self.assertEqual(error, "")

    def test_probe_candidate_uses_fallback_when_primary_fails(self) -> None:
        greenhouse = {
            "adapter": "greenhouse",
            "slug": "example",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
        }

        def fake_fetch(url: str, _: int) -> str:
            if "boards-api.greenhouse.io" in url:
                raise RuntimeError("HTTP Error 404: Not Found")
            if "boards.greenhouse.io/example" in url:
                return '<a href="https://boards.greenhouse.io/example/jobs/123">Role</a>'
            raise RuntimeError(f"unexpected URL: {url}")

        ok, count, error = sd.probe_candidate(greenhouse, timeout_s=5, fetcher=fake_fetch)
        self.assertTrue(ok)
        self.assertEqual(count, 1)
        self.assertEqual(error, "")

    def test_validate_candidate_for_probe_rejects_invalid_identity(self) -> None:
        valid, reason = sd.validate_candidate_for_probe({"adapter": "lever", "account": "12"})
        self.assertFalse(valid)
        self.assertIn("invalid", reason)

    def test_infer_provider_candidates_from_html_detects_embedded_urls(self) -> None:
        html = """
        <a href="https://boards.greenhouse.io/example/jobs/123">Job</a>
        <script>const api='https://api.lever.co/v0/postings/example?mode=json';</script>
        """
        rows = sd.infer_provider_candidates_from_html(
            "https://example.com/careers",
            html,
            studio="Example Studio",
            nl_priority=False,
            remote_friendly=True,
        )
        adapters = {str(row.get("adapter") or "") for row in rows}
        self.assertIn("greenhouse", adapters)
        self.assertIn("lever", adapters)

    def test_infer_provider_candidates_from_html_detects_provider_from_page_url(self) -> None:
        rows = sd.infer_provider_candidates_from_html(
            "https://example.jobs.personio.de/",
            "<html><body>Careers</body></html>",
            studio="Example Studio",
            nl_priority=False,
            remote_friendly=True,
            discovery_method="seed_careers_page",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0].get("adapter") or ""), "personio")
        self.assertEqual(str(rows[0].get("evidenceSource") or ""), "page_url")

    def test_infer_provider_candidates_from_html_collapses_competing_seed_page_variants(self) -> None:
        html = """
        <a href="https://boards.greenhouse.io/first-board/jobs/123">Job A</a>
        <a href="https://boards.greenhouse.io/second-board/jobs/456">Job B</a>
        """
        rows = sd.infer_provider_candidates_from_html(
            "https://example.com/careers",
            html,
            studio="Example Studio",
            nl_priority=False,
            remote_friendly=True,
            discovery_method="seed_careers_page",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0].get("adapter") or ""), "greenhouse")

    def test_discover_seed_careers_page_candidates_infers_provider_without_web_search(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "remoteFriendly": True,
                "careersUrl": "https://example.com/careers",
            }
        ]
        try:
            providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
                5,
                fetcher=lambda *_: '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>',
            )
        finally:
            sd.STUDIO_SEEDS = previous

        self.assertEqual(len(failures), 0)
        self.assertEqual(len(static_rows), 0)
        self.assertEqual(len(providers), 1)
        self.assertEqual(str(providers[0].get("adapter") or ""), "greenhouse")
        self.assertEqual(str(providers[0].get("discoveryMethod") or ""), "seed_careers_page")

    def test_discover_seed_careers_page_candidates_prefers_personio_provider_over_static(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "remoteFriendly": True,
                "careersUrl": "https://example.jobs.personio.de/",
            }
        ]
        try:
            providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
                5,
                fetcher=lambda *_: '<a href="/position/artist">Artist</a>',
            )
        finally:
            sd.STUDIO_SEEDS = previous

        self.assertEqual(len(failures), 0)
        self.assertEqual(len(providers), 1)
        self.assertEqual(len(static_rows), 0)
        self.assertEqual(str(providers[0].get("adapter") or ""), "personio")

    def test_discover_seed_careers_page_candidates_builds_static_candidate_without_web_search(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "remoteFriendly": True,
                "careersUrl": "https://example.com/careers",
            }
        ]
        try:
            providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
                5,
                fetcher=lambda *_: """
                <a href="/jobs/rendering-engineer">Rendering Engineer</a>
                <a href="/jobs/gameplay-engineer">Gameplay Engineer</a>
                """,
            )
        finally:
            sd.STUDIO_SEEDS = previous

        self.assertEqual(len(failures), 0)
        self.assertEqual(len(providers), 0)
        self.assertEqual(len(static_rows), 1)
        self.assertEqual(str(static_rows[0].get("adapter") or ""), "static")
        self.assertEqual(str(static_rows[0].get("discoveryMethod") or ""), "seed_careers_page")

    def test_build_static_candidate_from_page_records_evidence(self) -> None:
        html = """
        <a href="/jobs/rendering-engineer">Rendering Engineer</a>
        <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
        """
        row = sd.build_static_candidate_from_page(
            "https://example.com/careers",
            html,
            studio="Example Studio",
            nl_priority=False,
            remote_friendly=True,
            discovery_method="web_search",
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(str(row.get("adapter") or ""), "static")
        self.assertGreaterEqual(int(row.get("evidenceScore") or 0), sd.MIN_STATIC_EVIDENCE_TO_QUEUE)
        self.assertIn("jobposting_jsonld", row.get("evidenceTypes") or [])

    def test_build_static_candidate_from_page_blocks_linkedin_like_domains(self) -> None:
        row = sd.build_static_candidate_from_page(
            "https://www.linkedin.com/company/example/jobs/",
            '<a href="/jobs/test">Test</a>',
            studio="Example Studio",
            nl_priority=False,
            remote_friendly=True,
            discovery_method="web_search",
        )
        self.assertIsNone(row)

    def test_run_discovery_dynamic_tracks_stage_metrics_and_queue_contract(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STUDIO_SEEDS = []
                sd.STATIC_DISCOVERY_CANDIDATES = [
                    {
                        "name": "Demo Lever",
                        "studio": "Demo",
                        "adapter": "lever",
                        "account": "demo",
                        "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                        "remoteFriendly": True,
                        "nlPriority": True,
                    },
                    {
                        "name": "Demo Greenhouse",
                        "studio": "Demo",
                        "adapter": "greenhouse",
                        "slug": "demo",
                        "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                        "remoteFriendly": True,
                        "nlPriority": True,
                    },
                ]

                def fake_fetch(url: str, _: int) -> str:
                    if "api.lever.co" in url:
                        return json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
                    if "boards-api.greenhouse.io" in url:
                        return json.dumps({"jobs": [{}, {}]})
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=fake_fetch)
                summary = report["summary"]
                self.assertEqual(int(summary.get("foundEndpointCount") or 0), 2)
                self.assertEqual(int(summary.get("probedCandidateCount") or 0), 2)
                self.assertEqual(int(summary.get("queuedCandidateCount") or 0), 2)
                self.assertIn("generatedCountByStage", summary)
                self.assertIn("queuedCountByStage", summary)
                self.assertEqual(int((summary.get("queuedCountByStage") or {}).get("curated_seed") or 0), 2)

                queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
                self.assertEqual(len(queued), 2)
                for row in queued:
                    self.assertIn("evidenceScore", row)
                    self.assertIn("evidenceTypes", row)
                    self.assertIn("discoveryStage", row)
                    self.assertFalse(bool(row.get("deferred")))
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds

    def test_run_discovery_skips_duplicate_endpoint_fingerprints(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STUDIO_SEEDS = []
                sd.STATIC_DISCOVERY_CANDIDATES = [
                    {"name": "Demo Lever A", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"},
                    {"name": "Demo Lever A Duplicate", "studio": "Demo", "adapter": "lever", "account": "demo2", "api_url": "https://api.lever.co/v0/postings/demo?mode=json", "discoveryMethod": "pattern"},
                ]
                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_: json.dumps([{"id": 1}]))
                self.assertEqual(int(report["summary"].get("queuedCandidateCount") or 0), 1)
                self.assertGreaterEqual(int(report["summary"].get("skippedDuplicateCount") or 0), 1)
                self.assertIn("duplicateReasons", report["summary"])
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds

    def test_run_discovery_balances_queue_with_deferrals(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            prev_caps = dict(sd.ADAPTER_QUEUE_CAPS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STUDIO_SEEDS = []
                sd.ADAPTER_QUEUE_CAPS["lever"] = 1
                sd.STATIC_DISCOVERY_CANDIDATES = [
                    {"name": "Demo Lever A", "studio": "Demo A", "adapter": "lever", "account": "demoa", "api_url": "https://api.lever.co/v0/postings/demoa?mode=json"},
                    {"name": "Demo Lever B", "studio": "Demo B", "adapter": "lever", "account": "demob", "api_url": "https://api.lever.co/v0/postings/demob?mode=json"},
                ]

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_: json.dumps([{"id": 1}, {"id": 2}]))
                self.assertEqual(int(report["summary"].get("queuedCandidateCount") or 0), 1)
                self.assertEqual(int(report["summary"].get("discoverableButDeferredCount") or 0), 1)
                deferred = [row for row in (report.get("candidates") or []) if bool(row.get("deferred"))]
                self.assertEqual(len(deferred), 1)
                self.assertEqual(str(deferred[0].get("deferReason") or ""), "adapter_cap")
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds
                sd.ADAPTER_QUEUE_CAPS.clear()
                sd.ADAPTER_QUEUE_CAPS.update(prev_caps)

    def test_run_discovery_pattern_candidates_below_reinforced_threshold_are_skipped(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STATIC_DISCOVERY_CANDIDATES = []
                sd.STUDIO_SEEDS = [
                    {
                        "studio": "Example Studio",
                        "aliases": ["example-studio"],
                        "nlPriority": False,
                        "remoteFriendly": True,
                        "likelyProviders": ["teamtailor"],
                        "careersUrl": "https://example.com/careers",
                    }
                ]
                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_: json.dumps({"jobs": [{}]}))
                self.assertEqual(int(report["summary"].get("probedCandidateCount") or 0), 0)
                self.assertEqual(int(report["summary"].get("queuedCandidateCount") or 0), 0)
                stages = [str(row.get("stage") or "") for row in (report.get("failures") or [])]
                self.assertIn("probe_skipped", stages)
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds

    def test_run_discovery_tracks_probe_miss_separately_from_failures(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STUDIO_SEEDS = []
                sd.STATIC_DISCOVERY_CANDIDATES = [
                    {"name": "Demo Lever", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"}
                ]
                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("HTTP Error 404: Not Found")))
                self.assertEqual(int(report["summary"].get("probedCandidateCount") or 0), 1)
                self.assertEqual(int(report["summary"].get("failedProbeCount") or 0), 0)
                self.assertEqual(int(report["summary"].get("probeMissCount") or 0), 1)
                self.assertEqual(str((report.get("failures") or [])[0].get("stage") or ""), "probe_miss")
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds

    def test_run_discovery_uses_seed_careers_pages_without_web_search(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STATIC_DISCOVERY_CANDIDATES = []
                sd.STUDIO_SEEDS = [
                    {
                        "studio": "Example Studio",
                        "aliases": ["example-studio"],
                        "nlPriority": False,
                        "remoteFriendly": True,
                        "likelyProviders": ["teamtailor"],
                        "careersUrl": "https://example.com/careers",
                    }
                ]

                def fake_fetch(url: str, _: int) -> str:
                    if url == "https://example.com/careers":
                        return '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
                    if "boards-api.greenhouse.io" in url:
                        return json.dumps({"jobs": [{}, {}]})
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=fake_fetch)
                self.assertEqual(int(report["summary"].get("queuedCandidateCount") or 0), 1)
                self.assertEqual(int((report["summary"].get("queuedCountByStage") or {}).get("web_provider") or 0), 1)
                self.assertEqual(int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0), 1)
                self.assertEqual(int((report["summary"].get("generatedCountByStage") or {}).get("generic_static") or 0), 0)
                queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
                self.assertEqual(len(queued), 1)
                self.assertEqual(str(queued[0].get("discoveryMethod") or ""), "seed_careers_page")
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds

    def test_discovery_report_snapshot_contract(self) -> None:
        with self.workspace_tmpdir() as root:
            prev_paths = (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            )
            prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
            prev_seeds = list(sd.STUDIO_SEEDS)
            try:
                sd.ACTIVE_PATH = root / "active.json"
                sd.PENDING_PATH = root / "pending.json"
                sd.REJECTED_PATH = root / "rejected.json"
                sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
                sd.DISCOVERY_REPORT_PATH = root / "report.json"
                sd.STUDIO_SEEDS = []
                sd.STATIC_DISCOVERY_CANDIDATES = [
                    {"name": "Demo Lever", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"},
                    {"name": "Demo Greenhouse", "studio": "Demo", "adapter": "greenhouse", "slug": "demo", "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true"},
                ]

                def fake_fetch(url: str, _: int) -> str:
                    if "api.lever.co" in url:
                        return json.dumps([{"id": 1}, {"id": 2}])
                    if "boards-api.greenhouse.io" in url:
                        return json.dumps({"jobs": [{}]})
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=fake_fetch)
                snapshot = {
                    "schemaVersion": report.get("schemaVersion"),
                    "mode": str(report.get("mode")),
                    "summary": {
                        "foundEndpointCount": int(report["summary"].get("foundEndpointCount") or 0),
                        "probedCandidateCount": int(report["summary"].get("probedCandidateCount") or 0),
                        "queuedCandidateCount": int(report["summary"].get("queuedCandidateCount") or 0),
                        "discoverableButDeferredCount": int(report["summary"].get("discoverableButDeferredCount") or 0),
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
                self.assertEqual(snapshot, self.fixture_json("source_discovery_report_snapshot.json"))
            finally:
                (
                    sd.ACTIVE_PATH,
                    sd.PENDING_PATH,
                    sd.REJECTED_PATH,
                    sd.DISCOVERY_CANDIDATES_PATH,
                    sd.DISCOVERY_REPORT_PATH,
                ) = prev_paths
                sd.STATIC_DISCOVERY_CANDIDATES = prev_static
                sd.STUDIO_SEEDS = prev_seeds


if __name__ == "__main__":
    unittest.main()
