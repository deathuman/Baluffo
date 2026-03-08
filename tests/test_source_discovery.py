import json
import tempfile
import unittest
from pathlib import Path

from scripts import source_discovery as sd


class SourceDiscoveryTests(unittest.TestCase):
    def fixture_json(self, name: str):
        path = Path(__file__).parent / "fixtures" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_build_pattern_candidates_includes_expected_adapters(self) -> None:
        previous = list(sd.STUDIO_SEEDS)
        sd.STUDIO_SEEDS = [
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": True,
                "remoteFriendly": True,
            }
        ]
        try:
            rows = sd.build_pattern_candidates()
        finally:
            sd.STUDIO_SEEDS = previous

        adapters = {str(row.get("adapter")) for row in rows}
        self.assertTrue({"lever", "greenhouse", "smartrecruiters", "workable", "teamtailor", "ashby", "personio"}.issubset(adapters))

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

        teamtailor = {
            "adapter": "teamtailor",
            "listing_url": "https://example.teamtailor.com/jobs",
        }
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
        bad_lever = {"adapter": "lever", "account": "12"}
        valid, reason = sd.validate_candidate_for_probe(bad_lever)
        self.assertFalse(valid)
        self.assertIn("invalid", reason)

    def test_validate_candidate_for_probe_allows_custom_teamtailor_domain_with_jobs_path(self) -> None:
        custom_tt = {"adapter": "teamtailor", "listing_url": "https://career.paradoxplaza.com/jobs"}
        valid, reason = sd.validate_candidate_for_probe(custom_tt)
        self.assertTrue(valid)
        self.assertEqual(reason, "")

    def test_run_discovery_dynamic_adds_probe_metadata_and_summary_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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
                        "nlPriority": True,
                        "remoteFriendly": True,
                        "discoveryMethod": "seed",
                    },
                    {
                        "name": "Demo Greenhouse",
                        "studio": "Demo",
                        "adapter": "greenhouse",
                        "slug": "demo",
                        "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                        "nlPriority": True,
                        "remoteFriendly": True,
                        "discoveryMethod": "seed",
                    },
                ]

                def fake_fetch(url: str, _: int) -> str:
                    if "api.lever.co" in url:
                        return json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
                    if "boards-api.greenhouse.io" in url:
                        return json.dumps({"jobs": [{}, {}]})
                    raise RuntimeError(f"unexpected URL: {url}")

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=fake_fetch)

                self.assertEqual(report["summary"]["foundEndpointCount"], 2)
                self.assertEqual(report["summary"]["probedCandidateCount"], 2)
                self.assertEqual(report["summary"]["queuedCandidateCount"], 2)
                self.assertIn("adapterCounts", report["summary"])
                self.assertIn("methodCounts", report["summary"])

                queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
                self.assertEqual(len(queued), 2)
                for row in queued:
                    self.assertIn("jobsFound", row)
                    self.assertIn("discoveryMethod", row)
                    self.assertIn("confidence", row)
                    self.assertIn("lastProbedAt", row)
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
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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
                        "name": "Demo Lever A",
                        "studio": "Demo",
                        "adapter": "lever",
                        "account": "demo",
                        "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                        "discoveryMethod": "seed",
                    },
                    {
                        "name": "Demo Lever A Duplicate",
                        "studio": "Demo",
                        "adapter": "lever",
                        "account": "demo2",
                        "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                        "discoveryMethod": "pattern",
                    },
                ]

                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    fetcher=lambda *_: json.dumps([{"id": 1}]),
                )
                self.assertEqual(report["summary"]["queuedCandidateCount"], 1)
                self.assertGreaterEqual(report["summary"]["skippedDuplicateCount"], 1)
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
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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
                        "discoveryMethod": "seed",
                    }
                ]

                def fake_fetch(_url: str, _timeout: int) -> str:
                    raise RuntimeError("HTTP Error 404: Not Found")

                report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=fake_fetch)
                self.assertEqual(int(report["summary"].get("probedCandidateCount") or 0), 1)
                self.assertEqual(int(report["summary"].get("failedProbeCount") or 0), 0)
                self.assertEqual(int(report["summary"].get("probeMissCount") or 0), 1)
                failures = report.get("failures") or []
                self.assertEqual(str(failures[0].get("stage") or ""), "probe_miss")
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
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
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
                        "discoveryMethod": "seed",
                    },
                    {
                        "name": "Demo Greenhouse",
                        "studio": "Demo",
                        "adapter": "greenhouse",
                        "slug": "demo",
                        "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                        "discoveryMethod": "seed",
                    },
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
                        "failedProbeCount": int(report["summary"].get("failedProbeCount") or 0),
                    },
                    "counts": {
                        "candidates": len(report.get("candidates") or []),
                        "failures": len(report.get("failures") or []),
                    },
                    "adapterCounts": report["summary"].get("adapterCounts") or {},
                    "methodCounts": report["summary"].get("methodCounts") or {},
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
