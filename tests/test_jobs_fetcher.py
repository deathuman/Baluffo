import json
import tempfile
import unittest
from pathlib import Path

from scripts import jobs_fetcher as jf


class JobsFetcherTests(unittest.TestCase):
    def fixture(self, name: str) -> str:
        path = Path(__file__).parent / "fixtures" / name
        return path.read_text(encoding="utf-8")

    def test_parse_google_sheets_csv_fixture(self) -> None:
        rows = jf.parse_google_sheets_csv(self.fixture("google_sheets.csv"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Gameplay Programmer")
        self.assertEqual(rows[0]["company"], "Pixel Forge")

    def test_parse_remote_ok_payload_filters_game_roles(self) -> None:
        payload = json.loads(self.fixture("remoteok.json"))
        rows = jf.parse_remote_ok_payload(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sourceJobId"], "101")
        self.assertEqual(rows[0]["company"], "Nebula Games")

    def test_parse_gamesindustry_html_fixture(self) -> None:
        rows = jf.parse_gamesindustry_html(self.fixture("gamesindustry_jobs.html"), base_url="https://jobs.gamesindustry.biz")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "Senior Quality Analyst")
        self.assertEqual(rows[0]["company"], "Sharkmob")
        self.assertEqual(rows[0]["sourceJobId"], "43821")
        self.assertTrue(rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/"))
        titles = {row["title"] for row in rows}
        self.assertNotIn("Read more", titles)
        self.assertNotIn("Programming (6)", titles)

    def test_parse_greenhouse_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("greenhouse_guerrilla_jobs.json"))
        rows = jf.parse_greenhouse_jobs_payload(payload, "guerrilla-games")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["sourceJobId"].startswith("greenhouse:guerrilla-games:") for row in rows))
        self.assertEqual(rows[0]["company"], "Guerrilla Games")
        self.assertEqual(rows[0]["country"], "Netherlands")

    def test_parse_teamtailor_listing_links_fixture(self) -> None:
        rows = jf.parse_teamtailor_listing_links(
            self.fixture("teamtailor_listing.html"),
            base_url="https://career.paradoxplaza.com",
        )
        self.assertEqual(len(rows), 2)
        self.assertTrue(all("/jobs/" in row for row in rows))
        self.assertTrue(all("show_more" not in row for row in rows))

    def test_parse_jobpostings_from_html_teamtailor_fixture(self) -> None:
        rows = jf.parse_jobpostings_from_html(
            self.fixture("teamtailor_job.html"),
            base_url="https://career.paradoxplaza.com/jobs/6926996-game-programmer",
            fallback_company="Paradox Interactive",
            fallback_source_id_prefix="teamtailor:test",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Game Programmer")
        self.assertEqual(rows[0]["city"], "Delft")
        self.assertEqual(rows[0]["country"], "NL")

    def test_parse_wellfound_html_fixture(self) -> None:
        rows = jf.parse_wellfound_html(self.fixture("wellfound.html"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sourceJobId"], "wf-1")
        self.assertEqual(rows[0]["workType"], "Remote")

    def test_parse_lever_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("lever_jobs.json"))
        rows = jf.parse_lever_jobs_payload(payload, "sandboxvr", fallback_company="Sandbox VR")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Technical Artist")
        self.assertEqual(rows[0]["country"], "NL")

    def test_parse_smartrecruiters_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("smartrecruiters_jobs.json"))
        rows = jf.parse_smartrecruiters_jobs_payload(payload, "CDPROJEKTRED", fallback_company="CD PROJEKT RED")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Environment Artist")
        self.assertEqual(rows[0]["company"], "CD PROJEKT RED")

    def test_parse_workable_jobs_payload_fixture(self) -> None:
        payload = json.loads(self.fixture("workable_jobs.json"))
        rows = jf.parse_workable_jobs_payload(payload, "hutch", fallback_company="Hutch")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["workType"], "Remote")

    def test_parse_ashby_jobs_from_html_fixture(self) -> None:
        rows = jf.parse_ashby_jobs_from_html(self.fixture("ashby_jobs.html"), "https://jobs.ashbyhq.com/jagex/jobs", "Jagex")
        self.assertEqual(len(rows), 2)
        self.assertTrue(all("jobs.ashbyhq.com" in row["jobLink"] for row in rows))

    def test_parse_personio_feed_xml_fixture(self) -> None:
        rows = jf.parse_personio_feed_xml(self.fixture("personio_feed.xml"), source_name="InnoGames")
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(any(row["title"] == "Environment Artist" for row in rows))

    def test_run_greenhouse_boards_source_with_fixture(self) -> None:
        payload = self.fixture("greenhouse_guerrilla_jobs.json")
        previous = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Guerrilla Games",
                "studio": "Guerrilla Games",
                "adapter": "greenhouse",
                "slug": "guerrilla-games",
                "enabledByDefault": True,
            }
        ]

        try:
            def fake_fetch(url: str, _: int) -> str:
                self.assertIn("boards-api.greenhouse.io", url)
                self.assertIn("guerrilla-games", url)
                return payload

            rows = jf.run_greenhouse_boards_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertEqual(len(rows), 2)
            self.assertTrue(any("guerrilla-games/jobs/" in row["jobLink"] for row in rows))
        finally:
            jf.STUDIO_SOURCE_REGISTRY = previous

    def test_run_teamtailor_source_with_fixture(self) -> None:
        listing = self.fixture("teamtailor_listing.html")
        detail = self.fixture("teamtailor_job.html")

        def fake_fetch(url: str, _: int) -> str:
            if url == "https://career.paradoxplaza.com/jobs":
                return listing
            if "/jobs/" in url:
                return detail
            raise RuntimeError(f"Unexpected URL: {url}")

        rows = jf.run_teamtailor_sources_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
        self.assertGreaterEqual(len(rows), 1)
        self.assertTrue(any("career.paradoxplaza.com/jobs/" in row["jobLink"] for row in rows))

    def test_run_static_studio_pages_source_with_fixture(self) -> None:
        listing = self.fixture("littlechicken_jobs_page.html")
        detail = self.fixture("littlechicken_job_detail.html")
        prev = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = [
            {
                "name": "Little Chicken",
                "studio": "Little Chicken",
                "adapter": "static",
                "company": "Little Chicken",
                "pages": ["https://www.littlechicken.nl/about-us/jobs/"],
                "enabledByDefault": True,
            }
        ]

        try:
            def fake_fetch(url: str, _: int) -> str:
                if url == "https://www.littlechicken.nl/about-us/jobs/":
                    return listing
                if "/job/" in url:
                    return detail
                raise RuntimeError(f"Unexpected URL: {url}")

            rows = jf.run_static_studio_pages_source(fetch_text=fake_fetch, timeout_s=5, retries=0, backoff_s=0)
            self.assertGreaterEqual(len(rows), 2)
            self.assertTrue(any("littlechicken.nl/job/" in row["jobLink"] for row in rows))
        finally:
            jf.STUDIO_SOURCE_REGISTRY = prev

    def test_map_profession_recognizes_focus_synonyms(self) -> None:
        self.assertEqual(jf.map_profession("Senior Tech Artist"), "technical-artist")
        self.assertEqual(jf.map_profession("Material Artist"), "technical-artist")
        self.assertEqual(jf.map_profession("World Artist"), "environment-artist")
        self.assertEqual(jf.map_profession("Terrain Artist"), "environment-artist")

    def test_compute_focus_score_prioritizes_target_nl_and_remote(self) -> None:
        ta_nl = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Studio NL",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://example.com/ta-nl",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        ta_remote = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Studio Remote",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/ta-remote",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        non_target = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Studio Other",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://example.com/gameplay",
                "sector": "Game",
                "postedAt": "2026-03-01",
            },
            source="x",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(ta_nl)
        self.assertIsNotNone(ta_remote)
        self.assertIsNotNone(non_target)
        self.assertGreater(ta_nl["focusScore"], ta_remote["focusScore"])
        self.assertGreater(ta_remote["focusScore"], non_target["focusScore"])

    def test_dedup_primary_key_prefers_richer_latest_record(self) -> None:
        first = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Pixel Forge",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Hybrid",
                "contractType": "Full-time",
                "jobLink": "https://pixelforge.dev/jobs/123?utm_source=x",
                "sector": "Game",
                "postedAt": "2026-01-01",
            },
            source="a",
            fetched_at=jf.now_iso(),
        )
        second = jf.canonicalize_job(
            {
                "title": "Gameplay Programmer",
                "company": "Pixel Forge",
                "city": "Amsterdam",
                "country": "Netherlands",
                "workType": "Hybrid",
                "contractType": "Permanent",
                "jobLink": "https://pixelforge.dev/jobs/123",
                "sector": "Gaming",
                "postedAt": "2026-02-10",
                "sourceJobId": "r-2",
            },
            source="b",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        rows, stats = jf.deduplicate_jobs([first, second])
        self.assertEqual(stats["outputCount"], 1)
        self.assertEqual(rows[0]["sourceJobId"], "r-2")
        self.assertTrue(rows[0]["dedupKey"].startswith("url:"))

    def test_dedup_secondary_key_merges_without_link(self) -> None:
        first = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Contract",
                "jobLink": "",
                "sector": "Game",
                "postedAt": "2026-02-01",
            },
            source="a",
            fetched_at=jf.now_iso(),
        )
        second = jf.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Contract",
                "jobLink": "",
                "sector": "Game",
                "postedAt": "2026-02-05",
            },
            source="b",
            fetched_at=jf.now_iso(),
        )
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        rows, stats = jf.deduplicate_jobs([first, second])
        self.assertEqual(stats["outputCount"], 1)
        self.assertTrue(rows[0]["dedupKey"].startswith("secondary:"))
        self.assertGreaterEqual(int(rows[0].get("sourceBundleCount") or 0), 2)
        self.assertIsInstance(rows[0].get("sourceBundle"), list)

    def test_pipeline_partial_success_when_one_source_fails(self) -> None:
        def failing_loader(**_: object):
            raise RuntimeError("timeout")

        def ok_loader(**_: object):
            return [
                {
                    "title": "Gameplay Programmer",
                    "company": "Nebula Games",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/jobs/1",
                    "sector": "Game",
                    "sourceJobId": "ok-1",
                    "postedAt": "2026-02-10",
                }
            ]

        with tempfile.TemporaryDirectory() as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                source_loaders=[("failing", failing_loader), ("ok", ok_loader)],
            )

            self.assertEqual(report["summary"]["failedSources"], 1)
            self.assertEqual(report["summary"]["outputCount"], 1)

            output = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(output), 1)
            self.assertEqual(output[0]["source"], "ok")

    def test_pipeline_preserves_previous_output_when_current_is_empty(self) -> None:
        existing = [
            {
                "id": 1,
                "title": "Engine Programmer",
                "company": "Archive Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://archive.example/jobs/1",
                "sector": "Game",
                "profession": "engine",
                "companyType": "Game",
                "description": "Engine Programmer at Archive Studio",
                "source": "archive",
                "sourceJobId": "archive-1",
                "fetchedAt": "2026-02-01T00:00:00+00:00",
                "postedAt": "2026-01-30T00:00:00+00:00",
                "dedupKey": "url:archive",
                "qualityScore": 100,
            }
        ]

        def empty_loader(**_: object):
            return []

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
            report = jf.run_pipeline(output_dir=out, source_loaders=[("empty", empty_loader)])

            output = json.loads((out / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(output), 1)
            self.assertTrue(report["summary"]["preservedPreviousOutput"])

    def test_pipeline_output_contract_matches_frontend(self) -> None:
        def ok_loader(**_: object):
            return [
                {
                    "title": "Technical Artist",
                    "company": "Orion Labs",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "remote",
                    "contractType": "contract",
                    "jobLink": "https://example.com/jobs/ta",
                    "sector": "gaming",
                    "sourceJobId": "ta-1",
                    "postedAt": "2026-02-01",
                }
            ]

        with tempfile.TemporaryDirectory() as tmp:
            jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            for field in jf.REQUIRED_FIELDS:
                self.assertIn(field, row)
            for field in jf.OPTIONAL_FIELDS:
                self.assertIn(field, row)
            self.assertEqual(row["workType"], "Remote")
            self.assertIsInstance(row["focusScore"], int)

    def test_pipeline_default_sources_exclude_wellfound_and_include_guerrilla(self) -> None:
        google_csv = self.fixture("google_sheets.csv")
        remote_json = self.fixture("remoteok.json")
        gamesindustry_html = self.fixture("gamesindustry_jobs.html")
        greenhouse_json = self.fixture("greenhouse_guerrilla_jobs.json")
        greenhouse_playstation_json = self.fixture("greenhouse_playstation_jobs.json")
        teamtailor_listing = self.fixture("teamtailor_listing.html")
        teamtailor_job = self.fixture("teamtailor_job.html")
        littlechicken_listing = self.fixture("littlechicken_jobs_page.html")
        littlechicken_detail = self.fixture("littlechicken_job_detail.html")
        lever_json = self.fixture("lever_jobs.json")
        smart_json = self.fixture("smartrecruiters_jobs.json")
        workable_json = self.fixture("workable_jobs.json")
        ashby_html = self.fixture("ashby_jobs.html")
        personio_xml = self.fixture("personio_feed.xml")

        def fake_fetch(url: str, _: int) -> str:
            if "docs.google.com/spreadsheets" in url or "api.allorigins.win/raw" in url:
                return google_csv
            if "remoteok.com/api" in url:
                return remote_json
            if "jobs.gamesindustry.biz" in url:
                return gamesindustry_html
            if "boards-api.greenhouse.io" in url and "guerrilla-games" in url:
                return greenhouse_json
            if "boards-api.greenhouse.io" in url and "sonyinteractiveentertainmentglobal" in url:
                return greenhouse_playstation_json
            if url == "https://career.paradoxplaza.com/jobs":
                return teamtailor_listing
            if "career.paradoxplaza.com/jobs/" in url:
                return teamtailor_job
            if "api.lever.co" in url:
                return lever_json
            if "api.smartrecruiters.com" in url:
                return smart_json
            if "apply.workable.com/api/v1/widget/accounts" in url:
                return workable_json
            if "jobs.ashbyhq.com" in url:
                return ashby_html
            if "jobs.personio.de/xml" in url:
                return personio_xml
            if url == "https://www.littlechicken.nl/about-us/jobs/" or url == "https://www.littlechicken.nl/job/":
                return littlechicken_listing
            if "littlechicken.nl/job/" in url:
                return littlechicken_detail
            raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

        with tempfile.TemporaryDirectory() as tmp:
            report = jf.run_pipeline(
                output_dir=Path(tmp),
                fetch_text=fake_fetch,
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )

            sources = {row["name"]: row for row in report["sources"]}
            self.assertEqual(sources["google_sheets"]["status"], "ok")
            self.assertEqual(sources["remote_ok"]["status"], "ok")
            self.assertEqual(sources["gamesindustry"]["status"], "ok")
            self.assertEqual(sources["greenhouse_boards"]["status"], "ok")
            self.assertEqual(sources["teamtailor_sources"]["status"], "ok")
            self.assertEqual(sources["lever_sources"]["status"], "ok")
            self.assertEqual(sources["smartrecruiters_sources"]["status"], "ok")
            self.assertEqual(sources["workable_sources"]["status"], "ok")
            self.assertEqual(sources["ashby_sources"]["status"], "ok")
            self.assertEqual(sources["personio_sources"]["status"], "ok")
            self.assertEqual(sources["static_studio_pages"]["status"], "ok")
            self.assertEqual(sources["wellfound"]["status"], "excluded")
            self.assertIn("disabled_by_default", sources["wellfound"]["error"])
            self.assertEqual(sources["greenhouse_boards"]["adapter"], "greenhouse")
            self.assertEqual(sources["teamtailor_sources"]["adapter"], "teamtailor")
            self.assertEqual(sources["lever_sources"]["adapter"], "lever")
            self.assertEqual(sources["smartrecruiters_sources"]["adapter"], "smartrecruiters")
            self.assertEqual(sources["workable_sources"]["adapter"], "workable")
            self.assertEqual(sources["ashby_sources"]["adapter"], "ashby")
            self.assertEqual(sources["personio_sources"]["adapter"], "personio")
            self.assertEqual(sources["static_studio_pages"]["adapter"], "static")
            self.assertEqual(report["summary"]["failedSources"], 0)
            self.assertGreaterEqual(report["summary"]["excludedSources"], 1)
            self.assertIn("targetRoleCount", report["summary"])
            self.assertIn("netherlandsCount", report["summary"])
            self.assertIn("remoteCount", report["summary"])
            self.assertIn("rawFetchedCount", report["summary"])
            self.assertIn("uniqueOutputCount", report["summary"])
            self.assertIn("sourceBundleCollisions", report["summary"])

            rows = json.loads((Path(tmp) / "jobs-unified.json").read_text(encoding="utf-8"))
            self.assertTrue(any("guerrilla" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("playstation" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("paradox" in row.get("company", "").lower() for row in rows))
            self.assertTrue(any("little chicken" in row.get("company", "").lower() for row in rows))
            self.assertTrue(all("focusScore" in row for row in rows))
            self.assertTrue(all("sourceBundleCount" in row for row in rows))
            self.assertTrue(all("sourceBundle" in row for row in rows))
            all_errors = " ".join(row.get("error", "") for row in report["sources"])
            self.assertNotIn("403", all_errors)


if __name__ == "__main__":
    unittest.main()
