import json
from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from src.shared.json_io import read_json
from tests.helpers.job_fixtures import _fixture
from tests.helpers.temp_paths import workspace_tmpdir


def test_parse_google_sheets_csv_fixture() -> None:
    rows = jf.parse_google_sheets_csv(_fixture("google_sheets.csv"))
    assert len(rows) == 2
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["company"] == "Pixel Forge"


def test_parse_google_sheets_csv_normalizes_multi_location_rows() -> None:
    csv_text = (
        "Company,City,Country,Job Title,Link\n"
        'Studio A,"Munich, DE | München, DE",,Gameplay Programmer,https://example.com/jobs/1\n'
        'Studio B,"Vancouver, CA | CA",,Technical Artist,https://example.com/jobs/2\n'
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 2
    assert rows[0]["city"] == "Munich"
    assert rows[0]["country"] == "DE"
    assert rows[0]["locations"] == [{"city": "Munich", "country": "DE"}]
    assert rows[0]["locationSummary"] == "Munich, DE"
    assert rows[1]["city"] == "Vancouver"
    assert rows[1]["country"] == "CA"
    assert rows[1]["locations"] == [{"city": "Vancouver", "country": "CA"}]
    assert rows[1]["locationSummary"] == "Vancouver, CA"


def test_google_sheet_candidate_urls_prefer_gviz_and_pub_over_export() -> None:
    urls = jf.google_sheet_candidate_urls("sheet-id", "0")
    assert urls[0].endswith("/gviz/tq?tqx=out:csv&gid=0")
    assert urls[1].endswith("/pub?output=csv")
    assert urls[2].endswith("/export?format=csv&gid=0")


def test_parse_google_sheets_csv_supports_job_type_link_headers() -> None:
    csv_text = (
        "Intro row,,,,,,,,,\n"
        "Company,Company Category,Job Category,Job,Job Type,Postal Code,City,Fully Remote?,Link,Added\n"
        "Studio A,Developer,Programming,Gameplay Programmer,Full-Time,10115,Berlin,Yes,https://example.com/jobs/1,2026-03-10\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["company"] == "Studio A"
    assert rows[0]["contractType"] == "Full-Time"
    assert rows[0]["jobLink"] == "https://example.com/jobs/1"
    assert rows[0]["sector"] == "Developer"


def test_parse_google_sheets_csv_supports_studio_header_alias() -> None:
    csv_text = (
        "Studio,Country,Job Title,Experience Level,Link\n"
        "Acme Games,Germany,Senior Gameplay Engineer,Senior,https://example.com/jobs/42\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["company"] == "Acme Games"
    assert rows[0]["country"] == "DE"
    assert rows[0]["title"] == "Senior Gameplay Engineer"


def test_parse_google_sheets_csv_skips_known_bad_company_labels() -> None:
    csv_text = (
        "Company,Company Name,City,Country,Job Title,Link\n"
        "giant enemy crab,Actual Studio,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/77\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["company"] == "Actual Studio"


def test_parse_google_sheets_csv_preserves_untrustworthy_company_as_unknown() -> None:
    csv_text = (
        "Company,City,Country,Job Title,Link\n"
        "FarBridge,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/88\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["company"] == jf.UNKNOWN_COMPANY_LABEL


def test_parse_google_sheets_csv_recovers_job_link_from_source_contact() -> None:
    csv_text = (
        "Company,City,Country,Job Title,Job Link,Source/Contact\n"
        "Insomniac Games,Burbank,US,Character TD,,https://insomniac.games/careers/character-td\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://insomniac.games/careers/character-td"


def test_parse_google_sheets_csv_ignores_email_only_source_contact() -> None:
    csv_text = (
        "Company,City,Country,Job Title,Source/Contact\n"
        "Studio A,Amsterdam,NL,Gameplay Engineer,jobs@example.com\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["jobLink"] == ""


def test_parse_google_sheets_csv_infers_company_from_smartrecruiters_url() -> None:
    csv_text = (
        "Company,Job Title,City,Country,Job Link\n"
        ",Senior Environment Artist,Remote,Remote,https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839\n"
        ",Technical Artist,Burbank,US,https://www.smartrecruiters.com/Insomniac-Games/744000999\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 2
    assert rows[0]["company"] == "CDPROJEKTRED"
    assert rows[1]["company"] == "Insomniac Games"


def test_parse_google_sheets_csv_infers_company_from_smartrecruiters_url_when_company_is_unknown() -> (
    None
):
    csv_text = (
        "Company,Job Title,City,Country,Job Link\n"
        "FarBridge,Senior Environment Artist,Remote,Remote,https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839\n"
    )
    rows = jf.parse_google_sheets_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["company"] == "CDPROJEKTRED"


def test_canonicalize_job_with_reason_preserves_known_bad_company_labels_as_unknown() -> None:
    normalized, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Gameplay Engineer",
            "company": "giant enemy crab",
            "jobLink": "https://example.com/jobs/99",
        },
        source="google_sheets",
        fetched_at="2026-03-13T10:00:00Z",
    )
    assert normalized is not None
    assert normalized.company == jf.UNKNOWN_COMPANY_LABEL
    assert reason == ""


def test_canonicalize_google_sheets_rows_uses_redirect_cache_once_for_duplicates() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Technical Director",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Paris",
            "country": "France",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://gracklehq.com/rd/372393",
            "sector": "Game",
        },
        {
            "sourceJobId": "sheet-2",
            "title": "Technical Director",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Paris",
            "country": "France",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://gracklehq.com/rd/372393",
            "sector": "Game",
        },
    ]

    class _FakeResolver:
        def __init__(self) -> None:
            self.cache = {}
            self.calls = 0
            self.cache_hits = 0

        def resolve(self, url: str) -> str:
            if url in self.cache:
                self.cache_hits += 1
                return self.cache[url]
            self.calls += 1
            resolved = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
            self.cache[url] = resolved
            return resolved

        def snapshot_stats(self) -> dict:
            return {"cacheHits": self.cache_hits, "resolvedCount": self.calls}

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        redirect_resolver=_FakeResolver(),
        redirect_concurrency=4,
    )
    assert len(canonical_rows) == 2
    assert not drop_reasons
    assert stats["redirect_candidates"] == 2
    assert stats["redirect_resolved"] == 2
    assert stats["redirect_cache_hits"] == 1
    assert all("smartrecruiters.com" in row.jobLink for row in canonical_rows)


def test_canonicalize_google_sheets_rows_falls_back_when_redirect_resolution_fails() -> None:
    rows = [
        {
            "sourceJobId": "sheet-1",
            "title": "Character TD",
            "company": jf.UNKNOWN_COMPANY_LABEL,
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://gracklehq.com/rd/999999",
            "sector": "Game",
        }
    ]

    class _FakeResolver:
        def resolve(self, url: str) -> str:
            return url

        def snapshot_stats(self) -> dict:
            return {"cacheHits": 0, "resolvedCount": 0}

    canonical_rows, drop_reasons, stats = jf.canonicalize_google_sheets_rows(
        rows,
        source="google_sheets",
        fetched_at="2026-03-13T00:00:00+00:00",
        redirect_resolver=_FakeResolver(),
        redirect_concurrency=2,
    )
    assert len(canonical_rows) == 1
    assert not drop_reasons
    assert canonical_rows[0].jobLink == "https://gracklehq.com/rd/999999"
    assert stats["redirect_resolved"] == 0


def test_normalize_source_report_row_preserves_google_sheets_redirect_stats() -> None:
    row = jf.normalize_source_report_row(
        {
            "name": "google_sheets",
            "status": "ok",
            "adapter": "csv",
            "stageTimingsMs": {
                "parseCsv": 55,
                "redirectResolve": 91,
                "canonicalization": 120,
            },
            "details": [
                {
                    "adapter": "csv",
                    "studio": "community_sheet",
                    "name": "google_sheets",
                    "status": "ok",
                    "stats": {
                        "parse_csv_ms": 55,
                        "redirect_candidates": 7,
                        "redirect_resolved": 6,
                        "redirect_cache_hits": 2,
                        "redirect_resolve_ms": 91,
                    },
                }
            ],
        }
    )
    assert (row.get("stageTimingsMs") or {}).get("redirectResolve") == 91
    detail_stats = (row.get("details") or [{}])[0].get("stats") or {}
    assert int(detail_stats.get("redirect_candidates") or 0) == 7
    assert int(detail_stats.get("redirect_cache_hits") or 0) == 2


def test_run_pipeline_tracks_google_sheets_redirect_stats_in_report_and_state() -> None:
    csv_text = (
        "Company,City,Country,Fully Remote?,Job Type,Job,Link\n"
        f"{jf.UNKNOWN_COMPANY_LABEL},Montpellier,France,No,Full-time,Technical Director,https://gracklehq.com/rd/372393\n"
        f"{jf.UNKNOWN_COMPANY_LABEL},Burbank,United States,Yes,Internship,Character TD,https://example.com/jobs/character-td\n"
    )

    def google_loader(**kwargs):
        return jf.run_google_sheets_source(
            fetch_text=kwargs["fetch_text"],
            timeout_s=kwargs["timeout_s"],
            retries=kwargs["retries"],
            backoff_s=kwargs["backoff_s"],
            sheet_id="test-sheet",
            gid="0",
            diagnostics_name="google_sheets",
        )

    class _FakeResolver:
        def __init__(self) -> None:
            self.cache_hits = 0
            self.resolved_count = 0

        def resolve(self, url: str) -> str:
            self.resolved_count += 1
            return "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"

        def snapshot_stats(self) -> dict:
            return {"cacheHits": self.cache_hits, "resolvedCount": self.resolved_count}

        def close(self) -> None:
            """Match real resolver's close(self) signature."""
            return None

    with workspace_tmpdir("jobs-fetcher-google") as tmp:
        out = Path(tmp)
        with mock.patch.object(jf, "build_redirect_resolver", return_value=_FakeResolver()):

            def fake_fetch(url: str, _: int) -> str:
                if "docs.google.com" in url or "allorigins.win" in url:
                    return csv_text
                raise RuntimeError(f"Unexpected URL: {url}")

            report = jf.run_pipeline(
                output_dir=out,
                fetch_text=fake_fetch,
                source_loaders=[("google_sheets", google_loader)],
                google_sheets_redirect_concurrency=3,
            )

        runtime = report.get("runtime") or {}
        assert int(runtime.get("googleSheetsRedirectConcurrency") or 0) == 3
        source_row = report["sources"][0]
        assert source_row.get("adapter") == "csv"
        detail_stats = (source_row.get("details") or [{}])[0].get("stats") or {}
        assert int(detail_stats.get("redirect_candidates") or 0) == 1
        assert int(detail_stats.get("redirect_resolved") or 0) == 1
        assert "redirect_resolve_ms" in detail_stats

        state_payload = read_json(out / "jobs-source-state.json", {})
        source_state = (state_payload.get("sources") or {}).get("google_sheets") or {}
        assert str(source_state.get("lastAdapter") or "") == "csv"


def test_run_pipeline_reuses_and_persists_google_sheets_redirect_cache() -> None:
    redirect_url = "https://gracklehq.com/rd/372393"
    resolved_url = "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role"
    csv_text = (
        "Company,City,Country,Fully Remote?,Job Type,Job,Link\n"
        f"{jf.UNKNOWN_COMPANY_LABEL},Montpellier,France,No,Full-time,Technical Director,{redirect_url}\n"
    )

    def google_loader(**kwargs):
        return jf.run_google_sheets_source(
            fetch_text=kwargs["fetch_text"],
            timeout_s=kwargs["timeout_s"],
            retries=kwargs["retries"],
            backoff_s=kwargs["backoff_s"],
            sheet_id="test-sheet",
            gid="0",
            diagnostics_name="google_sheets",
        )

    class _FakeResolver:
        def __init__(self) -> None:
            self.seeded: dict[str, str] = {}
            self.cache_hits = 0
            self.resolved_count = 0

        def seed_cache(self, cache: dict[str, str]) -> None:
            self.seeded.update(cache)

        def resolve(self, url: str) -> str:
            if url in self.seeded:
                self.cache_hits += 1
                return self.seeded[url]
            self.resolved_count += 1
            return resolved_url

        def snapshot_stats(self) -> dict:
            return {"cacheHits": self.cache_hits, "resolvedCount": self.resolved_count}

        def snapshot_cache(self) -> dict[str, str]:
            return dict(self.seeded)

        def close(self) -> None:
            return None

    with workspace_tmpdir("jobs-fetcher-google-cache") as tmp:
        out = Path(tmp)
        state_payload = {
            "schemaVersion": jf.SCHEMA_VERSION,
            "updatedAt": "2026-03-23T00:00:00Z",
            "sources": {
                "google_sheets": {
                    "googleSheetsRedirectCache": {redirect_url: resolved_url},
                }
            },
        }
        (out / "jobs-source-state.json").write_text(
            json.dumps(state_payload, indent=2), encoding="utf-8"
        )

        with mock.patch.object(
            jf, "build_redirect_resolver", return_value=_FakeResolver()
        ) as builder:

            def fake_fetch(url: str, _: int) -> str:
                if "docs.google.com" in url or "allorigins.win" in url:
                    return csv_text
                raise RuntimeError(f"Unexpected URL: {url}")

            report = jf.run_pipeline(
                output_dir=out,
                fetch_text=fake_fetch,
                source_loaders=[("google_sheets", google_loader)],
            )

        resolver = builder.return_value
        assert resolver.seeded.get(redirect_url) == resolved_url
        assert int(resolver.cache_hits) >= 1
        assert int(resolver.resolved_count) == 0

        runtime = report.get("runtime") or {}
        assert int(runtime.get("googleSheetsRedirectConcurrency") or 0) == (
            jf.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
        )

        state_payload = read_json(out / "jobs-source-state.json", {})
        source_state = (state_payload.get("sources") or {}).get("google_sheets") or {}
        cache = source_state.get("googleSheetsRedirectCache") or {}
        assert cache.get(redirect_url) == resolved_url


def test_run_google_sheets_source_heartbeats_between_candidate_attempts() -> None:
    heartbeat_calls: list[str] = []
    csv_text = (
        "Title,Company,City,Country,Work Type,Job Link\n"
        "Gameplay Programmer,Studio One,Remote,Unknown,Remote,https://example.com/job\n"
    )

    with (
        mock.patch(
            "src.jobs.adapters.community.google_sheet_candidate_urls",
            return_value=["https://docs.google.com/sheets/1", "https://docs.google.com/sheets/2"],
        ),
        mock.patch("src.jobs.adapters.community.fetch_with_retries", return_value=csv_text),
        mock.patch(
            "src.jobs.adapters.community.parse_google_sheets_csv",
            return_value=[
                {
                    "sourceJobId": "sheet-1",
                    "title": "Gameplay Programmer",
                    "company": "Studio One",
                    "city": "Remote",
                    "country": "Unknown",
                    "workType": "Remote",
                    "contractType": "",
                    "jobLink": "https://example.com/job",
                    "sector": "Game",
                }
            ],
        ),
    ):
        jobs = jf.run_google_sheets_source(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0.0,
            sheet_id="test-sheet",
            gid="0",
            heartbeat_callback=lambda: heartbeat_calls.append("tick"),
        )

    assert len(jobs) == 1
    assert heartbeat_calls == ["tick", "tick"]


def test_parse_google_sheets_csv_emits_periodic_heartbeats() -> None:
    heartbeat_calls: list[str] = []
    lines = ["Title,Company,City,Country,Work Type,Job Link"]
    for idx in range(600):
        lines.append(
            f"Gameplay Programmer {idx},Studio {idx},Remote,Unknown,Remote,https://example.com/{idx}"
        )
    csv_text = "\n".join(lines)

    rows = jf.parse_google_sheets_csv(
        csv_text, heartbeat_callback=lambda: heartbeat_calls.append("tick")
    )

    assert len(rows) == 600
    assert len(heartbeat_calls) >= 3
