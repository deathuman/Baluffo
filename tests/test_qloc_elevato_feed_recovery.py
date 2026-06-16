import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import src.jobs.adapters as adapters_mod
from src import jobs_fetcher as jf
from src.jobs import pipeline as pipeline_mod
from src.jobs import pipeline_cli as pipeline_cli_mod
from src.jobs.adapters import static_sources as static_sources_mod
from src.shared.json_io import read_json
from src.storage.baluffo_store import BaluffoStore
from src.storage.source_registry_runtime import SourceRegistryRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir

QLOC_SOURCE = "static_source::static:listing_url:https://qloc.elevato.net/en/"
QLOC_J240 = "https://qloc.elevato.net/en/technical-artist,j,240"
QLOC_J229 = "https://qloc.elevato.net/pl/technical-artist,j,229"


def _qloc_job() -> dict[str, object]:
    return {
        "title": "Technical Artist",
        "company": "QLOC",
        "city": "Warsaw",
        "country": "Poland",
        "workType": "Onsite",
        "contractType": "Full-time",
        "jobLink": QLOC_J240,
        "sector": "Game",
        "source": QLOC_SOURCE,
        "sourceJobId": "elevato:240",
        "postedAt": "2026-06-01",
    }


def _stale_qloc_sheet_job() -> dict[str, object]:
    return {
        **_qloc_job(),
        "company": "Qloc careers",
        "jobLink": QLOC_J229,
        "source": "google_sheets",
        "sourceJobId": "sheet:qloc:technical-artist",
        "postedAt": "2026-05-01",
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": "google_sheets",
                "sourceJobId": "sheet:qloc:technical-artist",
                "jobLink": QLOC_J229,
                "adapter": "csv",
                "studio": "Qloc careers",
            }
        ],
    }


def _write_fresh_qloc_source_state(out: Path) -> None:
    future = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
    (out / "jobs-source-state.json").write_text(
        json.dumps(
            {
                "updatedAt": jf.now_iso(),
                "sources": {
                    QLOC_SOURCE: {
                        "lastAdapter": "custom",
                        "lastStatus": "ok",
                        "lastSuccessAt": jf.now_iso(),
                        "lastKeptCount": 9,
                        "nextEligibleCheckAt": future,
                        "cacheDecision": "skip_fresh",
                        "cacheDecisionReason": "within_freshness_window",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_source_check_only_qloc_state(out: Path) -> None:
    now = jf.now_iso()
    future = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
    (out / "jobs-source-state.json").write_text(
        json.dumps(
            {
                "updatedAt": now,
                "sources": {
                    QLOC_SOURCE: {
                        "lastAdapter": "static",
                        "lastStatus": "excluded",
                        "lastCheckedAt": now,
                        "lastSeenInFetchAt": now,
                        "lastSuccessAt": "",
                        "lastSuccessfulFetchAt": "",
                        "lastKeptCount": 0,
                        "lastJobsKept": 0,
                        "lastJobsFound": 9,
                        "nextEligibleCheckAt": future,
                        "cacheDecision": "skip_fresh",
                        "cacheDecisionReason": "within_freshness_window",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_qloc_active_registry(out: Path) -> None:
    (out / "source-registry-active.json").write_text(
        json.dumps(
            [
                {
                    "id": "static:listing_url:https://qloc.elevato.net/en/",
                    "name": "QLOC (Sheet)",
                    "adapter": "static",
                    "listing_url": "https://qloc.elevato.net/en/",
                    "enabledByDefault": True,
                }
            ]
        ),
        encoding="utf-8",
    )


def test_run_pipeline_does_not_treat_source_check_only_state_as_fresh() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-qloc-source-check-only") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(
            json.dumps([_stale_qloc_sheet_job()]),
            encoding="utf-8",
        )
        _write_source_check_only_qloc_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        rows = read_json(out / "jobs-unified.json", [])
        report_row = next(
            row for row in (report.get("sourceFamilies") or []) if row.get("name") == QLOC_SOURCE
        )
        assert calls["count"] == 1
        assert report_row["status"] == "ok"
        assert any(str(row.get("jobLink") or "") == QLOC_J240 for row in rows)
        assert not any(str(row.get("jobLink") or "") == QLOC_J229 for row in rows)


def test_run_pipeline_default_loaders_use_output_dir_active_registry(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_static_loader_builder(source_row: dict[str, object], _loader_name: str):
        def _loader(**_: object):
            calls.append(dict(source_row))
            return [_qloc_job()]

        return _loader

    monkeypatch.setattr(adapters_mod, "DEFAULT_SOURCE_LOADER_NAMES", [])
    monkeypatch.setattr(
        static_sources_mod, "_build_static_source_loader", _fake_static_loader_builder
    )

    with workspace_tmpdir("jobs-fetcher-output-dir-registry-static-loader") as tmp:
        out = Path(tmp)
        _write_qloc_active_registry(out)
        (out / "jobs-unified.json").write_text(
            json.dumps(
                [
                    {
                        "title": "Legacy Producer",
                        "company": "Other Studio",
                        "jobLink": "https://example.com/jobs/legacy-producer",
                        "source": "google_sheets",
                    }
                ]
            ),
            encoding="utf-8",
        )
        _write_source_check_only_qloc_state(out)

        report = jf.run_pipeline(output_dir=out, show_progress=False)

        rows = read_json(out / "jobs-unified.json", [])
        assert [str(row.get("listing_url") or "") for row in calls] == [
            "https://qloc.elevato.net/en/"
        ]
        assert int(report["summary"].get("outputCount") or 0) == 2
        assert any(str(row.get("jobLink") or "") == QLOC_J240 for row in rows)


def test_run_cli_only_sources_resolves_output_dir_active_registry(monkeypatch) -> None:
    selected_names: list[str] = []

    def _fake_static_loader_builder(_source_row: dict[str, object], _loader_name: str):
        def _loader(**_: object):
            return [_qloc_job()]

        return _loader

    def _fake_run_pipeline(**kwargs: object) -> dict[str, object]:
        source_loaders = kwargs.get("source_loaders")
        assert isinstance(source_loaders, list)
        selected_names.extend(name for name, _loader in source_loaders)
        return {
            "summary": {"outputCount": 1, "failedSources": 0},
            "outputs": {"report": "report.json"},
            "runtime": {},
        }

    monkeypatch.setattr(adapters_mod, "DEFAULT_SOURCE_LOADER_NAMES", [])
    monkeypatch.setattr(
        static_sources_mod, "_build_static_source_loader", _fake_static_loader_builder
    )

    with workspace_tmpdir("jobs-fetcher-cli-output-dir-registry-only-sources") as tmp:
        out = Path(tmp)
        _write_qloc_active_registry(out)
        args = argparse.Namespace(
            output_dir=str(out),
            social_config_path=str(out / "social-sources-config.json"),
            social_enabled=False,
            social_lookback_minutes=30,
            only_sources=QLOC_SOURCE,
            skip_successful_sources=False,
            no_seed_existing_output=False,
            timeout=1,
            retries=0,
            backoff=0.0,
            no_preserve_previous_on_empty=False,
            source_ttl_minutes=360,
            max_workers=1,
            max_per_domain=1,
            fetch_strategy="http",
            adapter_http_concurrency=1,
            google_sheets_redirect_concurrency=1,
            static_detail_concurrency=1,
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=180,
            browser_fallback_cooldown_minutes=30,
            circuit_breaker_zero_kept=3,
            respect_source_cadence=False,
            hot_source_cadence_minutes=15,
            cold_source_cadence_minutes=60,
            ignore_circuit_breaker=False,
            force_refresh_all=False,
            include_linked_static_validation=False,
            include_pending_provider_migration=False,
            quiet=True,
        )

        code = pipeline_cli_mod.run_cli(
            args,
            run_pipeline=_fake_run_pipeline,
            default_source_loaders=pipeline_mod.default_source_loaders,
        )

        assert code == 0
        assert selected_names == [QLOC_SOURCE]


def test_fetcher_child_process_loads_sqlite_registry_static_sources() -> None:
    with workspace_tmpdir("jobs-fetcher-sqlite-registry-static-loader") as tmp:
        out = Path(tmp)
        with BaluffoStore(out) as store:
            SourceRegistryRuntimeStore(store).replace_state(
                state={
                    "active": [
                        {
                            "id": "static:listing_url:https://qloc.elevato.net/en/",
                            "name": "QLOC (Sheet)",
                            "adapter": "static",
                            "listing_url": "https://qloc.elevato.net/en/",
                            "enabledByDefault": True,
                        }
                    ],
                    "pending": [],
                    "rejected": [],
                },
                tombstones={},
                generation="sqlite-qloc",
                reason="unit-test",
            )

        script = (
            "import json;"
            "from src import jobs_fetcher as jf;"
            "print(json.dumps([name for name, _loader in jf.default_source_loaders() "
            "if 'qloc.elevato.net' in name]))"
        )
        env = {**os.environ, "BALUFFO_DATA_DIR": str(out)}
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=True,
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            capture_output=True,
        )

        assert json.loads(result.stdout) == [QLOC_SOURCE]


def test_run_pipeline_refreshes_fresh_static_source_missing_from_published_feed() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-fresh-static-unpublished") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(
            json.dumps(
                [
                    {
                        "title": "Legacy Producer",
                        "company": "Other Studio",
                        "jobLink": "https://example.com/jobs/legacy-producer",
                        "source": "google_sheets",
                    }
                ]
            ),
            encoding="utf-8",
        )
        _write_fresh_qloc_source_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        rows = read_json(out / "jobs-unified.json", [])
        assert calls["count"] == 1
        assert int(report["summary"].get("outputCount") or 0) == 2
        assert any(str(row.get("jobLink") or "") == QLOC_J240 for row in rows)


def test_run_pipeline_still_skips_fresh_static_source_present_in_published_feed() -> None:
    calls = {"count": 0}

    def qloc_loader(**_: object):
        calls["count"] += 1
        return [_qloc_job()]

    with workspace_tmpdir("jobs-fetcher-fresh-static-published") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps([_qloc_job()]), encoding="utf-8")
        _write_fresh_qloc_source_state(out)

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(QLOC_SOURCE, qloc_loader)],
            show_progress=False,
        )

        excluded = [
            row for row in (report.get("sourceFamilies") or []) if row.get("name") == QLOC_SOURCE
        ]
        assert calls["count"] == 0
        assert len(excluded) == 1
        assert str(excluded[0].get("cacheDecision") or "") == "skip_fresh"


def test_deduplicate_jobs_prefers_live_elevato_static_over_stale_google_sheets_detail() -> None:
    stale_sheet = _stale_qloc_sheet_job()
    live_static = {
        **_qloc_job(),
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": QLOC_SOURCE,
                "sourceJobId": "elevato:240",
                "jobLink": QLOC_J240,
                "adapter": "static",
                "studio": "QLOC",
            }
        ],
    }

    rows, stats = jf.deduplicate_jobs([stale_sheet, live_static])

    assert stats["outputCount"] == 1
    assert int(stats.get("mergedBySparseIdentity") or 0) == 1
    payload = rows[0].to_dict()
    assert payload["source"] == QLOC_SOURCE
    assert payload["jobLink"] == QLOC_J240
    assert "technical-artist,j,229" not in json.dumps(payload.get("sourceBundle") or [])
