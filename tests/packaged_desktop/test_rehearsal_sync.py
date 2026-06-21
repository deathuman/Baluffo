"""Packaged desktop rehearsal tests for sync."""

from ._rehearsal_shared import (
    Path,
    Request,
    _write_packaged_sync_bundle_config,
    base64,
    json,
    mock,
    pytest,
    smoke,
    source_sync,
    urlopen,
    workspace_tmpdir,
)

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def test_packaged_sync_rehearsal_server_serves_fake_github_app_flow() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        config_path = _write_packaged_sync_bundle_config(portable_root)
        loaded_path, _raw_payload, packaged_config = (
            smoke._load_portable_packaged_sync_rehearsal_config(  # noqa: SLF001
                portable_root
            )
        )
        assert loaded_path == config_path
        base_url, stats, server, thread = smoke._start_packaged_sync_rehearsal_server(  # noqa: SLF001
            packaged_config=packaged_config,
            snapshot_payload={
                "schemaVersion": source_sync.SYNC_SCHEMA_VERSION,
                "generatedAt": "2026-04-19T12:00:00+00:00",
                "source": {"name": "packaged_sync_rehearsal"},
                "active": [],
                "pending": [],
                "rejected": [],
            },
        )
        try:
            token_request = Request(
                f"{base_url}/app/installations/999999/access_tokens",
                data=b"{}",
                headers={"Authorization": "Bearer rehearsal-jwt"},
                method="POST",
            )
            with urlopen(token_request, timeout=5) as response:  # noqa: S310
                token_payload = json.loads(response.read().decode("utf-8"))
            assert token_payload["token"] == "packaged-sync-rehearsal-token"

            content_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
            )
            with urlopen(content_request, timeout=5) as response:  # noqa: S310
                content_payload = json.loads(response.read().decode("utf-8"))
            decoded = json.loads(base64.b64decode(content_payload["content"]).decode("utf-8"))
            assert content_payload["sha"] == "packaged-sync-rehearsal-sha"
            assert decoded["source"]["name"] == "packaged_sync_rehearsal"

            shard_payload = b"shard payload"
            put_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync/shards/a.json.gz",
                data=json.dumps(
                    {
                        "message": "write shard",
                        "content": base64.b64encode(shard_payload).decode("ascii"),
                        "branch": "main",
                    }
                ).encode("utf-8"),
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
                method="PUT",
            )
            with urlopen(put_request, timeout=5) as response:  # noqa: S310
                put_payload = json.loads(response.read().decode("utf-8"))
            assert put_payload["content"]["sha"].startswith("packaged-sync-")

            list_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync/shards?ref=main",
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
            )
            with urlopen(list_request, timeout=5) as response:  # noqa: S310
                list_payload = json.loads(response.read().decode("utf-8"))
            assert any(
                row["path"] == "baluffo/source-sync/shards/a.json.gz" for row in list_payload
            )

            delete_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync/shards/a.json.gz",
                data=json.dumps(
                    {
                        "message": "delete shard",
                        "sha": put_payload["content"]["sha"],
                        "branch": "main",
                    }
                ).encode("utf-8"),
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
                method="DELETE",
            )
            with urlopen(delete_request, timeout=5) as response:  # noqa: S310
                assert response.status == 200
            assert stats["putRequests"] == 1
            assert stats["deleteRequests"] == 1
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


def test_load_portable_packaged_sync_rehearsal_config_rejects_machine_key_derivation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        _write_packaged_sync_bundle_config(portable_root, key_derivation="machine")
        with pytest.raises(RuntimeError, match="keyDerivation=machine"):
            smoke._load_portable_packaged_sync_rehearsal_config(portable_root)  # noqa: SLF001


def test_run_packaged_smoke_can_run_sync_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--sync-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_sync_rehearsal",
                return_value={
                    "name": "Packaged sync rehearsal",
                    "slug": "packaged-sync-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "sync.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "sync.stderr.log"),
                        "performanceProfileSnapshot": str(
                            artifacts_dir / "performance-profile.post-sync.json"
                        ),
                        "storageMetricsSnapshot": str(
                            artifacts_dir / "storage-metrics.post-sync.json"
                        ),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-sync-rehearsal"
        assert payload["artifacts"]["syncRehearsalStdout"] == str(artifacts_dir / "sync.stdout.log")
        assert payload["artifacts"]["syncRehearsalStderr"] == str(artifacts_dir / "sync.stderr.log")
        assert payload["artifacts"]["performanceProfileSnapshot"] == str(
            artifacts_dir / "performance-profile.post-sync.json"
        )
        assert payload["artifacts"]["storageMetricsSnapshot"] == str(
            artifacts_dir / "storage-metrics.post-sync.json"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_run_packaged_sync_rehearsal_captures_performance_profile() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        artifacts_dir = root / "artifacts"
        artifacts_dir.mkdir()
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        config_path = root / "github-app-sync-config.json"
        config_path.write_text("{}", encoding="utf-8")
        process = mock.Mock()
        process.pid = 1001
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        memory_sampler = mock.Mock()
        memory_sampler.stop.return_value = {"sampleCount": 1, "peakWorkingSetBytes": 123}

        with (
            mock.patch.object(
                smoke,
                "_load_portable_packaged_sync_rehearsal_config",
                return_value=(config_path, {"keyDerivation": "embedded"}, mock.Mock()),
            ),
            mock.patch.object(
                smoke,
                "_start_packaged_sync_rehearsal_server",
                return_value=(
                    "http://127.0.0.1:12345",
                    {
                        "tokenRequests": 1,
                        "contentRequests": 1,
                        "putRequests": 1,
                        "deleteRequests": 0,
                        "bytesWritten": 64,
                    },
                    None,
                    None,
                ),
            ),
            mock.patch.object(smoke, "choose_free_port", side_effect=[51001, 51002]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                return_value=(process, stdout_handle, stderr_handle),
            ),
            mock.patch.object(smoke, "ProcessMemorySampler", return_value=memory_sampler),
            mock.patch.object(smoke, "wait_for_packaged_runtime"),
            mock.patch.object(
                smoke,
                "request_json",
                return_value=(
                    200,
                    {
                        "config": {
                            "ready": True,
                            "credentialsPackaged": True,
                            "keyDerivation": "embedded",
                        }
                    },
                ),
            ),
            mock.patch.object(
                smoke,
                "post_json",
                side_effect=[
                    (200, {"ok": True, "remoteFound": True, "timing": {}}),
                    (200, {"ok": True, "timing": {"totalDurationMs": 11}}),
                    (200, {"ok": True, "timing": {"totalDurationMs": 22}}),
                ],
            ),
            mock.patch.object(
                smoke,
                "capture_performance_profile_snapshot",
                return_value={
                    "performanceProfileSnapshot": str(
                        artifacts_dir / "performance-profile.post-sync.json"
                    ),
                    "storageMetricsSnapshot": str(artifacts_dir / "storage-metrics.post-sync.json"),
                },
            ) as profile_mock,
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            result = smoke.run_packaged_sync_rehearsal(
                exe_path=exe_path,
                artifacts_dir=artifacts_dir,
                runtime_timeout_s=5.0,
            )

        assert result["status"] == "passed"
        assert result["details"]["performanceProfileSnapshot"] == str(
            artifacts_dir / "performance-profile.post-sync.json"
        )
        assert result["details"]["storageMetricsSnapshot"] == str(
            artifacts_dir / "storage-metrics.post-sync.json"
        )
        profile_mock.assert_called_once_with(
            "http://127.0.0.1:51002",
            artifacts_dir,
            filename="performance-profile.post-sync.json",
        )
