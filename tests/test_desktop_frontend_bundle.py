from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from scripts import desktop_frontend_bundle
from scripts.desktop_frontend_bundle import BUNDLED_PAGES, bundle_desktop_pages
from tests.helpers.temp_paths import workspace_tmpdir


def _version_dir(root: str, *, pages: tuple[str, ...] = BUNDLED_PAGES) -> Path:
    version_dir = Path(root) / "app" / "versions" / "1.2.3"
    for name in ("frontend/admin", "frontend/jobs", "frontend/saved"):
        (version_dir / name).mkdir(parents=True, exist_ok=True)
    for name in pages:
        (version_dir / name).write_text("<html></html>\n", encoding="utf-8")
    return version_dir


def _payload(root: Path) -> dict[str, list[dict[str, object]]]:
    pages: list[dict[str, object]] = []
    for name in BUNDLED_PAGES:
        stem = name.removesuffix(".html")
        asset = f"frontend/{stem}/{stem}.abc123.js"
        (root / asset).parent.mkdir(parents=True, exist_ok=True)
        (root / asset).write_text("// bundle\n", encoding="utf-8")
        pages.append(
            {
                "name": stem,
                "html": name,
                "entry": f"frontend/{stem}/index.js",
                "asset": asset,
                "chunks": [],
            }
        )
    return {"pages": pages}


def test_bundle_names_every_missing_page() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp, pages=("jobs.html",))

        with pytest.raises(RuntimeError) as excinfo:
            bundle_desktop_pages(version_dir)

        message = str(excinfo.value)
        assert "admin.html" in message
        assert "saved.html" in message
        # The page that is present must not be reported, or the message stops
        # naming what the build actually has to add.
        assert "jobs.html" not in message


def test_bundle_runs_the_esbuild_script_once_for_every_page() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        payload = _payload(version_dir)
        completed = mock.Mock(returncode=0, stdout=json.dumps(payload), stderr="")

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(
                desktop_frontend_bundle.subprocess, "run", return_value=completed
            ) as run,
        ):
            result = bundle_desktop_pages(version_dir)

        command = run.call_args.args[0]
        assert command[:2] == ["node", str(desktop_frontend_bundle.BUNDLER_SCRIPT)]
        assert command[2:] == ["--root", str(version_dir.resolve())]
        assert run.call_args.kwargs["cwd"] == desktop_frontend_bundle.ROOT
        assert run.call_args.kwargs["check"] is False
        assert run.call_count == 1
        assert [page["name"] for page in result["pages"]] == ["admin", "jobs", "saved"]


def test_bundle_failure_surfaces_the_node_error_and_the_install_hint() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        completed = mock.Mock(
            returncode=1,
            stdout="",
            stderr="Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'esbuild'",
        )

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(desktop_frontend_bundle.subprocess, "run", return_value=completed),
        ):
            with pytest.raises(RuntimeError) as excinfo:
                bundle_desktop_pages(version_dir)

        message = str(excinfo.value)
        assert "Cannot find package 'esbuild'" in message
        assert "npm ci" in message


def test_bundle_rejects_a_reported_asset_that_is_not_on_disk() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        payload = _payload(version_dir)
        payload["pages"][2]["asset"] = "frontend/saved/saved.missing.js"
        completed = mock.Mock(returncode=0, stdout=json.dumps(payload), stderr="")

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(desktop_frontend_bundle.subprocess, "run", return_value=completed),
        ):
            with pytest.raises(RuntimeError, match="missing asset"):
                bundle_desktop_pages(version_dir)


def test_bundle_rejects_a_split_page_chunk_that_is_not_on_disk() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        payload = _payload(version_dir)
        payload["pages"][0]["chunks"] = [
            {"asset": "frontend/admin/chunks/admin-lazy.js", "bytes": 12}
        ]
        completed = mock.Mock(returncode=0, stdout=json.dumps(payload), stderr="")

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(desktop_frontend_bundle.subprocess, "run", return_value=completed),
        ):
            with pytest.raises(RuntimeError, match="missing chunk"):
                bundle_desktop_pages(version_dir)


def test_bundle_accepts_a_split_page_when_its_chunk_exists() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        payload = _payload(version_dir)
        chunk = version_dir / "frontend" / "admin" / "chunks" / "admin-lazy.js"
        chunk.parent.mkdir(parents=True, exist_ok=True)
        chunk.write_text("// lazy\n", encoding="utf-8")
        payload["pages"][0]["chunks"] = [
            {"asset": "frontend/admin/chunks/admin-lazy.js", "bytes": 9}
        ]
        completed = mock.Mock(returncode=0, stdout=json.dumps(payload), stderr="")

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(desktop_frontend_bundle.subprocess, "run", return_value=completed),
        ):
            result = bundle_desktop_pages(version_dir)

        assert result["pages"][0]["chunks"][0]["asset"].endswith("admin-lazy.js")


def test_bundle_without_page_metadata_is_a_failure_not_a_silent_unbundled_page() -> None:
    with workspace_tmpdir("desktop-frontend-bundle") as tmp:
        version_dir = _version_dir(tmp)
        completed = mock.Mock(returncode=0, stdout="", stderr="")

        with (
            mock.patch.object(
                desktop_frontend_bundle, "resolve_node_command", return_value=["node"]
            ),
            mock.patch.object(desktop_frontend_bundle.subprocess, "run", return_value=completed),
        ):
            with pytest.raises(RuntimeError, match="printed no metadata"):
                bundle_desktop_pages(version_dir)
