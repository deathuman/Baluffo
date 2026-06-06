from __future__ import annotations

import gzip
import json
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from src.bridge.routes.get_routes import handle_get
from src.bridge.routes.post_routes import handle_post
from src.bridge.server.handler import make_handler
from src.bridge.server.static_files import StaticFileService
from src.runtime_seed import seed_runtime_data
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


@contextmanager
def _served(handler_cls: type):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _read_url(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
):
    request = Request(f"{base_url}{path}", method=method, headers=headers or {})
    with urlopen(request, timeout=3.0) as response:
        return response, response.read()


def _assert_no_cors(response) -> None:
    assert response.headers.get("Access-Control-Allow-Origin") is None
    assert response.headers.get("Access-Control-Allow-Methods") is None
    assert response.headers.get("Access-Control-Allow-Headers") is None


def _make_container_handler(root: Path, data_dir: Path):
    api = make_stub_bridge_api(data_dir, FakeDesktopLocalDataStore())
    api.runtime_config.root = root
    api.runtime_config.data_dir = data_dir
    api.runtime_config.container_mode = True
    return make_handler(
        api=api,
        static_service=StaticFileService(static_root=root, data_dir=data_dir),
    )


def test_container_handler_serves_static_data_and_runtime_config(tmp_path: Path) -> None:
    root = tmp_path / "root"
    data_dir = tmp_path / "data"
    (root / "styles").mkdir(parents=True)
    data_dir.mkdir(parents=True)
    (root / "index.html").write_text("<html>index</html>\n", encoding="utf-8")
    (root / "jobs.html").write_text("<html>jobs</html>\n", encoding="utf-8")
    (root / "styles" / "jobs.css").write_text("body { color: black; }\n", encoding="utf-8")
    (root / "frontend-runtime-config.js").write_text("stale desktop config\n", encoding="utf-8")
    (data_dir / "jobs-fetch-report.json").write_text('{"from":"runtime"}\n', encoding="utf-8")
    (data_dir / "source-registry-active.json").write_text('{"active":true}\n', encoding="utf-8")
    (data_dir / "local-user-data").mkdir()
    (data_dir / "local-user-data" / "profiles.json").write_text(
        '{"private":true}\n', encoding="utf-8"
    )

    with _served(_make_container_handler(root, data_dir)) as base_url:
        html_response, html_body = _read_url(base_url, "/jobs.html")
        page_response, page_body = _read_url(base_url, "/unknown/page")
        css_response, _css_body = _read_url(base_url, "/styles/jobs.css")
        config_response, config_body = _read_url(base_url, "/frontend-runtime-config.js?v=1")
        data_response, data_body = _read_url(base_url, "/data/jobs-fetch-report.json")
        root_data_response, root_data_body = _read_url(base_url, "/source-registry-active.json")

    assert b"<html>jobs</html>" in html_body
    assert html_response.headers["Cache-Control"].startswith("no-store")
    _assert_no_cors(html_response)
    assert b"<html>index</html>" in page_body
    assert page_response.headers["Cache-Control"].startswith("no-store")
    _assert_no_cors(page_response)
    assert css_response.headers["Cache-Control"] == "public, max-age=3600"
    _assert_no_cors(css_response)
    config_text = config_body.decode("utf-8")
    assert "BALUFFO_FRONTEND_RUNTIME_CONFIG" in config_text
    assert '"sameOrigin": true' in config_text
    assert '"mode": "container"' in config_text
    assert "127.0.0.1:8877" not in config_text
    assert config_response.headers["Cache-Control"].startswith("no-store")
    _assert_no_cors(config_response)
    assert json.loads(data_body.decode("utf-8")) == {"from": "runtime"}
    assert data_response.headers["Cache-Control"].startswith("no-store")
    _assert_no_cors(data_response)
    assert json.loads(root_data_body.decode("utf-8")) == {"active": True}
    assert root_data_response.headers["Cache-Control"].startswith("no-store")
    _assert_no_cors(root_data_response)

    with _served(_make_container_handler(root, data_dir)) as base_url:
        try:
            _read_url(base_url, "/data/local-user-data/profiles.json")
        except HTTPError as exc:
            private_response = exc
        else:  # pragma: no cover
            raise AssertionError("expected local user data to stay off static serving")
    assert private_response.code == 404


def test_container_handler_serves_generated_frontend_assets_with_immutable_cache(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    data_dir = tmp_path / "data"
    bundle_dir = root / ".container-frontend"
    asset_dir = bundle_dir / "assets"
    asset_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)
    (root / "admin.html").write_text("<html>repo admin</html>\n", encoding="utf-8")
    (bundle_dir / "admin.html").write_text(
        '<html><script type="module" src="/container-assets/assets/admin.abc123.js"></script></html>\n',
        encoding="utf-8",
    )
    bundle = b"console.log('container admin');\n"
    (asset_dir / "admin.abc123.js").write_bytes(bundle)
    (asset_dir / "admin.abc123.js.gz").write_bytes(gzip.compress(bundle))

    with _served(_make_container_handler(root, data_dir)) as base_url:
        html_response, html_body = _read_url(base_url, "/admin.html")
        asset_response, asset_body = _read_url(
            base_url,
            "/container-assets/assets/admin.abc123.js",
            headers={"Accept-Encoding": "gzip, deflate"},
        )

    assert b"container-assets/assets/admin.abc123.js" in html_body
    assert b"repo admin" not in html_body
    assert html_response.headers["Cache-Control"].startswith("no-store")
    assert asset_response.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    assert asset_response.headers["Content-Encoding"] == "gzip"
    assert gzip.decompress(asset_body) == bundle


def test_container_handler_omits_cors_headers_for_api_and_options(tmp_path: Path) -> None:
    root = tmp_path / "root"
    data_dir = tmp_path / "data"
    root.mkdir()
    data_dir.mkdir()

    with _served(_make_container_handler(root, data_dir)) as base_url:
        api_response, _api_body = _read_url(
            base_url,
            "/ops/health",
            headers={"Origin": "https://attacker.example"},
        )
        options_response, _options_body = _read_url(
            base_url,
            "/ops/health",
            method="OPTIONS",
            headers={"Origin": "https://attacker.example"},
        )

    _assert_no_cors(api_response)
    _assert_no_cors(options_response)


def test_desktop_handler_preserves_existing_cors_headers(tmp_path: Path) -> None:
    root = tmp_path / "root"
    data_dir = tmp_path / "data"
    (root / "styles").mkdir(parents=True)
    data_dir.mkdir()
    (root / "styles" / "jobs.css").write_text("body { color: black; }\n", encoding="utf-8")
    api = make_stub_bridge_api(data_dir, FakeDesktopLocalDataStore())
    api.runtime_config.root = root
    api.runtime_config.data_dir = data_dir
    handler_cls = make_handler(
        api=api,
        static_service=StaticFileService(static_root=root, data_dir=data_dir),
    )

    with _served(handler_cls) as base_url:
        api_response, _api_body = _read_url(base_url, "/ops/health")
        css_response, _css_body = _read_url(base_url, "/styles/jobs.css")
        options_response, _options_body = _read_url(base_url, "/ops/health", method="OPTIONS")

    assert api_response.headers["Access-Control-Allow-Origin"] == "*"
    assert api_response.headers["Access-Control-Allow-Methods"] == "GET,POST,OPTIONS"
    assert api_response.headers["Access-Control-Allow-Headers"] == "Content-Type"
    assert css_response.headers["Access-Control-Allow-Origin"] == "*"
    assert css_response.headers.get("Access-Control-Allow-Methods") is None
    assert css_response.headers.get("Access-Control-Allow-Headers") is None
    assert options_response.headers["Access-Control-Allow-Origin"] == "*"
    assert options_response.headers["Access-Control-Allow-Methods"] == "GET,POST,OPTIONS"
    assert options_response.headers["Access-Control-Allow-Headers"] == "Content-Type"


def test_container_handler_keeps_unknown_api_routes_as_json_404(tmp_path: Path) -> None:
    root = tmp_path / "root"
    data_dir = tmp_path / "data"
    root.mkdir()
    data_dir.mkdir()

    with _served(_make_container_handler(root, data_dir)) as base_url:
        try:
            _read_url(base_url, "/ops/not-a-route")
        except HTTPError as exc:
            response = exc
        else:  # pragma: no cover
            raise AssertionError("expected 404 response")

    assert getattr(response, "code", 0) == 404
    assert "application/json" in response.headers["Content-Type"]


def test_container_mode_disables_desktop_only_routes(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.runtime_config.container_mode = True

    get_handler = FakeHandler()
    assert handle_get(get_handler, api=api, path="/app/update-status", query={}) is True
    assert get_handler.sent[-1]["status"] == 409
    assert get_handler.sent[-1]["payload"] == {
        "ok": False,
        "error": "not available in container mode",
    }

    for path in (
        "/desktop-local-data/open-url",
        "/app/desktop-session-lifecycle",
        "/app/check-for-update",
        "/app/download-update",
        "/app/install-update",
    ):
        post_handler = FakeHandler()
        assert handle_post(post_handler, api=api, path=path, payload={"url": "https://example.com"})
        assert post_handler.sent[-1]["status"] == 409
        assert post_handler.sent[-1]["payload"]["error"] == "not available in container mode"


def test_seed_runtime_data_copies_defaults_and_never_overwrites(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[2]
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    existing_report = data_dir / "jobs-fetch-report.json"
    existing_report.write_text('{"keep":"me"}\n', encoding="utf-8")

    result = seed_runtime_data(data_dir, source_root=source_root, overwrite=False)

    assert (data_dir / "defaults" / "source-registry-active.seed.json").exists()
    assert (data_dir / "defaults" / "source-registry-pending.seed.json").exists()
    assert (data_dir / "contracts" / "country_acceptance.json").exists()
    assert json.loads(existing_report.read_text(encoding="utf-8")) == {"keep": "me"}
    assert (data_dir / "source-registry-rejected.json").exists()
    assert (data_dir / "jobs-source-state.json.gz").exists()
    assert "jobs-fetch-report.json" in result["skipped"]
