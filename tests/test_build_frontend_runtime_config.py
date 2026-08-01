from unittest import mock

from scripts import build_frontend_runtime_config as frontend_runtime_config


def test_build_payload_contains_only_frontend_safe_fields() -> None:
    with mock.patch.object(
        frontend_runtime_config,
        "get_bridge_defaults",
        return_value={"host": "192.168.1.10", "port": 9000},
    ):
        payload = frontend_runtime_config.build_frontend_runtime_config_payload()
    assert payload["bridge"] == {"host": "192.168.1.10", "port": 9000}
    assert set(payload.keys()) == {"bridge"}


def test_render_js_exports_frozen_payload() -> None:
    text = frontend_runtime_config.render_frontend_runtime_config_js(
        {
            "bridge": {"host": "127.0.0.1", "port": 8877},
        }
    )
    assert "BALUFFO_FRONTEND_RUNTIME_CONFIG" in text
    assert '"host": "127.0.0.1"' in text
    assert "Object.freeze" in text
    assert "globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG" in text
    assert "export const" not in text


def test_checked_in_generated_file_matches_current_render() -> None:
    expected = frontend_runtime_config.render_frontend_runtime_config_js(
        frontend_runtime_config.build_frontend_runtime_config_payload()
    )
    actual = (frontend_runtime_config.ROOT / "frontend-runtime-config.js").read_text(
        encoding="utf-8"
    )
    assert actual == expected, (
        "frontend-runtime-config.js is stale; run `npm run build:frontend-runtime-config`."
    )
