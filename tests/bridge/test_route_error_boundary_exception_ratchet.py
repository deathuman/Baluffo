from __future__ import annotations

import pytest

from src.bridge.routes.error_boundary import safe_bridge_log


class _BridgeLogApi:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def bridge_log(self, level: str, event: str, **fields: object) -> None:
        self.calls.append((level, event, fields))
        if self.error is not None:
            raise self.error


@pytest.mark.parametrize(
    "error",
    [
        AttributeError("bridge log unavailable"),
        OSError("diagnostic sink unavailable"),
        TypeError("bad diagnostic payload"),
        ValueError("invalid diagnostic value"),
    ],
)
def test_safe_bridge_log_suppresses_expected_logging_failures(error: BaseException) -> None:
    api = _BridgeLogApi(error)

    safe_bridge_log(api, "error", "route_failed", path="/discovery")

    assert api.calls == [("error", "route_failed", {"path": "/discovery"})]


def test_safe_bridge_log_does_not_swallow_unexpected_logging_bug() -> None:
    api = _BridgeLogApi(RuntimeError("unexpected logger bug"))

    with pytest.raises(RuntimeError, match="unexpected logger bug"):
        safe_bridge_log(api, "error", "route_failed", path="/discovery")
