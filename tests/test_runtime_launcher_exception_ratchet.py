from pathlib import Path
from unittest import mock

import pytest

from src.ship import runtime_launcher as rl


def test_quiet_site_handler_does_not_swallow_unexpected_failures() -> None:
    handler_cls = rl.build_site_request_handler(Path.cwd())
    handler = handler_cls.__new__(handler_cls)
    handler.close_connection = False

    with (
        mock.patch.object(
            rl.SimpleHTTPRequestHandler,
            "handle_one_request",
            side_effect=AssertionError("unexpected handler bug"),
        ),
        pytest.raises(AssertionError, match="unexpected handler bug"),
    ):
        handler.handle_one_request()

    assert handler.close_connection is False
