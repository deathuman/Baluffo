"""Hard wall-clock bounds for the HTTP read path.

httpx and urllib both expose per-operation timeouts, so a peer that trickles a byte
before each deadline can keep a response open indefinitely. These tests pin the
total-deadline guard that closes the response instead of waiting for the peer.
"""

from __future__ import annotations

import threading
import time

import pytest

from src.jobs.common.http import _read_response_body


class _DripResponse:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def read(self, _size: int = -1) -> bytes:
        self.closed.wait(5.0)
        return b"partial"

    def close(self) -> None:
        self.closed.set()


def test_read_response_body_aborts_a_trickling_response() -> None:
    response = _DripResponse()
    started = time.perf_counter()

    with pytest.raises(TimeoutError, match="request deadline exceeded"):
        _read_response_body(response, cap=1024, timeout_s=0.05)

    assert (
        time.perf_counter() - started < 1.0
    )  # timing-ok: 20x ceiling over the 0.05 s read deadline
    assert response.closed.is_set()
