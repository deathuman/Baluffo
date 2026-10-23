import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.ship import runtime_launcher as rl
from src.ship import startup_telemetry as telemetry


def test_wait_for_url_keeps_full_request_budget_for_slow_listener() -> None:
    class _SlowHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            time.sleep(0.35)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _SlowHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/slow"
        rl.wait_for_url(url, timeout_s=2.0, interval_s=0.05)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_warm_loopback_connect_returns_once_the_listener_accepts() -> None:
    accepted: list[str] = []

    class _AcceptHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(204)
            self.end_headers()

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), _AcceptHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    ready = threading.Event()

    def _mark_ready() -> None:
        accepted.append("ready")
        ready.set()

    try:
        warm = telemetry.warm_loopback_connect(
            "127.0.0.1",
            int(server.server_address[1]),
            deadline_s=2.0,
            on_ready=_mark_ready,
        )
        assert ready.wait(2.0), "warm-up never connected to a live listener"
        warm.join(timeout=2.0)
        assert not warm.is_alive()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert accepted == ["ready"]


def test_warm_loopback_connect_gives_up_without_a_listener() -> None:
    started = time.monotonic()
    warm = telemetry.warm_loopback_connect("127.0.0.1", 9, deadline_s=0.3)
    warm.join(timeout=3.0)

    assert not warm.is_alive()
    assert time.monotonic() - started < 3.0  # timing-ok: 0.3 s deadline with a 10x ceiling
