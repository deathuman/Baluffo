#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ship.runtime_launcher import build_site_request_handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve a static site with quiet client-disconnect handling."
    )
    parser.add_argument("--directory", default=".", help="Directory to serve.")
    parser.add_argument("--port", type=int, default=4173, help="Port to bind.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    handler = build_site_request_handler(Path(args.directory).expanduser().resolve())
    server = ThreadingHTTPServer((str(args.host), int(args.port)), handler)
    with server:
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
