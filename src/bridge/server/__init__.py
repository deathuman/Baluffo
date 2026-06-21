"""HTTP server components for the local admin bridge.

AI boundary owns: bridge server package surface for handler construction and HTTP server startup.
AI boundary implement in: this file for server package exports; request handling stays in server leaf modules.
AI boundary search before contracts: server handler/httpd modules, admin entrypoint runtime, and bridge server tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge server tests.
"""

from __future__ import annotations

from src.bridge.server.handler import make_handler
from src.bridge.server.httpd import run_http_server

__all__ = ["make_handler", "run_http_server"]
