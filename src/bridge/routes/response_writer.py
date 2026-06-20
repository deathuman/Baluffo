"""Bridge response writer protocol for route modules.

AI boundary owns: the minimal response-writing protocol consumed by route leaves.
AI boundary implement in: bridge server handlers and route response adapters.
AI boundary search before contracts: route callers, handler implementations, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

from typing import Any, Protocol


class BridgeResponseWriter(Protocol):
    def send_json(self, payload: Any, status: int = 200) -> None: ...

    def send_bytes(
        self,
        body: bytes,
        *,
        content_type: str,
        filename: str = "",
        disposition: str = "inline",
        status: int = 200,
        cache_control: str = "no-store",
        content_encoding: str = "",
    ) -> None: ...
