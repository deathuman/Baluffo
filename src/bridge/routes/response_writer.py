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
