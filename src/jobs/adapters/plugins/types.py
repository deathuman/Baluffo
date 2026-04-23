from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from src.jobs.models import RawJob


@dataclass(frozen=True)
class AdapterPluginContext:
    """Context passed to plugin selection and execution.

    This stays intentionally small/stable. Adapter families can extend selection
    by adding additional fields as optional keys.
    """

    family: str
    adapter_key: str
    source_identity: str = ""


class AdapterPlugin(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def family(self) -> str: ...

    @property
    def priority(self) -> int: ...

    def can_handle(self, ctx: AdapterPluginContext) -> bool: ...

    def run(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        **kwargs: Any,
    ) -> Sequence[RawJob]: ...


@dataclass(frozen=True)
class SimpleAdapterPlugin:
    """Concrete plugin implementation with function-based handlers."""

    name: str
    family: str
    can_handle_fn: Callable[[AdapterPluginContext], bool]
    run_fn: Callable[..., Sequence[RawJob]]
    priority: int = 100

    def can_handle(self, ctx: AdapterPluginContext) -> bool:
        return bool(self.can_handle_fn(ctx))

    def run(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        **kwargs: Any,
    ) -> Sequence[RawJob]:
        return self.run_fn(
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            **kwargs,
        )
