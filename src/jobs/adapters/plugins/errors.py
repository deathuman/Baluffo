"""Jobs adapter plugin error types.

AI boundary owns: shared plugin exception classes and error compatibility surface.
AI boundary implement in: this file for plugin error types only; classification belongs in source_error/provider leaves.
AI boundary search before contracts: plugin runners, source error handling, and adapter tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused plugin error tests.
"""

from __future__ import annotations


class PluginError(RuntimeError):
    pass


class NoPluginFoundError(PluginError):
    def __init__(self, *, family: str, context: object = None) -> None:
        msg = f"No plugin found for family={family!r}"
        if context is not None:
            msg = f"{msg} context={context!r}"
        super().__init__(msg)
        self.family = family
        self.context = context
