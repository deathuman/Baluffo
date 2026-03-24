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
