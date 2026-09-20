"""Background-script launch primitives shared by bridge task starters.

AI boundary owns: the identity-aware ``run_background_script`` call shape and its
legacy-signature fallback.
AI boundary implement in: this leaf for launch-call primitives only; callers own
run ids, lifecycle rows, and task payloads.
AI boundary search before contracts: task launch API, discovery service launch, and
task start payload tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused task launch and
discovery service tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def run_background_script_with_identity(
    run_background_script: Callable[..., int],
    script_name: str,
    args: list[str],
    *,
    extra_env: dict[str, str],
    run_id: str,
    task_type: str,
    metadata: dict[str, Any],
) -> int:
    """Launch a background script with task identity, tolerating legacy seams.

    Older ``run_background_script`` implementations accept only ``extra_env``.
    When one rejects the identity keywords, retry without them rather than
    failing the task start; any other ``TypeError`` still propagates.
    """
    try:
        return run_background_script(
            script_name,
            args,
            extra_env=extra_env,
            run_id=run_id,
            task_type=task_type,
            metadata=metadata,
        )
    except TypeError as exc:
        message = str(exc)
        if "unexpected keyword argument" not in message or "run_id" not in message:
            raise
        return run_background_script(script_name, args, extra_env=extra_env)


__all__ = ["run_background_script_with_identity"]
