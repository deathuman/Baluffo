from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.source_sync_checkpoint_tags import build_checkpoint_tag_plan


def _run_git(*args: str) -> None:
    subprocess.run(["git", *args], cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish source-sync checkpoint tags.")
    parser.add_argument("--branch-name", default="")
    parser.add_argument("--validation-conclusion", default="success")
    parser.add_argument("--commit-sha", default="")
    args = parser.parse_args()

    plan = build_checkpoint_tag_plan(
        branch_name=args.branch_name,
        validation_conclusion=args.validation_conclusion,
        commit_sha=args.commit_sha,
    )
    if not plan.publish:
        print(
            f"Skipping source-sync checkpoint tags: {plan.skip_reason or 'not publishable'}",
            flush=True,
        )
        return 0

    for tag_name in plan.tag_names:
        _run_git("tag", "-f", tag_name, plan.commit_sha)
    refspecs = [f"refs/tags/{tag_name}" for tag_name in plan.tag_names]
    _run_git("push", "--force", "origin", *refspecs)
    print(
        f"Published source-sync checkpoint tags for {plan.commit_sha}: {', '.join(plan.tag_names)}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
