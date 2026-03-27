from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bridge.lifecycle_cleanup import reset_admin_task_lifecycle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset admin task lifecycle artifacts to a clean runId-based baseline."
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing admin lifecycle JSON artifacts.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = reset_admin_task_lifecycle(Path(args.data_dir))
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
