from __future__ import annotations

"""CLI dispatch for the ship update manager."""

import argparse
import json
import os
from pathlib import Path

from .update_manager_apply import apply_update
from .update_manager_recovery import create_support_bundle, recover_previous, startup_check
from .update_manager_validation import sign_manifest

EXPECTED_CLI_FAILURES = (OSError, RuntimeError, ValueError)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baluffo ship update manager.")
    sub = parser.add_subparsers(dest="command", required=True)

    apply_parser = sub.add_parser("apply", help="Apply an update artifact atomically.")
    apply_parser.add_argument("--root", required=True)
    apply_parser.add_argument("--bundle-zip", required=True)
    apply_parser.add_argument("--manifest", required=True)
    apply_parser.add_argument("--signing-key", default=os.getenv("BALUFFO_UPDATE_SIGNING_KEY", ""))

    recover_parser = sub.add_parser("recover", help="Switch back to previous version.")
    recover_parser.add_argument("--root", required=True)

    check_parser = sub.add_parser(
        "startup-check", help="Validate active version and data path before startup."
    )
    check_parser.add_argument("--root", required=True)
    check_parser.add_argument("--data-dir", required=True)

    support_parser = sub.add_parser(
        "support-bundle", help="Build a support bundle for diagnostics."
    )
    support_parser.add_argument("--root", required=True)
    support_parser.add_argument("--output", default="")

    sign_parser = sub.add_parser("sign-manifest", help="Sign version/hash for manifest usage.")
    sign_parser.add_argument("--version", required=True)
    sign_parser.add_argument("--sha256", required=True)
    sign_parser.add_argument("--signing-key", default=os.getenv("BALUFFO_UPDATE_SIGNING_KEY", ""))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "apply":
            if not str(args.signing_key).strip():
                raise RuntimeError(
                    "Missing signing key. Use --signing-key or BALUFFO_UPDATE_SIGNING_KEY."
                )
            result = apply_update(
                Path(args.root),
                Path(args.bundle_zip),
                Path(args.manifest),
                str(args.signing_key),
            )
            print(json.dumps(result, indent=2))
            return 0

        if args.command == "recover":
            print(json.dumps(recover_previous(Path(args.root)), indent=2))
            return 0

        if args.command == "startup-check":
            print(json.dumps(startup_check(Path(args.root), Path(args.data_dir)), indent=2))
            return 0

        if args.command == "support-bundle":
            output = Path(args.output).expanduser().resolve() if str(args.output).strip() else None
            bundle = create_support_bundle(Path(args.root), output=output)
            print(json.dumps({"ok": True, "bundle": str(bundle)}, indent=2))
            return 0

        if args.command == "sign-manifest":
            if not str(args.signing_key).strip():
                raise RuntimeError(
                    "Missing signing key. Use --signing-key or BALUFFO_UPDATE_SIGNING_KEY."
                )
            print(sign_manifest(args.version, args.sha256, str(args.signing_key)))
            return 0

    except EXPECTED_CLI_FAILURES as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        return 1
    return 1
