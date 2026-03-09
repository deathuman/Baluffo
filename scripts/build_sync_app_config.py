#!/usr/bin/env python3
"""Build a packaged GitHub App sync config from a private key PEM."""

from __future__ import annotations

import argparse
import json
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import source_sync


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build packaged GitHub App sync config JSON.")
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--installation-id", required=True)
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--path", default="baluffo/source-sync.json")
    parser.add_argument("--private-key", required=True, help="Path to GitHub App private key PEM")
    parser.add_argument(
        "--output",
        default=str(ROOT / "packaging" / "github-app-sync-config.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--salt",
        default="",
        help="Optional base64url salt. If omitted, a new random salt is generated.",
    )
    parser.add_argument(
        "--plaintext",
        action="store_true",
        help="Write privateKeyPem in plaintext for local testing only.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    private_key_path = Path(args.private_key).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    private_key_pem = private_key_path.read_text(encoding="utf-8")

    payload = {
        "schemaVersion": 1,
        "appId": str(args.app_id).strip(),
        "installationId": str(args.installation_id).strip(),
        "repo": str(args.repo).strip(),
        "branch": str(args.branch).strip() or "main",
        "path": str(args.path).strip() or "baluffo/source-sync.json",
    }

    if args.plaintext:
        payload["privateKeyPem"] = private_key_pem
    else:
        salt_b64 = str(args.salt or "").strip() or source_sync._base64url_encode(secrets.token_bytes(18))  # noqa: SLF001
        payload["keySalt"] = salt_b64
        payload["privateKeyPemEnc"] = source_sync.encrypt_private_key_pem(
            private_key_pem,
            salt_b64=salt_b64,
            app_id=payload["appId"],
            installation_id=payload["installationId"],
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
