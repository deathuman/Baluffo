#!/usr/bin/env python3
"""Build a packaged GitHub App sync config from a private key PEM."""

from __future__ import annotations

import argparse
import json
import os
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
    parser.add_argument("--allowed-repo", default="", help="Optional hard allowlist repo (owner/repo).")
    parser.add_argument("--allowed-branch", default="", help="Optional hard allowlist branch.")
    parser.add_argument("--allowed-path-prefix", default="", help="Optional hard allowlist path prefix.")
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
    parser.add_argument(
        "--key-derivation",
        default=source_sync.KEY_DERIVATION_MACHINE,
        choices=[
            source_sync.KEY_DERIVATION_MACHINE,
            source_sync.KEY_DERIVATION_PASSPHRASE,
            source_sync.KEY_DERIVATION_EMBEDDED,
            source_sync.KEY_DERIVATION_PLAINTEXT,
        ],
        help="Private key derivation mode.",
    )
    parser.add_argument(
        "--portable-passphrase-env",
        default="",
        help=(
            "Environment variable name containing passphrase for portable encryption mode "
            f"(runtime also reads {source_sync.PACKAGED_SYNC_PASSPHRASE_ENV})."
        ),
    )
    parser.add_argument("--embedded-key-hint", default="", help="Optional embedded derivation hint.")
    parser.add_argument("--embedded-key-version", default=source_sync.EMBEDDED_KEY_VERSION_DEFAULT, help="Embedded derivation version token.")
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
    allowed_repo = str(args.allowed_repo).strip()
    allowed_branch = str(args.allowed_branch).strip()
    allowed_prefix = str(args.allowed_path_prefix).strip()
    if allowed_repo:
        payload["allowedRepo"] = allowed_repo
    if allowed_branch:
        payload["allowedBranch"] = allowed_branch
    if allowed_prefix:
        payload["allowedPathPrefix"] = allowed_prefix

    portable_passphrase_env = str(args.portable_passphrase_env or "").strip()
    key_derivation = str(args.key_derivation or source_sync.KEY_DERIVATION_MACHINE).strip().lower()
    if args.plaintext:
        key_derivation = source_sync.KEY_DERIVATION_PLAINTEXT
    if key_derivation == source_sync.KEY_DERIVATION_PLAINTEXT and portable_passphrase_env:
        raise RuntimeError("--plaintext and --portable-passphrase-env are mutually exclusive.")

    if key_derivation == source_sync.KEY_DERIVATION_PLAINTEXT:
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_PLAINTEXT
        payload["privateKeyPem"] = private_key_pem
    elif key_derivation == source_sync.KEY_DERIVATION_EMBEDDED:
        salt_b64 = str(args.salt or "").strip() or source_sync._base64url_encode(secrets.token_bytes(18))  # noqa: SLF001
        hint = str(args.embedded_key_hint or "").strip() or source_sync._base64url_encode(secrets.token_bytes(9))  # noqa: SLF001
        version = str(args.embedded_key_version or source_sync.EMBEDDED_KEY_VERSION_DEFAULT).strip() or source_sync.EMBEDDED_KEY_VERSION_DEFAULT
        passphrase = source_sync.build_embedded_passphrase(hint=hint, version=version)
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_EMBEDDED
        payload["embeddedKeyHint"] = hint
        payload["embeddedKeyVersion"] = version
        payload["keySalt"] = salt_b64
        payload["privateKeyPemEnc"] = source_sync.encrypt_private_key_pem_with_passphrase(
            private_key_pem,
            salt_b64=salt_b64,
            app_id=payload["appId"],
            installation_id=payload["installationId"],
            passphrase=passphrase,
        )
    elif key_derivation == source_sync.KEY_DERIVATION_PASSPHRASE:
        env_key = portable_passphrase_env or source_sync.PACKAGED_SYNC_PASSPHRASE_ENV
        passphrase = str(os.environ.get(env_key) or "")
        if not passphrase:
            raise RuntimeError(f"Missing passphrase value in environment variable: {env_key}")
        salt_b64 = str(args.salt or "").strip() or source_sync._base64url_encode(secrets.token_bytes(18))  # noqa: SLF001
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_PASSPHRASE
        payload["keySalt"] = salt_b64
        payload["privateKeyPemEnc"] = source_sync.encrypt_private_key_pem_with_passphrase(
            private_key_pem,
            salt_b64=salt_b64,
            app_id=payload["appId"],
            installation_id=payload["installationId"],
            passphrase=passphrase,
        )
    else:
        salt_b64 = str(args.salt or "").strip() or source_sync._base64url_encode(secrets.token_bytes(18))  # noqa: SLF001
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_MACHINE
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
