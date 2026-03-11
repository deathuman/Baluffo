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


def build_packaged_sync_payload(
    *,
    app_id: str,
    installation_id: str,
    repo: str,
    branch: str = "main",
    path: str = "baluffo/source-sync.json",
    allowed_repo: str = "",
    allowed_branch: str = "",
    allowed_path_prefix: str = "",
    private_key_pem: str,
    salt: str = "",
    plaintext: bool = False,
    key_derivation: str = source_sync.KEY_DERIVATION_MACHINE,
    portable_passphrase_env: str = "",
    embedded_key_hint: str = "",
    embedded_key_version: str = source_sync.EMBEDDED_KEY_VERSION_DEFAULT,
    env: dict[str, str] | None = None,
) -> dict:
    runtime_env = env if env is not None else os.environ
    payload = {
        "schemaVersion": 1,
        "appId": str(app_id).strip(),
        "installationId": str(installation_id).strip(),
        "repo": str(repo).strip(),
        "branch": str(branch).strip() or "main",
        "path": str(path).strip() or "baluffo/source-sync.json",
    }
    normalized_allowed_repo = str(allowed_repo).strip()
    normalized_allowed_branch = str(allowed_branch).strip()
    normalized_allowed_prefix = str(allowed_path_prefix).strip()
    if normalized_allowed_repo:
        payload["allowedRepo"] = normalized_allowed_repo
    if normalized_allowed_branch:
        payload["allowedBranch"] = normalized_allowed_branch
    if normalized_allowed_prefix:
        payload["allowedPathPrefix"] = normalized_allowed_prefix

    normalized_passphrase_env = str(portable_passphrase_env or "").strip()
    normalized_derivation = str(key_derivation or source_sync.KEY_DERIVATION_MACHINE).strip().lower()
    if plaintext:
        normalized_derivation = source_sync.KEY_DERIVATION_PLAINTEXT
    if normalized_derivation == source_sync.KEY_DERIVATION_PLAINTEXT and normalized_passphrase_env:
        raise RuntimeError("--plaintext and --portable-passphrase-env are mutually exclusive.")

    if normalized_derivation == source_sync.KEY_DERIVATION_PLAINTEXT:
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_PLAINTEXT
        payload["privateKeyPem"] = private_key_pem
        return payload

    salt_b64 = str(salt or "").strip() or source_sync._base64url_encode(secrets.token_bytes(18))  # noqa: SLF001
    if normalized_derivation == source_sync.KEY_DERIVATION_EMBEDDED:
        hint = str(embedded_key_hint or "").strip() or source_sync._base64url_encode(secrets.token_bytes(9))  # noqa: SLF001
        version = str(embedded_key_version or source_sync.EMBEDDED_KEY_VERSION_DEFAULT).strip() or source_sync.EMBEDDED_KEY_VERSION_DEFAULT
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
        return payload

    if normalized_derivation == source_sync.KEY_DERIVATION_PASSPHRASE:
        env_key = normalized_passphrase_env or source_sync.PACKAGED_SYNC_PASSPHRASE_ENV
        passphrase = str(runtime_env.get(env_key) or "")
        if not passphrase:
            raise RuntimeError(f"Missing passphrase value in environment variable: {env_key}")
        payload["keyDerivation"] = source_sync.KEY_DERIVATION_PASSPHRASE
        payload["keySalt"] = salt_b64
        payload["privateKeyPemEnc"] = source_sync.encrypt_private_key_pem_with_passphrase(
            private_key_pem,
            salt_b64=salt_b64,
            app_id=payload["appId"],
            installation_id=payload["installationId"],
            passphrase=passphrase,
        )
        return payload

    payload["keyDerivation"] = source_sync.KEY_DERIVATION_MACHINE
    payload["keySalt"] = salt_b64
    payload["privateKeyPemEnc"] = source_sync.encrypt_private_key_pem(
        private_key_pem,
        salt_b64=salt_b64,
        app_id=payload["appId"],
        installation_id=payload["installationId"],
    )
    return payload


def write_packaged_sync_config(output_path: Path, payload: dict) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output_path


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
    payload = build_packaged_sync_payload(
        app_id=str(args.app_id),
        installation_id=str(args.installation_id),
        repo=str(args.repo),
        branch=str(args.branch),
        path=str(args.path),
        allowed_repo=str(args.allowed_repo),
        allowed_branch=str(args.allowed_branch),
        allowed_path_prefix=str(args.allowed_path_prefix),
        private_key_pem=private_key_pem,
        salt=str(args.salt),
        plaintext=bool(args.plaintext),
        key_derivation=str(args.key_derivation),
        portable_passphrase_env=str(args.portable_passphrase_env),
        embedded_key_hint=str(args.embedded_key_hint),
        embedded_key_version=str(args.embedded_key_version),
    )
    print(str(write_packaged_sync_config(output_path, payload)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
