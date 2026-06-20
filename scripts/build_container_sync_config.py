#!/usr/bin/env python3
"""Build the container-packaged GitHub App sync config from BuildKit secrets."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_sync_app_config import build_packaged_sync_payload, write_packaged_sync_config
from src import source_sync

REQUIRED_SECRET_NAMES = (
    "BALUFFO_SYNC_BUILD_APP_ID",
    "BALUFFO_SYNC_BUILD_INSTALLATION_ID",
    "BALUFFO_SYNC_BUILD_REPO",
    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM",
)

OPTIONAL_SECRET_NAMES = (
    "BALUFFO_SYNC_BUILD_BRANCH",
    "BALUFFO_SYNC_BUILD_PATH",
    "BALUFFO_SYNC_BUILD_ALLOWED_REPO",
    "BALUFFO_SYNC_BUILD_ALLOWED_BRANCH",
    "BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX",
    "BALUFFO_SYNC_BUILD_KEY_DERIVATION",
    "BALUFFO_SYNC_BUILD_PASSPHRASE_ENV",
    "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT",
    "BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION",
    "BALUFFO_SYNC_BUILD_KEY_SALT",
)


def _read_secret(secret_dir: Path, name: str) -> str:
    path = secret_dir / name
    if not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _normalize_private_key_pem(value: str) -> str:
    text = str(value or "").strip()
    if "\\n" in text and "\n" not in text:
        text = text.replace("\\n", "\n")
    return text


def _read_secret_map(secret_dir: Path) -> dict[str, str]:
    return {
        name: _read_secret(secret_dir, name)
        for name in (*REQUIRED_SECRET_NAMES, *OPTIONAL_SECRET_NAMES)
    }


def build_container_sync_config_from_secrets(
    *,
    secret_dir: Path,
    output: Path,
    require: bool = False,
) -> Path | None:
    secrets = _read_secret_map(secret_dir)
    provided_required = {
        name: value for name, value in secrets.items() if name in REQUIRED_SECRET_NAMES and value
    }
    if not provided_required:
        if require:
            raise RuntimeError("Missing container source sync build secrets.")
        return None

    missing = [name for name in REQUIRED_SECRET_NAMES if not secrets.get(name)]
    if missing:
        raise RuntimeError(
            "Incomplete container source sync build secrets: " + ", ".join(sorted(missing))
        )

    payload = build_packaged_sync_payload(
        app_id=secrets["BALUFFO_SYNC_BUILD_APP_ID"],
        installation_id=secrets["BALUFFO_SYNC_BUILD_INSTALLATION_ID"],
        repo=secrets["BALUFFO_SYNC_BUILD_REPO"],
        branch=secrets["BALUFFO_SYNC_BUILD_BRANCH"] or source_sync.DEFAULT_BRANCH,
        path=secrets["BALUFFO_SYNC_BUILD_PATH"] or source_sync.DEFAULT_PATH,
        allowed_repo=secrets["BALUFFO_SYNC_BUILD_ALLOWED_REPO"]
        or secrets["BALUFFO_SYNC_BUILD_REPO"],
        allowed_branch=secrets["BALUFFO_SYNC_BUILD_ALLOWED_BRANCH"]
        or secrets["BALUFFO_SYNC_BUILD_BRANCH"]
        or source_sync.DEFAULT_BRANCH,
        allowed_path_prefix=secrets["BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX"]
        or secrets["BALUFFO_SYNC_BUILD_PATH"]
        or source_sync.DEFAULT_PATH,
        private_key_pem=_normalize_private_key_pem(secrets["BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM"]),
        salt=secrets["BALUFFO_SYNC_BUILD_KEY_SALT"],
        key_derivation=secrets["BALUFFO_SYNC_BUILD_KEY_DERIVATION"]
        or source_sync.KEY_DERIVATION_EMBEDDED,
        portable_passphrase_env=secrets["BALUFFO_SYNC_BUILD_PASSPHRASE_ENV"],
        embedded_key_hint=secrets["BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT"],
        embedded_key_version=secrets["BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION"]
        or source_sync.EMBEDDED_KEY_VERSION_DEFAULT,
    )
    return write_packaged_sync_config(output, payload)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build container packaged sync config from Docker BuildKit secrets."
    )
    parser.add_argument("--secret-dir", type=Path, default=Path("/run/secrets"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/app/packaging/github-app-sync-config.json"),
    )
    parser.add_argument(
        "--require",
        action="store_true",
        help="Fail when no complete sync build secrets are available.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output = build_container_sync_config_from_secrets(
        secret_dir=args.secret_dir,
        output=args.output,
        require=bool(args.require),
    )
    if output is None:
        print("Container source sync config not generated; build secrets were not provided.")
    else:
        print("Container source sync config generated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
