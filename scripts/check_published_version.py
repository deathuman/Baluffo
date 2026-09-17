#!/usr/bin/env python3
"""Check whether the current app version is already published on GHCR.

The container version gate (``tools/repo_health/container_version_policy.py``)
validates release *intent*: it fails when shipped code lands without a bump or a
``Release-tag:`` line. It cannot see whether the version it is asking you to
publish has already been published, because it is deliberately offline and runs
on every guardrail pass and pre-push.

That gap is the 0.2.140 reuse trap in a different disguise. Once a version is
published, every later ``main`` push republishes the same tag with newer code
while ``umbrel-app.yml`` still declares the old version string, so the box is
never offered an update and silently runs an older build.

This check closes it as an opt-in release lane: it is wired into
``release:preflight`` only, never into the always-on guardrail set, so the gate
stays fast and offline. It warns by default because republishing inside an open
release window is sometimes intentional; pass ``--strict`` to make a
already-published version a failure.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REGISTRY = "ghcr.io"
REPOSITORY = "deathuman/baluffo"
MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
DEFAULT_TIMEOUT = 20


@dataclass(frozen=True)
class PublishedState:
    """Outcome of a registry lookup for one version tag."""

    version: str
    published: bool
    digest: str | None
    detail: str


def _current_version() -> str:
    from src.app_version import APP_VERSION

    return APP_VERSION


def _fetch_anonymous_token(*, timeout: int = DEFAULT_TIMEOUT) -> str:
    """Return a pull-scoped anonymous token for the public package.

    The workflow's ``GITHUB_TOKEN`` lacks ``read:packages``, so the anonymous
    token flow is the supported way to inspect a public GHCR manifest.
    """
    url = f"https://{REGISTRY}/token?scope=repository:{REPOSITORY}:pull&service={REGISTRY}"
    with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - pinned https host
        payload = json.loads(response.read().decode("utf-8"))
    token = payload.get("token") or payload.get("access_token")
    if not token:
        raise OSError("registry token response carried no token field")
    return str(token)


def _manifest_digest(version: str, token: str, *, timeout: int = DEFAULT_TIMEOUT) -> str | None:
    """Return the manifest digest for ``version``, or None when the tag is absent."""
    url = f"https://{REGISTRY}/v2/{REPOSITORY}/manifests/{version}"
    request = urllib.request.Request(  # noqa: S310 - pinned https host
        url,
        method="HEAD",
        headers={"Authorization": f"Bearer {token}", "Accept": MANIFEST_ACCEPT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.headers.get("Docker-Content-Digest") or None
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 403):
            return None
        raise


def check_version_published(version: str, *, timeout: int = DEFAULT_TIMEOUT) -> PublishedState:
    """Look ``version`` up in the registry, degrading to a warn-style result on network failure."""
    try:
        token = _fetch_anonymous_token(timeout=timeout)
        digest = _manifest_digest(version, token, timeout=timeout)
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
        return PublishedState(
            version=version,
            published=False,
            digest=None,
            detail=f"registry lookup skipped ({exc}); verify manually before publishing",
        )
    if digest is None:
        return PublishedState(
            version=version,
            published=False,
            digest=None,
            detail=f"{REGISTRY}/{REPOSITORY}:{version} is not published yet",
        )
    return PublishedState(
        version=version,
        published=True,
        digest=digest,
        detail=(
            f"{REGISTRY}/{REPOSITORY}:{version} is ALREADY published ({digest}); "
            "pushing shipped code now republishes this tag with newer code while "
            "umbrel-app.yml keeps the same version string, so Umbrel will not "
            "offer an update -- bump the version instead"
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether the app version is already published on GHCR."
    )
    parser.add_argument(
        "--version", default=None, help="Version to look up (default: APP_VERSION)."
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the version is already published.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable results.")
    args = parser.parse_args()

    version = args.version or _current_version()
    state = check_version_published(version)

    if args.json:
        print(json.dumps(state.__dict__, indent=2))
    else:
        print(f"published-version check: {state.detail}")

    if args.strict and state.published:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
