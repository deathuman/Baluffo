#!/usr/bin/env python3
"""Verify a published Baluffo release end to end, in one command.

`docs/RELEASE.md`'s public closeout asks four questions after the tag workflows finish, and
each was answered by hand: is the GitHub release published rather than a draft; is every asset
the update manifests name uploaded with the digest and size the manifest declares; do both
platform manifests verify against the committed Ed25519 public key; and does GHCR serve a
multi-arch index built from the tagged commit. All four are mechanical, so they live here:

    python tools/verify_release.py              # the current src/app_version.py version
    python tools/verify_release.py --tag v0.3.027

Exit 0 means every check passed, 1 means a release defect was found, 2 means the tool could not
look (a missing release, a registry that refuses anonymous pull). It needs neither the `gh` CLI
nor a Docker daemon: the image is verified from registry metadata, never by pulling it. Set
`GITHUB_TOKEN` or `GH_TOKEN` to lift the unauthenticated API rate limit; no token is printed and
nothing is written to disk.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import certifi
except ImportError:  # pragma: no cover - certifi ships with the repo requirements
    certifi = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_version import get_app_version  # noqa: E402
from src.ship.desktop_update_manifest import (  # noqa: E402
    DESKTOP_UPDATE_SCHEMA_VERSION,
    PUBLIC_KEYS_FILE,
    verify_manifest_signature,
)

DEFAULT_REPO = "deathuman/Baluffo"
GITHUB_API = "https://api.github.com"
GHCR_ROOT = "https://ghcr.io"
PUBLIC_KEYS_PATH = ROOT / "packaging" / PUBLIC_KEYS_FILE

MANIFEST_ASSETS = (
    "baluffo-desktop-update-manifest.json",
    "baluffo-desktop-update-manifest-linux.json",
)
ARTIFACT_FIELDS = ("portable_artifact", "ship_recovery_artifact")
INDEX_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }
)
IMAGE_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)
ACCEPT_MANIFESTS = ", ".join(sorted(INDEX_MEDIA_TYPES | IMAGE_MEDIA_TYPES))
PLATFORMS = ("linux/amd64", "linux/arm64")


class VerifyError(RuntimeError):
    """The tool could not perform a check at all (exit 2)."""


class ReleaseNotFound(VerifyError):
    """No release exists at the requested tag."""


@dataclass(frozen=True)
class Check:
    label: str
    ok: bool
    detail: str


@dataclass
class Report:
    """Collected checks, notes, and the exit decision."""

    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add(self, label: str, ok: bool, detail: str) -> None:
        self.checks.append(Check(label=label, ok=bool(ok), detail=detail))

    def note(self, text: str) -> None:
        self.notes.append(text)

    @property
    def failures(self) -> list[Check]:
        return [check for check in self.checks if not check.ok]

    def render(self) -> str:
        lines = [
            f"[{'PASS' if check.ok else 'FAIL'}] {check.label}: {check.detail}"
            for check in self.checks
        ]
        lines.extend(f"[note] {text}" for text in self.notes)
        if self.failures:
            lines.append("")
            lines.append(
                f"FAILURES: {len(self.failures)} -> {[check.label for check in self.failures]}"
            )
        else:
            lines.append("")
            lines.append(f"all release checks passed ({len(self.checks)} checks)")
        return "\n".join(lines)


def digest_of(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def normalize_tag(value: str) -> str:
    tag = str(value).strip()
    return tag if tag.startswith("v") else f"v{tag}"


def repo_from_remote(url: str | None) -> str | None:
    """`owner/name` from a GitHub remote URL, or None when it is not one."""
    remote = str(url or "").strip()
    if not remote:
        return None
    remote = remote.removesuffix(".git")
    if remote.startswith("git@") and ":" in remote:
        remote = "https://" + remote[4:].replace(":", "/", 1)
    parsed = urllib.parse.urlparse(remote)
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.hostname == "github.com" and len(parts) >= 2:
        return f"{parts[-2]}/{parts[-1]}"
    return None


def expected_asset_names(version: str) -> set[str]:
    """Every asset the release contract requires for a current release."""
    return {
        *MANIFEST_ASSETS,
        f"Baluffo-{version}-x86_64.AppImage",
        f"baluffo-portable-{version}.zip",
        f"baluffo-ship-{version}.zip",
    }


def load_public_keys(path: Path = PUBLIC_KEYS_PATH) -> dict[str, bytes]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {key_id: base64.b64decode(value) for key_id, value in payload.items()}


def next_link(header: str | None) -> str | None:
    """The `rel="next"` URL from a registry `Link` header, relative to the host."""
    if not header or 'rel="next"' not in header:
        return None
    target = header.split(";", 1)[0].strip().strip("<> ")
    if target.startswith("http"):
        return target
    return GHCR_ROOT + target


def _ssl_context() -> ssl.SSLContext:
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


class Transport:
    """Minimal HTTPS GET with explicit status handling (no exceptions for 4xx/5xx)."""

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.context = _ssl_context()

    def get(
        self, url: str, headers: dict[str, str] | None = None
    ) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(url)
        for key, value in (headers or {}).items():
            request.add_header(key, value)
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=self.context
            ) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()
        except (urllib.error.URLError, OSError) as exc:
            raise VerifyError(f"{url}: {getattr(exc, 'reason', exc)}") from exc


class GitHubClient:
    """The three GitHub reads this tool makes, plus asset bytes."""

    def __init__(self, repo: str, *, transport: Transport, token: str | None = None) -> None:
        self.repo = repo
        self.transport = transport
        self.token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""

    def _json(self, path: str) -> Any:
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "baluffo-verify-release",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        status, _, body = self.transport.get(f"{GITHUB_API}{path}", headers)
        if status == 404:
            return None
        if status != 200:
            raise VerifyError(f"GitHub API {path} answered {status}: {body[:200]!r}")
        return json.loads(body)

    def release(self, tag: str) -> dict[str, Any]:
        payload = self._json(f"/repos/{self.repo}/releases/tags/{tag}")
        if payload is None:
            raise ReleaseNotFound(f"{self.repo} has no release at {tag}")
        return payload

    def tag_ref(self, tag: str) -> dict[str, Any] | None:
        return self._json(f"/repos/{self.repo}/git/ref/tags/{tag}")

    def annotated_tag(self, sha: str) -> dict[str, Any]:
        payload = self._json(f"/repos/{self.repo}/git/tags/{sha}")
        if payload is None:
            raise VerifyError(f"annotated tag object {sha} is missing")
        return payload

    def download_asset(self, asset: dict[str, Any]) -> bytes:
        """Public asset bytes. Deliberately unauthenticated: the CDN redirect must not
        receive the API token, and these assets are public by construction."""
        url = str(asset.get("browser_download_url") or "")
        if not url:
            raise VerifyError(f"asset {asset.get('name')!r} has no download URL")
        status, _, body = self.transport.get(url)
        if status != 200:
            raise VerifyError(f"asset {asset.get('name')!r} answered {status}")
        return body


class RegistryClient:
    """Anonymous GHCR reads: tags, manifests, and blob bytes."""

    def __init__(self, repository: str, *, transport: Transport) -> None:
        self.repository = repository
        self.transport = transport
        self._token: str | None = None

    def token(self) -> str:
        if self._token is None:
            status, _, body = self.transport.get(
                f"{GHCR_ROOT}/token?service=ghcr.io&scope=repository:{self.repository}:pull"
            )
            if status != 200:
                raise VerifyError(
                    f"GHCR token for {self.repository} answered {status}; "
                    "the package is not anonymously readable"
                )
            self._token = json.loads(body)["token"]
        return self._token

    def _get(self, path: str, accept: str) -> tuple[int, dict[str, str], bytes]:
        return self.transport.get(
            f"{GHCR_ROOT}/v2/{self.repository}/{path}",
            {"Accept": accept, "Authorization": f"Bearer {self.token()}"},
        )

    def tags(self) -> list[str]:
        collected: list[str] = []
        url = f"{GHCR_ROOT}/v2/{self.repository}/tags/list?n=1000"
        while url:
            status, headers, body = self.transport.get(
                url, {"Accept": "application/json", "Authorization": f"Bearer {self.token()}"}
            )
            if status != 200:
                raise VerifyError(f"GHCR tag list answered {status}: {body[:200]!r}")
            collected += json.loads(body).get("tags") or []
            url = next_link(headers.get("Link"))
        return collected

    def manifest(self, reference: str) -> tuple[str, bytes]:
        status, headers, body = self._get(f"manifests/{reference}", ACCEPT_MANIFESTS)
        if status != 200:
            raise VerifyError(f"GHCR manifest {reference} answered {status}: {body[:200]!r}")
        media_type = headers.get("Content-Type", "").split(";")[0].strip()
        return media_type, body

    def blob(self, digest: str) -> bytes:
        status, _, body = self._get(f"blobs/{digest}", "application/octet-stream")
        if status != 200:
            raise VerifyError(f"GHCR blob {digest} answered {status}")
        return body


def _check_artifact(
    report: Report,
    *,
    manifest_name: str,
    field_name: str,
    artifact: dict[str, Any],
    assets: dict[str, dict[str, Any]],
    tag: str,
) -> None:
    label = f"{manifest_name} {field_name}"
    url = str(artifact.get("url") or "")
    report.add(f"{label} tag", f"/download/{tag}/" in url, url or "no url")
    asset_name = url.rsplit("/", 1)[-1] if url else ""
    asset = assets.get(asset_name)
    if asset is None:
        report.add(f"{label} asset", False, f"{asset_name or '?'} is not an uploaded asset")
        return
    expected = str(artifact.get("sha256") or "")
    actual = str(asset.get("digest") or "")
    report.add(
        f"{label} sha256",
        actual == f"sha256:{expected}" and bool(expected),
        f"{asset_name} manifest={expected[:16]} asset={actual[7:23] or 'missing'}",
    )
    report.add(
        f"{label} size",
        asset.get("size") == artifact.get("size_bytes"),
        f"{asset.get('size')} bytes",
    )


def _check_manifest(
    report: Report,
    *,
    manifest_name: str,
    manifest: dict[str, Any],
    assets: dict[str, dict[str, Any]],
    version: str,
    tag: str,
    public_keys: dict[str, bytes],
) -> None:
    try:
        verify_manifest_signature(manifest, public_keys=public_keys)
    except Exception as exc:  # noqa: BLE001 - the exception is the finding
        report.add(f"{manifest_name} signature", False, f"{type(exc).__name__}: {exc}")
    else:
        report.add(f"{manifest_name} signature", True, f"key_id={manifest.get('key_id')}")

    report.add(
        f"{manifest_name} schema",
        manifest.get("schema_version") == DESKTOP_UPDATE_SCHEMA_VERSION,
        str(manifest.get("schema_version")),
    )
    report.add(
        f"{manifest_name} version", manifest.get("version") == version, str(manifest.get("version"))
    )
    report.add(
        f"{manifest_name} release notes",
        str(manifest.get("release_notes_url") or "").endswith(f"/{tag}"),
        str(manifest.get("release_notes_url")),
    )

    artifacts = {
        field_name: manifest[field_name]
        for field_name in ARTIFACT_FIELDS
        if isinstance(manifest.get(field_name), dict)
    }
    report.add(
        f"{manifest_name} portable artifact",
        "portable_artifact" in artifacts,
        "declared" if "portable_artifact" in artifacts else "missing",
    )
    for field_name, artifact in artifacts.items():
        _check_artifact(
            report,
            manifest_name=manifest_name,
            field_name=field_name,
            artifact=artifact,
            assets=assets,
            tag=tag,
        )


def _check_tag(
    report: Report,
    *,
    tag: str,
    github: Any,
    local_tag_commit: str | None,
    local_head: str | None,
) -> str | None:
    """Annotated tag identity; returns the tagged commit when it is known."""
    ref = github.tag_ref(tag)
    if ref is None:
        report.add("annotated tag", False, f"{tag} does not exist on the remote")
        return None
    object_type = str((ref.get("object") or {}).get("type") or "")
    report.add("annotated tag", object_type == "tag", f"ref object type={object_type or 'missing'}")
    if object_type != "tag":
        return None

    tag_object = github.annotated_tag(ref["object"]["sha"])
    report.add("tag name", tag_object.get("tag") == tag, str(tag_object.get("tag")))
    target = tag_object.get("object") or {}
    report.add("tag target", target.get("type") == "commit", f"target type={target.get('type')}")
    if target.get("type") != "commit":
        return None
    commit = str(target.get("sha") or "") or None

    if local_tag_commit is None:
        report.note(f"local tag {tag} is absent here; the remote target is authoritative")
    else:
        report.add(
            "local tag",
            local_tag_commit == commit,
            f"local={local_tag_commit[:12]} remote={str(commit)[:12]}",
        )
    if local_head is not None:
        report.note(
            f"local HEAD {local_head[:12]} {'is' if local_head == commit else 'is not'} the tagged commit"
        )
    return commit


def _check_image(
    report: Report,
    *,
    version: str,
    tag: str,
    registry: Any,
    commit: str | None,
) -> None:
    tags = registry.tags()
    report.add(
        "image tags",
        {f"v{version}", version} <= set(tags),
        f"{len(tags)} tags; v{version}={f'v{version}' in tags} {version}={version in tags}",
    )

    try:
        media_type, body = registry.manifest(f"v{version}")
    except VerifyError as exc:
        report.add("image index", False, str(exc))
        return
    report.add("image index", media_type in INDEX_MEDIA_TYPES, media_type)
    report.note(f"image index digest: {digest_of(body)}")
    index = json.loads(body)
    if not index.get("manifests"):
        report.add("image multi-arch", False, "the version tag is not a multi-platform index")
        return

    platforms: dict[str, str] = {}
    revisions: set[str] = set()
    label_versions: set[str] = set()
    attestations = 0
    for entry in index["manifests"]:
        annotations = entry.get("annotations") or {}
        platform = entry.get("platform") or {}
        if annotations.get("vnd.docker.reference.type") == "attestation-manifest":
            attestations += 1
            continue
        key = f"{platform.get('os')}/{platform.get('architecture')}"
        try:
            child_type, child_body = registry.manifest(entry["digest"])
        except VerifyError as exc:
            report.add(f"image {key} manifest", False, str(exc))
            continue
        if child_type not in IMAGE_MEDIA_TYPES:
            report.add(f"image {key} manifest", False, child_type)
            continue
        child = json.loads(child_body)
        config = json.loads(registry.blob(child["config"]["digest"]))
        report.add(
            f"image {key} config",
            config.get("os") == platform.get("os")
            and config.get("architecture") == platform.get("architecture"),
            f"config={config.get('os')}/{config.get('architecture')} manifest={entry['digest'][:19]}",
        )
        platforms[key] = entry["digest"]
        labels = (config.get("config") or {}).get("Labels") or {}
        if labels.get("org.opencontainers.image.revision"):
            revisions.add(labels["org.opencontainers.image.revision"])
        if labels.get("org.opencontainers.image.version"):
            label_versions.add(labels["org.opencontainers.image.version"])

    report.add(
        "image platforms",
        set(PLATFORMS) <= set(platforms),
        ", ".join(f"{key}={value[:19]}" for key, value in sorted(platforms.items())) or "none",
    )
    report.note(f"build attestations: {attestations}")
    if commit:
        report.add(
            "image revision",
            revisions == {commit},
            f"labels={sorted(revisions)} commit={commit[:12]}",
        )
    else:
        report.note("tagged commit unknown, so the image revision label was not compared")
    report.add(
        "image version",
        label_versions <= {version, tag},
        f"labels={sorted(label_versions)}",
    )

    if commit:
        candidates = [f"sha-{commit[:length]}" for length in (7, 8, len(commit))]
        sha_tag = next((name for name in candidates if name in tags), None)
        if sha_tag is None:
            report.add("image commit tag", False, f"none of {candidates} exists")
        else:
            _, sha_body = registry.manifest(sha_tag)
            report.add(
                "image commit tag",
                digest_of(sha_body) == digest_of(body),
                f"{sha_tag} resolves to the same index",
            )


def verify_release(
    *,
    tag: str,
    version: str,
    github: Any,
    registry: Any,
    public_keys: dict[str, bytes],
    local_tag_commit: str | None = None,
    local_head: str | None = None,
) -> Report:
    """Run every check against the injected clients and return the report."""
    report = Report()
    release = github.release(tag)
    report.add(
        "release published",
        not release.get("draft") and not release.get("prerelease"),
        f"{tag} at {release.get('published_at')}",
    )

    assets = {
        str(asset.get("name")): asset
        for asset in release.get("assets") or []
        if asset.get("state") == "uploaded"
    }
    for name in sorted(expected_asset_names(version)):
        report.add(
            f"asset {name}", name in assets, "uploaded" if name in assets else "not uploaded"
        )

    commit = _check_tag(
        report,
        tag=tag,
        github=github,
        local_tag_commit=local_tag_commit,
        local_head=local_head,
    )

    for name in MANIFEST_ASSETS:
        asset = assets.get(name)
        if asset is None:
            continue
        manifest = json.loads(github.download_asset(asset).decode("utf-8"))
        _check_manifest(
            report,
            manifest_name=name,
            manifest=manifest,
            assets=assets,
            version=version,
            tag=tag,
            public_keys=public_keys,
        )

    _check_image(report, version=version, tag=tag, registry=registry, commit=commit)
    return report


def _git(*args: str) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args], check=False, capture_output=True, text=True
    )
    output = completed.stdout.strip()
    return output if completed.returncode == 0 and output else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify a published release: assets, signed manifests, and the GHCR image."
    )
    parser.add_argument(
        "--tag",
        default=None,
        help="release tag to verify (default: v<src/app_version.py version>)",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help=f"GitHub repository as owner/name (default: origin remote, else {DEFAULT_REPO})",
    )
    parser.add_argument(
        "--image",
        default=None,
        help="GHCR repository (default: ghcr.io/<repo>, lowercased)",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTPS timeout in seconds")
    args = parser.parse_args(argv)

    version = get_app_version()
    tag = normalize_tag(args.tag) if args.tag else f"v{version}"
    version = tag.lstrip("v")
    repo = args.repo or repo_from_remote(_git("remote", "get-url", "origin")) or DEFAULT_REPO
    image = args.image or f"ghcr.io/{repo.lower()}"
    transport = Transport(args.timeout)

    print(f"verifying {repo} release {tag} and image {image}")
    try:
        report = verify_release(
            tag=tag,
            version=version,
            github=GitHubClient(repo, transport=transport),
            registry=RegistryClient(image, transport=transport),
            public_keys=load_public_keys(),
            local_tag_commit=_git("rev-parse", f"{tag}^{{commit}}"),
            local_head=_git("rev-parse", "HEAD"),
        )
    except VerifyError as exc:
        print(f"cannot verify: {exc}", file=sys.stderr)
        return 2

    print(report.render())
    return 1 if report.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
