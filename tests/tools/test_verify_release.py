"""What `tools/verify_release.py` must prove about a published release, without network.

The tool exists because the release closeout checks were mechanical and done by hand. These
tests pin the checks themselves: a complete release passes; a draft, a tampered digest, a
foreign signing key, a manifest that points at another release, a missing asset, a local tag
that disagrees with the remote, a platform-less index, and an image built from another commit
each fail on their own cause. The clients are faked, so nothing here touches GitHub or GHCR.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_spec = importlib.util.spec_from_file_location(
    "verify_release", _ROOT / "tools" / "verify_release.py"
)
assert _spec and _spec.loader
verify_release = importlib.util.module_from_spec(_spec)
sys.modules["verify_release"] = verify_release
_spec.loader.exec_module(verify_release)

from src.ship.desktop_update_manifest import sign_manifest  # noqa: E402

_VERSION = "9.9.9"
_TAG = f"v{_VERSION}"
_COMMIT = "a" * 40
_PRIVATE = Ed25519PrivateKey.generate()
_PUBLIC_KEYS = {"test-key": _PRIVATE.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)}

_PORTABLE = f"baluffo-portable-{_VERSION}.zip"
_SHIP = f"baluffo-ship-{_VERSION}.zip"
_APPIMAGE = f"Baluffo-{_VERSION}-x86_64.AppImage"
_PAYLOADS = {_PORTABLE: b"portable-zip", _SHIP: b"ship-zip", _APPIMAGE: b"app-image"}
_IMAGE_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
_INDEX = "application/vnd.oci.image.index.v1+json"


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _signed_manifest(
    artifacts: dict[str, tuple[str, bytes]],
    *,
    tag: str = _TAG,
    version: str = _VERSION,
    key_id: str = "test-key",
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "key_id": key_id,
        "channel": "stable",
        "version": version,
        "published_at": "2026-10-29T00:00:00+00:00",
        "release_notes_url": f"https://github.com/example/baluffo/releases/tag/{tag}",
        "min_desktop_updater_version": "2.0.1",
        "min_supported_current_version": "0.0.0",
        "data_schema_version": "2",
        "rollback_allowed": False,
        "migration_plan": [],
    }
    for field_name, (name, payload) in artifacts.items():
        manifest[field_name] = {
            "url": f"https://github.com/example/baluffo/releases/download/{tag}/{name}",
            "sha256": _sha(payload),
            "size_bytes": len(payload),
        }
    manifest["signature"] = sign_manifest(manifest, _PRIVATE.private_bytes_raw())
    return manifest


def _asset(name: str, payload: bytes) -> dict[str, Any]:
    return {
        "name": name,
        "state": "uploaded",
        "size": len(payload),
        "digest": f"sha256:{_sha(payload)}",
        "browser_download_url": f"https://example.test/{name}",
    }


def _assets(payloads: dict[str, bytes]) -> dict[str, dict[str, Any]]:
    return {name: _asset(name, payload) for name, payload in payloads.items()}


def _manifests(payloads: dict[str, bytes]) -> dict[str, dict[str, Any]]:
    return {
        "baluffo-desktop-update-manifest.json": _signed_manifest(
            {
                "portable_artifact": (_PORTABLE, payloads[_PORTABLE]),
                "ship_recovery_artifact": (_SHIP, payloads[_SHIP]),
            }
        ),
        "baluffo-desktop-update-manifest-linux.json": _signed_manifest(
            {"portable_artifact": (_APPIMAGE, payloads[_APPIMAGE])}
        ),
    }


def _image(
    *,
    label_commit: str = _COMMIT,
    label_version: str = _VERSION,
    single_platform: bool = False,
) -> dict[str, Any]:
    manifests: dict[str, tuple[str, bytes]] = {}
    blobs: dict[str, bytes] = {}
    entries: list[dict[str, Any]] = []
    for arch, seed in (("amd64", "1"), ("arm64", "2")):
        config = {
            "os": "linux",
            "architecture": arch,
            "config": {
                "Labels": {
                    "org.opencontainers.image.revision": label_commit,
                    "org.opencontainers.image.version": label_version,
                }
            },
        }
        config_bytes = json.dumps(config).encode()
        config_digest = f"sha256:{_sha(config_bytes)}"
        child = {
            "schemaVersion": 2,
            "mediaType": _IMAGE_MANIFEST,
            "config": {
                "digest": config_digest,
                "mediaType": "application/vnd.oci.image.config.v1+json",
            },
            "layers": [],
        }
        child_bytes = json.dumps(child).encode()
        child_digest = f"sha256:{seed * 64}"
        manifests[child_digest] = (_IMAGE_MANIFEST, child_bytes)
        blobs[config_digest] = config_bytes
        entries.append(
            {
                "mediaType": _IMAGE_MANIFEST,
                "digest": child_digest,
                "platform": {"os": "linux", "architecture": arch},
            }
        )
        if single_platform:
            manifests[_TAG] = (_IMAGE_MANIFEST, child_bytes)
    if not single_platform:
        entries.append(
            {
                "mediaType": _IMAGE_MANIFEST,
                "digest": f"sha256:{'3' * 64}",
                "platform": {"os": "unknown", "architecture": "unknown"},
                "annotations": {
                    "vnd.docker.reference.type": "attestation-manifest",
                    "vnd.docker.reference.digest": entries[0]["digest"],
                },
            }
        )
        index_bytes = json.dumps(
            {"schemaVersion": 2, "mediaType": _INDEX, "manifests": entries}
        ).encode()
        manifests[_TAG] = (_INDEX, index_bytes)
        manifests[f"sha-{_COMMIT[:7]}"] = (_INDEX, index_bytes)
    return {
        "tags": [_TAG, _VERSION, f"sha-{_COMMIT[:7]}"],
        "manifests": manifests,
        "blobs": blobs,
    }


class _FakeGitHub:
    def __init__(
        self,
        *,
        manifests: dict[str, dict[str, Any]],
        assets: dict[str, dict[str, Any]],
        commit: str = _COMMIT,
        draft: bool = False,
        prerelease: bool = False,
    ) -> None:
        self.manifests = manifests
        self.assets = assets
        self.commit = commit
        self.draft = draft
        self.prerelease = prerelease

    def release(self, tag: str) -> dict[str, Any]:
        return {
            "tag_name": tag,
            "draft": self.draft,
            "prerelease": self.prerelease,
            "published_at": "2026-10-29T00:00:00Z",
            "assets": list(self.assets.values()),
        }

    def tag_ref(self, tag: str) -> dict[str, Any]:
        return {"object": {"type": "tag", "sha": "tag-object-sha"}}

    def annotated_tag(self, sha: str) -> dict[str, Any]:
        return {"tag": _TAG, "object": {"type": "commit", "sha": self.commit}}

    def download_asset(self, asset: dict[str, Any]) -> bytes:
        return json.dumps(self.manifests[asset["name"]]).encode()


class _FakeRegistry:
    def __init__(
        self,
        *,
        tags: list[str],
        manifests: dict[str, tuple[str, bytes]],
        blobs: dict[str, bytes],
    ) -> None:
        self._tags = tags
        self._manifests = manifests
        self._blobs = blobs

    def tags(self) -> list[str]:
        return list(self._tags)

    def manifest(self, reference: str) -> tuple[str, bytes]:
        if reference not in self._manifests:
            raise verify_release.VerifyError(f"GHCR manifest {reference} answered 404")
        return self._manifests[reference]

    def blob(self, digest: str) -> bytes:
        return self._blobs[digest]


def _run(
    *,
    manifests: dict[str, dict[str, Any]] | None = None,
    assets: dict[str, dict[str, Any]] | None = None,
    image: dict[str, Any] | None = None,
    public_keys: dict[str, bytes] | None = None,
    local_tag_commit: str | None = _COMMIT,
    draft: bool = False,
    commit: str = _COMMIT,
) -> Any:
    payloads = dict(_PAYLOADS)
    resolved_manifests = manifests or _manifests(payloads)
    resolved_assets = dict(assets) if assets is not None else _assets(payloads)
    for name, manifest in resolved_manifests.items():
        resolved_assets.setdefault(name, _asset(name, json.dumps(manifest).encode()))
    return verify_release.verify_release(
        tag=_TAG,
        version=_VERSION,
        github=_FakeGitHub(
            manifests=resolved_manifests,
            assets=resolved_assets,
            draft=draft,
            commit=commit,
        ),
        registry=_FakeRegistry(**(image or _image())),
        public_keys=public_keys or _PUBLIC_KEYS,
        local_tag_commit=local_tag_commit,
        local_head=_COMMIT,
    )


def _failed_labels(report: Any) -> set[str]:
    return {check.label for check in report.failures}


def test_a_complete_release_passes() -> None:
    report = _run()
    assert not report.failures, report.render()
    assert "image index digest: sha256:" in report.render()


def test_a_draft_release_fails() -> None:
    report = _run(draft=True)
    assert "release published" in _failed_labels(report)


def test_classification_helpers_cover_remotes_and_tags() -> None:
    assert verify_release.repo_from_remote("https://github.com/example/baluffo.git") == (
        "example/baluffo"
    )
    assert verify_release.repo_from_remote("git@github.com:example/baluffo.git") == (
        "example/baluffo"
    )
    assert verify_release.repo_from_remote("https://gitlab.com/example/baluffo") is None
    assert verify_release.normalize_tag("9.9.9") == "v9.9.9"
    assert verify_release.normalize_tag("v9.9.9") == "v9.9.9"
    assert verify_release.expected_asset_names(_VERSION) == {
        *_PAYLOADS,
        "baluffo-desktop-update-manifest.json",
        "baluffo-desktop-update-manifest-linux.json",
    }
    assert verify_release.next_link('/v2/x/tags/list?n=1&last=abc; rel="next"') == (
        "https://ghcr.io/v2/x/tags/list?n=1&last=abc"
    )
    assert verify_release.next_link(None) is None


def test_a_missing_asset_fails() -> None:
    assets = _assets(dict(_PAYLOADS))
    assets.pop(_SHIP)
    report = _run(assets=assets)
    labels = _failed_labels(report)
    assert f"asset {_SHIP}" in labels
    assert "baluffo-desktop-update-manifest.json ship_recovery_artifact asset" in labels


def test_a_tampered_artifact_digest_fails() -> None:
    payloads = dict(_PAYLOADS)
    tampered = dict(payloads)
    tampered[_PORTABLE] = b"a different portable zip"
    report = _run(manifests=_manifests(tampered), assets=_assets(payloads))
    assert "baluffo-desktop-update-manifest.json portable_artifact sha256" in _failed_labels(report)


def test_a_manifest_signed_by_another_key_fails() -> None:
    other = Ed25519PrivateKey.generate()
    other_keys = {"test-key": other.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)}
    report = _run(public_keys=other_keys)
    assert "baluffo-desktop-update-manifest.json signature" in _failed_labels(report)
    assert "baluffo-desktop-update-manifest-linux.json signature" in _failed_labels(report)


def test_a_manifest_pointing_at_another_release_fails() -> None:
    payloads = dict(_PAYLOADS)
    report = _run(
        manifests=_manifests(payloads)
        | {
            "baluffo-desktop-update-manifest.json": _signed_manifest(
                {
                    "portable_artifact": (_PORTABLE, payloads[_PORTABLE]),
                    "ship_recovery_artifact": (_SHIP, payloads[_SHIP]),
                },
                tag="v0.0.1",
            )
        }
    )
    labels = _failed_labels(report)
    assert "baluffo-desktop-update-manifest.json release notes" in labels
    assert "baluffo-desktop-update-manifest.json portable_artifact tag" in labels


def test_a_local_tag_that_disagrees_with_the_remote_fails() -> None:
    report = _run(local_tag_commit="b" * 40)
    assert "local tag" in _failed_labels(report)


def test_single_platform_index_fails() -> None:
    report = _run(image=_image(single_platform=True))
    labels = _failed_labels(report)
    assert "image multi-arch" in labels
    assert "image index" in labels


def test_image_from_another_commit_fails() -> None:
    report = _run(image=_image(label_commit="b" * 40))
    assert "image revision" in _failed_labels(report)


def test_linux_manifest_needs_no_ship_recovery_artifact() -> None:
    report = _run()
    labels = {check.label for check in report.checks}
    assert "baluffo-desktop-update-manifest-linux.json ship_recovery_artifact asset" not in labels
    assert "baluffo-desktop-update-manifest-linux.json portable_artifact sha256" in labels
