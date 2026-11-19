"""In-memory GitHub Git Data API used by source-sync push tests.

The fake keeps a real commit -> tree -> blob graph so a test can assert what a
reader would see on the branch, not just which HTTP calls were made.
"""

from __future__ import annotations

import base64
import hashlib
import threading
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, urlparse


def _query_value(url: str, key: str, default: str = "") -> str:
    values = parse_qs(urlparse(url).query).get(key) or []
    return str(values[0]) if values else default


def _query_int(url: str, key: str, default: int) -> int:
    try:
        return int(_query_value(url, key, str(default)))
    except (TypeError, ValueError):
        return default


def _git_blob_sha1(payload: bytes) -> str:
    return hashlib.sha1(f"blob {len(payload)}\0".encode("ascii") + payload).hexdigest()


class GitDataSyncModule:
    """Minimal ``module`` surface backed by the Git Data API."""

    RUNTIME_STATE_REMOTE_CONFLICT = "remote_conflict"

    class SyncOperationError(RuntimeError):
        def __init__(self, code: str, message: str):
            super().__init__(message)
            self.code = code

    def __init__(
        self,
        *,
        head_commit: str = "parent-commit",
        base_tree: str = "base-tree",
    ) -> None:
        self.head_commit = head_commit
        self.commit_tree: dict[str, str] = {head_commit: base_tree}
        self.tree_by_sha: dict[str, dict[str, str]] = {base_tree: {}}
        self.blobs: dict[str, bytes] = {}
        self.commits: list[dict[str, Any]] = []
        self.calls: list[dict[str, Any]] = []
        self.runtime_state: tuple[str, str] | None = None
        self.conflict_on_ref_update = False
        self.tree_listing_truncated = False
        self.tree_listing_override: (
            Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None
        ) = None
        self.forced_blob_sha = ""
        self.contents_listing: list[dict[str, Any]] = []
        self.commit_history_status = 200
        self.commit_history_payload: list[dict[str, Any]] | None = None
        self.manifest_ref_status = 0
        self.delete_status = 200
        self.deleted_paths: list[str] = []
        self.manifest_path = "baluffo/source-sync/manifest.json"
        self._tree_counter = 0
        self._lock = threading.Lock()
        self.max_in_flight = 0
        self._in_flight = 0

    # -- graph inspection -------------------------------------------------
    def branch_tree(self) -> dict[str, str]:
        return dict(self._head_tree_ref())

    def _head_tree_ref(self) -> dict[str, str]:
        return self.tree_by_sha[self.commit_tree[self.head_commit]]

    def _tree_for_ref(self, ref: str, *, include_head: bool) -> dict[str, str] | None:
        ref_text = str(ref or "").strip()
        if ref_text and ref_text != "main" and ref_text in self.commit_tree:
            return self.tree_by_sha.get(self.commit_tree[ref_text])
        return self._head_tree_ref() if include_head else None

    def tree_entries(self, tree_sha: str) -> list[dict[str, Any]]:
        return [
            {
                "path": path,
                "mode": "100644",
                "type": "blob",
                "sha": sha,
                "size": len(self.blobs.get(sha, b"")),
            }
            for path, sha in sorted(self.tree_by_sha[tree_sha].items())
        ]

    # -- fake module surface ----------------------------------------------
    def _github_api_base(self) -> str:
        return "https://api.github.test"

    def validate_sync_config(self, config: Any) -> None:
        assert config.repo == "owner/repo"

    def _set_runtime_state(self, code: str, message: str) -> None:
        self.runtime_state = (code, message)

    def _clear_runtime_state(self, code: str) -> None:
        if self.runtime_state and self.runtime_state[0] == code:
            self.runtime_state = None

    def _request_json_value(self, **kwargs: Any) -> tuple[int, Any, dict[str, str]]:
        return self._request_json(**kwargs)

    def _request_json(self, **kwargs: Any) -> tuple[int, Any, dict[str, str]]:
        with self._lock:
            self._in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self._in_flight)
            self._in_flight -= 1
            self.calls.append(dict(kwargs))
        method = str(kwargs.get("method") or "").upper()
        url = str(kwargs.get("url") or "")
        payload = kwargs.get("payload") or {}

        if "/git/" in url:
            suffix = url.split("/git/", 1)[1]
            path_only = suffix.split("?", 1)[0]
            if method == "GET" and path_only.startswith("trees/"):
                tree_sha = path_only.split("/", 1)[1]
                tree_sha = self.commit_tree.get(tree_sha, tree_sha)
                if tree_sha not in self.tree_by_sha:
                    return 404, {"message": "Not Found"}, {}
                entries = self.tree_entries(tree_sha)
                if self.tree_listing_override is not None:
                    entries = self.tree_listing_override(entries)
                return (
                    200,
                    {
                        "sha": tree_sha,
                        "tree": entries,
                        "truncated": self.tree_listing_truncated,
                    },
                    {},
                )
            if method == "POST" and path_only == "blobs":
                raw = base64.b64decode(str(payload.get("content") or ""))
                blob_sha = _git_blob_sha1(raw)
                self.blobs[blob_sha] = raw
                return 201, {"sha": self.forced_blob_sha or blob_sha}, {}
            if method == "POST" and path_only == "trees":
                base_tree = str(payload.get("base_tree") or "")
                tree = dict(self.tree_by_sha.get(base_tree, {}))
                for entry in payload.get("tree") or []:
                    tree[str(entry["path"])] = str(entry["sha"])
                self._tree_counter += 1
                tree_sha = f"tree-{self._tree_counter}"
                self.tree_by_sha[tree_sha] = tree
                return 201, {"sha": tree_sha, "tree": [], "truncated": False}, {}
            if method == "POST" and path_only == "commits":
                parents = list(payload.get("parents") or [])
                if not parents:
                    raise AssertionError("source-sync commit must have a parent")
                tree_sha = str(payload.get("tree") or "")
                commit_sha = f"commit-{len(self.commits) + 1}"
                self.commits.append(
                    {
                        "sha": commit_sha,
                        "tree": tree_sha,
                        "parents": parents,
                        "message": str(payload.get("message") or ""),
                    }
                )
                self.commit_tree[commit_sha] = tree_sha
                return 201, {"sha": commit_sha, "tree": {"sha": tree_sha}}, {}
            if method == "PATCH" and path_only.startswith("refs/heads/"):
                if self.conflict_on_ref_update:
                    return 422, {"message": "Update is not a fast forward"}, {}
                commit_sha = str(payload.get("sha") or "")
                if commit_sha not in self.commit_tree:
                    return 422, {"message": "Unknown commit"}, {}
                self.head_commit = commit_sha
                return (
                    200,
                    {"ref": "refs/heads/main", "object": {"sha": commit_sha, "type": "commit"}},
                    {},
                )
            raise AssertionError(f"unexpected git request: {method} {url}")

        if method == "GET" and "/commits?" in url:
            if self.commit_history_status >= 400:
                return self.commit_history_status, {"message": "history unavailable"}, {}
            per_page = _query_int(url, "per_page", 1)
            if self.commit_history_payload is not None:
                return 200, self.commit_history_payload[:per_page], {}
            return (
                200,
                [{"sha": str(commit.get("sha") or "")} for commit in reversed(self.commits)][
                    :per_page
                ],
                {},
            )

        if method == "GET" and "/commits/" in url:
            return (
                200,
                {
                    "sha": self.head_commit,
                    "commit": {"tree": {"sha": self.commit_tree[self.head_commit]}},
                },
                {},
            )

        if "/contents/" in url:
            contents_path = url.split("/contents/", 1)[1].split("?", 1)[0]
            ref = _query_value(url, "ref")
            if contents_path == self.manifest_path and self.manifest_ref_status >= 400:
                return self.manifest_ref_status, {"message": "manifest unavailable"}, {}
            contents_tree: dict[str, str] | None = self._tree_for_ref(
                ref, include_head=contents_path != self.manifest_path
            )
            if method == "GET" and contents_tree is not None and contents_path in contents_tree:
                blob_sha = str(contents_tree.get(contents_path) or "")
                blob_bytes: bytes | None = self.blobs.get(blob_sha)
                if blob_bytes is not None:
                    return (
                        200,
                        {
                            "sha": blob_sha,
                            "content": base64.b64encode(blob_bytes).decode("ascii"),
                        },
                        {},
                    )
            if method == "GET" and contents_path == self.manifest_path:
                return 404, {"message": "Not Found"}, {}
            if method == "GET":
                if self.contents_listing:
                    return 200, self.contents_listing, {}
                if contents_tree is not None:
                    prefix = contents_path.rstrip("/") + "/"
                    rows = [
                        {"type": "file", "path": path, "sha": sha}
                        for path, sha in sorted(contents_tree.items())
                        if path.startswith(prefix)
                    ]
                    # GitHub's Contents API caps a directory at 1,000 entries.
                    rows = rows[:1000]
                    if rows:
                        return 200, rows, {}
                return 404, {"message": "Not Found"}, {}
            if method == "DELETE":
                self.deleted_paths.append(contents_path)
                self._head_tree_ref().pop(contents_path, None)
                if self.delete_status >= 400:
                    return self.delete_status, {"message": "delete denied"}, {}
                return 200, {"content": None}, {}

        raise AssertionError(f"unexpected request: {method} {url}")
