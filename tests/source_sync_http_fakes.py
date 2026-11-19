from __future__ import annotations

import base64
import hashlib
import json
from urllib.parse import parse_qs, urlparse


def _query_int(url: str, key: str, default: int) -> int:
    values = parse_qs(urlparse(url).query).get(key) or []
    try:
        return int(values[0])
    except (IndexError, TypeError, ValueError):
        return default


def _git_blob_sha1(payload: bytes) -> str:
    return hashlib.sha1(f"blob {len(payload)}\0".encode("ascii") + payload).hexdigest()


class FakeResponse:
    def __init__(self, status: int, payload: dict, headers: dict | None = None):
        self._status = int(status)
        self._payload = dict(payload)
        self.headers = dict(headers or {})

    def getcode(self):
        return self._status

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class Recorder:
    """Fake GitHub transport with an in-memory Git Data API branch.

    Contents reads come from the FIFO response list. The Git Data API is
    stateful, so a push builds a real blob -> tree -> commit graph and a retry
    reads back the branch it just tried to advance.
    """

    manifest_path = "baluffo/source-sync/manifest.json"

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self._uploaded_shards: dict[str, str] = {}
        self.head_commit = "parent-commit"
        self.commit_tree: dict[str, str] = {self.head_commit: "base-tree"}
        self.tree_by_sha: dict[str, dict[str, str]] = {"base-tree": {}}
        self.blobs: dict[str, bytes] = {}
        self.commits: list[dict] = []
        self.fail_next_blob: BaseException | None = None
        self.fail_next_blob_status = 0
        self.fail_next_ref_status = 0
        self._tree_counter = 0

    @property
    def manifest_sha(self) -> str:
        return self.branch_tree().get(self.manifest_path, "")

    def branch_tree(self) -> dict[str, str]:
        return dict(self.tree_by_sha[self.commit_tree[self.head_commit]])

    def _tree_entries(self, tree_sha: str) -> list[dict]:
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

    def _git_response(self, method: str, url: str, body: str):
        suffix = url.split("/git/", 1)[1]
        path_only = suffix.split("?", 1)[0]
        payload = json.loads(body) if body else {}
        if method == "GET" and path_only.startswith("trees/"):
            tree_sha = path_only.split("/", 1)[1]
            tree_sha = self.commit_tree.get(tree_sha, tree_sha)
            if tree_sha not in self.tree_by_sha:
                return FakeResponse(404, {"message": "Not Found"})
            return FakeResponse(
                200,
                {"sha": tree_sha, "tree": self._tree_entries(tree_sha), "truncated": False},
            )
        if method == "POST" and path_only == "blobs":
            if self.fail_next_blob is not None:
                error = self.fail_next_blob
                self.fail_next_blob = None
                raise error
            if self.fail_next_blob_status:
                status = self.fail_next_blob_status
                self.fail_next_blob_status = 0
                return FakeResponse(status, {"message": "Internal Server Error"})
            raw = base64.b64decode(str(payload.get("content") or ""))
            blob_sha = _git_blob_sha1(raw)
            self.blobs[blob_sha] = raw
            return FakeResponse(201, {"sha": blob_sha})
        if method == "POST" and path_only == "trees":
            tree = dict(self.tree_by_sha.get(str(payload.get("base_tree") or ""), {}))
            for entry in payload.get("tree") or []:
                tree[str(entry["path"])] = str(entry["sha"])
            self._tree_counter += 1
            tree_sha = f"tree-{self._tree_counter}"
            self.tree_by_sha[tree_sha] = tree
            return FakeResponse(201, {"sha": tree_sha, "tree": [], "truncated": False})
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
            return FakeResponse(201, {"sha": commit_sha, "tree": {"sha": tree_sha}})
        if method == "PATCH" and path_only.startswith("refs/heads/"):
            if self.fail_next_ref_status:
                status = self.fail_next_ref_status
                self.fail_next_ref_status = 0
                return FakeResponse(status, {"message": "Update is not a fast forward"})
            commit_sha = str(payload.get("sha") or "")
            self.head_commit = commit_sha
            return FakeResponse(
                200, {"ref": "refs/heads/main", "object": {"sha": commit_sha, "type": "commit"}}
            )
        raise AssertionError(f"unexpected git request: {method} {url}")

    def __call__(self, req, timeout=20):  # noqa: ANN001
        url = req.full_url
        method = req.get_method()
        body = req.data.decode("utf-8") if isinstance(req.data, bytes) else ""
        self.calls.append(
            {
                "url": url,
                "method": method,
                "headers": dict(req.header_items()),
                "body": body,
                "timeout": timeout,
            }
        )
        if "/git/" in url:
            return self._git_response(method, url, body)
        if method == "GET" and "/commits?" in url:
            per_page = _query_int(url, "per_page", 1)
            return FakeResponse(
                200,
                [{"sha": str(commit.get("sha") or "")} for commit in reversed(self.commits)][
                    :per_page
                ],
            )
        if method == "GET" and "/commits/" in url:
            return FakeResponse(
                200,
                {
                    "sha": self.head_commit,
                    "commit": {"tree": {"sha": self.commit_tree[self.head_commit]}},
                },
            )
        if method == "GET" and url.split("?ref=", 1)[0].endswith("/source-sync/manifest.json"):
            ref = url.split("?ref=", 1)[1] if "?ref=" in url else ""
            tree_sha = self.commit_tree.get(ref, "")
            tree = self.tree_by_sha.get(tree_sha, {}) if tree_sha else {}
            blob_sha = tree.get(self.manifest_path, "")
            raw = self.blobs.get(blob_sha)
            if raw is not None:
                return FakeResponse(
                    200,
                    {"sha": blob_sha, "content": base64.b64encode(raw).decode("ascii")},
                )
        if method == "GET" and url.split("?ref=", 1)[0].endswith("/source-sync/shards"):
            return FakeResponse(404, {"message": "Not Found"})
        if "/source-sync/shards/" in url:
            key = url.split("?ref=", 1)[0]
            if method == "PUT":
                self._uploaded_shards[key] = str(json.loads(body).get("content") or "")
                return FakeResponse(201, {"content": {"sha": "shard-sha"}})
            if method == "GET":
                if key in self._uploaded_shards:
                    return FakeResponse(200, {"content": self._uploaded_shards[key]})
                # Shards from an atomic Git Data commit live in the branch tree.
                path = key.split("/contents/", 1)[-1]
                blob_sha = self.branch_tree().get(path, "")
                raw = self.blobs.get(blob_sha)
                if raw is not None:
                    return FakeResponse(200, {"content": base64.b64encode(raw).decode("ascii")})
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item
