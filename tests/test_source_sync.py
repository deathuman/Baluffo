import base64
import json
import unittest
from urllib.error import HTTPError

from scripts import source_sync as sync


class _FakeResponse:
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


class _Recorder:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, req, timeout=20):  # noqa: ANN001
        self.calls.append({
            "url": req.full_url,
            "method": req.get_method(),
            "headers": dict(req.header_items()),
            "body": req.data.decode("utf-8") if isinstance(req.data, bytes) else "",
            "timeout": timeout,
        })
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class SourceSyncTests(unittest.TestCase):
    def test_config_status_reports_misconfigured_and_missing_by_default(self):
        cfg = sync.resolve_sync_config(env={})
        status = sync.config_status(cfg)
        self.assertTrue(status["enabled"])
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "misconfigured")

    def test_config_status_reports_disabled_when_explicitly_disabled(self):
        cfg = sync.resolve_sync_config(env={"BALUFFO_SYNC_ENABLED": "false"})
        status = sync.config_status(cfg)
        self.assertFalse(status["enabled"])
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "disabled")

    def test_config_status_reports_misconfigured_when_enabled_without_token_or_repo(self):
        cfg = sync.resolve_sync_config(env={"BALUFFO_SYNC_ENABLED": "true"})
        status = sync.config_status(cfg)
        self.assertTrue(status["enabled"])
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "misconfigured")
        self.assertIn("BALUFFO_SYNC_GITHUB_TOKEN", status["missing"])
        self.assertIn("BALUFFO_SYNC_REPO", status["missing"])

    def test_read_remote_snapshot_parses_contents_payload(self):
        snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T10:00:00+00:00",
            "source": {"name": "admin_bridge"},
            "active": [{"adapter": "teamtailor", "company": "A", "id": "teamtailor:name:a"}],
            "pending": [],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([_FakeResponse(200, {"sha": "abc123", "content": encoded})])
        cfg = sync.resolve_sync_config(env={
            "BALUFFO_SYNC_ENABLED": "true",
            "BALUFFO_SYNC_GITHUB_TOKEN": "tok",
            "BALUFFO_SYNC_REPO": "owner/repo",
        })
        result = sync.read_remote_snapshot(cfg, opener=opener)
        self.assertTrue(result["exists"])
        self.assertEqual(result["sha"], "abc123")
        self.assertEqual(len(result["snapshot"]["active"]), 1)

    def test_pull_and_merge_sources_unions_and_keeps_newer_record(self):
        local = {
            "active": [{"adapter": "static", "listing_url": "https://a.com/jobs", "updatedAt": "2026-03-09T10:00:00+00:00"}],
            "pending": [],
            "rejected": [],
        }
        remote_snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T11:00:00+00:00",
            "active": [
                {"adapter": "static", "listing_url": "https://a.com/jobs", "updatedAt": "2026-03-09T12:00:00+00:00", "studio": "Remote Wins"},
                {"adapter": "static", "listing_url": "https://b.com/jobs", "updatedAt": "2026-03-09T11:30:00+00:00"},
            ],
            "pending": [],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([_FakeResponse(200, {"sha": "s1", "content": encoded})])
        cfg = sync.resolve_sync_config(env={
            "BALUFFO_SYNC_ENABLED": "true",
            "BALUFFO_SYNC_GITHUB_TOKEN": "tok",
            "BALUFFO_SYNC_REPO": "owner/repo",
        })
        result = sync.pull_and_merge_sources(cfg, local, opener=opener)
        self.assertTrue(result["changed"])
        merged = result["mergedState"]
        self.assertEqual(len(merged["active"]), 2)
        first = next(row for row in merged["active"] if "a.com/jobs" in str(row.get("listing_url", "")))
        self.assertEqual(first.get("studio"), "Remote Wins")

    def test_merge_tie_break_keeps_local_when_timestamps_missing(self):
        local = {"active": [{"adapter": "static", "listing_url": "https://a.com/jobs", "studio": "Local"}], "pending": [], "rejected": []}
        remote = {"schemaVersion": 1, "active": [{"adapter": "static", "listing_url": "https://a.com/jobs", "studio": "Remote"}], "pending": [], "rejected": []}
        merged = sync.merge_registry_state(local, remote)
        self.assertEqual(len(merged["active"]), 1)
        self.assertEqual(merged["active"][0].get("studio"), "Local")

    def test_push_sources_snapshot_serializes_expected_payload(self):
        cfg = sync.resolve_sync_config(env={
            "BALUFFO_SYNC_ENABLED": "true",
            "BALUFFO_SYNC_GITHUB_TOKEN": "tok",
            "BALUFFO_SYNC_REPO": "owner/repo",
        })
        opener = _Recorder([
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ])
        local = {
            "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
            "pending": [{"adapter": "teamtailor", "name": "Foo"}],
            "rejected": [],
        }
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
        self.assertTrue(result["pushed"])
        self.assertEqual(result["remoteSha"], "newsha")
        self.assertEqual(len(opener.calls), 2)
        put_call = opener.calls[1]
        self.assertEqual(put_call["method"], "PUT")
        body = json.loads(put_call["body"])
        self.assertEqual(body["branch"], "main")
        decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
        self.assertEqual(int(decoded["schemaVersion"]), 1)
        self.assertIn("active", decoded)
        self.assertIn("pending", decoded)
        self.assertIn("rejected", decoded)


if __name__ == "__main__":
    unittest.main()
