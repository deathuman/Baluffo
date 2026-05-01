import base64
import json
from urllib.error import HTTPError

from src import source_sync as sync
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


class _FakeResponse:
    def __init__(self, status: int, payload: dict):
        self._status = int(status)
        self._payload = dict(payload)
        self.headers = {}

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
        self.calls.append(
            {
                "method": req.get_method(),
                "body": req.data.decode("utf-8") if isinstance(req.data, bytes) else "",
            }
        )
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _local_with_source_policy() -> dict[str, object]:
    return {
        "active": [{"adapter": "static", "listing_url": "https://legacy.example/jobs"}],
        "pending": [],
        "rejected": [],
        "sourcePolicy": {
            "reviewState": {
                "pairs": {"static||provider": {"manualSuppressionOverride": "force_pause"}}
            },
            "recommendations": {"pairs": [{"staticSourceId": "static"}]},
        },
        "sourcePolicyReviewState": {"pairs": {}},
        "sourcePolicyRecommendations": {"pairs": []},
    }


def test_build_snapshot_excludes_source_policy_artifacts(monkeypatch):
    monkeypatch.setattr(sync, "now_iso", lambda: "2026-04-09T20:55:07.978053+00:00")

    snapshot = sync.build_snapshot(_local_with_source_policy(), source_label="admin_bridge")

    assert sorted(snapshot.keys()) == [
        "active",
        "generatedAt",
        "pending",
        "schemaVersion",
        "source",
    ]
    assert "sourcePolicy" not in snapshot
    assert "sourcePolicyReviewState" not in snapshot
    assert "sourcePolicyRecommendations" not in snapshot


def test_push_sources_snapshot_excludes_source_policy_payload(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, _local_with_source_policy(), opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]

    assert result["pushed"]
    body = json.loads(opener.calls[2]["body"])
    decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
    assert "sourcePolicy" not in decoded
    assert "reviewState" not in decoded
    assert "recommendations" not in decoded
