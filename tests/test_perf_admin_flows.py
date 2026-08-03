"""Sampler hardening helpers for scripts/perf_admin_flows.py.

Two contract seams worth pinning:
  * `_is_gateway_timeout_body` — distinguishes Baluffo's container-gateway 504
    fallback (`bridge_degraded` + `gatewayReady:true`) from a hypothetical
    bridge-emitted 504. Used to populate `gatewayHit` in the sampler report,
    so benchmark findings don't misattribute noisy gateway contention to the
    bridge.
  * `_derive_abort_body` — POST `/tasks/abort` requires `runId`; the sampler
    previously fired with an empty body and got a 400 back, which obscured
    real abort signal in the reruns. Helper now extracts `runId` from the
    task-live summary when one is discoverable, falling back to
    `{"taskType": "fetch"}` (deterministic 400) so the leg still exercises
    the route end-to-end.
"""

from __future__ import annotations

import json

from scripts import perf_admin_flows


def test_is_gateway_timeout_body_recognizes_gateway_fallback_marker() -> None:
    body = json.dumps({"ok": False, "error": "bridge_degraded", "gatewayReady": True}).encode()
    assert perf_admin_flows._is_gateway_timeout_body(body) is True


def test_is_gateway_timeout_body_recognizes_container_gateway_source() -> None:
    body = json.dumps({"ok": False, "error": "bridge_down", "source": "container_gateway"}).encode()
    assert perf_admin_flows._is_gateway_timeout_body(body) is True


def test_is_gateway_timeout_body_rejects_bridge_504_without_marker() -> None:
    body = json.dumps({"ok": False, "error": "upstream_unreachable"}).encode()
    assert perf_admin_flows._is_gateway_timeout_body(body) is False


def test_is_gateway_timeout_body_handles_malformed_inputs() -> None:
    assert perf_admin_flows._is_gateway_timeout_body(b"") is False
    assert perf_admin_flows._is_gateway_timeout_body(b"not-json") is False
    assert perf_admin_flows._is_gateway_timeout_body(json.dumps(["a", "b"]).encode()) is False
    # gatewayReady without matching error string is ambiguous, treat as bridge.
    assert (
        perf_admin_flows._is_gateway_timeout_body(json.dumps({"gatewayReady": True}).encode())
        is False
    )


def test_derive_abort_body_prefers_captured_live_run_id() -> None:
    poll = {
        "runId": "fetch_captured",
        "finalParsed": {"tasks": [{"runId": "fetch_ignored"}]},
    }
    body = perf_admin_flows._derive_abort_body(poll)
    assert body == {"taskType": "fetch", "runId": "fetch_captured"}


def test_derive_abort_body_uses_final_parsed_when_no_capture() -> None:
    poll = {"finalParsed": {"tasks": [{"runId": "fetch_abc", "state": "running"}]}}
    body = perf_admin_flows._derive_abort_body(poll)
    assert body == {"taskType": "fetch", "runId": "fetch_abc"}


def test_derive_abort_body_falls_back_to_flat_run_id() -> None:
    poll = {"finalParsed": {"runId": "fetch_flat"}}
    body = perf_admin_flows._derive_abort_body(poll)
    assert body == {"taskType": "fetch", "runId": "fetch_flat"}


def test_derive_abort_body_falls_back_to_task_id_only_when_no_run_known() -> None:
    assert perf_admin_flows._derive_abort_body({}) == {"taskType": "fetch"}
    assert perf_admin_flows._derive_abort_body({"finalParsed": {}}) == {"taskType": "fetch"}
    assert perf_admin_flows._derive_abort_body(
        {"finalParsed": {"tasks": [{"state": "running"}]}}
    ) == {"taskType": "fetch"}
