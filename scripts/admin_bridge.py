#!/usr/bin/env python3
"""Local admin bridge for source discovery approval workflows."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.jobs_fetcher import DEFAULT_STUDIO_SOURCE_REGISTRY
from scripts.source_registry import (
    ACTIVE_PATH,
    APPROVAL_STATE_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    load_json_array,
    load_json_object,
    save_json_atomic,
    source_identity,
    unique_sources,
)


def read_json_from_request(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def ensure_active_registry() -> List[Dict[str, Any]]:
    active = load_json_array(ACTIVE_PATH, [])
    if active:
        return active
    active = [dict(row) for row in DEFAULT_STUDIO_SOURCE_REGISTRY]
    save_json_atomic(ACTIVE_PATH, active)
    return active


def normalize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    # Precedence is explicit: active > pending > rejected.
    seen = set()
    normalized: Dict[str, List[Dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    for bucket in ("active", "pending", "rejected"):
        for row in state.get(bucket, []):
            if not isinstance(row, dict):
                continue
            key = source_identity(row)
            if key in seen:
                continue
            seen.add(key)
            item = dict(row)
            item["id"] = key
            normalized[bucket].append(item)
    return normalized


def load_state() -> Dict[str, List[Dict[str, Any]]]:
    return normalize_state({
        "active": ensure_active_registry(),
        "pending": load_json_array(PENDING_PATH, []),
        "rejected": load_json_array(REJECTED_PATH, []),
    })


def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    return {
        "activeCount": len(state["active"]),
        "pendingCount": len(state["pending"]),
        "rejectedCount": len(state["rejected"]),
    }


def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    normalized = normalize_state(state)
    save_json_atomic(ACTIVE_PATH, normalized["active"])
    save_json_atomic(PENDING_PATH, normalized["pending"])
    save_json_atomic(REJECTED_PATH, normalized["rejected"])
    return normalized


def move_entries(pending: List[Dict[str, Any]], selected_ids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected = set(str(item) for item in selected_ids)
    moved: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []
    for row in pending:
        if source_identity(row) in selected:
            moved.append(row)
        else:
            remaining.append(row)
    return moved, remaining


def run_background_script(script_name: str, args: List[str] | None = None) -> None:
    command = [sys.executable, str(Path(__file__).resolve().parent / script_name)]
    command.extend(args or [])
    subprocess.Popen(command, cwd=str(Path(__file__).resolve().parents[1]))


class Handler(BaseHTTPRequestHandler):
    def _route_path(self) -> str:
        return urlparse(self.path).path

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        path = self._route_path()
        if path == "/registry/active":
            state = load_state()
            self._send_json({"sources": state["active"], "summary": summarize_state(state)})
            return
        if path == "/registry/pending":
            state = load_state()
            self._send_json({"sources": state["pending"], "summary": summarize_state(state)})
            return
        if path == "/registry/rejected":
            state = load_state()
            self._send_json({"sources": state["rejected"], "summary": summarize_state(state)})
            return
        if path == "/discovery/report":
            report = load_json_object(DISCOVERY_REPORT_PATH, {})
            self._send_json(report or {"summary": {}, "candidates": [], "failures": []})
            return
        if path == "/registry/summary":
            state = load_state()
            self._send_json({"summary": summarize_state(state)})
            return
        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path = self._route_path()
        payload = read_json_from_request(self)
        state = load_state()

        if path == "/registry/approve":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            for row in moved:
                row["enabledByDefault"] = True
            state["pending"] = remaining
            state["active"] = unique_sources([*state["active"], *moved])
            state = persist_state(state)
            approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
            approval["approvedSinceLastRun"] = int(approval.get("approvedSinceLastRun") or 0) + len(moved)
            save_json_atomic(APPROVAL_STATE_PATH, approval)
            self._send_json({"approved": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/reject":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            state["pending"] = remaining
            state["rejected"] = unique_sources([*state["rejected"], *moved])
            state = persist_state(state)
            self._send_json({"rejected": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/rollback":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            selected = set(str(item) for item in ids)
            moved: List[Dict[str, Any]] = []
            active_remaining: List[Dict[str, Any]] = []
            for row in state["active"]:
                if source_identity(row) in selected:
                    moved.append(row)
                else:
                    active_remaining.append(row)
            state["active"] = active_remaining
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state(state)
            self._send_json({"rolledBack": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/restore-rejected":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["rejected"], [str(item) for item in ids])
            state["rejected"] = remaining
            for row in moved:
                row["enabledByDefault"] = False
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state(state)
            self._send_json({"restored": len(moved), "summary": summarize_state(state)})
            return

        if path == "/tasks/run-discovery":
            run_background_script("source_discovery.py", ["--mode", "dynamic"])
            self._send_json({"started": True, "task": "source_discovery", "mode": "dynamic"})
            return

        if path == "/tasks/run-discovery-full":
            run_background_script("source_discovery.py", ["--mode", "dynamic"])
            self._send_json({"started": True, "task": "source_discovery", "mode": "dynamic"})
            return

        if path == "/tasks/run-fetcher":
            run_background_script("jobs_fetcher.py")
            approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
            approval["approvedSinceLastRun"] = 0
            save_json_atomic(APPROVAL_STATE_PATH, approval)
            self._send_json({"started": True, "task": "jobs_fetcher"})
            return

        self._send_json({"error": "Not found"}, status=404)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local admin bridge API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8877)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_active_registry()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Admin bridge listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
