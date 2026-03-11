#!/usr/bin/env python3
"""Minimal pywebview/WebView2 repro with file-based diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_event(artifacts_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": utc_now_iso(),
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    path = artifacts_dir / "repro-events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def truthy_env(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def resolve_browser_arguments(env: dict[str, str] | None = None) -> str:
    env_map = env if env is not None else os.environ
    tokens: list[str] = []
    explicit = str(env_map.get("BALUFFO_WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS") or "").strip()
    if explicit:
        tokens.append(explicit)
    inherited = str(env_map.get("WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS") or "").strip()
    if inherited:
        tokens.append(inherited)
    if truthy_env(env_map.get("BALUFFO_WEBVIEW2_DISABLE_GPU")):
        tokens.append("--disable-gpu")
    deduped: list[str] = []
    seen: set[str] = set()
    for token in " ".join(tokens).split():
        normalized = str(token).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return " ".join(deduped)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content or ""), encoding="utf-8")


def build_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WebView2 Minimal Repro</title>
  <style>
    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      display: grid;
      place-items: center;
      background: #0b0f17;
      color: #f5f7fb;
      font-family: "Segoe UI", sans-serif;
    }
    .card {
      padding: 20px 24px;
      border: 1px solid rgba(255,255,255,0.14);
      border-radius: 12px;
      background: rgba(255,255,255,0.04);
    }
  </style>
</head>
<body>
  <div class="card">
    <h1>WebView2 Minimal Repro</h1>
    <p id="ready">ready</p>
    <script>
      document.body.dataset.reproReady = "1";
      console.log("minimal repro ready");
    </script>
  </div>
</body>
</html>
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a minimal pywebview/WebView2 repro.")
    parser.add_argument("--artifacts-dir", default=str(Path("data") / "webview2-min-repro"))
    parser.add_argument("--title", default="Baluffo WebView2 Repro")
    parser.add_argument("--width", type=int, default=1100)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--timeout-s", type=float, default=15.0)
    parser.add_argument("--mode", choices=("html", "url"), default="html")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifacts_dir = Path(args.artifacts_dir).expanduser().resolve()
    storage_path = artifacts_dir / "local-user-data" / "webview"
    html_path = artifacts_dir / "repro.html"
    crash_path = artifacts_dir / "repro-error.txt"
    started = time.perf_counter()
    append_event(artifacts_dir, "repro_launch_start", frozen=bool(getattr(sys, "frozen", False)))

    try:
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(build_html(), encoding="utf-8")
        storage_path.mkdir(parents=True, exist_ok=True)
        append_event(artifacts_dir, "repro_storage_ready", storagePath=str(storage_path))

        import webview  # type: ignore
        runtime_path = str(os.environ.get("BALUFFO_WEBVIEW2_RUNTIME_PATH") or "").strip()
        if runtime_path:
            webview.settings["WEBVIEW2_RUNTIME_PATH"] = runtime_path
        browser_args = resolve_browser_arguments(os.environ)
        if browser_args:
            os.environ["WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"] = browser_args

        append_event(artifacts_dir, "repro_webview_imported", elapsedMs=int((time.perf_counter() - started) * 1000))
        if runtime_path or browser_args:
            append_event(
                artifacts_dir,
                "repro_runtime_configured",
                elapsedMs=int((time.perf_counter() - started) * 1000),
                runtimePath=runtime_path,
                browserArguments=browser_args,
            )
        target_url = html_path.resolve().as_uri()
        append_event(
            artifacts_dir,
            "repro_target_resolved",
            elapsedMs=int((time.perf_counter() - started) * 1000),
            mode=str(args.mode),
            url=target_url,
        )

        if str(args.mode) == "url":
            window = webview.create_window(
                str(args.title),
                url=target_url,
                min_size=(int(args.width), int(args.height)),
                hidden=False,
                background_color="#0B0F17",
            )
        else:
            window = webview.create_window(
                str(args.title),
                html=build_html(),
                min_size=(int(args.width), int(args.height)),
                hidden=False,
                background_color="#0B0F17",
            )

        append_event(artifacts_dir, "repro_window_created", elapsedMs=int((time.perf_counter() - started) * 1000))

        def schedule_close() -> None:
            time.sleep(max(1.0, float(args.timeout_s)))
            append_event(artifacts_dir, "repro_close_requested", elapsedMs=int((time.perf_counter() - started) * 1000))
            try:
                destroy = getattr(window, "destroy", None)
                if callable(destroy):
                    destroy()
            except Exception as exc:  # noqa: BLE001
                append_event(
                    artifacts_dir,
                    "repro_close_error",
                    elapsedMs=int((time.perf_counter() - started) * 1000),
                    error=str(exc),
                    errorType=type(exc).__name__,
                )

        def on_shown(*_args: object) -> None:
            append_event(artifacts_dir, "repro_window_shown", elapsedMs=int((time.perf_counter() - started) * 1000))

        def on_loaded(*_args: object) -> None:
            append_event(artifacts_dir, "repro_window_loaded", elapsedMs=int((time.perf_counter() - started) * 1000))

        events = getattr(window, "events", None)
        if events is not None:
            shown_event = getattr(events, "shown", None)
            loaded_event = getattr(events, "loaded", None)
            if shown_event is not None:
                shown_event += on_shown
            if loaded_event is not None:
                loaded_event += on_loaded

        threading.Thread(target=schedule_close, name="webview2-min-repro-close", daemon=True).start()
        append_event(artifacts_dir, "repro_webview_starting", elapsedMs=int((time.perf_counter() - started) * 1000))
        webview.start(private_mode=False, storage_path=str(storage_path))
        append_event(artifacts_dir, "repro_webview_start_returned", elapsedMs=int((time.perf_counter() - started) * 1000))
        return 0
    except Exception as exc:  # noqa: BLE001
        append_event(
            artifacts_dir,
            "repro_error",
            elapsedMs=int((time.perf_counter() - started) * 1000),
            error=str(exc),
            errorType=type(exc).__name__,
        )
        write_text(crash_path, traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
