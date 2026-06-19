#!/usr/bin/env python3
"""UI and diagnostics helpers. Side effects: tkinter windows, executable launch, native dialogs. Verify: npm run test:frontend:packaged:update-rehearsal."""

from __future__ import annotations

import contextlib
import json
import os
import queue
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from src.ship.desktop_update_shared import iso_now

root: Any | None = None

DESKTOP_UPDATER_NO_DIALOG_ENV = "BALUFFO_DESKTOP_UPDATER_NO_DIALOG"
DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV = "BALUFFO_DESKTOP_UPDATER_VERIFY_TIMEOUT_S"

HELPER_WINDOW_TOKENS = {
    "window_bg": "#17141f",
    "shell_bg": "#1f1a29",
    "panel_bg": "#24202f",
    "panel_border": "#3d3550",
    "title_fg": "#f4edff",
    "text_fg": "#d9d2e8",
    "muted_fg": "#b6accd",
    "accent": "#bb86fc",
    "accent_active": "#9f6dfd",
    "track": "#312a3f",
}
HELPER_WINDOW_SIZE = {"width": 420, "height": 188}
HELPER_BRAND_TEXT = "Baluffo Update"
HELPER_TITLE_TEXT = "Installing the latest portable build"
HELPER_SUPPORT_TEXT = "Baluffo can stay closed while the update finishes."


def _module() -> Any:
    return root if root is not None else sys.modules[__name__]


def _normalize_helper_message(message: str) -> str:
    return str(message or "").strip() or "Preparing update"


def _helper_window_theme_tokens() -> dict[str, str]:
    return dict(HELPER_WINDOW_TOKENS)


def _helper_window_layout(initial_message: str) -> dict[str, Any]:
    return {
        "size": dict(HELPER_WINDOW_SIZE),
        "brandText": HELPER_BRAND_TEXT,
        "titleText": HELPER_TITLE_TEXT,
        "supportText": HELPER_SUPPORT_TEXT,
        "initialMessage": _normalize_helper_message(initial_message),
        "tokens": _helper_window_theme_tokens(),
    }


def _drain_helper_queue(
    progress: HelperProgressWindow,
    *,
    on_message,
    on_close,
) -> bool:
    while True:
        try:
            kind, payload = progress._queue.get_nowait()
        except queue.Empty:
            return False
        if kind == "close":
            on_close()
            progress._closed.set()
            return True
        if kind == "message" and payload:
            on_message(payload)


class HelperProgressWindow:
    """Best-effort native progress window for the one-shot updater helper."""

    def __init__(self) -> None:
        self._queue: queue.SimpleQueue[tuple[str, str]] = queue.SimpleQueue()
        self._closed = threading.Event()

    def start(self, message: str) -> None:
        self.update(_normalize_helper_message(message))

    def update(self, message: str) -> None:
        self._queue.put(("message", _normalize_helper_message(message)))

    def close(self) -> None:
        self._queue.put(("close", ""))
        self._closed.wait(timeout=2.0)

    def run(self, initial_message: str = "Preparing update") -> None:
        if os.name != "nt":
            self._closed.wait()
            return
        try:
            import tkinter as tk
            from tkinter import ttk
        except (ImportError, OSError):
            self._closed.wait()
            return

        module = _module()
        layout = module._helper_window_layout(initial_message)
        tokens = layout["tokens"]
        root_window = tk.Tk()
        root_window.title(HELPER_BRAND_TEXT)
        root_window.resizable(False, False)
        root_window.attributes("-topmost", True)
        root_window.protocol("WM_DELETE_WINDOW", lambda: None)
        root_window.configure(bg=tokens["window_bg"])

        style = ttk.Style(root_window)
        with contextlib.suppress(tk.TclError, OSError):
            style.theme_use("clam")
        style.configure(
            "Baluffo.Helper.Horizontal.TProgressbar",
            troughcolor=tokens["track"],
            background=tokens["accent"],
            bordercolor=tokens["panel_border"],
            lightcolor=tokens["accent"],
            darkcolor=tokens["accent_active"],
            thickness=10,
        )

        shell = tk.Frame(
            root_window,
            bg=tokens["shell_bg"],
            highlightthickness=1,
            highlightbackground=tokens["panel_border"],
            bd=0,
            padx=18,
            pady=16,
        )
        shell.pack(fill="both", expand=True, padx=12, pady=12)
        brand = tk.Label(
            shell,
            text=layout["brandText"],
            bg=tokens["shell_bg"],
            fg=tokens["accent"],
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        )
        brand.pack(fill="x")
        panel = tk.Frame(
            shell,
            bg=tokens["panel_bg"],
            highlightthickness=1,
            highlightbackground=tokens["panel_border"],
            bd=0,
            padx=16,
            pady=14,
        )
        panel.pack(fill="both", expand=True, pady=(10, 0))
        title = tk.Label(
            panel,
            text=layout["titleText"],
            bg=tokens["panel_bg"],
            fg=tokens["title_fg"],
            font=("Segoe UI Semibold", 12),
            anchor="w",
            justify="left",
        )
        title.pack(fill="x")
        message_var = tk.StringVar(value=layout["initialMessage"])
        detail = tk.Label(
            panel,
            textvariable=message_var,
            bg=tokens["panel_bg"],
            fg=tokens["text_fg"],
            font=("Segoe UI", 10),
            anchor="w",
            justify="left",
            wraplength=340,
            pady=0,
        )
        detail.pack(fill="x", pady=(10, 0))
        support = tk.Label(
            panel,
            text=layout["supportText"],
            bg=tokens["panel_bg"],
            fg=tokens["muted_fg"],
            font=("Segoe UI", 9),
            anchor="w",
            justify="left",
            wraplength=340,
        )
        support.pack(fill="x", pady=(8, 0))
        bar = ttk.Progressbar(
            panel,
            mode="indeterminate",
            length=320,
            style="Baluffo.Helper.Horizontal.TProgressbar",
        )
        bar.pack(fill="x", expand=True, pady=(14, 0))
        bar.start(12)
        root_window.update_idletasks()
        width = max(root_window.winfo_width() or 0, int(layout["size"]["width"]))
        height = max(root_window.winfo_height() or 0, int(layout["size"]["height"]))
        screen_width = root_window.winfo_screenwidth()
        screen_height = root_window.winfo_screenheight()
        offset_x = max(0, int((screen_width - width) / 2))
        offset_y = max(0, int((screen_height - height) / 3))
        root_window.geometry(f"{width}x{height}+{offset_x}+{offset_y}")

        def drain() -> None:
            def stop_bar() -> None:
                with contextlib.suppress(tk.TclError, OSError):
                    bar.stop()

            should_close = module._drain_helper_queue(
                self,
                on_message=message_var.set,
                on_close=stop_bar,
            )
            if should_close:
                root_window.destroy()
                return
            root_window.after(120, drain)

        root_window.after(120, drain)
        with contextlib.suppress(tk.TclError, OSError):
            root_window.mainloop()
        self._closed.set()


class NullProgressWindow:
    def start(self, message: str) -> None:
        return None

    def update(self, message: str) -> None:
        return None

    def close(self) -> None:
        return None


def _append_helper_diagnostics(log_path: Path, event: str, **fields: Any) -> None:
    row = {
        "ts": iso_now(),
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        return


def _helper_diagnostics_path_for_plan(plan_path: Path) -> Path:
    try:
        raw = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return plan_path.parent / "desktop-updater-helper.diagnostics.jsonl"
    helper_path = Path(str(raw.get("helperDiagnosticsPath") or "")).expanduser()
    if str(helper_path).strip():
        return helper_path.resolve()
    updater_dir = Path(str(raw.get("updaterWorkingDir") or "")).expanduser()
    if str(updater_dir).strip():
        return updater_dir.resolve() / "desktop-updater-helper.diagnostics.jsonl"
    return plan_path.parent / "desktop-updater-helper.diagnostics.jsonl"


def _helper_failure_dialog_enabled(*, env: dict[str, str] | None = None) -> bool:
    env_map = env if env is not None else os.environ
    raw = str(env_map.get(DESKTOP_UPDATER_NO_DIALOG_ENV) or "").strip().lower()
    return raw not in {"1", "true", "yes", "on"}


def _helper_relaunch_verify_timeout_s(
    default: float = 90.0, *, env: dict[str, str] | None = None
) -> float:
    env_map = env if env is not None else os.environ
    raw = str(env_map.get(DESKTOP_UPDATER_VERIFY_TIMEOUT_ENV) or "").strip()
    if not raw:
        return float(default)
    try:
        return max(1.0, float(raw))
    except ValueError:
        return float(default)


def _launch_executable(
    executable_path: Path,
    *,
    clear_app_version_override: bool = False,
    data_dir: Path | str | None = None,
) -> None:
    if not executable_path.is_file():
        raise RuntimeError(f"Desktop executable not found: {executable_path}")
    creationflags = (
        int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0
    )
    env = None
    if clear_app_version_override or data_dir is not None:
        env = os.environ.copy()
    if clear_app_version_override and env is not None:
        env.pop("BALUFFO_APP_VERSION_OVERRIDE", None)
    if data_dir is not None and env is not None:
        env["BALUFFO_DATA_DIR"] = str(Path(data_dir).expanduser().resolve())
    subprocess.Popen(  # noqa: S603
        [str(executable_path)],
        cwd=str(executable_path.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
        env=env,
    )


def _show_message(title: str, message: str) -> None:
    module = _module()
    if not module._helper_failure_dialog_enabled():
        return
    if os.name == "nt":
        import ctypes

        ctypes.windll.user32.MessageBoxW(None, str(message or ""), str(title or "Baluffo"), 0)
        return
    print(f"{title}: {message}", file=sys.stderr)
