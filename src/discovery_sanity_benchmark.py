#!/usr/bin/env python3
"""Run an isolated discovery sanity benchmark under _out/."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _ensure_repo_on_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an isolated discovery sanity benchmark.")
    parser.add_argument(
        "--output-dir",
        default="_out/perf-sanity-discovery",
        help="Isolated data root for benchmark artifacts.",
    )
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument(
        "--preset",
        choices=("default", "quick", "capped", "capped-provider", "capped-gamedevmap"),
        default="default",
        help=(
            "Benchmark preset to apply. 'quick' keeps CI smoke bounded; "
            "'capped' enables heavier stages with strict limits; "
            "'capped-provider' and 'capped-gamedevmap' isolate heavier stage families."
        ),
    )
    parser.add_argument(
        "--include-web-search",
        action="store_true",
        help="Enable web search for this benchmark run.",
    )
    return parser.parse_args(argv)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def build_quick_discovery_config(discovery_config: dict[str, Any]) -> dict[str, Any]:
    """Return a bounded discovery config for quick performance sanity runs."""

    config: dict[str, Any] = {}
    for key, value in discovery_config.items():
        config[key] = dict(value) if isinstance(value, dict) else value

    config["autoApproveHealthyPendingOnComplete"] = False
    config["stageToggles"] = {
        "curatedSeed": True,
        "sheetDirectory": False,
        "providerPatterns": True,
        "seedCareersScan": False,
        "gamesmap": False,
        "gameprog": False,
        "gamedevmap": False,
        "webSearch": False,
    }

    for stage_key in ("gamesmap", "gameprog", "gamedevmap", "webSearch"):
        stage_config = dict(config.get(stage_key) or {})
        stage_config["enabled"] = False
        config[stage_key] = stage_config

    return config


def _with_capped_discovery_stages(
    discovery_config: dict[str, Any],
    *,
    include_provider_seed: bool,
    include_gameprog: bool,
    include_gamedevmap: bool,
) -> dict[str, Any]:
    """Return a strictly capped discovery config with selected heavier stages enabled."""

    config = build_quick_discovery_config(discovery_config)
    config["stageToggles"] = {
        "curatedSeed": bool(include_provider_seed),
        "sheetDirectory": False,
        "providerPatterns": bool(include_provider_seed),
        "seedCareersScan": False,
        "gamesmap": False,
        "gameprog": bool(include_gameprog),
        "gamedevmap": bool(include_gamedevmap),
        "webSearch": False,
    }
    gameprog = dict(config.get("gameprog") or {})
    gameprog.update(
        {
            "enabled": bool(include_gameprog),
            "maxStudios": 25,
            "websiteOnlyFallback": True,
        }
    )
    config["gameprog"] = gameprog
    gamedevmap = dict(config.get("gamedevmap") or {})
    gamedevmap.update(
        {
            "enabled": bool(include_gamedevmap),
            "maxRows": 20,
            "maxHomepageFetches": 10,
            "activeAuditBatchSize": 5,
            "activeAuditMaxBatchesPerDiscoveryRun": 1,
            "activeAuditHomepageFetchConcurrency": 4,
            "activeAuditRecoveryFetchConcurrency": 8,
            "activeAuditRecoveryPerHostConcurrency": 1,
            "activeAuditRecoveryTimeoutSeconds": 2,
            "activeAuditBrowserRecoveryLimit": 0,
        }
    )
    config["gamedevmap"] = gamedevmap
    return config


def build_capped_discovery_config(discovery_config: dict[str, Any]) -> dict[str, Any]:
    """Return a broader but strictly capped discovery config for manual benchmarks."""

    return _with_capped_discovery_stages(
        discovery_config,
        include_provider_seed=True,
        include_gameprog=True,
        include_gamedevmap=True,
    )


def build_capped_provider_discovery_config(discovery_config: dict[str, Any]) -> dict[str, Any]:
    """Return a provider-pattern plus Gameprog capped benchmark config."""

    return _with_capped_discovery_stages(
        discovery_config,
        include_provider_seed=True,
        include_gameprog=True,
        include_gamedevmap=False,
    )


def build_capped_gamedevmap_discovery_config(discovery_config: dict[str, Any]) -> dict[str, Any]:
    """Return a GameDevMap-only capped benchmark config."""

    return _with_capped_discovery_stages(
        discovery_config,
        include_provider_seed=False,
        include_gameprog=False,
        include_gamedevmap=True,
    )


def _stage_durations_ms(runtime: dict[str, Any]) -> dict[str, int]:
    runtime_stage_timings = runtime.get("stageTimingsMs")
    if isinstance(runtime_stage_timings, dict):
        stages: dict[str, int] = {}
        for key, value in runtime_stage_timings.items():
            try:
                duration = int(float(value))
            except (TypeError, ValueError):
                continue
            if duration > 0:
                stages[str(key)] = duration
        if stages:
            return dict(sorted(stages.items()))

    stage_keys = {
        "generation": ("generationDurationMs", "generationMs"),
        "dedupe": ("dedupeDurationMs", "dedupeMs"),
        "probe": ("probeDurationMs", "probeMs"),
        "finalization": ("finalizationDurationMs", "finalizeDurationMs", "finalizeMs"),
    }
    stages: dict[str, int] = {}
    for stage, keys in stage_keys.items():
        for key in keys:
            value = runtime.get(key)
            try:
                duration = int(float(value))
            except (TypeError, ValueError):
                continue
            if duration > 0:
                stages[stage] = duration
                break
    return stages


def main(argv: list[str] | None = None) -> int:
    root = _ensure_repo_on_path()
    args = parse_args(argv)
    from src.baluffo_config import get_storage_defaults
    from src.shared.json_io import (
        copy_json_file_to_storage,
        existing_json_candidate,
        gzip_backed_json_storage_path,
    )

    data_dir = Path(args.output_dir)
    if not data_dir.is_absolute():
        data_dir = (root / data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    live_data_dir = Path(get_storage_defaults()["data_dir"])
    for name in ("jobs-source-state.json", "source-discovery-config.json"):
        source_path = existing_json_candidate(live_data_dir / name) or live_data_dir / name
        target_path = data_dir / name
        if source_path.exists() and not gzip_backed_json_storage_path(target_path).exists():
            copy_json_file_to_storage(source_path, target_path)
    os.environ["BALUFFO_DATA_DIR"] = str(data_dir)

    from src.source_discovery.config import load_discovery_config
    from src.source_discovery.orchestrator import run_discovery

    discovery_config = dict(load_discovery_config())
    discovery_config["autoApproveHealthyPendingOnComplete"] = False
    if str(args.preset) == "quick":
        discovery_config = build_quick_discovery_config(discovery_config)
    elif str(args.preset) == "capped":
        discovery_config = build_capped_discovery_config(discovery_config)
    elif str(args.preset) == "capped-provider":
        discovery_config = build_capped_provider_discovery_config(discovery_config)
    elif str(args.preset) == "capped-gamedevmap":
        discovery_config = build_capped_gamedevmap_discovery_config(discovery_config)

    report = run_discovery(
        timeout_s=int(args.timeout),
        top_n=int(args.top),
        mode=str(args.mode),
        include_web_search=bool(args.include_web_search),
        discovery_config=discovery_config,
    )
    summary = _as_dict(report.get("summary"))
    runtime = _as_dict(report.get("runtime"))
    outputs = _as_dict(report.get("outputs"))
    payload = {
        "benchmarkPreset": str(args.preset),
        "outputDir": str(data_dir),
        "reportPath": str(outputs.get("report")),
        "queuedCandidateCount": int(summary.get("queuedCandidateCount") or 0),
        "discoverableButDeferredCount": int(summary.get("discoverableButDeferredCount") or 0),
        "failedProbeCount": int(summary.get("failedProbeCount") or 0),
        "queuedByAdapter": dict(summary.get("queuedByAdapter") or {}),
        "deferredByAdapter": dict(summary.get("deferredByAdapter") or {}),
        "healthyButDeferredByAdapter": dict(summary.get("healthyButDeferredByAdapter") or {}),
        "suppressedStaticCount": int(summary.get("suppressedStaticCount") or 0),
        "suppressedStaticByReason": dict(summary.get("suppressedStaticByReason") or {}),
        "suppressedStaticByStage": dict(summary.get("suppressedStaticByStage") or {}),
        "queuedProviderCount": int(summary.get("queuedProviderCount") or 0),
        "queuedStaticCount": int(summary.get("queuedStaticCount") or 0),
        "deferredReasons": dict(summary.get("deferredReasons") or {}),
        "totalDurationMs": int(runtime.get("totalDurationMs") or 0),
        "stageDurationsMs": _stage_durations_ms(runtime),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
