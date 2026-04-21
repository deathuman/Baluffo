from __future__ import annotations

"""End-to-end discovery orchestration (CLI + core flow).

Phases:
1. Candidate generation (curated seeds, sheet directory, provider patterns, Gamesmap, web search)
2. De-duplication across runs (IDs + endpoint fingerprints)
3. Probe (HTTP checks) with concurrency limits
4. Queue balancing (caps by adapter/domain, top-N)
5. Summary + report writing
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.jobs.state import read_source_state as _read_source_state
from src.shared.utils import now_iso
from src.source_registry import (
    APPROVAL_STATE_PATH as _DEFAULT_APPROVAL_STATE_PATH,
)
from src.source_registry import (
    URL_PATCH_MANIFEST_PATH as DEFAULT_URL_PATCH_MANIFEST_PATH,
)
from src.source_registry import (
    apply_discovery_auto_approval as _apply_discovery_auto_approval,
)
from src.source_registry import (
    save_json_atomic,
)

from . import orchestrator_finalize as orchestrator_finalize_mod
from . import orchestrator_generation as orchestrator_generation_mod
from . import orchestrator_probe as orchestrator_probe_mod
from .bootstrap import discovery_report_write_path, prime_bridge_discovery_report
from .config import (
    ADAPTER_QUEUE_CAPS,
    DOMAIN_QUEUE_CAP_DEFAULT,
    UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS,
    UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP,
    load_discovery_config,
)
from .core import apply_sheet_directory_static_probe_cap as _apply_sheet_directory_static_probe_cap
from .gamedevmap import discover_gamedevmap_candidates as _discover_gamedevmap_candidates
from .gameprog import discover_gameprog_candidates as _discover_gameprog_candidates
from .gamesmap import discover_gamesmap_candidates as _discover_gamesmap_candidates
from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState
from .probe import async_probe_candidate as _async_probe_candidate
from .provider_patterns import build_pattern_candidates as _build_pattern_candidates
from .reporting import (
    emit_log,
)
from .reporting import (
    merge_candidate_streams as _merge_candidate_streams,
)
from .reporting import (
    stage_curated_seed_candidates as _stage_curated_seed_candidates,
)
from .reporting import (
    write_discovery_progress_report as _write_discovery_progress_report,
)
from .scoring import resolve_discovery_thresholds
from .sheet_directory import (
    discover_game_studio_sheet_candidates as _discover_game_studio_sheet_candidates,
)
from .stage_control import apply_discovery_cli_args_to_config as _apply_discovery_cli_args_to_config
from .url_patches import (
    load_url_patches as _load_url_patches,
)
from .url_patches import (
    resolve_patch_target as _resolve_patch_target,
)
from .url_patches import (
    save_url_patch_manifest as _save_url_patch_manifest,
)
from .web_search import (
    discover_seed_careers_page_candidates as _discover_seed_careers_page_candidates,
)
from .web_search import (
    discover_web_search_candidates as _discover_web_search_candidates,
)
from .web_search import (
    fetch_text,
)

orchestrator_generation_mod.root = sys.modules[__name__]
orchestrator_probe_mod.root = sys.modules[__name__]
orchestrator_finalize_mod.root = sys.modules[__name__]

DEFAULT_APPROVAL_STATE_PATH = _DEFAULT_APPROVAL_STATE_PATH
apply_discovery_auto_approval = _apply_discovery_auto_approval
read_source_state = _read_source_state
apply_sheet_directory_static_probe_cap = _apply_sheet_directory_static_probe_cap
discover_gamedevmap_candidates = _discover_gamedevmap_candidates
discover_gameprog_candidates = _discover_gameprog_candidates
discover_gamesmap_candidates = _discover_gamesmap_candidates
async_probe_candidate = _async_probe_candidate
build_pattern_candidates = _build_pattern_candidates
merge_candidate_streams = _merge_candidate_streams
stage_curated_seed_candidates = _stage_curated_seed_candidates
write_discovery_progress_report = _write_discovery_progress_report
discover_game_studio_sheet_candidates = _discover_game_studio_sheet_candidates
load_url_patches = _load_url_patches
resolve_patch_target = _resolve_patch_target
save_url_patch_manifest = _save_url_patch_manifest
discover_seed_careers_page_candidates = _discover_seed_careers_page_candidates
discover_web_search_candidates = _discover_web_search_candidates


def _discovery_report_write_path() -> Path:
    return discovery_report_write_path()


def _prime_bridge_discovery_report(*, run_id: str, started_at: str, mode: str) -> None:
    prime_bridge_discovery_report(
        run_id=run_id,
        started_at=started_at,
        mode=mode,
        save_json_atomic=save_json_atomic,
        now_iso=now_iso,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover new job source candidates.")
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument(
        "--top", type=int, default=0, help="Limit new candidates written this run; 0 = no limit."
    )
    parser.add_argument("--preset", choices=("default", "uncapped"), default="default")
    parser.add_argument("--mode", choices=("dynamic", "static"), default="dynamic")
    parser.add_argument(
        "--no-web-search", action="store_true", help="Disable lightweight web search phase."
    )
    parser.add_argument(
        "--gamesmap-website-only-fallback",
        action="store_true",
        help="Manual-only mode: include Gamesmap homepage-only candidates in this run.",
    )
    parser.add_argument(
        "--gamesmap-max-detail-pages",
        type=int,
        default=0,
        help="Optional Gamesmap crawl cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gamedevmap-enabled",
        action="store_true",
        help="Enable GameDevMap directory scanning.",
    )
    parser.add_argument(
        "--gamedevmap-max-rows",
        type=int,
        default=0,
        help="Optional GameDevMap representative row cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gamedevmap-max-homepage-fetches",
        type=int,
        default=0,
        help="Optional GameDevMap homepage fetch cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--only-gamedevmap",
        action="store_true",
        help="Run only the GameDevMap discovery stage and skip other candidate-generation stages.",
    )
    parser.add_argument(
        "--gameprog-enabled",
        action="store_true",
        help="Enable Gameprog directory scanning.",
    )
    parser.add_argument(
        "--gameprog-max-studios",
        type=int,
        default=0,
        help="Optional Gameprog studio cap override for this run; 0 = config default.",
    )
    parser.add_argument(
        "--gameprog-website-only-fallback",
        action="store_true",
        help="Include Gameprog website-only candidates.",
    )
    return parser.parse_args(argv)


def run_discovery(
    *,
    timeout_s: int,
    top_n: int,
    preset: str = "default",
    mode: str = "dynamic",
    include_web_search: bool = True,
    discovery_config: dict[str, Any] | None = None,
    run_id: str = "",
    started_at_override: str = "",
    fetcher=fetch_text,
    cli_args: argparse.Namespace | None = None,
) -> dict[str, Any]:
    started_at = str(started_at_override or now_iso()).strip()
    run_id = str(run_id or "").strip()
    run_started_mono = time.perf_counter()
    emit_log(
        f"Discovery worker run_discovery() begin runId={run_id!r} "
        f"report_path={discovery_report_write_path()!s}"
    )
    _prime_bridge_discovery_report(
        run_id=run_id,
        started_at=started_at,
        mode=str(mode or "dynamic"),
    )

    effective_config = (
        discovery_config if isinstance(discovery_config, dict) else load_discovery_config()
    )
    if cli_args is not None:
        effective_config = _apply_discovery_cli_args_to_config(effective_config, cli_args)
    thresholds = resolve_discovery_thresholds(effective_config)

    preset_name = str(preset or "default").strip().lower() or "default"
    if preset_name not in {"default", "uncapped"}:
        preset_name = "default"
    top_cap_bypassed = int(top_n or 0) <= 0
    sheet_static_probe_cap_bypassed = preset_name in {"default", "uncapped"}
    queue_domain_cap = DOMAIN_QUEUE_CAP_DEFAULT
    queue_adapter_caps = ADAPTER_QUEUE_CAPS
    if preset_name == "uncapped":
        queue_domain_cap = UNCAPPED_DISCOVERY_DOMAIN_QUEUE_CAP
        queue_adapter_caps = UNCAPPED_DISCOVERY_ADAPTER_QUEUE_CAPS

    url_patch_manifest_path = Path(source_registry_module.URL_PATCH_MANIFEST_PATH)
    url_patch_manifest_enabled = bool(url_patch_manifest_path) and (
        fetcher is fetch_text
        or str(url_patch_manifest_path) != str(DEFAULT_URL_PATCH_MANIFEST_PATH)
    )

    deps = DiscoveryRunDeps(
        timeout_s=timeout_s,
        top_n=top_n,
        preset_name=preset_name,
        mode=mode,
        include_web_search=include_web_search,
        effective_config=effective_config,
        thresholds=thresholds,
        run_id=run_id,
        started_at=started_at,
        run_started_mono=run_started_mono,
        fetcher=fetcher,
        top_cap_bypassed=top_cap_bypassed,
        sheet_static_probe_cap_bypassed=sheet_static_probe_cap_bypassed,
        queue_domain_cap=queue_domain_cap,
        queue_adapter_caps=queue_adapter_caps,
        url_patch_manifest_path=url_patch_manifest_path,
        url_patch_manifest_enabled=url_patch_manifest_enabled,
    )
    state = DiscoveryRunState()
    state.write_progress_report(
        [],
        phase="starting",
        phase_label="Initializing scan",
        deps=deps,
        root=sys.modules[__name__],
    )
    state.write_progress_report(
        [],
        phase="generating_candidates",
        phase_label="Generating seed candidates",
        deps=deps,
        root=sys.modules[__name__],
    )

    orchestrator_generation_mod.prepare_probe_inputs(deps=deps, state=state)
    orchestrator_probe_mod.probe_and_recover(deps=deps, state=state)
    return orchestrator_finalize_mod.finalize_run(deps=deps, state=state)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env_run_id = str(os.environ.get("BALUFFO_DISCOVERY_RUN_ID") or "").strip()
    env_started_at = str(os.environ.get("BALUFFO_DISCOVERY_STARTED_AT") or "").strip()
    bridge_spawned = bool(env_run_id and env_started_at)
    if bridge_spawned:
        _prime_bridge_discovery_report(
            run_id=env_run_id,
            started_at=env_started_at,
            mode=str(getattr(args, "mode", None) or "dynamic"),
        )
    if bridge_spawned:
        discovery_config = None
    else:
        discovery_config = _apply_discovery_cli_args_to_config(load_discovery_config(), args)
    run_discovery(
        timeout_s=int(args.timeout),
        top_n=int(args.top),
        preset=str(args.preset or "default"),
        mode=str(args.mode),
        include_web_search=not bool(args.no_web_search),
        discovery_config=discovery_config,
        run_id=env_run_id,
        started_at_override=env_started_at,
        cli_args=args,
    )
    return 0
