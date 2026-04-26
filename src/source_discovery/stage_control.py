from __future__ import annotations

import argparse
from typing import Any


def apply_discovery_cli_args_to_config(
    discovery_config: dict[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    cfg = dict(discovery_config)
    if bool(getattr(args, "gamesmap_website_only_fallback", False)):
        gamesmap_cfg = dict(cfg.get("gamesmap") or {})
        gamesmap_cfg["websiteOnlyFallback"] = True
        gamesmap_cfg["websiteOnlyManualOnly"] = True
        cfg["gamesmap"] = gamesmap_cfg
    if int(getattr(args, "gamesmap_max_detail_pages", 0) or 0) > 0:
        gamesmap_cfg = dict(cfg.get("gamesmap") or {})
        gamesmap_cfg["maxDetailPages"] = int(args.gamesmap_max_detail_pages)
        cfg["gamesmap"] = gamesmap_cfg
    if bool(getattr(args, "gamedevmap_enabled", False)):
        gamedevmap_cfg = dict(cfg.get("gamedevmap") or {})
        gamedevmap_cfg["enabled"] = True
        cfg["gamedevmap"] = gamedevmap_cfg
    if int(getattr(args, "gamedevmap_max_rows", 0) or 0) > 0:
        gamedevmap_cfg = dict(cfg.get("gamedevmap") or {})
        gamedevmap_cfg["maxRows"] = int(args.gamedevmap_max_rows)
        cfg["gamedevmap"] = gamedevmap_cfg
    if int(getattr(args, "gamedevmap_max_homepage_fetches", 0) or 0) > 0:
        gamedevmap_cfg = dict(cfg.get("gamedevmap") or {})
        gamedevmap_cfg["maxHomepageFetches"] = int(args.gamedevmap_max_homepage_fetches)
        cfg["gamedevmap"] = gamedevmap_cfg
    if bool(getattr(args, "only_gamedevmap", False)) or bool(
        getattr(args, "gamedevmap_active_dry_run", False)
    ):
        cfg["stageToggles"] = {
            "curatedSeed": False,
            "sheetDirectory": False,
            "providerPatterns": False,
            "seedCareersScan": False,
            "gamesmap": False,
            "gameprog": False,
            "gamedevmap": True,
            "webSearch": False,
        }
        gamedevmap_cfg = dict(cfg.get("gamedevmap") or {})
        gamedevmap_cfg["enabled"] = True
        cfg["gamedevmap"] = gamedevmap_cfg
    if bool(getattr(args, "gameprog_enabled", False)):
        gameprog_cfg = dict(cfg.get("gameprog") or {})
        gameprog_cfg["enabled"] = True
        cfg["gameprog"] = gameprog_cfg
    if int(getattr(args, "gameprog_max_studios", 0) or 0) > 0:
        gameprog_cfg = dict(cfg.get("gameprog") or {})
        gameprog_cfg["maxStudios"] = int(args.gameprog_max_studios)
        cfg["gameprog"] = gameprog_cfg
    if bool(getattr(args, "gameprog_website_only_fallback", False)):
        gameprog_cfg = dict(cfg.get("gameprog") or {})
        gameprog_cfg["websiteOnlyFallback"] = True
        cfg["gameprog"] = gameprog_cfg
    return cfg


def discovery_stage_enabled(
    discovery_config: dict[str, Any] | None, stage_key: str, *, default: bool = True
) -> bool:
    config = discovery_config if isinstance(discovery_config, dict) else {}
    stage_toggles = config.get("stageToggles")
    if not isinstance(stage_toggles, dict):
        return default
    value = stage_toggles.get(stage_key)
    if value is None:
        return default
    return bool(value)
