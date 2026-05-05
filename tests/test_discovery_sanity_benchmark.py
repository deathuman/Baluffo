from __future__ import annotations

from src import discovery_sanity_benchmark as benchmark


def test_parse_args_accepts_quick_preset() -> None:
    args = benchmark.parse_args(["--preset", "quick"])

    assert args.preset == "quick"


def test_parse_args_accepts_capped_preset() -> None:
    args = benchmark.parse_args(["--preset", "capped"])

    assert args.preset == "capped"


def test_parse_args_accepts_split_capped_presets() -> None:
    provider_args = benchmark.parse_args(["--preset", "capped-provider"])
    gamedevmap_args = benchmark.parse_args(["--preset", "capped-gamedevmap"])

    assert provider_args.preset == "capped-provider"
    assert gamedevmap_args.preset == "capped-gamedevmap"


def test_quick_discovery_config_disables_expensive_stages() -> None:
    base_config = {
        "stageToggles": {
            "gamedevmap": True,
            "gameprog": True,
            "gamesmap": True,
            "webSearch": True,
        },
        "gamesmap": {"enabled": True, "maxDetailPages": 100},
        "gameprog": {"enabled": True, "maxStudios": 100},
        "gamedevmap": {"enabled": True, "maxRows": 0},
        "webSearch": {"enabled": True},
    }

    quick_config = benchmark.build_quick_discovery_config(base_config)

    assert quick_config["stageToggles"] == {
        "curatedSeed": True,
        "sheetDirectory": False,
        "providerPatterns": True,
        "seedCareersScan": False,
        "gamesmap": False,
        "gameprog": False,
        "gamedevmap": False,
        "webSearch": False,
    }
    assert quick_config["gamesmap"]["enabled"] is False
    assert quick_config["gameprog"]["enabled"] is False
    assert quick_config["gamedevmap"]["enabled"] is False
    assert quick_config["webSearch"]["enabled"] is False
    assert quick_config["autoApproveHealthyPendingOnComplete"] is False
    assert base_config["gamesmap"]["enabled"] is True
    assert base_config["gameprog"]["enabled"] is True
    assert base_config["gamedevmap"]["enabled"] is True
    assert base_config["webSearch"]["enabled"] is True


def test_capped_discovery_config_enables_limited_heavy_stages() -> None:
    capped_config = benchmark.build_capped_discovery_config(
        {
            "stageToggles": {},
            "gameprog": {"enabled": False},
            "gamedevmap": {"enabled": False},
            "webSearch": {"enabled": True},
        }
    )

    assert capped_config["stageToggles"]["gameprog"] is True
    assert capped_config["stageToggles"]["gamedevmap"] is True
    assert capped_config["stageToggles"]["webSearch"] is False
    assert capped_config["gameprog"]["enabled"] is True
    assert capped_config["gameprog"]["maxStudios"] == 25
    assert capped_config["gamedevmap"]["enabled"] is True
    assert capped_config["gamedevmap"]["maxRows"] == 20
    assert capped_config["gamedevmap"]["maxHomepageFetches"] == 10
    assert capped_config["gamedevmap"]["activeAuditBatchSize"] == 5
    assert capped_config["gamedevmap"]["activeAuditMaxBatchesPerDiscoveryRun"] == 1


def test_split_capped_discovery_configs_isolate_heavy_stages() -> None:
    provider_config = benchmark.build_capped_provider_discovery_config({"stageToggles": {}})
    gamedevmap_config = benchmark.build_capped_gamedevmap_discovery_config({"stageToggles": {}})

    assert provider_config["stageToggles"]["providerPatterns"] is True
    assert provider_config["stageToggles"]["gameprog"] is True
    assert provider_config["stageToggles"]["gamedevmap"] is False
    assert provider_config["gamedevmap"]["enabled"] is False

    assert gamedevmap_config["stageToggles"]["providerPatterns"] is False
    assert gamedevmap_config["stageToggles"]["gameprog"] is False
    assert gamedevmap_config["stageToggles"]["gamedevmap"] is True
    assert gamedevmap_config["gameprog"]["enabled"] is False


def test_stage_durations_reads_discovery_runtime_stage_timings() -> None:
    stages = benchmark._stage_durations_ms(
        {"stageTimingsMs": {"probe": 120, "curatedSeed": 0, "gameprog": "45"}}
    )

    assert stages == {"gameprog": 45, "probe": 120}
