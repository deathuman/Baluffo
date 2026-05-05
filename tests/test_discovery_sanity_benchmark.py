from __future__ import annotations

from src import discovery_sanity_benchmark as benchmark


def test_parse_args_accepts_quick_preset() -> None:
    args = benchmark.parse_args(["--preset", "quick"])

    assert args.preset == "quick"


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
