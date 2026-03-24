"""Game/studio job detection (extracted from common)."""

from __future__ import annotations

from typing import Any

GAME_KEYWORDS = {
    "game",
    "gaming",
    "unity",
    "unreal",
    "gamedev",
    "gameplay",
    "technical artist",
    "tech art",
    "tech artist",
    "shader",
    "shader artist",
    "material artist",
    "world artist",
    "terrain artist",
    "environment art",
    "environment artist",
    "character artist",
    "engine programmer",
    "graphics programmer",
}


def looks_like_game_job(*values: Any) -> bool:
    """True if any value string contains a game-related keyword."""
    text = " ".join(str(v or "").strip().lower() for v in values if v is not None)
    return bool(text) and any(keyword in text for keyword in GAME_KEYWORDS)
