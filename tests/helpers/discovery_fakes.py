from __future__ import annotations

from typing import Any


def seeds() -> list[dict[str, Any]]:
    return [
        {
            "studio": "Seed Studio",
            "careersUrl": "https://seed.example/careers",
            "nlPriority": True,
        },
        {
            "studio": "Search Studio",
            "nlPriority": False,
        },
    ]


def assert_gamesindustry(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["title"] == "Senior Quality Analyst"
    assert rows[0]["company"] == "Sharkmob"
    assert rows[0]["sourceJobId"] == "43821"
    assert rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/")
    titles = {row["title"] for row in rows}
    assert "Read more" not in titles
    assert "Programming (6)" not in titles
