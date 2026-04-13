import json
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def _fixture_json(name: str):
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _gamesmap_next_payload_html(companies: list[dict[str, object]]) -> str:
    payload = f'payload-start "companies":{json.dumps(companies, ensure_ascii=False)},"regions":[] payload-end'
    return (
        '<!DOCTYPE html><html lang="en"><body><script>'
        f"self.__next_f.push([1,{json.dumps(payload, ensure_ascii=False)}]);"
        "</script></body></html>"
    )


__all__ = [name for name in globals() if not name.startswith("__")]
