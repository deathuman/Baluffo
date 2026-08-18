from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from src.jobs.text_utils import (
    get_city_filter_option_values,
    load_city_noise_contract,
    load_country_acceptance_contract,
    sanitize_location_text,
)

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "location_sanitizer_parity.json"


def _load_cases() -> list[dict[str, Any]]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return list(payload["cases"])


def _frontend_probe(repo_root: Path, cases: list[dict[str, Any]]) -> dict[str, Any]:
    domain_uri = (repo_root / "frontend" / "jobs" / "domain.js").resolve().as_uri()
    city_noise_uri = (
        (repo_root / "frontend" / "shared" / "data" / "city-noise.js").resolve().as_uri()
    )
    country_acceptance_uri = (
        (repo_root / "frontend" / "shared" / "data" / "country-acceptance.js").resolve().as_uri()
    )
    script = f"""
import {{ getCityFilterOptionValues, sanitizeLocationField, isSemanticallyValidLocationValue }} from {json.dumps(domain_uri)};
import {{ CITY_NOISE_CONTRACT }} from {json.dumps(city_noise_uri)};
import {{ COUNTRY_ACCEPTANCE }} from {json.dumps(country_acceptance_uri)};

const cases = {json.dumps(cases)};
const results = cases.map((entry) => {{
  const sanitized = sanitizeLocationField(entry.value, entry.field);
  const valid = entry.field === "city"
    ? isSemanticallyValidLocationValue(entry.value, entry.field)
    : Boolean(sanitized);
  const cityFilterOptions = entry.field === "city"
    ? getCityFilterOptionValues(entry.value, entry.cityFilterCountry || "")
    : [];
  return {{
    field: entry.field,
    value: entry.value,
    sanitized,
    valid,
    cityFilterOptions,
  }};
}});

process.stdout.write(JSON.stringify({{
  cityNoiseContract: CITY_NOISE_CONTRACT,
  countryAcceptanceContract: {{
    version: COUNTRY_ACCEPTANCE.version,
    exactLabelMap: Object.fromEntries(COUNTRY_ACCEPTANCE.exactLabelMap),
    aliasToCanonical: Object.fromEntries(COUNTRY_ACCEPTANCE.aliasToCanonical),
  }},
  results,
}}));
"""
    completed = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=repo_root,
        capture_output=True,
        encoding="utf-8",
        text=True,
        check=True,
    )
    loaded = json.loads(completed.stdout)
    return loaded if isinstance(loaded, dict) else {}


def test_location_contract_normalization_stays_in_sync_with_frontend(repo_root: Path) -> None:
    frontend = _frontend_probe(repo_root, [])
    assert frontend["cityNoiseContract"] == load_city_noise_contract()
    assert frontend["countryAcceptanceContract"] == load_country_acceptance_contract()


def test_location_sanitizer_outputs_stay_in_sync_with_frontend(repo_root: Path) -> None:
    cases = _load_cases()
    frontend = _frontend_probe(repo_root, cases)
    frontend_results = {
        (str(item["field"]), str(item["value"])): item for item in frontend["results"]
    }

    for case in cases:
        key = (str(case["field"]), str(case["value"]))
        frontend_result = frontend_results[key]
        sanitized, _reason = sanitize_location_text(case["value"], field_name=str(case["field"]))
        valid = bool(sanitized)

        assert sanitized == case["sanitized"]
        assert valid is case["valid"]
        assert frontend_result["sanitized"] == case["sanitized"]
        assert frontend_result["valid"] is case["valid"]
        assert frontend_result["sanitized"] == sanitized
        assert frontend_result["valid"] is valid


def test_city_filter_option_outputs_stay_in_sync_with_frontend(repo_root: Path) -> None:
    cases = [
        {"field": "city", "value": "Tokyo or Fukuoka", "cityFilterCountry": "Japan"},
        {"field": "city", "value": "Tokyo or Fukuoka", "cityFilterCountry": ""},
        {"field": "city", "value": "New York or London", "cityFilterCountry": "US"},
        {"field": "city", "value": "S.F. or North America", "cityFilterCountry": "Unknown"},
        {"field": "city", "value": "McLean", "cityFilterCountry": "US"},
    ]
    frontend = _frontend_probe(repo_root, cases)

    for case, frontend_result in zip(cases, frontend["results"], strict=True):
        assert frontend_result["cityFilterOptions"] == get_city_filter_option_values(
            case["value"],
            case.get("cityFilterCountry", ""),
        )
