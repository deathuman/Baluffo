from __future__ import annotations

import json

import pytest

import src.jobs.text_utils as jobs_text_utils


def _clear_contract_loader_caches() -> None:
    jobs_text_utils.load_city_noise_contract.cache_clear()
    jobs_text_utils.load_country_acceptance_contract.cache_clear()


def _make_packaged_text_utils_path(tmp_path) -> str:
    versioned_module_path = (
        tmp_path / "ship" / "app" / "versions" / "1.2.3" / "src" / "jobs" / "text_utils.py"
    )
    versioned_module_path.parent.mkdir(parents=True, exist_ok=True)
    versioned_module_path.write_text("# test stub\n", encoding="utf-8")
    return str(versioned_module_path)


@pytest.mark.parametrize(
    ("loader_name", "contract_name", "shared_payload", "expected_contract"),
    [
        (
            "load_city_noise_contract",
            "city_noise_contract.json",
            {
                "version": 1,
                "proseFragments": ["Bachelor's Degree"],
                "sentencePrefixes": ["Learn"],
                "placeholderFragments": ["%label_"],
                "knownJunkTokens": ["????"],
                "cityFilterAllowedTokens": ["McLean"],
                "cityFilterRejectedTokens": ["sqs"],
                "cityFilterRejectedFragments": ["For all applicants"],
                "cityFilterRejectedPrefixes": ["or "],
                "cityFilterSplitCountryHints": {"Tokyo": "Japan"},
            },
            {
                "version": 1,
                "proseFragments": ["bachelor's degree"],
                "sentencePrefixes": ["learn"],
                "placeholderFragments": ["%label_"],
                "knownJunkTokens": ["????"],
                "cityFilterAllowedTokens": ["mclean"],
                "cityFilterRejectedTokens": ["sqs"],
                "cityFilterRejectedFragments": ["for all applicants"],
                "cityFilterRejectedPrefixes": ["or"],
                "cityFilterSplitCountryHints": {"tokyo": "Japan"},
            },
        ),
        (
            "load_country_acceptance_contract",
            "country_acceptance.json",
            {
                "version": 1,
                "acceptedExactLabels": ["United States"],
                "normalizeAliasesToValue": {"usa": "United States"},
            },
            {
                "version": 1,
                "exactLabelMap": {"unitedstates": "United States"},
                "aliasToCanonical": {"usa": "United States"},
            },
        ),
    ],
)
def test_contract_loaders_fall_back_to_packaged_ship_data(
    tmp_path, monkeypatch, loader_name, contract_name, shared_payload, expected_contract
) -> None:
    contract_path = tmp_path / "ship" / "data" / "contracts" / contract_name
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(shared_payload), encoding="utf-8")

    monkeypatch.setattr(jobs_text_utils, "__file__", _make_packaged_text_utils_path(tmp_path))
    _clear_contract_loader_caches()
    try:
        contract = getattr(jobs_text_utils, loader_name)()
    finally:
        _clear_contract_loader_caches()

    assert contract == expected_contract


@pytest.mark.parametrize(
    ("loader_name", "contract_name", "shared_payload", "version_payload", "expected_contract"),
    [
        (
            "load_city_noise_contract",
            "city_noise_contract.json",
            {
                "version": 1,
                "knownJunkTokens": ["shared"],
            },
            {
                "version": 2,
                "knownJunkTokens": ["version-local"],
            },
            {
                "version": 2,
                "proseFragments": [],
                "sentencePrefixes": [],
                "placeholderFragments": [],
                "knownJunkTokens": ["version-local"],
                "cityFilterAllowedTokens": [],
                "cityFilterRejectedTokens": [],
                "cityFilterRejectedFragments": [],
                "cityFilterRejectedPrefixes": [],
                "cityFilterSplitCountryHints": {},
            },
        ),
        (
            "load_country_acceptance_contract",
            "country_acceptance.json",
            {
                "version": 1,
                "acceptedExactLabels": ["Shared Country"],
                "normalizeAliasesToValue": {"shared": "Shared Country"},
            },
            {
                "version": 2,
                "acceptedExactLabels": ["Version Local Country"],
                "normalizeAliasesToValue": {"vlc": "Version Local Country"},
            },
            {
                "version": 2,
                "exactLabelMap": {"versionlocalcountry": "Version Local Country"},
                "aliasToCanonical": {"vlc": "Version Local Country"},
            },
        ),
    ],
)
def test_contract_loaders_prefer_version_local_packaged_contracts(
    tmp_path,
    monkeypatch,
    loader_name,
    contract_name,
    shared_payload,
    version_payload,
    expected_contract,
) -> None:
    shared_contract_path = tmp_path / "ship" / "data" / "contracts" / contract_name
    shared_contract_path.parent.mkdir(parents=True, exist_ok=True)
    shared_contract_path.write_text(json.dumps(shared_payload), encoding="utf-8")
    version_contract_path = (
        tmp_path / "ship" / "app" / "versions" / "1.2.3" / "data" / "contracts" / contract_name
    )
    version_contract_path.parent.mkdir(parents=True, exist_ok=True)
    version_contract_path.write_text(json.dumps(version_payload), encoding="utf-8")

    monkeypatch.setattr(jobs_text_utils, "__file__", _make_packaged_text_utils_path(tmp_path))
    _clear_contract_loader_caches()
    try:
        contract = getattr(jobs_text_utils, loader_name)()
    finally:
        _clear_contract_loader_caches()

    assert contract == expected_contract
