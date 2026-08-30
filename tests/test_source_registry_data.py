"""Prove the shared known-collisions loader serves both consumers identically.

The reviewed-collision allowlist (``data/defaults/source-registry-known-url-collisions.json``)
is parsed by the runtime twin rule (``source_registry_policy.known_twin_career_urls``)
and the commit-time guardrail (``tools.repo_health.source_registry_duplicate_url_policy``).
These tests pin that both consumers see the exact same set of canonical URLs for the
same file -- the committed baseline and synthetic payloads -- and that their only
difference is the documented failure mode (``None`` vs empty set when the file is
missing/unreadable/shape-mismatched).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src import source_registry_data as registry_data
from src.source_registry_policy import known_twin_career_urls
from tools.repo_health import source_registry_duplicate_url_policy as policy

ROOT = Path(__file__).resolve().parents[1]


def test_runtime_and_guardrail_load_identical_committed_baseline(monkeypatch) -> None:
    defaults = ROOT / "data" / "defaults"
    assert (defaults / registry_data.KNOWN_TWIN_URLS_FILENAME).exists()
    # Resolve DEFAULTS_DIR to the repo location explicitly so the runtime loader and
    # the guardrail read byte-identical input regardless of the ambient environment.
    monkeypatch.setattr(registry_data, "DEFAULTS_DIR", defaults)

    runtime = known_twin_career_urls()
    guardrail = policy._load_known_collisions(ROOT)

    assert runtime is not None
    assert guardrail == runtime
    assert len(runtime) >= 2  # the committed allowlist is non-empty
    assert all(isinstance(url, str) and url for url in runtime)


@pytest.mark.parametrize(
    "payload",
    [
        {"scopely.com/en/join-us": "reviewed collision", "a4vr.com/jobs": True},
        ["scopely.com/en/join-us", "a4vr.com/jobs"],
    ],
    ids=["mapping-payload", "list-payload"],
)
def test_both_consumers_see_identical_data_from_same_file(tmp_path, monkeypatch, payload) -> None:
    defaults = tmp_path / "data" / "defaults"
    defaults.mkdir(parents=True)
    (defaults / registry_data.KNOWN_TWIN_URLS_FILENAME).write_text(
        json.dumps(payload), encoding="utf-8"
    )
    monkeypatch.setattr(registry_data, "DEFAULTS_DIR", defaults)

    runtime = known_twin_career_urls()
    guardrail = policy._load_known_collisions(tmp_path)

    expected = {"scopely.com/en/join-us", "a4vr.com/jobs"}
    assert runtime == expected
    assert guardrail == expected


def test_missing_file_policy_is_none_and_guardrail_empty(tmp_path, monkeypatch) -> None:
    defaults = tmp_path / "data" / "defaults"
    defaults.mkdir(parents=True)
    monkeypatch.setattr(registry_data, "DEFAULTS_DIR", defaults)

    assert known_twin_career_urls() is None
    assert policy._load_known_collisions(tmp_path) == set()


def test_bad_shape_policy_is_none_and_guardrail_empty(tmp_path, monkeypatch) -> None:
    defaults = tmp_path / "data" / "defaults"
    defaults.mkdir(parents=True)
    (defaults / registry_data.KNOWN_TWIN_URLS_FILENAME).write_text("42", encoding="utf-8")
    monkeypatch.setattr(registry_data, "DEFAULTS_DIR", defaults)

    assert known_twin_career_urls() is None
    assert policy._load_known_collisions(tmp_path) == set()


def test_leaf_normalization_matches_shared_entrypoint_shapes() -> None:
    assert registry_data._known_collision_set({"a": 1, "b": None}) == {"a", "b"}
    assert registry_data._known_collision_set(["a", "b"]) == {"a", "b"}
    assert registry_data._known_collision_set("not-a-payload") == set()
