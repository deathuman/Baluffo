"""Shared JSON shape coercion helpers.

AI boundary owns: public dict/list coercion helpers for untrusted JSON-like payloads.
AI boundary implement in: this file for shape coercion only; callers own domain schema defaults.
AI boundary search before contracts: jobs reporting callers, bridge route leaves, and DATA_CONTRACT.md.
AI boundary verify: `npm run lint:repo-guardrails` plus focused JSON shape tests.
"""

from __future__ import annotations

from typing import Any, cast

JsonObject = dict[str, Any]
JsonObjectList = list[JsonObject]


def as_json_object(value: Any) -> JsonObject:
    if isinstance(value, dict):
        return cast(JsonObject, value)
    return {}


def copy_json_object(value: Any) -> JsonObject:
    return dict(as_json_object(value))


def as_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def json_object_rows(value: Any) -> JsonObjectList:
    return [cast(JsonObject, item) for item in as_json_list(value) if isinstance(item, dict)]


def json_object_values(value: Any) -> JsonObjectList:
    if isinstance(value, dict):
        return [cast(JsonObject, item) for item in value.values() if isinstance(item, dict)]
    return json_object_rows(value)
