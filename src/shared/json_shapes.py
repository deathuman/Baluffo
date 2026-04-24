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
