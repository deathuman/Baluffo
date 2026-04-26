import json

from src.pipeline_io import serialize_rows_for_json


def test_unified_json_serialization_is_compact_and_valid() -> None:
    serialized = serialize_rows_for_json(
        [{"title": "Technical Artist", "company": "Orion Labs"}],
        ["title", "company"],
    )

    assert json.loads(serialized) == [{"title": "Technical Artist", "company": "Orion Labs"}]
    assert "\n" not in serialized
    assert ": " not in serialized
