"""Baluffo-specific release version ordering helpers."""

from __future__ import annotations


def parse_baluffo_version(value: str) -> tuple[int, int, int, int, int] | None:
    text = str(value or "").strip()
    parts = text.split(".")
    if len(parts) != 3 or any(not part.isdigit() for part in parts):
        return None
    major = int(parts[0])
    minor = int(parts[1])
    patch_token = parts[2]
    patch_major = int(patch_token[0])
    patch_increment = int(patch_token[1:] or "0")
    return (major, minor, patch_major, patch_increment, len(patch_token))


def compare_baluffo_versions(left: str, right: str) -> int:
    left_parts = parse_baluffo_version(left)
    right_parts = parse_baluffo_version(right)
    if left_parts and right_parts:
        return (left_parts > right_parts) - (left_parts < right_parts)
    return (str(left) > str(right)) - (str(left) < str(right))
