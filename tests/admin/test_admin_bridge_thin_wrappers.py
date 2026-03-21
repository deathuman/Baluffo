"""Tests for thin wrapper functions and utility functions in admin_bridge."""

from unittest import mock

import pytest

from src import admin_bridge


class TestParseIso:
    """Tests for parse_iso utility function."""

    def test_parse_iso_returns_none_for_none(self):
        assert admin_bridge.parse_iso(None) is None

    def test_parse_iso_returns_none_for_empty_string(self):
        assert admin_bridge.parse_iso("") is None

    def test_parse_iso_returns_none_for_whitespace(self):
        assert admin_bridge.parse_iso("   ") is None

    def test_parse_iso_parses_iso_format(self):
        result = admin_bridge.parse_iso("2026-03-21T12:00:00")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 21
        assert result.hour == 12

    def test_parse_iso_parses_iso_format_with_timezone(self):
        result = admin_bridge.parse_iso("2026-03-21T12:00:00+00:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_parse_iso_parses_iso_format_with_z_suffix(self):
        result = admin_bridge.parse_iso("2026-03-21T12:00:00Z")
        assert result is not None
        assert result.tzinfo is not None

    def test_parse_iso_returns_none_for_invalid_format(self):
        assert admin_bridge.parse_iso("not-a-date") is None

    def test_parse_iso_returns_none_for_partial_date(self):
        assert admin_bridge.parse_iso("2026-03") is None


class TestPidIsRunning:
    """Tests for pid_is_running utility function."""

    def test_pid_is_running_returns_false_for_zero(self):
        assert admin_bridge.pid_is_running(0) is False

    def test_pid_is_running_returns_false_for_negative(self):
        assert admin_bridge.pid_is_running(-1) is False
        assert admin_bridge.pid_is_running(-100) is False

    def test_pid_is_running_returns_false_for_none(self):
        assert admin_bridge.pid_is_running(None) is False

    def test_pid_is_running_returns_false_for_none_as_int(self):
        assert admin_bridge.pid_is_running(int(None or 0)) is False


class TestThinWrappers:
    """Tests for thin wrapper functions that delegate to services."""

    def test_ensure_active_registry_returns_list(self, admin_bridge_entrypoint_root):
        result = admin_bridge.ensure_active_registry()
        assert isinstance(result, list)

    def test_normalize_state_returns_dict_with_buckets(self, admin_bridge_entrypoint_root):
        state = {"active": [], "pending": [], "rejected": []}
        result = admin_bridge.normalize_state(state)
        assert isinstance(result, dict)
        assert "active" in result
        assert "pending" in result
        assert "rejected" in result

    def test_load_state_returns_dict_with_buckets(self, admin_bridge_entrypoint_root):
        result = admin_bridge.load_state()
        assert isinstance(result, dict)
        assert "active" in result
        assert "pending" in result
        assert "rejected" in result

    def test_summarize_state_returns_counts(self, admin_bridge_entrypoint_root):
        state = {"active": [{}, {}], "pending": [{}], "rejected": []}
        result = admin_bridge.summarize_state(state)
        assert isinstance(result, dict)
        assert result["activeCount"] == 2
        assert result["pendingCount"] == 1
        assert result["rejectedCount"] == 0

    def test_persist_state_returns_normalized_state(self, admin_bridge_entrypoint_root):
        state = {"active": [], "pending": [], "rejected": []}
        result = admin_bridge.persist_state(state)
        assert isinstance(result, dict)

    def test_move_entries_returns_tuple(self, admin_bridge_entrypoint_root):
        row1 = {"name": "Test 1", "pages": ["https://test1.example.com"]}
        row1 = admin_bridge.ensure_source_id(row1)
        row2 = {"name": "Test 2", "pages": ["https://test2.example.com"]}
        row2 = admin_bridge.ensure_source_id(row2)
        pending = [row1, row2]
        selected_ids = [admin_bridge.source_identity(row1)]
        moved, remaining = admin_bridge.move_entries(pending, selected_ids)
        assert isinstance(moved, list)
        assert isinstance(remaining, list)
        assert len(moved) == 1
        assert len(remaining) == 1

    def test_build_manual_candidate_returns_none_for_empty_url(self, admin_bridge_entrypoint_root):
        result = admin_bridge.build_manual_candidate("")
        assert result is None

    def test_build_manual_candidate_returns_none_for_none_url(self, admin_bridge_entrypoint_root):
        result = admin_bridge.build_manual_candidate(None)
        assert result is None

    def test_build_manual_candidate_returns_dict_for_valid_url(self, admin_bridge_entrypoint_root):
        result = admin_bridge.build_manual_candidate("https://example.com/careers")
        assert result is not None
        assert isinstance(result, dict)
        assert "id" in result


class TestTaskState:
    """Tests for task state functions."""

    def test_clear_task_state_completes_without_error(self, admin_bridge_entrypoint_root):
        admin_bridge.clear_task_state("test_task")
        admin_bridge.wait_for_sync_tasks(timeout_s=1.0)

    def test_task_running_from_state_returns_false_for_unknown_task(self, admin_bridge_entrypoint_root):
        result = admin_bridge.task_running_from_state("nonexistent_task_type_xyz")
        assert result is False


class TestLogEnabled:
    """Tests for _log_enabled utility function."""

    def test_log_enabled_with_default_config(self, admin_bridge_entrypoint_root):
        result = admin_bridge._log_enabled("info")
        assert isinstance(result, bool)
