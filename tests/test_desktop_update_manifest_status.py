"""Tests for desktop update manifest status behavior."""

from tests.helpers.desktop_update_leaf_namespace import du


def test_manifest_to_status_marks_0_1_3_available_over_0_1_23() -> None:
    status = du._manifest_to_status(
        current_version="0.1.23",
        manifest={
            "version": "0.1.3",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.23"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.3"


def test_manifest_to_status_marks_matching_0_1_3_up_to_date() -> None:
    status = du._manifest_to_status(
        current_version="0.1.3",
        manifest={
            "version": "0.1.3",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.3"),
    )

    assert status["availability"] == "up_to_date"
    assert status["updateAvailable"] is False


def test_manifest_to_status_marks_0_1_31_available_over_0_1_23() -> None:
    status = du._manifest_to_status(
        current_version="0.1.23",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.23"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.31"


def test_manifest_to_status_marks_0_1_31_available_over_0_1_3() -> None:
    status = du._manifest_to_status(
        current_version="0.1.3",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.3"),
    )

    assert status["availability"] == "available"
    assert status["updateAvailable"] is True
    assert status["latestVersion"] == "0.1.31"


def test_manifest_to_status_marks_matching_0_1_31_up_to_date() -> None:
    status = du._manifest_to_status(
        current_version="0.1.31",
        manifest={
            "version": "0.1.31",
            "channel": du.DESKTOP_UPDATE_CHANNEL,
            "min_desktop_updater_version": "1.0.0",
            "min_supported_current_version": "0.1.0",
        },
        existing=du.default_status_payload(current_version="0.1.31"),
    )

    assert status["availability"] == "up_to_date"
    assert status["updateAvailable"] is False
