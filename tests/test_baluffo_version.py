from src.baluffo_version import compare_baluffo_versions, parse_baluffo_version


def test_parse_baluffo_version_splits_patch_major_and_increment() -> None:
    assert parse_baluffo_version("0.1.3") == (0, 1, 3, 0, 1)
    assert parse_baluffo_version("0.1.23") == (0, 1, 2, 3, 2)
    assert parse_baluffo_version("0.1.30") == (0, 1, 3, 0, 2)


def test_compare_baluffo_versions_uses_custom_0_1_x_ordering() -> None:
    assert compare_baluffo_versions("0.1.3", "0.1.23") > 0
    assert compare_baluffo_versions("0.1.3", "0.1.24") > 0
    assert compare_baluffo_versions("0.1.3", "0.1.29") > 0
    assert compare_baluffo_versions("0.1.4", "0.1.39") > 0
    assert compare_baluffo_versions("0.1.23", "0.1.22") > 0
    assert compare_baluffo_versions("0.1.30", "0.1.29") > 0


def test_compare_baluffo_versions_falls_back_safely_for_malformed_values() -> None:
    assert compare_baluffo_versions("release-candidate", "0.1.3") > 0
    assert compare_baluffo_versions("0.1.3", "release-candidate") < 0
