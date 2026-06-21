"""Tests for jobs fetcher quality work-type normalization."""


def test_normalize_work_type_derives_remote_from_title_when_field_empty() -> None:
    from src.jobs.normalizers import normalize_work_type

    assert normalize_work_type("", "Technical Artist (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Gameplay Programmer (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Senior Engineer - Remote") == "Remote"
    assert normalize_work_type("", "Ui Programmer (Remote)") == "Remote"
    assert normalize_work_type("", "Ai Programmer (Malta/Remote)") == "Remote"

    assert normalize_work_type("", "Senior Engineer - Onsite") == "Onsite"
    assert normalize_work_type("", "Office Assistant (Malta)") == "Onsite"
    assert normalize_work_type("", "Project Manager (Malta)") == "Onsite"

    assert normalize_work_type("Remote", "Some Onsite Job") == "Remote"
    assert normalize_work_type("Hybrid", "Onsite Engineer") == "Hybrid"
    assert normalize_work_type("", "Engineer - Hybrid") == "Hybrid"
    assert normalize_work_type("", "Mixed Mode Artist") == "Hybrid"
