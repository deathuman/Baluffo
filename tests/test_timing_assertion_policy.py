"""The wall-clock assertion gate must fire on the shape it exists for, and only that."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import tools.repo_health.timing_assertion_policy as policy
from tests.helpers.temp_paths import workspace_tmpdir
from tests.helpers.timing import forbid
from tools.repo_health import repo_guardrails

REPO_ROOT = Path(__file__).resolve().parents[1]


def _suite(tmp: Path, source: str, name: str = "test_sample.py") -> Path:
    tests_dir = tmp / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / name).write_text(source, encoding="utf-8")
    return tmp


def _baseline(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp / "timing_assertion_baseline.json"
    monkeypatch.setattr(policy, "baseline_path", lambda: path)
    return path


def test_an_elapsed_value_compared_to_a_literal_is_flagged() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def test_slow():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        "    assert elapsed < 1.0\n"
    )

    with workspace_tmpdir("timing-policy-upper") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert [(finding.path, finding.line) for finding in findings] == [("tests/test_sample.py", 7)]
    assert findings[0].source == "assert elapsed < 1.0"


def test_structural_assertions_are_not_flagged() -> None:
    source = (
        "from tests.helpers.timing import ScriptedClock\n"
        "import time\n"
        "\n"
        "\n"
        "def test_structural():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        "    clock = ScriptedClock()\n"
        "    assert elapsed >= 0.0\n"
        "    assert len([1]) == 1\n"
        "    assert clock.now - 1_000.0 == 0.0\n"
    )

    with workspace_tmpdir("timing-policy-structural") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert findings == []


def test_a_marked_deadline_is_skipped_and_a_bare_marker_is_not() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def test_marks():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        "    assert elapsed < 1.0  # timing-ok: half of a 2 s blocker\n"
        "    assert elapsed < 2.0  # timing-ok:\n"
        "    # timing-ok: 5 s against a 500 s hang\n"
        "    assert elapsed < 5.0\n"
    )

    with workspace_tmpdir("timing-policy-marker") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    # Inline and standalone-comment markers are honoured; the bare marker and the
    # neighbouring assert's marker are not.
    assert [finding.source for finding in findings] == ["assert elapsed < 2.0"]


def test_aliases_from_imports_and_chained_bounds_are_flagged() -> None:
    source = (
        "import time as _t\n"
        "from time import monotonic as clock\n"
        "\n"
        "\n"
        "def test_aliased():\n"
        "    begun = _t.perf_counter()\n"
        "    assert _t.perf_counter() - begun < 2.0\n"
        "    t0 = clock()\n"
        "    assert 0.0 <= clock() - t0 < 3\n"
    )

    with workspace_tmpdir("timing-policy-aliases") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert [finding.line for finding in findings] == [7, 9]
    assert "0.0 <= clock() - t0 < 3" in findings[1].source


def test_a_named_constant_bound_is_flagged() -> None:
    source = (
        "import time\n"
        "\n"
        "BUDGET_S = 5.0\n"
        "\n"
        "\n"
        "def test_ceiling():\n"
        "    started = time.perf_counter()\n"
        "    assert time.perf_counter() - started < BUDGET_S\n"
    )

    with workspace_tmpdir("timing-policy-constant") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert len(findings) == 1
    assert findings[0].line == 8


def test_a_payload_field_read_back_is_flagged_and_a_sibling_key_is_not() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def test_payload():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        '    payload = {"elapsed": elapsed, "source": "board", "count": 3}\n'
        '    assert payload["elapsed"] < 1.0\n'
        '    assert payload.get("elapsed", 0.0) < 1.0\n'
        '    assert payload["source"] < 2\n'
        '    assert payload["count"] == 3\n'
    )

    with workspace_tmpdir("timing-policy-payload") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert [finding.line for finding in findings] == [8, 9]


def test_written_payload_fields_carry_the_clock() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "class Report:\n"
        "    pass\n"
        "\n"
        "\n"
        "def test_fields():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        "    report = Report()\n"
        "    report.elapsed = elapsed\n"
        "    entry = {}\n"
        '    entry["elapsed"] = elapsed\n'
        "    entry.update(measured=elapsed)\n"
        '    entry.setdefault("settled", elapsed)\n'
        "    built = dict(elapsed=elapsed)\n"
        "    assert report.elapsed < 1.0\n"
        '    assert entry["elapsed"] < 1.0\n'
        '    assert entry["measured"] < 1.0\n'
        '    assert entry["settled"] < 1.0\n'
        '    assert built["elapsed"] < 1.0\n'
        '    assert entry["missing"] < 1.0\n'
    )

    with workspace_tmpdir("timing-policy-fields") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    # Every written field is followed; a key never written from a clock is not.
    assert [finding.line for finding in findings] == [18, 19, 20, 21, 22]


def test_an_average_or_reduction_of_clock_samples_is_flagged() -> None:
    source = (
        "import statistics\n"
        "import time\n"
        "\n"
        "\n"
        "def test_average():\n"
        "    samples = []\n"
        "    for _ in range(3):\n"
        "        started = time.perf_counter()\n"
        "        samples.append(time.perf_counter() - started)\n"
        "    average = sum(samples) / len(samples)\n"
        "    typical = statistics.mean(samples)\n"
        "    slowest = max(samples)\n"
        "    spread = samples.mean()\n"
        "    assert average < 1.0\n"
        "    assert typical < 1.0\n"
        "    assert slowest < 1.0\n"
        "    assert spread < 1.0\n"
        "    assert statistics.fmean([1.0, 2.0]) < 3.0\n"
    )

    with workspace_tmpdir("timing-policy-average") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert [finding.line for finding in findings] == [14, 15, 16, 17]


def test_a_helper_returning_a_duration_is_followed_to_the_assertion() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def measure(started):\n"
        "    return time.perf_counter() - started\n"
        "\n"
        "\n"
        "def count():\n"
        "    return 3\n"
        "\n"
        "\n"
        "def test_helper():\n"
        "    t0 = time.perf_counter()\n"
        "    assert measure(t0) < 1.0\n"
        "    assert measure(0.0) < 1.0\n"
        "    assert count() < 4\n"
        "    elapsed = measure(t0)\n"
        "    assert elapsed < 1.0\n"
    )

    with workspace_tmpdir("timing-policy-helper") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    # The helper is a clock only when its argument is; a helper returning a plain
    # number is not, so neither call's bound is a host measurement.
    assert [finding.line for finding in findings] == [14, 18]


def test_a_helper_returning_a_payload_field_is_followed_through_one_hop() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def measure(started):\n"
        "    return time.perf_counter() - started\n"
        "\n"
        "\n"
        "def measure_payload():\n"
        "    started = time.perf_counter()\n"
        '    return {"elapsed": time.perf_counter() - started, "count": 1}\n'
        "\n"
        "\n"
        "def chained():\n"
        "    started = time.perf_counter()\n"
        '    return {"elapsed": measure(started)}\n'
        "\n"
        "\n"
        "def test_payload():\n"
        "    payload = measure_payload()\n"
        '    assert payload["elapsed"] < 1.0\n'
        '    assert payload["count"] == 1\n'
        '    assert measure_payload()["elapsed"] < 1.0\n'
        "    hop = chained()\n"
        '    assert hop["elapsed"] < 1.0\n'
    )

    with workspace_tmpdir("timing-policy-helper-payload") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    # The returned key carries the clock, its sibling does not, and a payload whose
    # field came from another helper still resolves.
    assert [finding.line for finding in findings] == [20, 22, 24]


def test_unrelated_containers_and_field_names_are_not_flagged() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def test_unrelated():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        '    payload = {"elapsed": elapsed, "count": 3}\n'
        "    waits = [0.5, 1.5]\n"
        '    assert payload["count"] == 3\n'
        "    assert len(payload) < 3\n"
        "    assert sum(waits) < 3.0\n"
        "    assert elapsed >= 0.0\n"
    )

    with workspace_tmpdir("timing-policy-unrelated") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert findings == []


def test_a_marked_payload_deadline_is_skipped() -> None:
    source = (
        "import time\n"
        "\n"
        "\n"
        "def test_marked_field():\n"
        "    started = time.perf_counter()\n"
        "    elapsed = time.perf_counter() - started\n"
        '    payload = {"elapsed": elapsed}\n'
        '    assert payload["elapsed"] < 1.0  # timing-ok: half of a 2 s blocker\n'
        '    assert payload["elapsed"] < 2.0\n'
    )

    with workspace_tmpdir("timing-policy-marked-field") as tmp:
        findings = policy.collect_findings(_suite(tmp, source))

    assert [finding.line for finding in findings] == [9]


def test_a_new_finding_fails_and_a_removed_one_goes_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = (
        "import time\n"
        "\n"
        "\n"
        "def test_one():\n"
        "    started = time.perf_counter()\n"
        "    assert time.perf_counter() - started < 1.0\n"
    )
    second = first + (
        "\n"
        "\n"
        "def test_two():\n"
        "    started = time.perf_counter()\n"
        "    assert time.perf_counter() - started < 2.0\n"
    )

    with workspace_tmpdir("timing-policy-baseline") as tmp:
        _baseline(tmp, monkeypatch)
        _suite(tmp, first)
        assert policy.write_baseline(tmp) == 1
        assert policy.check_timing_assertions(tmp) == []
        assert policy.check_timing_assertion_stale_baseline(tmp) == []

        _suite(tmp, second)
        uncovered = policy.check_timing_assertions(tmp)
        assert len(uncovered) == 1
        assert "assert time.perf_counter() - started < 2.0" in uncovered[0]
        assert "tests/test_sample.py:11" in uncovered[0]

        _suite(tmp, second.split("def test_one", 1)[0])
        stale = policy.check_timing_assertion_stale_baseline(tmp)
        assert len(stale) == 1
        assert all("no longer detected" in message for message in stale)


def test_a_missing_baseline_is_reported_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("timing-policy-missing") as tmp:
        _baseline(tmp, monkeypatch)
        assert policy.check_timing_assertions(tmp) == [
            f"{policy.BASELINE_FILENAME} is missing or unusable; "
            "run `python tools/repo_health/timing_assertion_policy.py --update`"
        ]


def test_the_baseline_file_holds_no_debt() -> None:
    payload = json.loads(policy.baseline_path().read_text(encoding="utf-8"))
    assert payload["findings"] == [], (
        "every wall-clock bound in the suite carries `# timing-ok: <reason>`; a new "
        "entry here is undeclared debt"
    )


def test_precomputed_findings_feed_both_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(policy, "collect_findings", forbid("a second suite scan"))

    assert policy.check_timing_assertions(REPO_ROOT, []) == []
    assert policy.check_timing_assertion_stale_baseline(REPO_ROOT, []) == []


def test_the_group_is_wired_into_repo_guardrails() -> None:
    assert "timing" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["timing"] is repo_guardrails.run_timing_group


def test_the_real_suite_has_no_unbaselined_wall_clock_assertions() -> None:
    findings = policy.collect_findings(REPO_ROOT)
    assert policy.check_timing_assertions(REPO_ROOT, findings) == []
    assert policy.check_timing_assertion_stale_baseline(REPO_ROOT, findings) == []
