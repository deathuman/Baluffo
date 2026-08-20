"""Regression tests for the split-fidelity checker.

Covers the four failure/success categories a split verification must report —
MISSING (a unit vanished), DIFF (a unit changed bytes), AMBIGUOUS (a unit
appears in two leaves), EXTRA (informational) — plus CRLF normalization,
decorator ownership (single and stacked), UTF-8 content, the trailing
``__all__`` fold rule, and the ``--snapshot`` CLI path for untracked originals.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

from tools.repo_health.bin import verify_split_fidelity as vf

# Fixture source modeled on a real pre-split module: functions, a class, and an
# annotated constant, so `units()` must extract all three kinds.
ORIGINAL = """\
def first(row):
    return True


class Helper:
    pass


def second():
    return 1


TRIAGE_CONST: tuple = (
    {"name": "a"},
)


def third():
    return 2
"""

# One leaf per unit — the byte-exact post-split shape.
LEAVES = [
    "def first(row):\n    return True\n",
    "class Helper:\n    pass\n",
    "def second():\n    return 1\n",
    'TRIAGE_CONST: tuple = (\n    {"name": "a"},\n)',
    "def third():\n    return 2\n",
]


def _write(tmp_path: pathlib.Path, contents: list[str]) -> list[str]:
    paths = []
    for idx, content in enumerate(contents):
        path = tmp_path / f"leaf_{idx}.py"
        path.write_text(content, encoding="utf-8")
        paths.append(str(path))
    return paths


def test_units_extracts_defs_classes_and_constants() -> None:
    parsed = vf.units(ORIGINAL)
    by_name = {name: kind for name, kind, _body in parsed}

    assert by_name == {
        "first": "def/class",
        "Helper": "def/class",
        "second": "def/class",
        "TRIAGE_CONST": "const",
        "third": "def/class",
    }


def test_verify_all_units_byte_identical_across_leaves(tmp_path) -> None:
    report = vf.verify(ORIGINAL, _write(tmp_path, LEAVES))

    assert report["ok"] is True
    assert report["missing"] == []
    assert report["diffs"] == []
    assert report["ambiguous"] == []
    assert len(report["owned_by"]) == 5


def test_verify_missing_unit_is_reported(tmp_path) -> None:
    # Leaves omit `second` (index 2) entirely; the other four survive.
    report = vf.verify(ORIGINAL, _write(tmp_path, [LEAVES[0], LEAVES[1], LEAVES[3], LEAVES[4]]))

    assert report["ok"] is False
    assert report["missing"] == ["second"]


def test_verify_changed_unit_is_a_diff(tmp_path) -> None:
    drifted = ["def second():\n    return 999\n"]
    report = vf.verify(ORIGINAL, _write(tmp_path, [LEAVES[0], drifted[0], LEAVES[4]]))

    assert report["ok"] is False
    assert [name for name, _leaf, _ref, _cur in report["diffs"]] == ["second"]


def test_verify_same_unit_in_two_leaves_is_ambiguous(tmp_path) -> None:
    # `second` appears again in another leaf: ownership is not unique.
    report = vf.verify(ORIGINAL, _write(tmp_path, [LEAVES[0], LEAVES[2], LEAVES[2], LEAVES[4]]))

    assert report["ok"] is False
    assert [name for name, _matches in report["ambiguous"]] == ["second"]


def test_verify_extra_units_are_informational_only(tmp_path) -> None:
    # A helper the original never had is allowed to exist in a leaf; it is
    # reported as EXTRA but must not fail the check by itself.
    with_extra = LEAVES + ["def new_helper():\n    pass\n"]
    report = vf.verify(ORIGINAL, _write(tmp_path, with_extra))

    assert report["ok"] is True
    assert [name for name, _leaf in report["extra"]] == ["new_helper"]


def test_verify_normalizes_crlf_working_trees(tmp_path) -> None:
    # Original as git stores it (LF); leaves as a CRLF Windows working tree.
    # Write CRLF bytes explicitly so the OS text-mode newline translation
    # (\n -> \r\n on Windows) cannot double up the \r.
    original_crlf = ORIGINAL.replace("\n", "\r\n")
    paths = _write(tmp_path, LEAVES)
    for path in paths:
        text = pathlib.Path(path).read_text(encoding="utf-8")
        pathlib.Path(path).write_bytes(text.replace("\n", "\r\n").encode("utf-8"))

    report = vf.verify(original_crlf, paths)

    assert report["ok"] is True
    assert report["diffs"] == []


def test_verify_ignores_trailing_blank_line_differences(tmp_path) -> None:
    padded = LEAVES.copy()
    padded[4] += "\n\n"  # extra trailing blank lines at end of the last leaf
    report = vf.verify(ORIGINAL, _write(tmp_path, padded))

    assert report["ok"] is True


def test_verify_decorators_belong_to_the_decorated_unit(tmp_path) -> None:
    # A decorator line before a class must not leak into the preceding unit's
    # slice: `before` is byte-identical even though the next unit is decorated.
    original = (
        "def before():\n    return 0\n\n\n"
        "@dataclass(frozen=True)\nclass Decorated:\n    value: int = 0\n\n\n"
        "def after():\n    return 1\n"
    )
    leaves = [
        "def before():\n    return 0\n",
        "@dataclass(frozen=True)\nclass Decorated:\n    value: int = 0\n",
        "def after():\n    return 1\n",
    ]

    report = vf.verify(original, _write(tmp_path, leaves))

    assert report["ok"] is True
    assert report["diffs"] == []
    assert [name for name, _leaf in report["extra"]] == []


def test_verify_stacked_decorators_attach_to_one_unit(tmp_path) -> None:
    # Two decorators stacked above a class all belong to that class, so the
    # preceding unit's slice stops at the first `@`.
    original = "def before():\n    return 0\n\n\n@one\n@two\nclass Stacked:\n    pass\n"
    leaves = [
        "def before():\n    return 0\n",
        "@one\n@two\nclass Stacked:\n    pass\n",
    ]

    report = vf.verify(original, _write(tmp_path, leaves))

    assert report["ok"] is True
    assert report["diffs"] == []


# ---- trailing __all__ fold rule (function-module splits) ----

# The unit regex never matches ``__all__`` (lowercase after the underscore), so
# a trailing ``__all__`` block is folded into the LAST unit's body (observed on
# registry_conflict_adjudication.py): the run leaf carries the verbatim tail,
# and a coordinator's own ``__all__`` sits before its first matched unit.
ALL_TAIL_ORIGINAL = (
    "def first(row):\n    return True\n\n\n"
    "def third():\n    return 2\n\n\n"
    '__all__ = [\n    "first",\n    "third",\n]\n'
)
ALL_TAIL = '\n\n__all__ = [\n    "first",\n    "third",\n]\n'


def test_verify_trailing_all_folds_into_last_unit(tmp_path) -> None:
    # Coordinator's own __all__ before its first unit is invisible; the last
    # unit carries the verbatim tail (folded into its body by the checker).
    leaves = [
        ALL_TAIL + "def first(row):\n    return True\n",
        "def third():\n    return 2\n" + ALL_TAIL,
    ]

    report = vf.verify(ALL_TAIL_ORIGINAL, _write(tmp_path, leaves))

    assert report["ok"] is True
    assert report["diffs"] == [] and report["missing"] == []
    assert report["ambiguous"] == [] and report["extra"] == []
    assert sorted(report["owned_by"]) == ["first", "third"]


def test_verify_trailing_all_after_wrong_unit_is_a_diff(tmp_path) -> None:
    # The live failure mode: the tail is detached from the last unit, so
    # `third` loses its folded tail and `first` absorbs the tail — both DIFF.
    leaves = [
        "def third():\n    return 2\n",
        "def first(row):\n    return True\n" + ALL_TAIL,
    ]

    report = vf.verify(ALL_TAIL_ORIGINAL, _write(tmp_path, leaves))

    assert report["ok"] is False
    assert sorted(name for name, _leaf, _ref, _cur in report["diffs"]) == ["first", "third"]


def test_verify_utf8_content_is_byte_identical(tmp_path) -> None:
    # Non-ASCII bytes (em dash, accented text) in a body must hash identically;
    # a mojibake decode of the reference would show up as a spurious DIFF.
    original = 'def greet(name: str) -> str:\n    # A real comment — with an em dash and café.\n    return f"Hello, {name}!"\n'

    report = vf.verify(original, _write(tmp_path, [original]))

    assert report["ok"] is True
    assert report["diffs"] == []


def test_git_show_head_decodes_utf8(monkeypatch) -> None:
    # `_git_show_head` must decode `git show` output as UTF-8; a locale-encoding
    # decode (cp1252 on Windows) would mojibake non-ASCII originals and produce
    # false DIFFs (observed against static_listing.py, 177 non-ASCII bytes).
    captured: dict = {}

    class FakeCompleted:
        returncode = 0
        stdout = 'def f():\n    return "café — ok"\n'
        stderr = ""

    def fake_run(cmd, **kwargs):
        captured["encoding"] = kwargs.get("encoding")
        return FakeCompleted()

    monkeypatch.setattr(vf.subprocess, "run", fake_run)

    text = vf._git_show_head("src/foo.py")

    assert captured["encoding"] == "utf-8"
    assert "café — ok" in text


# ---- class-methods mode (mixin splits) ----

CLASS_ORIGINAL = """\
class Service:
    def run(self) -> str:
        return self._name()

    def _name(self) -> str:
        return "svc"
"""

# Coordinator (keeps __init__ only) + two mixin leaves + a state base with a
# `raise NotImplementedError` stub for the cross-mixin call.
CLASS_LEAVES = [
    "class Service:\n    def __init__(self) -> None:\n        pass\n",
    (
        "class ServiceState:\n"
        "    def _name(self) -> str:\n"
        "        raise NotImplementedError\n"
        "\n\n"
        "class ServiceCoreMixin(ServiceState):\n"
        "    def run(self) -> str:\n"
        "        return self._name()\n"
    ),
    'class ServiceNameMixin(ServiceState):\n    def _name(self) -> str:\n        return "svc"\n',
]


def _write_files(tmp_path: pathlib.Path, contents: list[str]) -> list[str]:
    paths = []
    for idx, content in enumerate(contents):
        path = tmp_path / f"leaf_{idx}.py"
        path.write_text(content, encoding="utf-8")
        paths.append(str(path))
    return paths


def test_class_methods_mode_all_byte_identical(tmp_path) -> None:
    report = vf.verify(
        CLASS_ORIGINAL, _write_files(tmp_path, CLASS_LEAVES), class_methods="Service"
    )

    assert report["ok"] is True
    assert report["class_methods"]["missing"] == []
    assert report["class_methods"]["diffs"] == []
    assert report["class_methods"]["ambiguous"] == []
    assert len(report["class_methods"]["owned_by"]) == 2


def test_class_methods_mode_stub_is_not_ambiguous(tmp_path) -> None:
    # The state base declares `_name` with a raise-stub; the real body lives in
    # the name mixin. Stubs must not count as a second implementation.
    report = vf.verify(
        CLASS_ORIGINAL, _write_files(tmp_path, CLASS_LEAVES), class_methods="Service"
    )

    assert report["ok"] is True
    assert report["class_methods"]["ambiguous"] == []


def test_class_methods_mode_missing_method_fails(tmp_path) -> None:
    leaves = [CLASS_LEAVES[0], CLASS_LEAVES[1]]  # `_name` mixin dropped
    report = vf.verify(CLASS_ORIGINAL, _write_files(tmp_path, leaves), class_methods="Service")

    assert report["ok"] is False
    assert report["class_methods"]["missing"] == ["_name"]


def test_class_methods_mode_diff_method_fails(tmp_path) -> None:
    drifted = CLASS_LEAVES.copy()
    drifted[2] = (
        'class ServiceNameMixin(ServiceState):\n    def _name(self) -> str:\n        return "other"\n'
    )
    report = vf.verify(CLASS_ORIGINAL, _write_files(tmp_path, drifted), class_methods="Service")

    assert report["ok"] is False
    assert [name for name, _leaf, _ref, _cur in report["class_methods"]["diffs"]] == ["_name"]


def test_cli_snapshot_path_exits_zero_on_complete_split(tmp_path, monkeypatch) -> None:
    original = tmp_path / "original.py"
    original.write_text(ORIGINAL, encoding="utf-8")
    leaf = tmp_path / "leaf.py"
    # A complete single-leaf split of the original (all units in one file).
    leaf.write_text("\n\n\n".join(LEAVES), encoding="utf-8")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_split_fidelity.py",
            "original.py",
            "--snapshot",
            str(original),
            "--leaves",
            str(leaf),
        ],
    )
    with pytest.raises(SystemExit) as excinfo:
        vf.main()
    assert excinfo.value.code == 0


def test_cli_snapshot_path_exits_one_on_missing(tmp_path, monkeypatch) -> None:
    original = tmp_path / "original.py"
    original.write_text(ORIGINAL, encoding="utf-8")
    leaf = tmp_path / "leaf.py"
    leaf.write_text(LEAVES[0], encoding="utf-8")  # only `first` survived

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_split_fidelity.py",
            "original.py",
            "--snapshot",
            str(original),
            "--leaves",
            str(leaf),
        ],
    )
    with pytest.raises(SystemExit) as excinfo:
        vf.main()
    assert excinfo.value.code == 1
