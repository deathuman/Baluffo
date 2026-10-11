"""What a replay's differing rows are compared against: the record's own declaration, per host.

The measured case is the LinkedIn company sheets. The static lane fetches one shared browse page
once per consuming source, LinkedIn renders per-fetch ``refId``/``trackingId`` tokens into the job
card links, and the store keeps one response per request key -- so a replay builds one consumer's
rows and not the others'. On the 2026-10-10 subset the record kept 2,973 rows and the replay 2,493,
and every one of the 1,314 differing rows belonged to a host that epoch's conflict audit declared.

Three properties are pinned here rather than described. **The residue is read, never inferred**: the
record's rows (through ``EPOCH.json.recordedRunDir``) and the replay's are compared by job identity,
each differing row is attributed to the host of the source that carried it, and the bind needs no
verdict to be true. **The bind is refused the moment one differing source falls outside the declared
hosts** -- as it is for a differing row naming no source host, for an epoch whose record rows are
gone, and for an epoch that declares nothing: the conservative direction every time, because a bound
drift is a pass and a pass must not be reachable by accident. And **the declaration is read from the
document that carries it**: the full per-host report when it is there, the manifest's top-five note
when it is not, labelled as the partial list it is.

The gate's own behaviour -- the verdict name, the receipt and manifest shapes, the birth decision and
the study's pair comparison -- is in ``test_corpus_binding_gate.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.helpers.corpus_binding_fixtures import (  # noqa: E402
    BOARDS,
    LINKEDIN,
    SHEET,
    declare,
    diverging_pair,
    epoch_dir,
    row,
    run_tree,
)
from tools.measurements.corpus import binding, conflicts  # noqa: E402
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus.store import CorpusStore  # noqa: E402


def test_the_declaration_is_read_per_host_from_the_conflict_report(tmp_path: Path) -> None:
    """The report the audit itself writes is the complete list, and it is read as written."""

    store = CorpusStore(tmp_path / "corpus")
    first = store.record(
        url=f"https://{LINKEDIN}/company/studio/jobs/",
        outcome="ok",
        text="render one",
        status_code=200,
        surface="fetch_text",
    )
    store.record(
        url=f"https://{LINKEDIN}/company/studio/jobs/",
        outcome="ok",
        text="render two",
        status_code=200,
        surface="fetch_text",
    )
    assert first["bodySha256"]
    conflicts.write_report(
        conflicts.audit(corpus_dir=store.root), store.root / conflicts.REPORT_NAME
    )

    declaration = binding.declared(store.root)
    assert declaration["hosts"] == [LINKEDIN]
    assert declaration["source"] == "report" and declaration["complete"] is True
    assert declaration["document"] == conflicts.REPORT_NAME


def test_a_missing_report_falls_back_to_the_note_and_labels_the_list_partial(
    tmp_path: Path,
) -> None:
    """An epoch whose report is gone still declares, from the manifest's note -- labelled, because
    that note names the top hosts only, so a residue on an unnamed host is refused."""

    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-160000", record_run=None)
    manifest = epoch_mod.read_epoch(epoch)
    manifest["deltaReferee"]["notes"] = [
        {
            "reason": conflicts.NOTE_REASON,
            "hosts": [LINKEDIN],
            "hostCount": 9,
            "replacedKeys": 73,
            "orphanedBodies": 36,
            "detail": "declared",
        }
    ]
    epoch_mod.write_epoch(epoch, manifest)

    declaration = binding.declared(epoch)
    assert declaration["hosts"] == [LINKEDIN]
    assert declaration["hostCount"] == 9
    assert declaration["source"] == "note" and declaration["complete"] is False
    assert declaration["document"] == epoch_mod.EPOCH_FILE


def test_source_hosts_read_every_source_that_carried_a_row() -> None:
    """The winning source and every bundle entry are read; a row with neither falls back to its
    link, and a row with no host anywhere attributes to nothing rather than to silence."""

    carried = row("https://linkedin.com/job/1", bundles=(SHEET, BOARDS))
    assert binding.source_hosts(carried) == (LINKEDIN, "boards.example.com")

    linkless = {"company": "Studio", "title": "Artist", "jobLink": "https://sg.linkedin.com/job/2"}
    assert binding.source_hosts(linkless) == ("sg.linkedin.com",)

    assert binding.source_hosts({"company": "Studio", "title": "Artist"}) == ()


def test_a_residue_inside_the_declared_hosts_is_bound(tmp_path: Path) -> None:
    """The whole point: record-only, replay-only and moved-field rows on a declared host bind, and
    the block says what was accepted -- which hosts, how much, and which fields moved."""

    epoch, replay_run = diverging_pair(tmp_path, declared=(LINKEDIN, "koeitecmo.vn"))
    block = binding.bind(epoch_dir=epoch, run_dir=replay_run)

    assert block["bound"] is True
    assert LINKEDIN in block["reason"] and "3 differing row(s)" in block["reason"]
    assert block["declared"]["source"] == "report" and block["declared"]["hostCount"] == 2

    analysis = block["residue"]
    assert analysis["recordRows"] == 3 and analysis["replayRows"] == 3
    assert analysis["recordOnly"] == 1 and analysis["replayOnly"] == 1
    assert analysis["drifted"] == 1 and analysis["rows"] == 3
    assert analysis["attributedRows"] == 3
    assert analysis["fields"] == {"company": 1}
    assert analysis["unattributed"] == 0 and analysis["duplicateIdentities"] == 0
    assert analysis["hosts"] == [{"host": LINKEDIN, "recordOnly": 1, "replayOnly": 1, "drifted": 1}]
    assert analysis["outside"] == []

    compact = binding.summary(block)
    assert compact["bound"] is True
    assert compact["declaredHostsSource"] == "report" and compact["declaredHostCount"] == 2
    assert compact["residueHosts"] == [LINKEDIN] and compact["residueHostCount"] == 1
    assert compact["residueRows"] == 3 and compact["outsideHosts"] == []
    assert compact["recordOnly"] == 1 and compact["replayOnly"] == 1 and compact["drifted"] == 1


def test_one_differing_source_outside_the_declared_hosts_refuses(tmp_path: Path) -> None:
    """A single host the record never declared is enough: the residue is then not the class the
    record accounted for, and the refusal names the host so the finding is actionable."""

    epoch, replay_run = diverging_pair(tmp_path, declared=("koeitecmo.vn",))
    block = binding.bind(epoch_dir=epoch, run_dir=replay_run)

    assert block["bound"] is False
    assert LINKEDIN in block["reason"] and "did not declare" in block["reason"]
    assert block["declared"]["hosts"] == ["koeitecmo.vn"]
    assert binding.summary(block)["outsideHosts"] == [LINKEDIN]
    assert block["residue"]["hosts"] == [
        {"host": LINKEDIN, "recordOnly": 1, "replayOnly": 1, "drifted": 1}
    ]


def test_a_row_carried_by_an_undeclared_source_refuses_even_when_its_winner_is_declared(
    tmp_path: Path,
) -> None:
    """A row can be published by several sources. The merge picks one, but the row can differ
    because of any of them, so a carrier outside the declared set refuses the bind."""

    record_rows = [
        row("https://linkedin.com/job/1"),
        row("https://linkedin.com/job/2", bundles=(SHEET,)),
    ]
    replay_rows = [row("https://linkedin.com/job/1")]
    record_run = run_tree(tmp_path / "record", record_rows)
    replay_run = run_tree(tmp_path / "replay", replay_rows)
    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-150000", record_run=record_run)
    declare(epoch, LINKEDIN)

    assert binding.bind(epoch_dir=epoch, run_dir=replay_run)["bound"] is True

    # The same residue, but the dropped row was also carried by a board the record never declared.
    record_rows[1]["sourceBundle"].append(
        {
            "source": BOARDS,
            "sourceJobId": "static:Studio-2",
            "jobLink": "https://linkedin.com/job/2",
        }
    )
    run_tree(tmp_path / "record", record_rows)
    refused = binding.bind(epoch_dir=epoch, run_dir=replay_run)

    assert refused["bound"] is False
    # Exact, not a substring: the host may only be named by the wording that refuses the bind,
    # and a substring check on a host reads to CodeQL as URL sanitisation (it flagged it).
    assert refused["reason"] == (
        "differing rows belong to 1 host(s) the record did not declare: boards.example.com"
    )
    assert binding.summary(refused)["outsideHosts"] == ["boards.example.com"]


def test_a_differing_row_with_no_host_at_all_is_refused(tmp_path: Path) -> None:
    """A row that names no source host is outside the declared set by construction -- it cannot be
    attributed to the class, so it is not bound to it."""

    record_rows = [row("https://linkedin.com/job/1"), {"company": "Studio", "title": "Ghost"}]
    record_run = run_tree(tmp_path / "record", record_rows)
    replay_run = run_tree(tmp_path / "replay", [row("https://linkedin.com/job/1")])
    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-140000", record_run=record_run)
    declare(epoch, LINKEDIN)

    block = binding.bind(epoch_dir=epoch, run_dir=replay_run)
    assert block["bound"] is False
    assert "name no source host" in block["reason"]
    assert block["residue"]["unattributed"] == 1 and block["residue"]["rows"] == 1


def test_an_epoch_whose_record_rows_are_gone_cannot_be_bound(tmp_path: Path) -> None:
    """An unreadable residue is not an empty one: the bind refuses rather than reading a missing
    artifact as a clean comparison -- and names which side could not be read."""

    _, replay_run = diverging_pair(tmp_path, declared=(LINKEDIN,))
    gone = epoch_dir(
        tmp_path, "corpus-subset-20261010-130000", record_run=tmp_path / "cleaned-away"
    )
    declare(gone, LINKEDIN)
    block = binding.bind(epoch_dir=gone, run_dir=replay_run)
    assert block["bound"] is False
    assert "no longer on disk" in block["reason"]

    record_run = run_tree(tmp_path / "record", [row("https://linkedin.com/job/1")])
    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-120000", record_run=record_run)
    declare(epoch, LINKEDIN)
    empty = binding.bind(epoch_dir=epoch, run_dir=tmp_path / "no-rows")
    assert empty["bound"] is False
    assert "the replay wrote no rows file" in empty["reason"]


def test_an_epoch_that_declares_nothing_refuses(tmp_path: Path) -> None:
    """No declaration, nothing to bind to -- the drift is the plain divergence it always was."""

    _, replay_run = diverging_pair(tmp_path, declared=())
    clean = epoch_dir(tmp_path, "corpus-subset-20261010-110000", record_run=None)
    declare(clean)
    block = binding.bind(epoch_dir=clean, run_dir=replay_run)
    assert block["bound"] is False
    assert "no conflict host" in block["reason"]
    assert block["residue"] == {}
    assert block["declared"]["source"] == "none"


def test_duplicate_identities_refuse_because_the_residue_is_an_under_count(
    tmp_path: Path,
) -> None:
    """Two rows for one job make every count here an under-count, which is where a divergence on an
    undeclared host could hide -- so the bind refuses instead of certifying an inexact residue."""

    record_rows = [row("https://linkedin.com/job/1"), row("https://linkedin.com/job/1")]
    record_run = run_tree(tmp_path / "record", record_rows)
    replay_run = run_tree(tmp_path / "replay", [row("https://linkedin.com/job/1")])
    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-100000", record_run=record_run)
    declare(epoch, LINKEDIN)

    block = binding.bind(epoch_dir=epoch, run_dir=replay_run)
    assert block["bound"] is False
    assert "share a job identity" in block["reason"]
    assert block["residue"]["duplicateIdentities"] == 1


def test_an_unnamed_epoch_cannot_be_bound() -> None:
    """A caller with no epoch has no declaration to read, and says so rather than binding to
    nothing -- which is the shape a plain divergence verdict comes back in."""

    block = binding.bind(epoch_dir=None, run_dir="/tmp/run")
    assert block["bound"] is False
    assert "no epoch was named" in block["reason"]
    assert binding.summary(block)["bound"] is False
