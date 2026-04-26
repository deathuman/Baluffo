import json
from pathlib import Path

from src import source_registry as sr

REPO_ROOT = Path(__file__).resolve().parents[1]

UNSUPPORTED_STATIC_IDS = {
    "static:listing_url:https://www.ycombinator.com/companies/gym-class-by-irl-studios/jobs",
    "static:listing_url:http://jobs.andarion-games.com",
    "static:listing_url:http://www.reply.com/careers",
    "static:listing_url:https://bkomstudios.zohorecruit.com/jobs/careers",
    "static:listing_url:https://careers.playsimple.in/jobs/careers",
    "static:listing_url:https://poncle1.homerun.co/?lang=en",
    "static:listing_url:https://www.linkedin.com/company/1047games/jobs/",
    "static:listing_url:https://www.linkedin.com/company/high-5-games/jobs/",
    "static:listing_url:https://www.linkedin.com/company/ironbelly-studios/",
    "static:listing_url:https://www.linkedin.com/company/netease-games/jobs/",
    "static:listing_url:https://www.linkedin.com/company/nexus-studios/",
    "static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega",
    "static:listing_url:https://www.linkedin.com/posts/wsalbright_were-hiring-combat-waffle-studios-is-activity-7348670199409074177-hlro",
}


def test_demote_duplicate_active_variants_keeps_best_family_winner() -> None:
    active = [
        {
            "id": "static:scopely",
            "name": "Scopely (Sheet)",
            "studio": "Scopely",
            "adapter": "static",
            "careersUrl": "https://www.scopely.com/en/careers",
            "registryState": "active",
            "candidateState": "live",
            "rankScore": 50,
        },
        {
            "id": "greenhouse:scopely",
            "name": "Scopely (Greenhouse)",
            "studio": "Scopely",
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/scopely/jobs?content=true",
            "registryState": "active",
            "candidateState": "live",
            "rankScore": 1,
        },
    ]

    next_active, demoted = sr.demote_duplicate_active_variants(
        active,
        target_families=["Scopely"],
        source_state={"Scopely (Greenhouse)": {"lastStatus": "ok", "lastKeptCount": 8}},
        at="2026-04-26T00:00:00Z",
    )

    assert [row["name"] for row in next_active] == ["Scopely (Greenhouse)"]
    assert [row["name"] for row in demoted] == ["Scopely (Sheet)"]
    assert demoted[0]["registryState"] == "pending"
    assert demoted[0]["candidateState"] == "hidden"
    assert demoted[0]["hiddenFromDefault"] is True
    assert demoted[0]["pendingReason"] == sr.REGISTRY_REASON_DUPLICATE_FAMILY
    assert demoted[0]["duplicateOfSourceId"] == "greenhouse:scopely"


def test_hide_repeated_zero_job_pending_rows_after_threshold() -> None:
    visible = sr.hide_repeated_zero_job_pending(
        {
            "id": "candidate:visible",
            "name": "Visible Pending",
            "adapter": "static",
            "registryState": "pending",
            "candidateState": "validated",
            "jobsFound": 0,
            "deferCount": 2,
        },
        at="2026-04-26T00:00:00Z",
    )
    hidden = sr.hide_repeated_zero_job_pending(
        {
            "id": "candidate:hidden",
            "name": "Hidden Pending",
            "adapter": "static",
            "registryState": "pending",
            "candidateState": "validated",
            "jobsFound": 0,
            "deferCount": 3,
        },
        at="2026-04-26T00:00:00Z",
    )

    assert visible["candidateState"] == "validated"
    assert "hiddenFromDefault" not in visible
    assert hidden["candidateState"] == "hidden"
    assert hidden["hiddenFromDefault"] is True
    assert hidden["pendingReason"] == sr.REGISTRY_REASON_REPEATED_ZERO_JOBS


def test_static_residual_cleanup_preserves_rows_outside_active_defaults() -> None:
    active = json.loads((REPO_ROOT / "data/source-registry-active.json").read_text())
    pending = json.loads((REPO_ROOT / "data/source-registry-pending.json").read_text())

    active_by_id = {row["id"]: row for row in active}
    pending_by_id = {row["id"]: row for row in pending}

    assert "static:listing_url:https://lucky-vr.breezy.hr/" not in active_by_id
    assert active_by_id["breezy:board_url:https://lucky-vr.breezy.hr/"]["adapter"] == "breezy"
    assert (
        active_by_id["breezy:board_url:https://lucky-vr.breezy.hr/"]["board_url"]
        == "https://lucky-vr.breezy.hr/"
    )

    assert UNSUPPORTED_STATIC_IDS.isdisjoint(active_by_id)
    assert UNSUPPORTED_STATIC_IDS <= pending_by_id.keys()
    for source_id in UNSUPPORTED_STATIC_IDS:
        row = pending_by_id[source_id]
        assert row["registryState"] == "pending"
        assert row["candidateState"] == "hidden"
        assert row["hiddenFromDefault"] is True
        assert row["enabledByDefault"] is False
        assert row["pendingReason"] == "unsupported_static_source"
        assert row["unsupportedStaticReason"]
