from src import source_registry as sr


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
