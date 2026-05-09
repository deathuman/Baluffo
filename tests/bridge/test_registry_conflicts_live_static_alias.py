from src.bridge.registry_conflicts import derive_registry_conflict_queue


def test_registry_conflicts_apply_live_counts_from_static_alias_probe() -> None:
    state = {
        "active": [
            {
                "id": "static:listing_url:https://studio.example/work-with-us",
                "name": "Studio (Sheet)",
                "studio": "Studio",
                "adapter": "static",
                "registryState": "active",
                "jobsFound": 8,
                "rankScore": 58,
            },
            {
                "id": "greenhouse:slug:studio",
                "name": "Studio (Greenhouse)",
                "studio": "Studio",
                "adapter": "greenhouse",
                "registryState": "active",
                "jobsFound": 6,
                "rankScore": 70,
            },
        ],
        "pending": [],
        "rejected": [],
    }
    adjudication = {
        "families": [
            {
                "familyKey": "studio",
                "status": "recommended_demotion",
                "winnerSourceId": "greenhouse:slug:studio",
                "probes": [
                    {
                        "sourceId": (
                            "static:listing_url:https://studio.example/work-with-us/index.html"
                        ),
                        "adapter": "static",
                        "endpointUrl": "https://studio.example/work-with-us/index.html",
                        "finalUrl": "https://studio.example/work-with-us/index.html",
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 6,
                    },
                    {
                        "sourceId": "greenhouse:slug:studio",
                        "adapter": "greenhouse",
                        "httpStatus": 200,
                        "ok": True,
                        "jobsFound": 6,
                    },
                ],
            }
        ]
    }

    source_state = {
        "sources": [
            {
                "sourceId": "static:listing_url:https://studio.example/work-with-us",
                "lastStatus": "ok",
            }
        ]
    }

    conflict = derive_registry_conflict_queue(
        state,
        source_state,
        adjudication_payload=adjudication,
    )["conflicts"][0]

    assert conflict["winner"]["id"] == "greenhouse:slug:studio"
    assert conflict["effectiveWinnerSource"] == "live_adjudication"
    assert conflict["liveAdjudicationComplete"] is True
    static_row = next(
        row for row in conflict["rows"] if row["id"].startswith("static:listing_url:")
    )
    assert static_row["registryJobsFound"] == 8
    assert static_row["liveJobsFound"] == 6
    assert static_row["jobsFound"] == 6
