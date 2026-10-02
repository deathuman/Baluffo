"""Voodoo's discovery seed after its careers board moved from Lever to Ashby.

Voodoo's Lever board was deleted -- both ``api.lever.co/v0/postings/voodoo`` and
``jobs.lever.co/voodoo`` return 404 -- while ``jobs.ashbyhq.com/voodoo`` serves
120 live openings. Discovery could never notice, because Voodoo was pinned to
``lever`` in two independent seed surfaces and ``likely_providers_for_seed``
treats an explicit list as exhaustive. These tests pin both surfaces.

The registration side of this move is covered by ``tests/test_ashby_registry_refresh.py``.
"""

import json

from ._helpers import (
    likely_providers_for_seed,
    provider_reinforcement_score,
    sd,
    stage_curated_seed_candidates,
)


def test_voodoo_seed_targets_its_ashby_board_not_the_deleted_lever_one() -> None:
    seeds = sd.load_studio_seeds()
    voodoo = next(row for row in seeds if row.get("studio") == "Voodoo")

    assert voodoo["likelyProviders"] == ["ashby"]
    assert voodoo["careersUrl"] == "https://jobs.ashbyhq.com/voodoo"
    assert "lever" not in json.dumps(voodoo).lower()

    # The provider set is derived from likelyProviders, so this is what discovery
    # would actually probe for Voodoo.
    assert likely_providers_for_seed(voodoo) == ["ashby"]
    assert provider_reinforcement_score(voodoo, "ashby") > 0
    assert provider_reinforcement_score(voodoo, "lever") == 0


def test_curated_candidate_stages_voodoo_on_its_ashby_board() -> None:
    """The curated-candidate list must not re-stage the dead Lever board.

    ``STATIC_DISCOVERY_CANDIDATES`` is a separate seed surface from the studio
    catalog, and it carried its own dead Lever row, which would have kept a dead
    candidate in every discovery run even with the catalog fixed.
    """
    rows = stage_curated_seed_candidates()
    voodoo_rows = [row for row in rows if "voodoo" in json.dumps(row).lower()]

    assert len(voodoo_rows) == 1, f"expected one curated Voodoo candidate, got {voodoo_rows}"
    row = voodoo_rows[0]
    assert row["adapter"] == "ashby"
    assert row["board_url"] == "https://jobs.ashbyhq.com/voodoo"
    assert row["careersUrl"] == "https://jobs.ashbyhq.com/voodoo"
    assert row["id"] == "ashby:board_url:https://jobs.ashbyhq.com/voodoo"
    assert "lever" not in json.dumps(row).lower()
