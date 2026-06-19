from src.ship import update_manager as um
from src.ship.update_manager_apply import apply_update
from src.ship.update_manager_paths import ShipPaths
from src.ship.update_manager_recovery import startup_check
from src.ship.update_manager_state import ensure_state, iso_now
from src.ship.update_manager_validation import compute_sha256, sign_manifest


def test_update_manager_facade_keeps_public_api_exports() -> None:
    assert um.ShipPaths is ShipPaths
    assert um.apply_update is apply_update
    assert um.compute_sha256 is compute_sha256
    assert um.ensure_state is ensure_state
    assert um.iso_now is iso_now
    assert um.sign_manifest is sign_manifest
    assert um.startup_check is startup_check
