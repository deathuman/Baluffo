import tempfile
import unittest
from pathlib import Path

from scripts import source_registry as sr


class SourceRegistryTests(unittest.TestCase):
    def test_source_identity_uses_explicit_id_when_present(self) -> None:
        row = {"id": "lever:account:sandboxvr", "adapter": "lever", "account": "sandboxvr"}
        token = sr.source_identity(row)
        self.assertEqual(token, "lever:account:sandboxvr")

    def test_source_identity_prefers_adapter_keyed_fields(self) -> None:
        row = {"adapter": "lever", "account": "sandboxvr"}
        token = sr.source_identity(row)
        self.assertIn("lever:account:sandboxvr", token)

    def test_unique_sources_deduplicates_by_identity(self) -> None:
        rows = [
            {"adapter": "workable", "account": "hutch"},
            {"adapter": "workable", "account": "hutch"},
            {"adapter": "workable", "account": "wargaming"},
        ]
        deduped = sr.unique_sources(rows)
        self.assertEqual(len(deduped), 2)

    def test_save_json_atomic_and_load_array(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.json"
            payload = [{"adapter": "smartrecruiters", "company_id": "Gameloft"}]
            sr.save_json_atomic(path, payload)
            loaded = sr.load_json_array(path, [])
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["company_id"], "Gameloft")


if __name__ == "__main__":
    unittest.main()
