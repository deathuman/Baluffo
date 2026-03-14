import unittest
from pathlib import Path

from scripts import source_registry as sr
from tests.helpers.temp_paths import workspace_tmpdir


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
        with workspace_tmpdir("source-registry") as tmp:
            path = Path(tmp) / "registry.json"
            payload = [{"adapter": "smartrecruiters", "company_id": "Gameloft"}]
            sr.save_json_atomic(path, payload)
            loaded = sr.load_json_array(path, [])
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["company_id"], "Gameloft")

    def test_normalize_source_url_trims_query_trailing_slash_and_case(self) -> None:
        normalized = sr.normalize_source_url("HTTPS://Jobs.Ashbyhq.com/Acme/jobs/?foo=1#frag")
        self.assertEqual(normalized, "https://jobs.ashbyhq.com/Acme/jobs")

    def test_source_url_fingerprint_prefers_endpoint_fields(self) -> None:
        row = {
            "adapter": "workable",
            "account": "acme",
            "api_url": "https://apply.workable.com/api/v1/widget/accounts/acme/?details=true",
        }
        self.assertEqual(
            sr.source_url_fingerprint(row),
            "https://apply.workable.com/api/v1/widget/accounts/acme",
        )

    def test_source_url_fingerprint_uses_static_pages_when_no_endpoint_field(self) -> None:
        row = {
            "adapter": "static",
            "pages": ["https://milestone.it/careers/?utm_source=x"],
        }
        self.assertEqual(sr.source_url_fingerprint(row), "https://milestone.it/careers")


if __name__ == "__main__":
    unittest.main()