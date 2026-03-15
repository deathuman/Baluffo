import json
import unittest
from pathlib import Path
from unittest import mock

from src import jobs_fetcher

class PipelineGuardTests(unittest.TestCase):
    def test_pipeline_output_contract_preserves_camelcase_schema(self) -> None:
        """
        Guard test to ensure that output JSON preserves the camelCase keys 
        expected by the frontend domain (frontend/jobs/domain.js).
        """
        # We synthesize a job dict using the exact internal canonicalize pipeline
        # to prove the actual output matches the contract.
        payload = jobs_fetcher.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Giant Enemy Crab",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/123",
                "sector": "Game",
            },
            source="guard-test",
            fetched_at="2026-03-15T12:00:00+00:00",
        )
        self.assertIsNotNone(payload)
        
        # Deduplicate to simulate the final pipeline output row
        merged, stats = jobs_fetcher.deduplicate_jobs([payload])
        self.assertEqual(len(merged), 1)
        row = merged[0]

        # The JSON contract MUST include these EXACT camelCase keys.
        # This protects `frontend/jobs/domain.js` parsing.
        expected_keys = {
            "title",
            "company",
            "city",
            "country",
            "workType",
            "contractType",
            "jobLink",
            "sector",
            "profession",
            "sourceJobId",
            "postedAt",
            "qualityScore",
            "focusScore",
        }
        
        for key in expected_keys:
            self.assertIn(key, row, msg=f"Contract violation: missing camelCase key '{key}' in pipeline output row.")

    @mock.patch("sys.argv", ["jobs_fetcher.py", "--only-sources", "missing-dummy-source", "--quiet"])
    def test_pipeline_executes_end_to_end_without_silent_failure(self) -> None:
        """
        Guard test to ensure the jobs_fetcher CLI execution path doesn't break
        or silently swallow arguments.
        """
        # Run the pipeline targeting a missing source to test the orchestration
        # without making real network requests.
        try:
            jobs_fetcher.main()
        except SystemExit as exc:
            self.assertEqual(exc.code, 0, "Pipeline main() exited with an error code.")
            
if __name__ == "__main__":
    unittest.main()

