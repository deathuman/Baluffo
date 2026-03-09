import unittest

from scripts import fetcher_metrics as fm


class FetcherMetricsTests(unittest.TestCase):
    def test_build_metrics_computes_duplicate_and_history_stats(self) -> None:
        report = {
            "startedAt": "2026-03-09T10:00:00+00:00",
            "finishedAt": "2026-03-09T10:02:00+00:00",
            "summary": {"inputCount": 10, "mergedCount": 2, "outputCount": 8},
            "sources": [
                {"name": "a", "status": "ok", "durationMs": 10},
                {"name": "b", "status": "error", "durationMs": 40},
                {"name": "c", "status": "excluded", "durationMs": 0},
            ],
        }
        history = [
            {"type": "fetch", "durationMs": 1000, "finishedAt": "2026-03-09T10:02:00+00:00"},
            {"type": "fetch", "durationMs": 3000, "finishedAt": "2026-03-09T09:02:00+00:00"},
            {"type": "discovery", "durationMs": 4000, "finishedAt": "2026-03-09T08:02:00+00:00"},
        ]
        metrics = fm.build_metrics(report, history, window=5)
        latest = metrics["latestRun"]
        self.assertEqual(latest["duplicateRate"], 0.2)
        self.assertEqual(latest["outputYieldRate"], 0.8)
        self.assertEqual(latest["sourceFailureRate"], 0.3333)
        self.assertEqual(latest["failedSources"], 1)
        self.assertEqual(metrics["history"]["windowRuns"], 2)
        self.assertEqual(metrics["history"]["medianDurationMs"], 2000)

    def test_sanitize_source_label_removes_control_chars_and_truncates(self) -> None:
        raw = "bad\x00source\tname\nwith\rcntl and a very very very very very very very long tail"
        clean = fm.sanitize_source_label(raw, max_len=32)
        self.assertNotIn("\x00", clean)
        self.assertNotIn("\n", clean)
        self.assertNotIn("\r", clean)
        self.assertLessEqual(len(clean), 32)

    def test_sanitize_source_label_normalizes_static_source_listing_prefix(self) -> None:
        raw = "static_source::static:listing_url:https://studio.example.com/careers/jobs?utm=x"
        clean = fm.sanitize_source_label(raw)
        self.assertTrue(clean.startswith("static:studio.example.com/careers/jobs"))


if __name__ == "__main__":
    unittest.main()
