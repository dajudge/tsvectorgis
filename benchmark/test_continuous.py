"""Unit tests for deterministic telemetry generation and metric summaries."""
import csv
import io
import json
import unittest
from datetime import datetime, timezone

from continuous import distribution, make_batch, prepare_batches, summary_md


class ContinuousBenchmarkTest(unittest.TestCase):
    def test_batch_repeatable_across_batch_boundaries(self):
        anchor = datetime(2026, 11, 2, tzinfo=timezone.utc)
        all_rows = make_batch(0, 3, 100, anchor, 1000, 42)
        split = (
            make_batch(0, 1, 100, anchor, 1000, 42)
            + make_batch(1, 2, 100, anchor, 1000, 42)
        )
        self.assertEqual(all_rows, split)
        parsed = list(csv.reader(io.StringIO(all_rows.decode("utf-8"))))
        self.assertEqual(len(parsed), 3)
        self.assertEqual(len(parsed[0]), 7)
        self.assertEqual(len(json.loads(parsed[0][-1])), 64)
        self.assertTrue(parsed[0][2].startswith("SRID=4326;POINT("))
        self.assertLess(parsed[0][0], parsed[1][0])

    def test_prepared_batches_are_identical_to_direct_generator(self):
        anchor = datetime(2026, 11, 2, tzinfo=timezone.utc)
        prepared, generation_seconds = prepare_batches(11, 4, 100, anchor, 1000, 42)
        expected = make_batch(0, 11, 100, anchor, 1000, 42)
        self.assertEqual(len(prepared), 3)
        self.assertEqual(b"".join(prepared), expected)
        self.assertEqual([len(list(csv.reader(io.StringIO(b.decode("utf-8"))))) for b in prepared], [4, 4, 3])
        self.assertGreaterEqual(generation_seconds, 0)

    def test_distribution(self):
        self.assertEqual(distribution([])["count"], 0)
        self.assertEqual(distribution([1, 2, 3, 4, 5])["p95_ms"], 5)
        self.assertEqual(distribution([1, 2, 3, 4])["p50_ms"], 2.5)

    def test_summary_with_no_query_workers(self):
        result = {
            "config": {
                "query_workers": 0, "ingest_target_rows_s": 100,
                "duration_s": 1,
            },
            "ingest": {
                "rows": 99, "rows_s": 99,
                "copy_batch_count": 1, "avg_rows_per_batch": 99,
                "target_achievement_pct": 99,
                "copy_batches": {"p95_ms": 1.2},
            },
            "queries": {
                "neighbors": distribution([]),
                "hotspots": distribution([]),
            },
            "database_bytes": 1048576,
        }
        self.assertIn("99 rows/s", summary_md(result))
        self.assertIn("| neighbors query count | 0 |", summary_md(result))


if __name__ == "__main__":
    unittest.main()
