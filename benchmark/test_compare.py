"""Tests for matrix scenario generation and output summaries."""
import unittest

from compare import scenarios, parse_ints, markdown_table


class ComparisonTest(unittest.TestCase):
    def test_matrix(self):
        cases = list(scenarios([500, 2000], [2, 4], ["auto", "spatial", "hnsw"]))
        self.assertEqual(len(cases), 14)
        self.assertEqual(sum(x[1] == 0 for x in cases), 2)

    def test_rate_parsing(self):
        self.assertEqual(parse_ints("4,2,4"), [2, 4])
        with self.assertRaises(ValueError):
            parse_ints("2,0")

    def test_missing_latency(self):
        row = {
            "target_rows_s": 500, "workers": 0, "strategy": "auto",
            "actual_rows_s": 480, "achieved_pct": 96,
            "neighbors_p95_ms": None, "hotspots_p95_ms": None,
        }
        self.assertIn("n/a", markdown_table([row]))


if __name__ == "__main__":
    unittest.main()
