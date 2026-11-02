#!/usr/bin/env python3
"""Run repeatable load scenarios and report their relative performance."""
import argparse
import csv
import json
import os
from pathlib import Path
from types import SimpleNamespace

from continuous import run_benchmark


def parse_ints(value):
    values = sorted(set(int(x.strip()) for x in value.split(",")))
    if not values or any(x <= 0 for x in values):
        raise ValueError("Expected positive integers")
    return values


def scenarios(rates, workers, strategies):
    for rate in rates:
        yield rate, 0, "auto"
        for worker in workers:
            for strategy in strategies:
                yield rate, worker, strategy


def flatten(result):
    cfg, ingest, queries = result["config"], result["ingest"], result["queries"]
    return {
        "target_rows_s": cfg["ingest_target_rows_s"],
        "workers": cfg["query_workers"],
        "strategy": cfg["search_strategy"],
        "actual_rows_s": ingest["rows_s"],
        "achieved_pct": ingest["target_achievement_pct"],
        "copy_batches": ingest["copy_batch_count"],
        "rows_per_batch": ingest["avg_rows_per_batch"],
        "copy_p95_ms": ingest["copy_batches"]["p95_ms"],
        "neighbors_count": queries["neighbors"]["count"],
        "neighbors_p95_ms": queries["neighbors"]["p95_ms"],
        "hotspots_count": queries["hotspots"]["count"],
        "hotspots_p95_ms": queries["hotspots"]["p95_ms"],
        "database_bytes": result["database_bytes"],
    }


def markdown_table(rows):
    headings = [
        ("target_rows_s", "Target rows/s"),
        ("workers", "Workers"),
        ("strategy", "Strategy"),
        ("actual_rows_s", "Actual rows/s"),
        ("achieved_pct", "Achieved %"),
        ("neighbors_p95_ms", "Vector p95 ms"),
        ("hotspots_p95_ms", "Hotspot p95 ms"),
    ]
    lines = [
        "| " + " | ".join(label for _, label in headings) + " |",
        "|" + "|".join("---" for _ in headings) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(
            "n/a" if row[key] is None else str(row[key])
            for key, _ in headings
        ) + " |")
    return "\n".join(lines) + "\n"


def chart(rows, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for field, y_label, filename in (
        ("actual_rows_s", "Rows / second", "throughput.png"),
        ("neighbors_p95_ms", "Vector search p95 (ms)", "vector-p95.png"),
        ("hotspots_p95_ms", "Hotspot query p95 (ms)", "hotspots-p95.png"),
    ):
        fig, ax = plt.subplots(figsize=(10, 5))
        groups = sorted(set((r["target_rows_s"], r["strategy"]) for r in rows))
        for rate, strategy in groups:
            subset = sorted(
                (r for r in rows if r["target_rows_s"] == rate and
                 (r["strategy"] == strategy or r["workers"] == 0)),
                key=lambda r: r["workers"],
            )
            points = [(r["workers"], r[field]) for r in subset if r[field] is not None]
            if points:
                x, y = zip(*points)
                ax.plot(x, y, marker="o", label=f"{rate:,} rows/s · {strategy}")
        ax.set(xlabel="Concurrent query workers", ylabel=y_label)
        ax.set_xticks(sorted(set(r["workers"] for r in rows)))
        ax.grid(alpha=0.25)
        if ax.lines:
            ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(output / filename, dpi=150)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", default="500,2000")
    parser.add_argument("--workers", default="2,4,8")
    parser.add_argument("--strategies", default="auto,spatial,hnsw")
    parser.add_argument("--duration", type=int, default=8)
    parser.add_argument("--output", type=Path, default=Path("results/comparison"))
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error("--duration must be positive")
    rates, workers = parse_ints(args.rates), parse_ints(args.workers)
    strategies = [s.strip() for s in args.strategies.split(",")]
    if not strategies or any(s not in ("auto", "spatial", "hnsw") for s in strategies):
        parser.error("Strategies must be auto, spatial or hnsw")
    dsn = os.getenv("DATABASE_URL", "postgresql://bench:bench@localhost:5432/bench")
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for rate, workers_count, strategy in scenarios(rates, workers, strategies):
        print(f"rate={rate}, workers={workers_count}, strategy={strategy}", flush=True)
        config = SimpleNamespace(
            duration=args.duration, ingest_rate=rate, batch_size=100,
            query_workers=workers_count, query_interval_ms=500,
            statement_timeout_ms=30000, trucks=1000, seed=43,
            search_strategy=strategy,
        )
        results.append(run_benchmark(config, dsn))
        (args.output / "comparison.json").write_text(json.dumps(results, indent=2) + "\n")
    rows = [flatten(r) for r in results]
    with (args.output / "comparison.csv").open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = (
        "# Load comparison\n\n" + markdown_table(rows) +
        "\nCharts: throughput.png, vector-p95.png, hotspots-p95.png.\n\n" +
        "Scenarios run sequentially against a growing database. CI runners are shared, " +
        "so comparisons are indicative, not controlled benchmarks. HNSW uses forced " +
        "planner settings. Spatial strategy computes exact matches after spatial filtering.\n"
    )
    (args.output / "summary.md").write_text(summary)
    chart(rows, args.output)
    print(summary)


if __name__ == "__main__":
    main()
