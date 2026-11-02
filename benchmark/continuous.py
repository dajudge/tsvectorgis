#!/usr/bin/env python3
"""Measure paced COPY ingestion while time/geographic/vector queries run."""
import argparse
import csv
import io
import json
import math
import os
from pathlib import Path
import random
import statistics
import threading
import time
from datetime import timedelta

import psycopg

from run import COPY_SQL, EXACT_NEIGHBORS_SQL, HOTSPOTS_SQL, MUNICH_LAT, MUNICH_LON, NEIGHBORS_SQL


def make_batch(first, count, rate, anchor, trucks, seed):
    """Make reproducible CSV rows independent of thread scheduling."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for number in range(first, first + count):
        rng = random.Random(seed + number * 104729)
        fault = number % 37 == 0
        signature = [
            (0.8 if j % 8 < 4 else -0.8) + rng.uniform(-0.04, 0.04)
            if fault else
            (0.4 if j % 8 < 4 else 0.6) + rng.uniform(-0.04, 0.04)
            for j in range(64)
        ]
        timestamp = anchor + timedelta(seconds=(number + 1) / rate)
        longitude = MUNICH_LON + rng.uniform(-0.55, 0.55)
        latitude = MUNICH_LAT + rng.uniform(-0.45, 0.45)
        writer.writerow([
            timestamp.isoformat(),
            number % trucks,
            f"SRID=4326;POINT({longitude:.6f} {latitude:.6f})",
            f"{(8.0 if fault else 4.0) + rng.uniform(-0.6, 0.6):.3f}",
            f"{(95.0 if fault else 65.0) + rng.uniform(-10, 10):.3f}",
            f"{(1.40 if fault else 0.25) + rng.uniform(-0.08, 0.08):.3f}",
            "[" + ",".join(f"{x:.5f}" for x in signature) + "]",
        ])
    return buffer.getvalue().encode("utf-8")


def distribution(values):
    if not values:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "max_ms": None}
    sorted_values = sorted(values)
    return {
        "count": len(values),
        "p50_ms": round(statistics.median(sorted_values), 2),
        "p95_ms": round(sorted_values[math.ceil(0.95 * len(values)) - 1], 2),
        "max_ms": round(sorted_values[-1], 2),
    }


def run_benchmark(args, dsn):
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*), max(ts) FROM fleet_telemetry")
            before, anchor = cursor.fetchone()
            if before == 0:
                raise RuntimeError("Seed the database with benchmark/run.py first")
            cursor.execute("""SELECT signature::text FROM fleet_telemetry
                              WHERE vibration > 1 ORDER BY ts DESC LIMIT 1""")
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("No failure fingerprint available in seed data")
            fingerprint = row[0]

    # A fixed window covers both the initial seed and rows arriving during this run.
    begin = anchor - timedelta(hours=2)
    end = anchor + timedelta(seconds=args.duration + 60)
    strategy = getattr(args, "search_strategy", "auto")
    neighbor_sql = EXACT_NEIGHBORS_SQL if strategy == "spatial" else NEIGHBORS_SQL
    neighbor_params = (begin, end, fingerprint) if strategy == "spatial" else (fingerprint, begin, end, fingerprint)
    queries = {
        "neighbors": (neighbor_sql, neighbor_params),
        "hotspots": (
            HOTSPOTS_SQL,
            (fingerprint, begin, end),
        ),
    }

    ready = threading.Barrier(args.query_workers + 2)  # writer, readers, main
    go = threading.Event()
    stop = threading.Event()
    started = [0.0]
    errors = []
    writer_result = {"rows": 0, "batches_ms": [], "finished_at": None}
    reader_results = [
        {"neighbors": [], "hotspots": []}
        for _ in range(args.query_workers)
    ]

    def writer():
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                ready.wait(timeout=45)
                go.wait()
                start = started[0]
                deadline = start + args.duration
                sent = 0
                target_rows = args.duration * args.ingest_rate
                while not stop.is_set() and sent < target_rows:
                    now = time.monotonic()
                    closing = now >= deadline
                    due = min(
                        math.floor((now - start) * args.ingest_rate),
                        target_rows,
                    )
                    # Wait for an entire batch to become due. At the deadline,
                    # send at most one final partial batch rather than draining a
                    # backlog and hiding that the target rate was unsustainable.
                    batch_target = min(args.batch_size, target_rows - sent)
                    available = due - sent
                    if not closing and available < batch_target:
                        wait_until = start + (sent + batch_target) / args.ingest_rate
                        time.sleep(min(0.02, max(0.001, wait_until - now)))
                        continue
                    amount = min(batch_target, available)
                    if amount <= 0:
                        break
                    payload = make_batch(sent, amount, args.ingest_rate, anchor, args.trucks, args.seed)
                    batch_start = time.monotonic()
                    with conn.cursor() as cursor:
                        with cursor.copy(COPY_SQL) as copy:
                            copy.write(payload)
                    writer_result["batches_ms"].append((time.monotonic() - batch_start) * 1000)
                    sent += amount
                    writer_result["rows"] = sent
                    if closing:
                        break
                writer_result["finished_at"] = time.monotonic()
        except Exception as error:
            errors.append(f"writer: {type(error).__name__}: {error}")
            stop.set()
            ready.abort()

    def reader(worker_id):
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SET hnsw.iterative_scan = strict_order")
                    cursor.execute("SET hnsw.ef_search = 100")
                    cursor.execute("SET hnsw.max_scan_tuples = 100000")
                    cursor.execute("SELECT set_config('statement_timeout', %s, false)",
                                   (str(args.statement_timeout_ms),))
                ready.wait(timeout=45)
                go.wait()
                deadline = started[0] + args.duration
                next_due = started[0]
                sequence = worker_id
                while not stop.is_set():
                    now = time.monotonic()
                    if now >= deadline:
                        break
                    if now < next_due:
                        time.sleep(min(next_due - now, 0.05))
                        continue
                    name = "neighbors" if sequence % 2 == 0 else "hotspots"
                    sql, params = queries[name]
                    query_start = time.monotonic()
                    with conn.cursor() as cursor:
                        cursor.execute(sql, params)
                        cursor.fetchall()
                    reader_results[worker_id][name].append(
                        (time.monotonic() - query_start) * 1000
                    )
                    sequence += 1
                    next_due = max(next_due + args.query_interval_ms / 1000, time.monotonic())
        except Exception as error:
            errors.append(f"reader {worker_id}: {type(error).__name__}: {error}")
            stop.set()
            ready.abort()

    threads = [threading.Thread(target=writer, name="ingest")]
    threads.extend(
        threading.Thread(target=reader, args=(i,), name=f"query-{i}")
        for i in range(args.query_workers)
    )
    for thread in threads:
        thread.start()
    try:
        ready.wait(timeout=45)
        started[0] = time.monotonic()
        go.set()
    except threading.BrokenBarrierError:
        stop.set()
        go.set()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError("; ".join(errors))
    if not writer_result["finished_at"]:
        raise RuntimeError("Ingestion worker did not finish")

    elapsed = writer_result["finished_at"] - started[0]
    inserted = writer_result["rows"]
    expected = before + inserted
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM fleet_telemetry")
            actual = cursor.fetchone()[0]
            cursor.execute("SELECT pg_database_size(current_database())")
            database_bytes = cursor.fetchone()[0]
    if actual != expected:
        raise RuntimeError(f"Expected {expected} rows after ingestion, got {actual}")
    if inserted == 0:
        raise RuntimeError("No telemetry was inserted")

    latency = {
        name: distribution([
            value
            for worker in reader_results
            for value in worker[name]
        ])
        for name in queries
    }
    return {
        "config": {
            "duration_s": args.duration, "ingest_target_rows_s": args.ingest_rate,
            "batch_size": args.batch_size, "query_workers": args.query_workers,
            "query_interval_ms": args.query_interval_ms, "trucks": args.trucks,
            "seed": args.seed,
        },
        "ingest": {
            "rows": inserted,
            "copy_batch_count": len(writer_result["batches_ms"]),
            "avg_rows_per_batch": round(inserted / len(writer_result["batches_ms"]), 1),
            "elapsed_s": round(elapsed, 3),
            "rows_s": round(inserted / elapsed, 2),
            "target_achievement_pct": round(100 * inserted / (elapsed * args.ingest_rate), 1),
            "copy_batches": distribution(writer_result["batches_ms"]),
        },
        "queries": latency,
        "records_before": before,
        "records_after": actual,
        "database_bytes": database_bytes,
    }


def summary_md(result):
    config = result["config"]
    ingest = result["ingest"]
    lines = [
        f"## Continuous load ({config['query_workers']} query workers)", "",
        f"**Target:** {config['ingest_target_rows_s']:,} rows/s for "
        f"{config['duration_s']}s, {config['query_workers']} query workers",
        "",
        "| Metric | Result |", "|---|---:|",
        f"| Inserted | {ingest['rows']:,} rows |",
        f"| Actual ingest rate | {ingest['rows_s']:,.0f} rows/s |",
        f"| Target achieved | {ingest['target_achievement_pct']}% |",
        f"| COPY batches | {ingest['copy_batch_count']} |",
        f"| Average rows/COPY | {ingest['avg_rows_per_batch']} |",
        f"| COPY batch p95 | {ingest['copy_batches']['p95_ms']} ms |",
    ]
    for name, latency in result["queries"].items():
        lines.extend([
            f"| {name} query count | {latency['count']} |",
            f"| {name} p50 | {latency['p50_ms']} ms |",
            f"| {name} p95 | {latency['p95_ms']} ms |",
        ])
    lines.extend([
        f"| Database size | {result['database_bytes'] / (1024**2):,.1f} MiB |",
        "",
        "Each query worker alternates similarity search and time-bucketed hotspot aggregation, "
        "both spatially and temporally filtered. Runs are sequential, not hardware-isolated; "
        "the two phases have slightly different database sizes.",
        "",
    ])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=15, help="Active load in seconds")
    parser.add_argument("--ingest-rate", type=int, default=500, help="Target COPY rows/second")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--query-workers", type=int, default=2)
    parser.add_argument("--search-strategy", choices=("auto", "spatial", "hnsw"), default="auto")
    parser.add_argument("--query-interval-ms", type=int, default=500)
    parser.add_argument("--statement-timeout-ms", type=int, default=10000)
    parser.add_argument("--trucks", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=43)
    parser.add_argument("--output", type=Path, default=Path("results/continuous"))
    args = parser.parse_args()
    for name in ("duration", "ingest_rate", "batch_size", "query_interval_ms",
                 "statement_timeout_ms", "trucks"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.query_workers < 0:
        parser.error("--query-workers must not be negative")
    if args.query_workers > 32:
        parser.error("--query-workers must be at most 32")
    if args.seed < 0:
        parser.error("--seed must be nonnegative")

    result = run_benchmark(
        args,
        os.getenv("DATABASE_URL", "postgresql://bench:bench@localhost:5432/bench"),
    )
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n")
    summary = summary_md(result)
    (args.output / "summary.md").write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()
