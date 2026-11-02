#!/usr/bin/env python3
"""Reproducible COPY ingestion and spatiotemporal/vector queries."""
import argparse
import csv
import json
import math
import os
from pathlib import Path
import random
import statistics
import tempfile
import time
from datetime import datetime, timedelta, timezone

import psycopg


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
MUNICH_LON, MUNICH_LAT = 11.58, 48.14
COPY_SQL = """COPY fleet_telemetry
    (ts, truck_id, position, cargo_temp, compressor_load, vibration, signature)
    FROM STDIN WITH (FORMAT csv)"""

NEIGHBORS_SQL = """
SELECT truck_id, ts, ST_AsText(position::geometry), vibration,
       signature <=> %s::vector AS similarity_distance
FROM fleet_telemetry
WHERE ts >= %s AND ts <= %s
  AND ST_DWithin(
      position,
      ST_SetSRID(ST_MakePoint(11.58, 48.14), 4326)::geography,
      20000
  )
ORDER BY signature <=> %s::vector
LIMIT 20
"""

# This materialized CTE first applies the time/geographic predicates, then ranks
# every matching row exactly; no approximate HNSW index can affect this baseline.
EXACT_NEIGHBORS_SQL = """
WITH candidates AS MATERIALIZED (
    SELECT truck_id, ts, position, vibration, signature
    FROM fleet_telemetry
    WHERE ts >= %s AND ts <= %s
      AND ST_DWithin(
          position,
          ST_SetSRID(ST_MakePoint(11.58, 48.14), 4326)::geography,
          20000
      )
)
SELECT truck_id, ts, ST_AsText(position::geometry), vibration,
       signature <=> %s::vector AS similarity_distance
FROM candidates
ORDER BY similarity_distance, truck_id, ts
LIMIT 20
"""

HOTSPOTS_SQL = """
SELECT time_bucket('15 minutes', ts) AS bucket,
       count(*) AS observations,
       count(*) FILTER (WHERE (signature <=> %s::vector) < 0.15) AS fault_like,
       avg(cargo_temp) AS avg_cargo_temp
FROM fleet_telemetry
WHERE ts >= %s AND ts <= %s
  AND ST_DWithin(
      position,
      ST_SetSRID(ST_MakePoint(11.58, 48.14), 4326)::geography,
      20000
  )
GROUP BY 1
ORDER BY 1
"""


def generate_csv(path: Path, rows: int, trucks: int, seed: int) -> float:
    """Generate sparse truck telemetry and known failure-like fingerprints."""
    rng = random.Random(seed)
    started = time.perf_counter()
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        for i in range(rows):
            fault = i % 37 == 0
            truck = i % trucks
            ts = START + timedelta(seconds=(i // trucks) * 10)
            lon = MUNICH_LON + rng.uniform(-0.55, 0.55)
            lat = MUNICH_LAT + rng.uniform(-0.45, 0.45)
            # Separated prototype vectors, with small reproducible sensor noise.
            signature = [
                (0.8 if j % 8 < 4 else -0.8) + rng.uniform(-0.04, 0.04)
                if fault else
                (0.4 if j % 8 < 4 else 0.6) + rng.uniform(-0.04, 0.04)
                for j in range(64)
            ]
            writer.writerow([
                ts.isoformat(), truck,
                f"SRID=4326;POINT({lon:.6f} {lat:.6f})",
                f"{(8.0 if fault else 4.0) + rng.uniform(-0.6, 0.6):.3f}",
                f"{(95.0 if fault else 65.0) + rng.uniform(-10, 10):.3f}",
                f"{(1.40 if fault else 0.25) + rng.uniform(-0.08, 0.08):.3f}",
                "[" + ",".join(f"{x:.5f}" for x in signature) + "]"
            ])
    return time.perf_counter() - started


def ingest(conn: psycopg.Connection, path: Path) -> float:
    start = time.perf_counter()
    with conn.cursor() as cursor:
        with cursor.copy(COPY_SQL) as copy:
            with path.open("rb") as source:
                while data := source.read(1024 * 1024):
                    copy.write(data)
    conn.commit()
    return time.perf_counter() - start


def run_query(conn, name, sql, params, iterations, results_dir):
    durations = []
    row_count = 0
    with conn.cursor() as cursor:
        for _ in range(iterations):
            start = time.perf_counter()
            cursor.execute(sql, params)
            records = cursor.fetchall()
            durations.append((time.perf_counter() - start) * 1000)
            row_count = len(records)
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql, params)
        plan = "\n".join(row[0] for row in cursor.fetchall())
        (results_dir / f"{name}-explain.txt").write_text(plan + "\n")
    durations.sort()
    return {
        "rows_returned": row_count,
        "p50_ms": round(statistics.median(durations), 2),
        "p95_ms": round(durations[math.ceil(0.95 * len(durations)) - 1], 2),
    }, records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--trucks", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("results"))
    args = parser.parse_args()
    if min(args.rows, args.trucks, args.iterations) < 1:
        parser.error("--rows, --trucks and --iterations must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    dsn = os.getenv("DATABASE_URL", "postgresql://bench:bench@localhost:5432/bench")

    with tempfile.TemporaryDirectory(prefix="tsvectorgis-") as tmp:
        csv_path = Path(tmp) / "telemetry.csv"
        generation_seconds = generate_csv(csv_path, args.rows, args.trucks, args.seed)
        csv_bytes = csv_path.stat().st_size

        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM fleet_telemetry")
                if cur.fetchone()[0]:
                    raise RuntimeError("Database is not empty; run 'docker compose down -v' first")
            load_seconds = ingest(conn, csv_path)
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM fleet_telemetry")
                loaded_rows = cur.fetchone()[0]
                assert loaded_rows == args.rows, (loaded_rows, args.rows)
                index_start = time.perf_counter()
                cur.execute("""CREATE INDEX fleet_signature_hnsw_idx
                            ON fleet_telemetry USING hnsw (signature vector_cosine_ops)""")
                cur.execute("ANALYZE fleet_telemetry")
                conn.commit()
                hnsw_seconds = time.perf_counter() - index_start
                cur.execute("SELECT min(ts), max(ts) FROM fleet_telemetry")
                _, end = cur.fetchone()
                begin = end - timedelta(hours=2)
                cur.execute("""SELECT signature::text FROM fleet_telemetry
                            WHERE vibration > 1 ORDER BY ts DESC LIMIT 1""")
                fingerprint = cur.fetchone()[0]
                cur.execute("""SELECT extname, extversion FROM pg_extension
                            WHERE extname IN ('timescaledb', 'postgis', 'vector')""")
                versions = dict(cur.fetchall())
                cur.execute("SELECT pg_database_size(current_database())")
                database_bytes = cur.fetchone()[0]

            # Post-filtering can discard HNSW candidates. Let pgvector keep
            # scanning until it finds enough points inside the time/geofence.
            with conn.cursor() as cur:
                cur.execute("SET hnsw.iterative_scan = strict_order")
                cur.execute("SET hnsw.ef_search = 100")
                cur.execute("SET hnsw.max_scan_tuples = 100000")

            neighbors, approximate_rows = run_query(
                conn, "neighbors", NEIGHBORS_SQL,
                (fingerprint, begin, end, fingerprint), args.iterations, args.output
            )
            exact, exact_rows = run_query(
                conn, "neighbors-exact", EXACT_NEIGHBORS_SQL,
                (begin, end, fingerprint), args.iterations, args.output
            )
            # truck_id + timestamp uniquely identifies a generated sample.
            approx_ids = {(r[0], r[1]) for r in approximate_rows}
            exact_ids = {(r[0], r[1]) for r in exact_rows}
            recall = len(approx_ids & exact_ids) / len(exact_ids) if exact_ids else 1.0
            neighbors["recall_at_20"] = round(recall, 4)
            neighbors["expected_rows"] = len(exact_rows)
            if len(approximate_rows) != len(exact_rows):
                raise RuntimeError(
                    f"Filtered neighbor query returned {len(approximate_rows)} rows; "
                    f"exact search returned {len(exact_rows)}. "
                    "Inspect neighbors-explain.txt and HNSW scan limits."
                )

            hotspots, _ = run_query(
                conn, "hotspots", HOTSPOTS_SQL,
                (fingerprint, begin, end), args.iterations, args.output
            )
            queries = {
                "neighbors": neighbors,
                "neighbors_exact": exact,
                "hotspots": hotspots,
            }

    result = {
        "extensions": versions, "rows": loaded_rows, "trucks": args.trucks,
        "csv_bytes": csv_bytes, "database_bytes": database_bytes,
        "generation_seconds": round(generation_seconds, 3),
        "copy_seconds": round(load_seconds, 3),
        "copy_rows_per_second": round(loaded_rows / load_seconds),
        "hnsw_build_seconds": round(hnsw_seconds, 3),
        "queries": queries,
    }
    (args.output / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n")
    summary = [
        "# TSVectorGIS benchmark", "",
        f"**Extensions:** {', '.join(f'{k} {v}' for k, v in versions.items())}",
        f"**Rows:** {loaded_rows:,} | **Trucks:** {args.trucks:,}", "",
        "| Metric | Value |", "|---|---:|",
        f"| CSV generation | {generation_seconds:.2f} s |",
        f"| PostgreSQL COPY | {load_seconds:.2f} s |",
        f"| COPY throughput | {loaded_rows / load_seconds:,.0f} rows/s |",
        f"| HNSW index build + ANALYZE | {hnsw_seconds:.2f} s |",
        f"| DB size (including indexes) | {database_bytes / 1024**2:,.1f} MiB |",
    ]
    for name, outcome in queries.items():
        summary += [
            f"| {name} p50 | {outcome['p50_ms']:.2f} ms |",
            f"| {name} p95 | {outcome['p95_ms']:.2f} ms |",
            f"| {name} result rows | {outcome['rows_returned']} |",
        ]
    summary += [
        f"| neighbors recall@20 vs exact | {neighbors['recall_at_20']:.1%} |",
        f"| neighbors expected rows | {neighbors['expected_rows']} |",
        "",
        "COPY measures loading a pre-generated CSV. Queries run sequentially, not concurrently with ingestion. "
        "HNSW is approximate; the exact baseline uses a materialized, filtered candidate set. "
        "Check EXPLAIN plans to see whether PostgreSQL actually chose the HNSW index."
    ]
    content = "\n".join(summary) + "\n"
    (args.output / "summary.md").write_text(content)
    print(content)


if __name__ == "__main__":
    main()
