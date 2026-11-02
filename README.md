# TSVectorGIS

Benchmark **TimescaleDB + PostGIS + pgvector** together on synthetic refrigerated-truck telemetry.

A single hypertable contains timestamps, GPS positions, sensor readings, and 64-dimensional machine-state embeddings. Known failure patterns are injected into reproducible data.

## Run locally

Requires Docker Compose and Python 3.12+.

```bash
docker compose up -d --build --wait
python -m pip install -r benchmark/requirements.txt
python benchmark/run.py --rows 100000 --output results
docker compose down -v
```

The Python client reads `DATABASE_URL` (default: `postgresql://bench:bench@localhost:5432/bench`).

**Important:** the benchmark expects an empty database. `docker compose down -v` deletes its database volume.

## What we measure

- Generate deterministic CSV (generation time reported separately).
- `COPY` ingestion throughput in rows/second, including network transfer.
- Build-time cost of an HNSW cosine-similarity index after ingestion.
- Query p50/p95 latency (five sequential runs) and `EXPLAIN (ANALYZE, BUFFERS)` plans.
- Database size including indexes.

**Queries:**
1. The 20 most similar compressor-failure fingerprints within 20 km of Munich, in the last two hours of generated data.
2. Fault-like records around Munich, aggregated into 15-minute time buckets.

Output: `results/summary.md`, `results/benchmark.json` and two query plans.

## GitHub Actions

Every PR and push to main runs 100k rows. Use **Actions → Benchmark → Run workflow** to choose 100k, 1M or 5M rows. Results are saved as workflow artifacts and as a job summary.

These are exploratory, single-run benchmarks on shared CI hardware. They are **not** precise hardware benchmarks. Queries currently run **after** ingestion, not concurrently.

## Architecture

The custom image starts from `timescale/timescaledb:2.31.0-pg18`. pgvector is bundled upstream; PostGIS is added from Alpine packages and its extension files installed into the image's PostgreSQL paths. The database initialization creates the three extensions and a hypertable.

PostGIS's Alpine package is compiled against PostgreSQL 18, so the PostgreSQL major version is intentionally pinned to 18.
