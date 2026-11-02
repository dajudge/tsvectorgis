# TSVectorGIS

Benchmark **TimescaleDB + PostGIS + pgvector** together on synthetic refrigerated-truck telemetry.

A single hypertable contains timestamps, GPS positions, sensor readings and 64-dimensional machine-state embeddings. Known failure patterns are injected into reproducible data.

## Run locally

Requires Docker Compose and Python 3.12+.

```bash
docker compose up -d --build --wait
python -m pip install -r benchmark/requirements.txt
python -m unittest discover -s benchmark -p 'test_*.py'
python benchmark/run.py --rows 100000 --output results/bulk
python benchmark/continuous.py --duration 15 --ingest-rate 500 --query-workers 0 --output results/ingest-only
python benchmark/continuous.py --duration 15 --ingest-rate 500 --query-workers 2 --output results/concurrent
docker compose down -v
```

The Python client reads `DATABASE_URL` (default: `postgresql://bench:bench@localhost:5432/bench`).

**Important:** `benchmark/run.py` expects an empty database. `docker compose down -v` deletes its database volume.

## Benchmarks

**Bulk seed:** deterministic CSV generation, PostgreSQL `COPY` throughput (excluding generation), HNSW index build time, database size, query p50/p95 latency and `EXPLAIN (ANALYZE, BUFFERS)` plans. Its query checks compare HNSW results with an exact baseline, including recall@20. A separate forced-HNSW diagnostic verifies that the vector index is actually used.

**Continuous load:** paced `COPY` inserts at a configurable target rows/second, while 0–32 independent query connections run spatially and temporally filtered similarity search and hotspot aggregation. Measurements include achieved rows/second, COPY batch p95, both query p50/p95, query counts and database size. Writer and readers use separate PostgreSQL connections and start together.

For comparison, we run two sequential phases on the same database:
1. Ingest-only baseline (0 query workers).
2. Ingestion with concurrent query workers (default 2).

```bash
python benchmark/continuous.py --duration 30 --ingest-rate 1000 --batch-size 100 \
  --query-workers 4 --query-interval-ms 500 --output results/load-test
```

All result directories contain `benchmark.json` and `summary.md`. The bulk benchmark additionally outputs four query plans.

## GitHub Actions

PRs and pushes to main run 100k seed rows and two 15-second continuous phases (500 rows/s, two query workers). Use **Actions → Benchmark → Run workflow** for larger datasets and selectable write rate, duration and query-worker count. Results are uploaded as artifacts and shown in the job summary.

These are exploratory tests on shared CI runners, **not precise hardware comparisons**. The database grows between phases, each test lasts only a short interval, and results may vary with runner load.

## Architecture

The custom image starts from `timescale/timescaledb:2.31.0-pg18`. pgvector is bundled upstream; PostGIS is added from Alpine packages and its extension files installed into the image's PostgreSQL paths. The database initialization creates the three extensions and a hypertable.

PostGIS's Alpine package is compiled against PostgreSQL 18, so the PostgreSQL major version is intentionally pinned to 18.

## Scale comparisons and charts

`benchmark/compare.py` runs a matrix of paced inserts against the same database, varying:
- Seed size: 1M and 5M records.
- Target ingestion: 1,000 and 5,000 rows/second.
- Concurrent query workers: 0 (baseline), 2, 4 and 8.
- Vector search: `auto` (PostgreSQL planner), `spatial` (exact ranking after geographic/time filtering), and `hnsw` (vector index preferred via planner settings).

```bash
python benchmark/compare.py --rates 1000,5000 --workers 2,4,8 --duration 8 --output results/comparison
```

The scale workflow runs **1M and 5M** on the feature branch; afterwards its manual dispatch controls allow selecting either size and test duration. Outputs include `comparison.json`, `comparison.csv`, Markdown and three PNG plots (throughput, vector p95 and hotspot p95).

**Limitations:** CI is shared infrastructure. Each scenario writes new records, so later scenarios see a larger database and may see different cache/index conditions. The comparison is exploratory, not statistically controlled. Forced HNSW measures an alternative access path, not necessarily PostgreSQL's best plan.
