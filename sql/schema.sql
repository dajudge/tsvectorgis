CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE fleet_telemetry (
    ts              timestamptz NOT NULL,
    truck_id        integer NOT NULL,
    position        geography(Point, 4326) NOT NULL,
    cargo_temp      real NOT NULL,
    compressor_load real NOT NULL,
    vibration       real NOT NULL,
    signature       vector(64) NOT NULL
);

SELECT create_hypertable('fleet_telemetry', by_range('ts', INTERVAL '1 hour'));

CREATE INDEX fleet_truck_time_idx ON fleet_telemetry (truck_id, ts DESC);
CREATE INDEX fleet_position_idx ON fleet_telemetry USING gist (position);

-- The HNSW index is built after bulk ingestion to measure its build cost separately.
