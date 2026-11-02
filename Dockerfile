# pgvector is already bundled with the official TimescaleDB image.
# PostgreSQL 18 matches Alpine's current PostGIS package.
FROM timescale/timescaledb:2.31.0-pg18

USER root
RUN set -eux;     apk add --no-cache postgis;     install -d "$(pg_config --pkglibdir)" "$(pg_config --sharedir)/extension";     cp /usr/lib/postgresql18/postgis*.so "$(pg_config --pkglibdir)/";     cp /usr/share/postgresql18/extension/postgis* "$(pg_config --sharedir)/extension/";     test -f "$(pg_config --sharedir)/extension/postgis.control"

COPY sql/schema.sql /docker-entrypoint-initdb.d/20-schema.sql
