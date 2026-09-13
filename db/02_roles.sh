#!/bin/bash
# Least-privilege roles. Passwords come from the environment (.env), the
# superuser is only used by this init step. No set -e here: the entrypoint
# sources this file when it is not executable, ON_ERROR_STOP does the job.

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -v spark_pw="$SPARK_DB_PASSWORD" -v grafana_pw="$GRAFANA_DB_PASSWORD" <<-'EOSQL'
    -- the upsert needs SELECT as well: ON CONFLICT reads the existing row
    CREATE ROLE spark LOGIN PASSWORD :'spark_pw';
    GRANT SELECT, INSERT, UPDATE ON agg_tumbling_6h, agg_sliding_24h TO spark;

    CREATE ROLE grafana LOGIN PASSWORD :'grafana_pw';
    GRANT SELECT ON agg_tumbling_6h, agg_sliding_24h TO grafana;
EOSQL
