"""Metadata-only discovery of candidate PMEi historical stores.

Caller must supply a dedicated, idle psycopg connection.
No connection is opened here; no API route or memory records are modified.
"""

CANDIDATE_TABLES = frozenset({
    "memory_shards",
    "memory_store",
    "reflections",
    "continuity_records",
})


def discover_historical_stores(conn):
    """Inspect visible table columns in an isolated read-only transaction."""

    # Psycopg 3 reports IDLE as zero. Refuse a connection already
    # participating in a transaction rather than rolling back other work.
    if conn.info.transaction_status != 0:
        raise RuntimeError(
            "Diagnostic requires a dedicated idle database connection"
        )

    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")

            cur.execute(
                """
                SELECT table_schema, table_name, column_name
                FROM information_schema.columns
                WHERE table_name = ANY(%s)
                  AND table_schema NOT IN (
                      'pg_catalog', 'information_schema'
                  )
                ORDER BY table_schema, table_name, ordinal_position
                """,
                (sorted(CANDIDATE_TABLES),),
            )

            rows = cur.fetchall()

        stores = {}

        for schema, table, column in rows:
            stores.setdefault((schema, table), []).append(column)

        visible_names = {table for _, table in stores}

        return {
            "status": "METADATA_ONLY",
            "stores": [
                {
                    "schema": schema,
                    "table": table,
                    "columns": columns,
                    "record_count": None,
                    "earliest_record": None,
                    "latest_record": None,
                    "coverage": "UNVERIFIED",
                }
                for (schema, table), columns in sorted(stores.items())
            ],
            "missing_or_inaccessible": sorted(
                CANDIDATE_TABLES - visible_names
            ),
        }

    finally:
        conn.rollback()
