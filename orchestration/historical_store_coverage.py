"""Read-only aggregate coverage of four known PMEi stores.

No record content is selected. Table-wide coverage is not proof of ownership.
The caller supplies a dedicated, idle psycopg connection.
"""

from datetime import date, datetime

STORE_SPECS = (
    ("continuity_records", "timestamp", "user_id"),
    ("reflections", "ts", "user_id"),
    ("memory_store", "ts", "user_id"),
    ("memory_shards", "timestamp", None),
)


def _date_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise ValueError("Unexpected database date type")


def inspect_historical_coverage(conn, owner_user_id):
    """Return aggregate counts/date bounds without reading record contents."""

    if conn.info.transaction_status != 0:
        raise RuntimeError(
            "Coverage requires a dedicated idle database connection"
        )

    if not isinstance(owner_user_id, str) or not owner_user_id.strip():
        raise ValueError("Owner identifier is required")

    owner = owner_user_id.strip().lower()
    stores = []

    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")

            # Check the exact expected public columns before executing
            # any aggregate query. Missing columns fail the entire job.
            cur.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ANY(%s)
                """,
                ([spec[0] for spec in STORE_SPECS],),
            )

            visible = {}
            for table, column in cur.fetchall():
                visible.setdefault(table, set()).add(column)

            for table, date_column, owner_column in STORE_SPECS:
                required = {date_column}
                if owner_column:
                    required.add(owner_column)

                if not required.issubset(visible.get(table, set())):
                    raise RuntimeError(
                        "Required historical coverage columns unavailable"
                    )

            # SQL identifiers below are fixed constants, not user input.
            for table, date_column, owner_column in STORE_SPECS:
                cur.execute(
                    f"""
                    SELECT COUNT(*), MIN({date_column}), MAX({date_column})
                    FROM public.{table}
                    """
                )
                count, earliest, latest = cur.fetchone()

                result = {
                    "table": table,
                    "table_wide": {
                        "record_count": count,
                        "earliest_record": _date_value(earliest),
                        "latest_record": _date_value(latest),
                        "ownership": "UNATTRIBUTED",
                    },
                    "owner_identifier_match": None,
                }

                if owner_column:
                    cur.execute(
                        f"""
                        SELECT COUNT(*),
                               MIN({date_column}),
                               MAX({date_column})
                        FROM public.{table}
                        WHERE LOWER({owner_column}) = %s
                        """,
                        (owner,),
                    )
                    matched_count, matched_earliest, matched_latest = (
                        cur.fetchone()
                    )

                    result["owner_identifier_match"] = {
                        "record_count": matched_count,
                        "earliest_record": _date_value(matched_earliest),
                        "latest_record": _date_value(matched_latest),
                        "ownership": "IDENTIFIER_MATCH_UNVERIFIED",
                    }

                stores.append(result)

        return {
            "status": "AGGREGATE_COVERAGE_ONLY",
            "exhaustive_retrieval": False,
            "stores": stores,
        }

    finally:
        conn.rollback()