"""pg-analytics-mcp — PostgreSQL analytics MCP server.

A general-purpose "DBA-lite" MCP server that provides semantic tools for
schema discovery, data exploration, relationship mapping, performance
analysis, and data quality checks on any PostgreSQL database.

Environment variables:
    PG_DEV_URL    — PostgreSQL DSN for DEV  (at least one URL required)
    PG_STG_URL    — PostgreSQL DSN for STG  (optional)
    PG_PROD_URL   — PostgreSQL DSN for PROD (optional)
    PG_READ_ONLY  — reserved for future write tools (not yet used)
"""

from fastmcp import FastMCP

from pg_analytics_mcp.db import (
    AVAILABLE_ENVS,
    DEFAULT_ENV,
    INTERNAL_SCHEMAS,
    connect,
    query,
    resolve_env,
    safe_ident,
)

# ── MCP server ───────────────────────────────────────────────────────────────

mcp = FastMCP(
    "pg-analytics",
    instructions=(
        f"PostgreSQL analytics tools. Available environments: {', '.join(AVAILABLE_ENVS)}. "
        f"Default environment: {DEFAULT_ENV}. "
        "Use database_summary for an overview, describe_table for column details, "
        "find_tables/find_columns to search, foreign_keys for relationships, "
        "recent_rows to peek at data, and column_stats/null_report for data quality.\n\n"
        "When working with tool results, write down any important information you might need later "
        "in your response, as the original tool result may be cleared later.\n\n"
        "IMPORTANT: Always present results and analysis in English, regardless of the user's language."
    ),
)

# ── Limits ───────────────────────────────────────────────────────────────────

MAX_ROW_LIMIT = 100
MAX_AGG_LIMIT = 200
QUERY_TIMEOUT_S = 30


def _clamp_row(limit: int) -> int:
    return max(1, min(limit, MAX_ROW_LIMIT))


def _clamp_agg(limit: int) -> int:
    return max(1, min(limit, MAX_AGG_LIMIT))


# ═════════════════════════════════════════════════════════════════════════════
# Schema Discovery
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def database_summary(env: str = DEFAULT_ENV) -> dict:
    """Get a high-level overview of the database: schema count, table count,
    view count, FK count, index count, total size, and installed extensions."""
    env = resolve_env(env)

    summary = query(
        env,
        """
        SELECT
            (SELECT count(DISTINCT table_schema) FROM information_schema.tables
             WHERE table_schema NOT IN ('pg_catalog','information_schema')) AS schema_count,
            (SELECT count(*) FROM information_schema.tables
             WHERE table_type = 'BASE TABLE'
               AND table_schema NOT IN ('pg_catalog','information_schema')) AS table_count,
            (SELECT count(*) FROM information_schema.tables
             WHERE table_type = 'VIEW'
               AND table_schema NOT IN ('pg_catalog','information_schema')) AS view_count,
            (SELECT count(*) FROM information_schema.table_constraints
             WHERE constraint_type = 'FOREIGN KEY') AS fk_count,
            (SELECT count(*) FROM pg_indexes
             WHERE schemaname NOT IN ('pg_catalog','information_schema')) AS index_count,
            (SELECT pg_size_pretty(pg_database_size(current_database()))) AS total_size
    """,
    )[0]

    extensions = query(
        env, "SELECT extname, extversion FROM pg_extension ORDER BY extname"
    )
    summary["extensions"] = [f"{e['extname']} {e['extversion']}" for e in extensions]
    summary["env"] = env

    # Materialized views
    mat_views = query(
        env,
        """
        SELECT count(*) AS cnt FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog','information_schema')
    """,
    )
    summary["materialized_view_count"] = mat_views[0]["cnt"]

    return summary


@mcp.tool()
def scan_schemas(env: str = DEFAULT_ENV) -> list[dict]:
    """Scan all user schemas and return row counts for every table.

    Returns a list of {schema, table, row_count} sorted by schema and table name.
    Skips internal PostgreSQL and TimescaleDB schemas.
    """
    env = resolve_env(env)
    sql = """
        SELECT table_schema AS schema, table_name AS tname
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN %s
        ORDER BY table_schema, table_name
    """
    tables = query(env, sql, (tuple(INTERNAL_SCHEMAS),))

    results = []
    with connect(env) as conn:
        with conn.cursor() as cur:
            for row in tables:
                cur.execute(
                    f"SELECT COUNT(*) AS row_count FROM {safe_ident(row['schema'])}.{safe_ident(row['tname'])}"
                )
                count = cur.fetchone()["row_count"]
                results.append(
                    {
                        "env": env,
                        "schema": row["schema"],
                        "table": row["tname"],
                        "row_count": count,
                        "status": "populated" if count > 0 else "empty",
                    }
                )
    return results


@mcp.tool()
def describe_table(
    table: str, schema: str = "public", env: str = DEFAULT_ENV
) -> list[dict]:
    """Describe a table's columns: name, data type, nullable, default value, and ordinal position."""
    env = resolve_env(env)
    safe_ident(schema)
    safe_ident(table)

    return query(
        env,
        """
        SELECT column_name, data_type, udt_name,
               is_nullable, column_default, ordinal_position,
               character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """,
        (schema, table),
    )


@mcp.tool()
def table_sizes(
    schema: str | None = None, env: str = DEFAULT_ENV, limit: int = 50
) -> list[dict]:
    """Show table sizes (data + indexes + toast) ordered by total size descending.

    Optionally filter by schema. Returns up to `limit` rows (max 200).
    """
    env = resolve_env(env)
    limit = _clamp_agg(limit)

    where = ""
    params: list = []
    if schema:
        safe_ident(schema)
        where = "WHERE schemaname = %s"
        params.append(schema)

    params.append(limit)
    return query(
        env,
        f"""
        SELECT schemaname AS schema,
               relname AS table,
               pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size,
               pg_total_relation_size(schemaname || '.' || relname) AS total_size_bytes,
               pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS data_size,
               pg_size_pretty(pg_indexes_size((schemaname || '.' || relname)::regclass)) AS index_size,
               n_live_tup AS estimated_rows
        FROM pg_stat_user_tables
        {where}
        ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
        LIMIT %s
    """,
        params,
    )


@mcp.tool()
def find_tables(pattern: str, env: str = DEFAULT_ENV) -> list[dict]:
    """Find tables whose name matches a LIKE pattern (case-insensitive).

    Example: find_tables('%price%') returns all tables with 'price' in the name.
    """
    env = resolve_env(env)
    return query(
        env,
        """
        SELECT table_schema AS schema, table_name AS table, table_type
        FROM information_schema.tables
        WHERE table_name ILIKE %s
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
    """,
        (pattern,),
    )


@mcp.tool()
def find_columns(column_pattern: str, env: str = DEFAULT_ENV) -> list[dict]:
    """Find all tables that have a column matching a LIKE pattern (case-insensitive).

    Example: find_columns('%email%') finds every table with an email-like column.
    """
    env = resolve_env(env)
    return query(
        env,
        """
        SELECT table_schema AS schema, table_name AS table,
               column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE column_name ILIKE %s
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position
    """,
        (column_pattern,),
    )


@mcp.tool()
def list_empty_tables(env: str = DEFAULT_ENV) -> list[dict]:
    """Return all tables with 0 rows in the given environment."""
    all_tables = scan_schemas(env=env)
    return [t for t in all_tables if t["row_count"] == 0]


@mcp.tool()
def list_environments() -> list[dict]:
    """List all configured PostgreSQL environments."""
    return [{"env": env, "configured": True} for env in AVAILABLE_ENVS]


# ═════════════════════════════════════════════════════════════════════════════
# Data Exploration
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def recent_rows(
    table: str,
    schema: str = "public",
    env: str = DEFAULT_ENV,
    limit: int = 20,
    order_by: str | None = None,
    order_dir: str = "DESC",
) -> list[dict]:
    """Fetch the most recent rows from a table.

    If order_by is not specified, the tool auto-detects a timestamp column
    (inserted_at, created_at, updated_at, timestamp) or falls back to the primary key.
    order_dir must be ASC or DESC.
    """
    env = resolve_env(env)
    limit = _clamp_row(limit)

    if order_dir.upper() not in ("ASC", "DESC"):
        raise ValueError("order_dir must be ASC or DESC")
    order_dir = order_dir.upper()

    s, t = safe_ident(schema), safe_ident(table)

    if order_by:
        order_col = safe_ident(order_by)
    else:
        # Auto-detect ordering column
        candidates = query(
            env,
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name IN ('inserted_at', 'created_at', 'updated_at', 'timestamp')
            ORDER BY ordinal_position
            LIMIT 1
        """,
            (schema, table),
        )

        if candidates:
            order_col = safe_ident(candidates[0]["column_name"])
        else:
            # Fall back to primary key
            pk = query(
                env,
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY array_position(i.indkey, a.attnum)
                LIMIT 1
            """,
                (f"{schema}.{table}",),
            )
            order_col = safe_ident(pk[0]["attname"]) if pk else None

    order_clause = f"ORDER BY {order_col} {order_dir}" if order_col else ""
    return query(env, f"SELECT * FROM {s}.{t} {order_clause} LIMIT %s", (limit,))


@mcp.tool()
def column_value_counts(
    table: str,
    column: str,
    schema: str = "public",
    env: str = DEFAULT_ENV,
    limit: int = 50,
) -> list[dict]:
    """Show distinct values and their frequency for a column.

    Useful for understanding cardinality and data distribution.
    """
    env = resolve_env(env)
    limit = _clamp_agg(limit)
    s, t, c = safe_ident(schema), safe_ident(table), safe_ident(column)

    return query(
        env,
        f"""
        SELECT {c} AS value, COUNT(*) AS count,
               ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct
        FROM {s}.{t}
        GROUP BY {c}
        ORDER BY count DESC
        LIMIT %s
    """,
        (limit,),
        timeout_s=QUERY_TIMEOUT_S,
    )


@mcp.tool()
def column_stats(
    table: str,
    column: str,
    schema: str = "public",
    env: str = DEFAULT_ENV,
) -> dict:
    """Get statistics for a column: min, max, avg (numeric), null count, distinct count, total rows."""
    env = resolve_env(env)
    s, t, c = safe_ident(schema), safe_ident(table), safe_ident(column)

    rows = query(
        env,
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT({c}) AS non_null_count,
            COUNT(*) - COUNT({c}) AS null_count,
            ROUND((COUNT(*) - COUNT({c})) * 100.0 / NULLIF(COUNT(*), 0), 2) AS null_pct,
            COUNT(DISTINCT {c}) AS distinct_count,
            MIN({c}::text) AS min_value,
            MAX({c}::text) AS max_value
        FROM {s}.{t}
    """,
        timeout_s=QUERY_TIMEOUT_S,
    )

    result = rows[0]

    # Try numeric avg
    try:
        avg_rows = query(
            env,
            f"SELECT ROUND(AVG({c}::numeric), 4) AS avg_value FROM {s}.{t}",
            timeout_s=QUERY_TIMEOUT_S,
        )
        result["avg_value"] = (
            str(avg_rows[0]["avg_value"])
            if avg_rows[0]["avg_value"] is not None
            else None
        )
    except Exception:
        result["avg_value"] = None

    return result


# ═════════════════════════════════════════════════════════════════════════════
# Relationships
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def list_constraints(
    table: str, schema: str = "public", env: str = DEFAULT_ENV
) -> list[dict]:
    """List all constraints (PK, unique, check, FK) for a table."""
    env = resolve_env(env)
    safe_ident(schema)
    safe_ident(table)

    return query(
        env,
        """
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns,
            ccu.table_schema AS ref_schema,
            ccu.table_name AS ref_table,
            STRING_AGG(DISTINCT ccu.column_name, ', ') AS ref_columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            AND tc.constraint_type = 'FOREIGN KEY'
        WHERE tc.table_schema = %s AND tc.table_name = %s
        GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_schema, ccu.table_name
        ORDER BY tc.constraint_type, tc.constraint_name
    """,
        (schema, table),
    )


@mcp.tool()
def foreign_keys(
    table: str,
    schema: str = "public",
    env: str = DEFAULT_ENV,
    direction: str = "both",
) -> dict:
    """Show foreign key relationships for a table.

    direction: 'outgoing' (this table references), 'incoming' (referenced by), or 'both'.
    """
    env = resolve_env(env)
    safe_ident(schema)
    safe_ident(table)

    if direction not in ("outgoing", "incoming", "both"):
        raise ValueError("direction must be 'outgoing', 'incoming', or 'both'")

    result = {}

    if direction in ("outgoing", "both"):
        result["outgoing"] = query(
            env,
            """
            SELECT
                tc.constraint_name,
                kcu.column_name AS column,
                ccu.table_schema AS ref_schema,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY kcu.column_name
        """,
            (schema, table),
        )

    if direction in ("incoming", "both"):
        result["incoming"] = query(
            env,
            """
            SELECT
                tc.constraint_name,
                tc.table_schema AS from_schema,
                tc.table_name AS from_table,
                kcu.column_name AS from_column,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
            WHERE ccu.table_schema = %s AND ccu.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_schema, tc.table_name
        """,
            (schema, table),
        )

    return result


@mcp.tool()
def compare_envs(table: str, schema: str = "public") -> list[dict]:
    """Compare row counts for a table across all configured environments.

    Returns [{env, row_count, status}] for each available environment.
    """
    results = []
    for env in AVAILABLE_ENVS:
        try:
            s, t = safe_ident(schema), safe_ident(table)
            rows = query(env, f"SELECT COUNT(*) AS cnt FROM {s}.{t}")
            count = rows[0]["cnt"]
            results.append(
                {"env": env, "schema": schema, "table": table, "row_count": count}
            )
        except Exception as e:
            results.append(
                {"env": env, "schema": schema, "table": table, "error": str(e)}
            )
    return results


# ═════════════════════════════════════════════════════════════════════════════
# Performance
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def index_usage(
    schema: str | None = None,
    env: str = DEFAULT_ENV,
    min_size_bytes: int = 0,
) -> list[dict]:
    """Show index usage statistics: scans, reads, size, and whether the index is actually used.

    Optionally filter by schema and minimum index size.
    """
    env = resolve_env(env)

    where_parts = ["schemaname NOT IN ('pg_catalog', 'information_schema')"]
    params: list = []

    if schema:
        safe_ident(schema)
        where_parts.append("schemaname = %s")
        params.append(schema)

    if min_size_bytes > 0:
        where_parts.append("pg_relation_size(indexrelid) >= %s")
        params.append(min_size_bytes)

    where = "WHERE " + " AND ".join(where_parts)

    return query(
        env,
        f"""
        SELECT
            schemaname AS schema,
            relname AS table,
            indexrelname AS index,
            idx_scan AS scans,
            idx_tup_read AS tuples_read,
            idx_tup_fetch AS tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
            pg_relation_size(indexrelid) AS index_size_bytes,
            CASE WHEN idx_scan = 0 THEN 'unused' ELSE 'used' END AS status
        FROM pg_stat_user_indexes
        {where}
        ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
        LIMIT 200
    """,
        params,
    )


@mcp.tool()
def slow_query_candidates(
    env: str = DEFAULT_ENV, min_seq_scan: int = 100
) -> list[dict]:
    """Find tables with high sequential scan counts relative to index scans.

    These are candidates for missing indexes or query optimisation.
    """
    env = resolve_env(env)
    return query(
        env,
        """
        SELECT
            schemaname AS schema,
            relname AS table,
            seq_scan,
            idx_scan,
            CASE WHEN idx_scan > 0
                 THEN ROUND(seq_scan::numeric / idx_scan, 2)
                 ELSE seq_scan END AS seq_to_idx_ratio,
            seq_tup_read,
            n_live_tup AS estimated_rows,
            pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size
        FROM pg_stat_user_tables
        WHERE seq_scan >= %s
        ORDER BY seq_scan DESC
        LIMIT 100
    """,
        (min_seq_scan,),
    )


@mcp.tool()
def bloat_estimate(env: str = DEFAULT_ENV, min_dead_tup: int = 1000) -> list[dict]:
    """Find tables with significant dead tuples that may benefit from VACUUM.

    Returns tables with at least min_dead_tup dead tuples, ordered by dead tuple count.
    """
    env = resolve_env(env)
    return query(
        env,
        """
        SELECT
            schemaname AS schema,
            relname AS table,
            n_live_tup AS live_tuples,
            n_dead_tup AS dead_tuples,
            CASE WHEN n_live_tup > 0
                 THEN ROUND(n_dead_tup * 100.0 / n_live_tup, 2)
                 ELSE 0 END AS dead_pct,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE n_dead_tup >= %s
        ORDER BY n_dead_tup DESC
        LIMIT 100
    """,
        (min_dead_tup,),
    )


# ═════════════════════════════════════════════════════════════════════════════
# Data Quality
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def table_health(table: str, schema: str = "public", env: str = DEFAULT_ENV) -> dict:
    """Get health stats for a specific table: row count, last inserted_at, last updated_at.

    Returns {env, schema, table, row_count, last_inserted_at, last_updated_at}.
    """
    env = resolve_env(env)

    s, t = safe_ident(schema), safe_ident(table)

    rows = query(env, f"SELECT COUNT(*) AS cnt FROM {s}.{t}")
    row_count = rows[0]["cnt"]

    cols = query(
        env,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND column_name IN ('inserted_at', 'updated_at', 'source_updated_at')
    """,
        (schema, table),
    )
    col_names = {r["column_name"] for r in cols}

    last_inserted_at = None
    last_updated_at = None

    if row_count > 0:
        if "inserted_at" in col_names:
            r = query(env, f"SELECT MAX(inserted_at) AS v FROM {s}.{t}")
            last_inserted_at = str(r[0]["v"]) if r[0]["v"] else None
        if "updated_at" in col_names:
            r = query(env, f"SELECT MAX(updated_at) AS v FROM {s}.{t}")
            last_updated_at = str(r[0]["v"]) if r[0]["v"] else None

    return {
        "env": env,
        "schema": schema,
        "table": table,
        "row_count": row_count,
        "status": "populated" if row_count > 0 else "empty",
        "last_inserted_at": last_inserted_at,
        "last_updated_at": last_updated_at,
    }


@mcp.tool()
def null_report(
    table: str, schema: str = "public", env: str = DEFAULT_ENV
) -> list[dict]:
    """For every column in a table, report the null count and null percentage.

    Useful for spotting data quality issues.
    """
    env = resolve_env(env)
    s, t = safe_ident(schema), safe_ident(table)

    cols = query(
        env,
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """,
        (schema, table),
    )

    if not cols:
        return [{"error": f"Table {schema}.{table} not found or has no columns"}]

    # Build a single query that checks all columns at once
    parts = []
    for col in cols:
        c = safe_ident(col["column_name"])
        parts.append(
            f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {safe_ident('null_' + col['column_name'])}"
        )

    count_sql = f"SELECT COUNT(*) AS total, {', '.join(parts)} FROM {s}.{t}"
    row = query(env, count_sql, timeout_s=QUERY_TIMEOUT_S)[0]
    total = row["total"]

    results = []
    for col in cols:
        null_count = row[f"null_{col['column_name']}"]
        results.append(
            {
                "column": col["column_name"],
                "null_count": null_count,
                "null_pct": round(null_count * 100.0 / total, 2) if total > 0 else 0,
                "total_rows": total,
            }
        )

    return sorted(results, key=lambda r: r["null_pct"], reverse=True)


@mcp.tool()
def duplicate_check(
    table: str,
    columns: list[str],
    schema: str = "public",
    env: str = DEFAULT_ENV,
    limit: int = 20,
) -> list[dict]:
    """Check for duplicate rows based on a set of columns.

    Returns groups of values that appear more than once, with their count.
    """
    env = resolve_env(env)
    limit = _clamp_row(limit)

    safe_cols = [safe_ident(c) for c in columns]
    col_list = ", ".join(safe_cols)
    s, t = safe_ident(schema), safe_ident(table)

    return query(
        env,
        f"""
        SELECT {col_list}, COUNT(*) AS duplicate_count
        FROM {s}.{t}
        GROUP BY {col_list}
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT %s
    """,
        (limit,),
        timeout_s=QUERY_TIMEOUT_S,
    )


# ═════════════════════════════════════════════════════════════════════════════
# Legacy — Pipeline-specific tools (kept for backward compatibility)
# ═════════════════════════════════════════════════════════════════════════════


@mcp.tool()
def ingestion_failures(
    env: str = DEFAULT_ENV,
    limit: int = 50,
    asset_name: str | None = None,
) -> list[dict]:
    """Inspect recent ingestion failures from pipeline.ingestion_failures.

    Optionally filter by asset_name. Returns the most recent failures first.
    """
    env = resolve_env(env)
    limit = _clamp_row(limit)

    exists = query(
        env,
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
    """,
    )
    if not exists:
        return [
            {"error": "pipeline.ingestion_failures table not found in this environment"}
        ]

    cols = query(
        env,
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
        ORDER BY ordinal_position
    """,
    )
    col_names = [r["column_name"] for r in cols]

    where = ""
    params: list = []
    if asset_name:
        filter_col = next(
            (c for c in col_names if c == "table_name"),
            next((c for c in col_names if "asset" in c.lower()), None),
        )
        if filter_col:
            where = f'WHERE "{filter_col}" ILIKE %s'
            params.append(f"%{asset_name}%")

    order_col = next(
        (c for c in col_names if c.endswith("_at") or c == "timestamp"), None
    )
    order = f'ORDER BY "{order_col}" DESC' if order_col else ""

    sql = f"SELECT * FROM pipeline.ingestion_failures {where} {order} LIMIT %s"
    params.append(limit)

    return query(env, sql, params)


@mcp.tool()
def ingestion_failures_summary(env: str = DEFAULT_ENV) -> list[dict]:
    """Summarise ingestion failures grouped by asset/error type with counts.

    Useful for identifying which assets are failing most frequently.
    """
    env = resolve_env(env)

    exists = query(
        env,
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
    """,
    )
    if not exists:
        return [{"error": "pipeline.ingestion_failures table not found"}]

    cols = query(
        env,
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
        ORDER BY ordinal_position
    """,
    )
    col_names = [r["column_name"] for r in cols]

    group_col = next(
        (c for c in col_names if c == "table_name"),
        next((c for c in col_names if "asset" in c.lower()), None),
    )
    if not group_col:
        return [{"error": "Cannot determine grouping column"}]

    comment_col = next((c for c in col_names if c == "comment"), None)
    if comment_col:
        return query(
            env,
            f"""
            SELECT "{group_col}" AS table_name, "{comment_col}" AS reason, COUNT(*) AS failures
            FROM pipeline.ingestion_failures
            GROUP BY "{group_col}", "{comment_col}"
            ORDER BY failures DESC
            LIMIT 50
        """,
        )

    return query(
        env,
        f"""
        SELECT "{group_col}" AS table_name, COUNT(*) AS failures
        FROM pipeline.ingestion_failures
        GROUP BY "{group_col}"
        ORDER BY failures DESC
        LIMIT 50
    """,
    )


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
