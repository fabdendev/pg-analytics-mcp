"""pg-analytics-mcp — PostgreSQL analytics MCP server.

A general-purpose "DBA-lite" MCP server that provides semantic tools for
schema discovery, data exploration, relationship mapping, performance
analysis, and data quality checks on any PostgreSQL database.

Environment variables:
    PG_LOCAL_URL       — PostgreSQL DSN for LOCAL (optional)
    PG_DEV_URL         — PostgreSQL DSN for DEV   (at least one URL required)
    PG_STG_URL         — PostgreSQL DSN for STG   (optional)
    PG_PROD_URL        — PostgreSQL DSN for PROD  (optional)
    PG_INCLUDE_SCHEMAS — comma-separated allowlist of schemas to scan (optional)
    PG_IGNORE_SCHEMAS  — comma-separated schemas to skip (optional)
    PG_READ_ONLY       — reserved for future write tools (not yet used)
"""

import re

from fastmcp import FastMCP

from pg_analytics_mcp.db import (
    AVAILABLE_ENVS,
    DEFAULT_ENV,
    EXCLUDED_SCHEMAS,
    INCLUDE_SCHEMAS,
    INTERNAL_SCHEMAS,
    connect,
    query,
    resolve_env,
    safe_ident,
    schema_filter,
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
    view count, FK count, index count, total size, and installed extensions.

    Respects PG_INCLUDE_SCHEMAS / PG_IGNORE_SCHEMAS filtering.
    """
    env = resolve_env(env)
    filt, filt_params = schema_filter("table_schema")
    filt_s, filt_s_params = schema_filter("schemaname")

    summary = {}

    r = query(
        env,
        f"SELECT count(DISTINCT table_schema) AS v FROM information_schema.tables WHERE {filt}",
        filt_params,
    )
    summary["schema_count"] = r[0]["v"]

    r = query(
        env,
        f"SELECT count(*) AS v FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND {filt}",
        filt_params,
    )
    summary["table_count"] = r[0]["v"]

    r = query(
        env,
        f"SELECT count(*) AS v FROM information_schema.tables WHERE table_type = 'VIEW' AND {filt}",
        filt_params,
    )
    summary["view_count"] = r[0]["v"]

    r = query(
        env,
        "SELECT count(*) AS v FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY'",
    )
    summary["fk_count"] = r[0]["v"]

    r = query(
        env,
        f"SELECT count(*) AS v FROM pg_indexes WHERE {filt_s}",
        filt_s_params,
    )
    summary["index_count"] = r[0]["v"]

    r = query(
        env,
        "SELECT pg_size_pretty(pg_database_size(current_database())) AS v",
    )
    summary["total_size"] = r[0]["v"]

    extensions = query(
        env, "SELECT extname, extversion FROM pg_extension ORDER BY extname"
    )
    summary["extensions"] = [f"{e['extname']} {e['extversion']}" for e in extensions]
    summary["env"] = env

    r = query(
        env,
        f"SELECT count(*) AS v FROM pg_matviews WHERE {filt_s}",
        filt_s_params,
    )
    summary["materialized_view_count"] = r[0]["v"]

    if INCLUDE_SCHEMAS:
        summary["schema_filter"] = f"include: {', '.join(sorted(INCLUDE_SCHEMAS))}"
    elif EXCLUDED_SCHEMAS - INTERNAL_SCHEMAS:
        summary["schema_filter"] = (
            f"ignore: {', '.join(sorted(EXCLUDED_SCHEMAS - INTERNAL_SCHEMAS))}"
        )

    return summary


@mcp.tool()
def scan_schemas(env: str = DEFAULT_ENV) -> list[dict]:
    """Scan all user schemas and return row counts for every table.

    Returns a list of {schema, table, row_count} sorted by schema and table name.
    Respects PG_INCLUDE_SCHEMAS / PG_IGNORE_SCHEMAS filtering.
    """
    env = resolve_env(env)
    filt, filt_params = schema_filter("table_schema")
    sql = f"""
        SELECT table_schema AS schema, table_name AS tname
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND {filt}
        ORDER BY table_schema, table_name
    """
    tables = query(env, sql, filt_params)

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

    where_parts = [
        "n.nspname NOT LIKE 'pg_temp%%'",
        "n.nspname NOT LIKE 'pg_toast%%'",
    ]
    params: list = []
    if schema:
        safe_ident(schema)
        where_parts.append("n.nspname = %s")
        params.append(schema)

    where = "WHERE " + " AND ".join(where_parts)
    params.append(limit)
    return query(
        env,
        f"""
        SELECT n.nspname AS schema,
               c.relname AS table,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
               pg_total_relation_size(c.oid) AS total_size_bytes,
               pg_size_pretty(pg_relation_size(c.oid)) AS data_size,
               pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
               c.reltuples::bigint AS estimated_rows
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        {where}
          AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
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
    filt, filt_params = schema_filter("table_schema")
    return query(
        env,
        f"""
        SELECT table_schema AS schema, table_name AS table, table_type
        FROM information_schema.tables
        WHERE table_name ILIKE %s
          AND {filt}
        ORDER BY table_schema, table_name
    """,
        (pattern, *filt_params),
    )


@mcp.tool()
def find_columns(column_pattern: str, env: str = DEFAULT_ENV) -> list[dict]:
    """Find all tables that have a column matching a LIKE pattern (case-insensitive).

    Example: find_columns('%email%') finds every table with an email-like column.
    """
    env = resolve_env(env)
    filt, filt_params = schema_filter("table_schema")
    return query(
        env,
        f"""
        SELECT table_schema AS schema, table_name AS table,
               column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE column_name ILIKE %s
          AND {filt}
        ORDER BY table_schema, table_name, ordinal_position
    """,
        (column_pattern, *filt_params),
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

    filt, filt_params = schema_filter("schemaname")
    where_parts = [filt]
    params: list = list(filt_params)

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
# Pipeline Failures (v2) — Multi-table fail tracking
# ═════════════════════════════════════════════════════════════════════════════

_FAIL_TABLE_REQUIRED_COLS = {"run_id", "stage", "comment", "failed_at"}
_UUID_RE = re.compile(r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$", re.I)


def _validate_uuid(value: str) -> str:
    """Validate UUID format, raise ValueError if invalid."""
    if not _UUID_RE.match(value):
        raise ValueError(f"Invalid UUID format: {value}")
    return value


def _clamp_days(days: int) -> int:
    """Clamp days to 1-90 range."""
    return max(1, min(days, 90))


def _discover_fail_tables(env: str) -> list[dict]:
    """Discover pipeline.*_fails tables with required metadata columns."""
    rows = query(
        env,
        """
        SELECT t.table_name
        FROM information_schema.tables t
        WHERE t.table_schema = 'pipeline'
          AND t.table_name LIKE '%%\\_fails'
          AND (SELECT COUNT(*) FROM information_schema.columns c
               WHERE c.table_schema = t.table_schema
                 AND c.table_name = t.table_name
                 AND c.column_name IN ('run_id','stage','comment','failed_at')) = 4
        ORDER BY t.table_name
        """,
    )
    return [
        {
            "table": r["table_name"],
            "entity": r["table_name"].removesuffix("_fails"),
        }
        for r in rows
    ]


@mcp.tool()
def pipeline_fail_tables(env: str = DEFAULT_ENV) -> list[dict]:
    """Discover all pipeline.*_fails tables and their stats.

    Returns per table: entity, table, row_count, latest_failure, oldest_failure,
    distinct_stages, distinct_runs.
    """
    env = resolve_env(env)
    tables = _discover_fail_tables(env)
    if not tables:
        return [{"info": "No pipeline.*_fails tables found in this environment"}]

    results = []
    with connect(env) as conn:
        with conn.cursor() as cur:
            for t in tables:
                tname = safe_ident(t["table"])
                cur.execute(
                    f"""
                    SELECT COUNT(*) AS row_count,
                           MAX(failed_at) AS latest_failure,
                           MIN(failed_at) AS oldest_failure,
                           COUNT(DISTINCT stage) AS distinct_stages,
                           COUNT(DISTINCT run_id) AS distinct_runs
                    FROM pipeline.{tname}
                    """
                )
                row = cur.fetchone()
                results.append(
                    {
                        "entity": t["entity"],
                        "table": f"pipeline.{t['table']}",
                        "row_count": row["row_count"],
                        "latest_failure": str(row["latest_failure"])
                        if row["latest_failure"]
                        else None,
                        "oldest_failure": str(row["oldest_failure"])
                        if row["oldest_failure"]
                        else None,
                        "distinct_stages": row["distinct_stages"],
                        "distinct_runs": row["distinct_runs"],
                    }
                )
    return results


@mcp.tool()
def pipeline_fail_summary(
    env: str = DEFAULT_ENV,
    days: int = 7,
    group_by: str = "entity",
) -> list[dict]:
    """Summarise pipeline failures across all *_fails tables.

    Groups by 'entity', 'stage', or 'entity_stage'. Looks back `days` days (1-90).
    """
    env = resolve_env(env)
    days = _clamp_days(days)

    if group_by not in ("entity", "stage", "entity_stage"):
        raise ValueError("group_by must be 'entity', 'stage', or 'entity_stage'")

    tables = _discover_fail_tables(env)
    if not tables:
        return [{"info": "No pipeline.*_fails tables found in this environment"}]

    unions = []
    for t in tables:
        tname = safe_ident(t["table"])
        entity_literal = t["entity"].replace("'", "''")
        unions.append(
            f"SELECT '{entity_literal}' AS entity, stage, run_id, failed_at "
            f"FROM pipeline.{tname} "
            f"WHERE failed_at >= NOW() - INTERVAL '{days} days'"
        )

    union_sql = " UNION ALL ".join(unions)

    if group_by == "entity":
        select = "entity"
        group = "entity"
        order = "total_failures DESC"
    elif group_by == "stage":
        select = "stage"
        group = "stage"
        order = "total_failures DESC"
    else:
        select = "entity, stage"
        group = "entity, stage"
        order = "total_failures DESC"

    sql = f"""
        SELECT {select},
               COUNT(*) AS total_failures,
               COUNT(DISTINCT run_id) AS distinct_runs,
               MIN(failed_at) AS earliest,
               MAX(failed_at) AS latest
        FROM ({union_sql}) sub
        GROUP BY {group}
        ORDER BY {order}
        LIMIT 200
    """
    rows = query(env, sql)
    for r in rows:
        if r.get("earliest"):
            r["earliest"] = str(r["earliest"])
        if r.get("latest"):
            r["latest"] = str(r["latest"])
    return rows


@mcp.tool()
def pipeline_fail_details(
    entity: str,
    env: str = DEFAULT_ENV,
    limit: int = 50,
    stage: str | None = None,
    run_id: str | None = None,
    days: int | None = None,
) -> list[dict]:
    """Drill into failure details for a specific entity's fail table.

    entity: e.g. 'invoices' (looks up pipeline.invoices_fails).
    Optional filters: stage, run_id (UUID), days (time window).
    """
    env = resolve_env(env)
    limit = _clamp_row(limit)

    table_name = f"{entity}_fails"
    safe_table = safe_ident(table_name)

    # Verify table exists
    exists = query(
        env,
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pipeline' AND table_name = %s
        """,
        (table_name,),
    )
    if not exists:
        return [{"error": f"Table pipeline.{table_name} not found in this environment"}]

    where_parts: list[str] = []
    params: list = []

    if stage:
        where_parts.append("stage = %s")
        params.append(stage)

    if run_id:
        _validate_uuid(run_id)
        where_parts.append("run_id = %s")
        params.append(run_id)

    if days is not None:
        days = _clamp_days(days)
        where_parts.append(f"failed_at >= NOW() - INTERVAL '{days} days'")

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    params.append(limit)

    return query(
        env,
        f"SELECT * FROM pipeline.{safe_table} {where} ORDER BY failed_at DESC LIMIT %s",
        params,
    )


@mcp.tool()
def pipeline_fail_runs(
    env: str = DEFAULT_ENV,
    entity: str | None = None,
    limit: int = 50,
    days: int = 7,
) -> list[dict]:
    """Analyse which pipeline runs generated the most failures.

    Optionally scope to a single entity. Looks back `days` days (1-90).
    Returns: run_id, entities_affected, total_failures, stages, earliest_failure, latest_failure.
    """
    env = resolve_env(env)
    limit = _clamp_agg(limit)
    days = _clamp_days(days)

    if entity:
        table_name = f"{entity}_fails"
        safe_table = safe_ident(table_name)
        exists = query(
            env,
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'pipeline' AND table_name = %s
            """,
            (table_name,),
        )
        if not exists:
            return [{"error": f"Table pipeline.{table_name} not found"}]

        entity_literal = entity.replace("'", "''")
        unions = [
            f"SELECT '{entity_literal}' AS entity, run_id, stage, failed_at "
            f"FROM pipeline.{safe_table} "
            f"WHERE failed_at >= NOW() - INTERVAL '{days} days'"
        ]
    else:
        tables = _discover_fail_tables(env)
        if not tables:
            return [{"info": "No pipeline.*_fails tables found in this environment"}]
        unions = []
        for t in tables:
            tname = safe_ident(t["table"])
            entity_literal = t["entity"].replace("'", "''")
            unions.append(
                f"SELECT '{entity_literal}' AS entity, run_id, stage, failed_at "
                f"FROM pipeline.{tname} "
                f"WHERE failed_at >= NOW() - INTERVAL '{days} days'"
            )

    union_sql = " UNION ALL ".join(unions)

    sql = f"""
        SELECT run_id,
               COUNT(DISTINCT entity) AS entities_affected,
               COUNT(*) AS total_failures,
               STRING_AGG(DISTINCT stage, ', ' ORDER BY stage) AS stages,
               MIN(failed_at) AS earliest_failure,
               MAX(failed_at) AS latest_failure
        FROM ({union_sql}) sub
        GROUP BY run_id
        ORDER BY total_failures DESC
        LIMIT %s
    """
    rows = query(env, sql, (limit,))
    for r in rows:
        if r.get("run_id"):
            r["run_id"] = str(r["run_id"])
        if r.get("earliest_failure"):
            r["earliest_failure"] = str(r["earliest_failure"])
        if r.get("latest_failure"):
            r["latest_failure"] = str(r["latest_failure"])
    return rows


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
