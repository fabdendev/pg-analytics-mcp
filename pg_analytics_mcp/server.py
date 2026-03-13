"""pg-analytics-mcp — PostgreSQL analytics MCP server.

Provides semantic tools for schema health, row counts, ingestion monitoring,
and multi-environment comparison. Read-only by default.

Environment variables:
    PG_DEV_URL    — PostgreSQL DSN for DEV  (at least one URL required)
    PG_STG_URL    — PostgreSQL DSN for STG  (optional)
    PG_PROD_URL   — PostgreSQL DSN for PROD (optional)
    PG_READ_ONLY  — reserved for future write tools (not yet used)
"""

import os
import re
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
from fastmcp import FastMCP

# ── Config ────────────────────────────────────────────────────────────────────

ENVS: dict[str, str] = {}
for _env, _var in [("dev", "PG_DEV_URL"), ("stg", "PG_STG_URL"), ("prod", "PG_PROD_URL")]:
    _url = os.environ.get(_var, "")
    if _url:
        ENVS[_env] = _url.replace("postgresql+asyncpg://", "postgresql://")

if not ENVS:
    raise RuntimeError(
        "No PostgreSQL URLs configured. Set PG_DEV_URL, PG_STG_URL, or PG_PROD_URL."
    )

AVAILABLE_ENVS = list(ENVS.keys())
DEFAULT_ENV = AVAILABLE_ENVS[0]

INTERNAL_SCHEMAS = {
    "_timescaledb_cache", "_timescaledb_catalog", "_timescaledb_config",
    "_timescaledb_internal", "pg_catalog", "information_schema", "public",
}

mcp = FastMCP(
    "pg-analytics",
    instructions=(
        f"PostgreSQL analytics tools. Available environments: {', '.join(AVAILABLE_ENVS)}. "
        f"Default environment: {DEFAULT_ENV}. "
        "Use scan_schemas for an overview, table_health for details on a specific table, "
        "ingestion_failures to inspect pipeline errors, and compare_envs to diff two environments.\n\n"
        "When working with tool results, write down any important information you might need later "
        "in your response, as the original tool result may be cleared later.\n\n"
        "IMPORTANT: Always present results and analysis in English, regardless of the user's language."
    ),
)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _resolve_env(env: str) -> str:
    env = env.lower()
    if env not in ENVS:
        raise ValueError(f"Unknown environment '{env}'. Available: {', '.join(AVAILABLE_ENVS)}")
    return env


@contextmanager
def _connect(env: str) -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(ENVS[env], cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def _query(env: str, sql: str, params=None) -> list[dict]:
    with _connect(env) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_ident(name: str) -> str:
    """Validate and quote a SQL identifier to prevent injection."""
    if not _IDENT_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def scan_schemas(env: str = DEFAULT_ENV) -> list[dict]:
    """Scan all user schemas and return row counts for every table.

    Returns a list of {schema, table, row_count} sorted by schema and table name.
    Skips internal PostgreSQL and TimescaleDB schemas.
    """
    env = _resolve_env(env)
    sql = """
        SELECT table_schema AS schema, table_name AS tname
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN %s
        ORDER BY table_schema, table_name
    """
    tables = _query(env, sql, (tuple(INTERNAL_SCHEMAS),))

    results = []
    with _connect(env) as conn:
        with conn.cursor() as cur:
            for row in tables:
                cur.execute(
                    f"SELECT COUNT(*) AS row_count FROM {_safe_ident(row['schema'])}.{_safe_ident(row['tname'])}"
                )
                count = cur.fetchone()["row_count"]
                results.append({
                    "env": env,
                    "schema": row["schema"],
                    "table": row["tname"],
                    "row_count": count,
                    "status": "populated" if count > 0 else "empty",
                })
    return results


@mcp.tool()
def table_health(table: str, schema: str = "core", env: str = DEFAULT_ENV) -> dict:
    """Get health stats for a specific table: row count, last inserted_at, nulls on key columns.

    Returns {env, schema, table, row_count, last_inserted_at, last_updated_at, null_counts}.
    """
    env = _resolve_env(env)

    s, t = _safe_ident(schema), _safe_ident(table)

    # Row count
    rows = _query(env, f"SELECT COUNT(*) AS cnt FROM {s}.{t}")
    row_count = rows[0]["cnt"]

    # Check which timestamp columns exist
    cols = _query(env, """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND column_name IN ('inserted_at', 'updated_at', 'source_updated_at')
    """, (schema, table))
    col_names = {r["column_name"] for r in cols}

    last_inserted_at = None
    last_updated_at = None

    if row_count > 0:
        if "inserted_at" in col_names:
            r = _query(env, f"SELECT MAX(inserted_at) AS v FROM {s}.{t}")
            last_inserted_at = str(r[0]["v"]) if r[0]["v"] else None
        if "updated_at" in col_names:
            r = _query(env, f"SELECT MAX(updated_at) AS v FROM {s}.{t}")
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
def ingestion_failures(
    env: str = DEFAULT_ENV,
    limit: int = 50,
    asset_name: str | None = None,
) -> list[dict]:
    """Inspect recent ingestion failures from pipeline.ingestion_failures.

    Optionally filter by asset_name. Returns the most recent failures first.
    """
    env = _resolve_env(env)

    # Check table exists
    exists = _query(env, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
    """)
    if not exists:
        return [{"error": "pipeline.ingestion_failures table not found in this environment"}]

    # Discover columns
    cols = _query(env, """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
        ORDER BY ordinal_position
    """)
    col_names = [r["column_name"] for r in cols]

    where = ""
    params: list = []
    if asset_name:
        # Prefer table_name, then fall back to asset-like columns
        filter_col = next(
            (c for c in col_names if c == "table_name"),
            next((c for c in col_names if "asset" in c.lower()), None),
        )
        if filter_col:
            where = f'WHERE "{filter_col}" ILIKE %s'
            params.append(f"%{asset_name}%")

    order_col = next((c for c in col_names if c.endswith("_at") or c == "timestamp"), None)
    order = f'ORDER BY "{order_col}" DESC' if order_col else ""

    sql = f'SELECT * FROM pipeline.ingestion_failures {where} {order} LIMIT %s'
    params.append(limit)

    return _query(env, sql, params)


@mcp.tool()
def ingestion_failures_summary(env: str = DEFAULT_ENV) -> list[dict]:
    """Summarise ingestion failures grouped by asset/error type with counts.

    Useful for identifying which assets are failing most frequently.
    """
    env = _resolve_env(env)

    exists = _query(env, """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
    """)
    if not exists:
        return [{"error": "pipeline.ingestion_failures table not found"}]

    cols = _query(env, """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'pipeline' AND table_name = 'ingestion_failures'
        ORDER BY ordinal_position
    """)
    col_names = [r["column_name"] for r in cols]

    # Prefer table_name, then fall back to asset-like columns
    group_col = next(
        (c for c in col_names if c == "table_name"),
        next((c for c in col_names if "asset" in c.lower()), None),
    )
    if not group_col:
        return [{"error": "Cannot determine grouping column"}]

    # Also group by comment (constraint name) if available
    comment_col = next((c for c in col_names if c == "comment"), None)
    if comment_col:
        return _query(env, f"""
            SELECT "{group_col}" AS table_name, "{comment_col}" AS reason, COUNT(*) AS failures
            FROM pipeline.ingestion_failures
            GROUP BY "{group_col}", "{comment_col}"
            ORDER BY failures DESC
            LIMIT 50
        """)

    return _query(env, f"""
        SELECT "{group_col}" AS table_name, COUNT(*) AS failures
        FROM pipeline.ingestion_failures
        GROUP BY "{group_col}"
        ORDER BY failures DESC
        LIMIT 50
    """)


@mcp.tool()
def compare_envs(table: str, schema: str = "core") -> list[dict]:
    """Compare row counts for a table across all configured environments.

    Returns [{env, row_count, status}] for each available environment.
    """
    results = []
    for env in AVAILABLE_ENVS:
        try:
            s, t = _safe_ident(schema), _safe_ident(table)
            rows = _query(env, f"SELECT COUNT(*) AS cnt FROM {s}.{t}")
            count = rows[0]["cnt"]
            results.append({"env": env, "schema": schema, "table": table, "row_count": count})
        except Exception as e:
            results.append({"env": env, "schema": schema, "table": table, "error": str(e)})
    return results


@mcp.tool()
def list_empty_tables(env: str = DEFAULT_ENV) -> list[dict]:
    """Return all tables with 0 rows in the given environment."""
    all_tables = scan_schemas(env=env)
    return [t for t in all_tables if t["row_count"] == 0]


@mcp.tool()
def list_environments() -> list[dict]:
    """List all configured PostgreSQL environments."""
    return [{"env": env, "configured": True} for env in AVAILABLE_ENVS]


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
