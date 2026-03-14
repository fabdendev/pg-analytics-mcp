"""Database helpers for pg-analytics-mcp.

Provides connection management, query execution, identifier sanitisation,
and environment resolution. All tools in server.py depend on this module.
"""

import os
import re
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────────

ENVS: dict[str, str] = {}
for _env, _var in [
    ("local", "PG_LOCAL_URL"),
    ("dev", "PG_DEV_URL"),
    ("stg", "PG_STG_URL"),
    ("prod", "PG_PROD_URL"),
]:
    _url = os.environ.get(_var, "")
    if _url:
        ENVS[_env] = _url.replace("postgresql+asyncpg://", "postgresql://")

if not ENVS:
    raise RuntimeError(
        "No PostgreSQL URLs configured. Set PG_LOCAL_URL, PG_DEV_URL, PG_STG_URL, or PG_PROD_URL."
    )

AVAILABLE_ENVS = list(ENVS.keys())
DEFAULT_ENV = AVAILABLE_ENVS[0]

INTERNAL_SCHEMAS = {
    "_timescaledb_cache",
    "_timescaledb_catalog",
    "_timescaledb_config",
    "_timescaledb_internal",
    "pg_catalog",
    "information_schema",
}

# ── Schema filtering ────────────────────────────────────────────────────────
# PG_INCLUDE_SCHEMAS=core,trading  → only scan these schemas (allowlist)
# PG_IGNORE_SCHEMAS=orion,shared   → skip these schemas in addition to internal ones
# If both set, PG_INCLUDE_SCHEMAS takes precedence.

_include_raw = os.environ.get("PG_INCLUDE_SCHEMAS", "").strip()
_ignore_raw = os.environ.get("PG_IGNORE_SCHEMAS", "").strip()

INCLUDE_SCHEMAS: set[str] | None = (
    {s.strip() for s in _include_raw.split(",") if s.strip()} if _include_raw else None
)
EXCLUDED_SCHEMAS: set[str] = INTERNAL_SCHEMAS | (
    {s.strip() for s in _ignore_raw.split(",") if s.strip()} if _ignore_raw else set()
)


# ── Fail table tracking (opt-in) ────────────────────────────────────────────
# PG_FAIL_SCHEMA=pipeline → enables pipeline_fail_* tools for that schema
# Tables must match *_fails and have columns: run_id, stage, comment, failed_at

FAIL_SCHEMA: str | None = os.environ.get("PG_FAIL_SCHEMA", "").strip() or None


def schema_filter(col: str = "table_schema") -> tuple[str, tuple]:
    """Return (sql_fragment, params) for WHERE clause filtering schemas.

    If PG_INCLUDE_SCHEMAS is set, returns ``col IN %s``.
    Otherwise returns ``col NOT IN %s`` using EXCLUDED_SCHEMAS.
    """
    safe_ident(col.split(".")[-1])  # validate identifier
    if INCLUDE_SCHEMAS:
        return f"{col} IN %s", (tuple(INCLUDE_SCHEMAS),)
    return f"{col} NOT IN %s", (tuple(EXCLUDED_SCHEMAS),)


# ── Identifier safety ────────────────────────────────────────────────────────

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def safe_ident(name: str) -> str:
    """Validate and quote a SQL identifier to prevent injection."""
    if not _IDENT_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


# ── Environment resolution ───────────────────────────────────────────────────


def resolve_env(env: str) -> str:
    env = env.lower()
    if env not in ENVS:
        raise ValueError(
            f"Unknown environment '{env}'. Available: {', '.join(AVAILABLE_ENVS)}"
        )
    return env


# ── Connection & query ───────────────────────────────────────────────────────


@contextmanager
def connect(
    env: str, timeout_s: int = 0
) -> Generator[psycopg2.extensions.connection, None, None]:
    """Open a read-only connection. Optional statement_timeout in seconds."""
    conn = psycopg2.connect(ENVS[env], cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        if timeout_s > 0:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = '{timeout_s}s'")
        yield conn
    finally:
        conn.close()


def query(env: str, sql: str, params=None, timeout_s: int = 0) -> list[dict]:
    """Execute a read-only query and return rows as dicts."""
    with connect(env, timeout_s=timeout_s) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
