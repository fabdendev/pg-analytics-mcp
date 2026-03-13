# pg-analytics-mcp

An [MCP](https://modelcontextprotocol.io/) server for **PostgreSQL analytics** — gives any MCP client (Claude Code, Cursor, etc.) instant visibility into schema health, row counts, ingestion monitoring, and multi-environment comparison.

## What it does

Exposes 7 read-only tools that let you inspect and monitor one or more PostgreSQL databases:

- **Schema scanning** — row counts for every table, grouped by schema
- **Table health** — row count, last `inserted_at`/`updated_at` for a specific table
- **Ingestion monitoring** — recent failures and failure summaries from a `pipeline.ingestion_failures` table
- **Multi-env comparison** — compare row counts across DEV / STG / PROD
- **Empty table detection** — quickly find tables with 0 rows

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- One or more PostgreSQL instances

### Install

**Option A — run directly with uvx (no clone needed):**

```bash
uvx pg-analytics-mcp
```

**Option B — clone and run:**

```bash
git clone https://github.com/fabdendev/pg-analytics-mcp.git
cd pg-analytics-mcp
uv sync
```

### Configure

Set environment variables for each PostgreSQL environment (at least one is required):

| Variable | Description | Required |
|----------|-------------|----------|
| `PG_DEV_URL` | PostgreSQL DSN for DEV | At least one URL |
| `PG_STG_URL` | PostgreSQL DSN for STG | Optional |
| `PG_PROD_URL` | PostgreSQL DSN for PROD | Optional |
| `PG_READ_ONLY` | Reserved for future write tools (not yet used) | Optional |

```bash
export PG_DEV_URL="postgresql://user:pass@host:5432/dbname"
export PG_STG_URL="postgresql://user:pass@host:5432/dbname"   # optional
export PG_PROD_URL="postgresql://user:pass@host:5432/dbname"  # optional
```

Supports `postgresql+asyncpg://` URLs (the driver prefix is stripped automatically).

### Add to Claude Code

Add to your Claude Code MCP settings (`~/.claude/settings.json` or `.mcp.json`):

**If using uvx:**

```json
{
  "mcpServers": {
    "pg-analytics": {
      "command": "uvx",
      "args": ["pg-analytics-mcp"],
      "env": {
        "PG_DEV_URL": "postgresql://user:pass@host:5432/dbname",
        "PG_STG_URL": "postgresql://user:pass@host:5432/dbname"
      }
    }
  }
}
```

**If installed from clone:**

```json
{
  "mcpServers": {
    "pg-analytics": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/pg-analytics-mcp", "pg-analytics-mcp"],
      "env": {
        "PG_DEV_URL": "postgresql://user:pass@host:5432/dbname"
      }
    }
  }
}
```

## Tools

| Tool | Description |
|------|-------------|
| `scan_schemas` | Row counts for all tables, grouped by schema |
| `table_health` | Row count + last `inserted_at`/`updated_at` for a specific table |
| `ingestion_failures` | Recent records from `pipeline.ingestion_failures` (filterable by asset) |
| `ingestion_failures_summary` | Failures grouped by table + error reason with counts |
| `compare_envs` | Compare row counts for a table across all configured environments |
| `list_empty_tables` | All tables with 0 rows in a given environment |
| `list_environments` | List which environments are configured |

All tools are **read-only**. No data is ever modified.

## How it works

The server connects to each configured PostgreSQL instance using `psycopg2` and runs read-only queries against `information_schema` and your application tables. Internal schemas (TimescaleDB, `pg_catalog`, `information_schema`, `public`) are filtered out by default.

The `ingestion_failures` and `ingestion_failures_summary` tools expect a `pipeline.ingestion_failures` table — if it doesn't exist in your database, they return a clear error message instead of failing.

## Multi-environment support

Configure up to 3 environments (DEV, STG, PROD). The first configured environment becomes the default. Use `compare_envs` to quickly spot row count differences across environments — useful for verifying that ETL pipelines are running consistently.

## Development

```bash
uv sync --extra dev
uv run ruff check pg_analytics_mcp/    # lint
uv run python -m pg_analytics_mcp      # start server locally
```

## License

MIT
