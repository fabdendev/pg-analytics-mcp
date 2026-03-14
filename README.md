# pg-analytics-mcp

An [MCP](https://modelcontextprotocol.io/) server for **PostgreSQL analytics** — a general-purpose "DBA-lite" toolkit that gives any MCP client (Claude Code, Cursor, etc.) instant visibility into schema structure, data quality, relationships, performance, and multi-environment comparison.

## What it does

Exposes **26 read-only tools** organised in 7 categories:

### Schema Discovery
- **`database_summary`** — high-level overview: schema/table/view/FK/index counts, total size, extensions
- **`scan_schemas`** — row counts for every table, grouped by schema
- **`describe_table`** — column details: name, type, nullable, default, position
- **`table_sizes`** — disk usage (data + indexes + toast) per table, ordered by size
- **`find_tables`** — search tables by name pattern (ILIKE)
- **`find_columns`** — find tables that have a column matching a pattern
- **`list_empty_tables`** — quickly find tables with 0 rows
- **`list_environments`** — list configured environments

### Data Exploration
- **`recent_rows`** — peek at the most recent rows (auto-detects timestamp/PK ordering)
- **`column_value_counts`** — distinct values and frequencies for a column
- **`column_stats`** — min, max, avg, null count, distinct count for a column

### Relationships
- **`list_constraints`** — all constraints (PK, unique, check, FK) for a table
- **`foreign_keys`** — bidirectional FK relationships (incoming + outgoing)
- **`compare_envs`** — compare row counts across DEV / STG / PROD

### Performance
- **`index_usage`** — index scan stats and unused index detection
- **`slow_query_candidates`** — tables with high sequential scan counts (missing index candidates)
- **`bloat_estimate`** — tables with dead tuples that may need VACUUM

### Data Quality
- **`table_health`** — row count + last inserted_at/updated_at for a table
- **`null_report`** — null percentage for every column in a table
- **`duplicate_check`** — find duplicate rows based on a set of columns

### Pipeline Failures (v2)
- **`pipeline_fail_tables`** — discover all `pipeline.*_fails` tables with row counts and stats
- **`pipeline_fail_summary`** — cross-entity failure summary grouped by entity, stage, or both
- **`pipeline_fail_details`** — drill into a specific entity's fail table with optional filters
- **`pipeline_fail_runs`** — analyse which pipeline runs generated the most failures

### Legacy (pipeline-specific)
- **`ingestion_failures`** — recent records from `pipeline.ingestion_failures` (legacy monolithic table)
- **`ingestion_failures_summary`** — failures grouped by table + error reason (legacy monolithic table)

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
| `PG_LOCAL_URL` | PostgreSQL DSN for LOCAL | At least one URL |
| `PG_DEV_URL` | PostgreSQL DSN for DEV | At least one URL |
| `PG_STG_URL` | PostgreSQL DSN for STG | Optional |
| `PG_PROD_URL` | PostgreSQL DSN for PROD | Optional |
| `PG_INCLUDE_SCHEMAS` | Comma-separated allowlist of schemas to scan | Optional |
| `PG_IGNORE_SCHEMAS` | Comma-separated schemas to skip (added to internal exclusions) | Optional |
| `PG_READ_ONLY` | Reserved for future write tools (not yet used) | Optional |

```bash
export PG_DEV_URL="postgresql://user:pass@host:5432/dbname"
export PG_STG_URL="postgresql://user:pass@host:5432/dbname"   # optional
export PG_PROD_URL="postgresql://user:pass@host:5432/dbname"  # optional

# Schema filtering (optional — pick one, not both)
export PG_INCLUDE_SCHEMAS="core,trading,pipeline"  # only scan these
export PG_IGNORE_SCHEMAS="orion,shared"             # skip these
```

Supports `postgresql+asyncpg://` URLs (the driver prefix is stripped automatically).
If both `PG_INCLUDE_SCHEMAS` and `PG_IGNORE_SCHEMAS` are set, the include list takes precedence.

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

## Security

All tools are **read-only**. No data is ever modified. Additional safeguards:

- **Identifier validation** — all user-provided schema/table/column names are validated against `^[a-zA-Z_][a-zA-Z0-9_]*$` and quoted
- **Row limits** — row-level queries capped at 100, aggregation queries at 200
- **Statement timeout** — potentially expensive queries (null_report, column_stats, duplicate_check, column_value_counts) use a 30s timeout
- **Direction validation** — order_dir restricted to ASC/DESC only

## Multi-environment support

Configure up to 4 environments (LOCAL, DEV, STG, PROD). The first configured environment becomes the default. Use `compare_envs` to quickly spot row count differences across environments.

## Development

```bash
uv sync --extra dev
uv run ruff check pg_analytics_mcp/    # lint
uv run python -m pg_analytics_mcp      # start server locally
```

## License

MIT
