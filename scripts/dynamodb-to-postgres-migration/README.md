# DynamoDB → PostgreSQL Migration

One-shot migration tool that reads **issues** and **move mappings** from a
DynamoDB single-table design and writes them into a fresh PostgreSQL database
using the `slackmgr/plugins/postgres` plugin.

Alerts and channel processing state records are intentionally skipped:
alerts are debug-only, and processing state is transient operational data that
the manager will recreate on its own.

## Pre-conditions

In both normal and dry-run mode the script runs schema migrations first (safe
to run on an already-initialised database), then verifies that `issues` and
`move_mappings` contain zero rows before proceeding. The other tables may
already exist and are not checked.

## Setup

Configuration is loaded from a `.env` file in this directory. A template with
all available variables is provided as `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` and fill in the required values. The file is gitignored so secrets
stay local. The Makefile sources it automatically — no manual `export` needed.

## Environment variables

All variables are read from `.env` (or the shell environment if you prefer).

### AWS / DynamoDB (source)

| Variable | Required | Description |
|---|---|---|
| `AWS_REGION` | yes | AWS region of the DynamoDB table |
| `DYNAMODB_TABLE` | yes | Name of the DynamoDB table to read from |
| `AWS_PROFILE` | no | AWS named profile (standard auth chain) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | no | Explicit credentials (standard auth chain) |

AWS authentication follows the standard SDK v2 chain: environment variables →
`~/.aws/credentials` → instance/task IAM role → SSO. Set `AWS_PROFILE` to
choose a named profile.

### PostgreSQL (target)

| Variable | Required | Default | Description |
|---|---|---|---|
| `PG_HOST` | no | `localhost` | Postgres host |
| `PG_PORT` | no | `5432` | Postgres port |
| `PG_USER` | yes | — | Postgres user |
| `PG_PASSWORD` | no | — | Postgres password |
| `PG_DATABASE` | yes | — | Postgres database name |
| `PG_SSL_MODE` | no | `prefer` | SSL mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `PG_SSL_ROOT_CERT` | no | — | Path to CA certificate file (useful with `verify-ca` / `verify-full`) |
| `PG_SSL_CERT` | no | — | Path to client certificate file (mutual TLS) |
| `PG_SSL_KEY` | no | — | Path to client private key file (mutual TLS) |
| `PG_ISSUES_TABLE` | no | `issues` | Name of the issues table |
| `PG_MOVE_MAPPINGS_TABLE` | no | `move_mappings` | Name of the move mappings table |

### Behaviour

| Variable | Required | Default | Description |
|---|---|---|---|
| `DRYRUN` | no | `false` | Set to `true` to scan DynamoDB without writing to Postgres |

## How to run

```bash
cd scripts/dynamodb-to-postgres-migration

# First time only: create your local config from the template
cp .env.example .env

# Edit .env and fill in AWS_REGION, DYNAMODB_TABLE, PG_USER, PG_DATABASE, etc.

# Dry run — scans DynamoDB and validates data, writes nothing to Postgres
make dry-run

# Live migration
make run
```

`DRYRUN` in `.env` is ignored by `make dry-run` — it always forces dry-run mode
regardless of what the file contains.

Progress is logged every 5 seconds:

```
2026/03/31 12:00:00 Source: DynamoDB table="slackmgr-prod" region=eu-west-1
2026/03/31 12:00:00 Target: Postgres slackmgr@my-db.example.com:5432/slackmgr (ssl=verify-full)
2026/03/31 12:00:00 Loading AWS configuration...
2026/03/31 12:00:00 Connecting to Postgres at my-db.example.com:5432/slackmgr...
2026/03/31 12:00:00 Running Postgres schema migrations...
2026/03/31 12:00:00 Schema ready.
2026/03/31 12:00:00 Checking that issues and move_mappings tables are empty...
2026/03/31 12:00:00 Pre-check passed: data tables are empty.
2026/03/31 12:00:00 Scanning DynamoDB table "slackmgr-prod" (region: eu-west-1)...
2026/03/31 12:00:05   scanned=1500  issues=800  move_mappings=12  skipped=688
2026/03/31 12:00:08 Migration complete.
2026/03/31 12:00:08   scanned=2100  issues=1100  move_mappings=15  skipped=985
```

## How it works

1. **Config validation** — all required environment variables are checked before any connections are made.
2. **AWS auth** — uses the standard AWS SDK v2 config chain; `AWS_PROFILE` and instance roles work out of the box.
3. **Schema creation** — runs the postgres plugin's built-in migrations to create all tables and indexes (no-op if already applied).
4. **Pre-check** — verifies that `issues` and `move_mappings` contain zero rows. Aborts if either table has data.
5. **DynamoDB full scan** — pages through every item in the table using `Scan` with `ExclusiveStartKey` for pagination.
6. **Item dispatch** — each item is classified by its sort key prefix:
   - `ISSUE#` → parsed (and written via `postgres.SaveIssue` unless dry-run)
   - `MOVEMAPPING#` → parsed (and written via `postgres.SaveMoveMapping` unless dry-run)
   - all other prefixes (`ALERT#`, `PROCESSINGSTATE#`, unknown) → skipped
7. **Error handling** — any single-item failure stops the migration immediately with a descriptive error that includes the sort key of the failing item.
