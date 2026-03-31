# CLAUDE.md

This file provides guidance to Claude Code when working with the `postgres` plugin. See also the root `CLAUDE.md` for monorepo-wide conventions.

## Module Overview

`github.com/slackmgr/plugins/postgres` is a Go plugin that implements the `types.DB` interface from `github.com/slackmgr/types`. It provides PostgreSQL storage for Slack Manager using `pgxpool` for connection pooling.

## Build Commands

Standard targets (`make test`, `make lint`, etc.) are documented in the root `CLAUDE.md`. Additional postgres-specific targets:

```bash
make test-integration  # Run integration tests (requires local PostgreSQL)
make open-cover-report # View HTML coverage report
```

Integration tests require a running PostgreSQL instance. Start one with:

```bash
docker-compose up -d
```

This starts PostgreSQL 17 on `localhost:5432` with user `postgres`, password `qwerty`, database `slack_manager`. Integration tests use prefixed table names (`__*_integration_test`) and clean up after themselves.

For running a single integration test:
```bash
go test -tags=integration -run TestName ./...
```

## Architecture

**Core components:**

- **`client.go`** — Core PostgreSQL client implementing `types.DB`. Init flow: `New(opts...)` → `Connect(ctx)` → `Init(ctx, skipSchemaValidation bool)`. `Connect` establishes the pgxpool connection; `Init` runs pending schema migrations (via `runMigrations` / `applyMigration`) and starts the background TTL cleanup goroutine. The `skipSchemaValidation` parameter is accepted for interface compatibility but ignored. `Close(ctx)` stops the goroutine and closes the pool.

- **`options.go`** — Functional options: connection config (`WithHost`, `WithPort`, `WithUser`, `WithPassword`, `WithDatabase`, `WithSSLMode`, SSL cert paths), pool tuning (`WithPool*`), table names (`WithIssuesTable`, `WithSchemaMigrationsTable`, etc.), and TTL config (`WithAlertsTimeToLive`, `WithIssuesTimeToLive`, `WithTTLCleanupInterval`, `WithTTLCleanupDisabled`). Defines the `migration` struct and `migrations()` method that returns the ordered list of schema migrations.

- **`export_test.go`** — Exports internal symbols (`validateTableName`, `getIssueInsertSQL`, etc.) for use in `_internal_test.go` files.

**Database schema:**

Five tables (names configurable via `With*Table` options):
- `issues` — with correlation IDs, open/closed state, TTL (`expires_at`)
- `alerts` — with TTL (`expires_at`)
- `move_mappings` — issue move tracking between channels
- `channel_processing_state` — per-channel processing timestamps
- `schema_migrations` — tracks applied migration versions (`version INTEGER PRIMARY KEY`, `applied_at TIMESTAMPTZ`)

All data tables include a `version` column and an `attrs` JSONB column for the serialised entity.

**Adding a future migration:** append a new `migration{version: N, stmts: [...]}` entry to `migrations()` in `options.go`. Migrations are applied in order, each exactly once, under a `pg_advisory_xact_lock`.

**TTL:** Closed issues expire after 180 days; alerts after 30 days (both configurable). A background goroutine (started by `Init`, stopped by `Close`) deletes expired rows on a configurable interval (default 1 hour). Expired records are excluded from reads regardless of whether the cleanup goroutine runs.

## Dependencies

- `github.com/jackc/pgx/v5` — PostgreSQL driver with connection pooling
- `github.com/pashagolub/pgxmock/v4` — Mock PostgreSQL connections for unit tests
- `github.com/slackmgr/types` — shared Slack Manager types and interfaces
- `github.com/stretchr/testify` — test assertions
