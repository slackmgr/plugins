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

- **`options.go`** — Functional options: connection config (`WithHost`, `WithPort`, `WithUser`, `WithPassword`, `WithDatabase`, `WithSSLMode`, SSL cert paths), pool tuning (`WithPool*`), table names (`WithIssuesTable`, `WithSchemaMigrationsTable`, etc.), and TTL config (`WithAlertsTimeToLive`, `WithIssuesTimeToLive`, `WithTTLCleanupInterval`, `WithTTLCleanupDisabled`), and insights config (`WithInsightsEnabled`, `WithInsightsEventsTable`, `WithInsightsRetention`, `WithInsightsPoolMaxConnections`, `WithInsightsStatementTimeout`, `WithInsightsReadStatementTimeout`). Defines the `migration` struct and `migrations()` method that returns the ordered list of schema migrations.

- **`insights_writer.go`** — Insights write side on `Client`: `InsightsSink`, `WriteInsightsEvents` (`CopyFrom` on the dedicated insights pool), the plugin-owned `attrs` JSON layout (`insightsAttrs`), and daily partition maintenance (`maintainInsightsPartitions`: create yesterday..today+3, drop beyond retention; each step in its own tx under `pg_try_advisory_xact_lock(insightsPartitionLockKey)`; run at the end of `Init`, then hourly in a goroutine drained by `Close`; errors logged, never fail `Init`). Uses the injectable clock `options.nowFn`.

- **`insights_reader.go`** — `insightsQueries` (shared SQL for `types.InsightsReader`: whitelisted dimension/aggregate expressions, bind parameters only, keyset pagination on `(occurred_at, id)`), used by `Client.InsightsReader()` (insights pool) and the standalone read-only `InsightsReader` (`NewInsightsReader`, `Connect`, `Close`, `CheckSchema`). The standalone reader never runs migrations/DDL or goroutines; its pool has `default_transaction_read_only=on`. Read-only consumers must not call `Client.Init`.

- **`export_test.go`** — Exports internal symbols (`validateTableName`, `getIssueInsertSQL`, etc.) for use in `_internal_test.go` files.

**Database schema:**

Six tables (names configurable via `With*Table` options):
- `issues` — with correlation IDs, open/closed state, TTL (`expires_at`)
- `alerts` — with TTL (`expires_at`)
- `move_mappings` — issue move tracking between channels
- `channel_processing_state` — per-channel processing timestamps
- `insights_events` — insights events (migration 4), `PARTITION BY RANGE (occurred_at)` with daily partitions `<table>_pYYYYMMDD` managed by the writer `Client` (no default partition). Columns for filterable fields, nullable `attrs` JSONB for the rest. Table name must be an identifier of at most 40 chars. Always created, even when insights is disabled, to keep migrations linear. `dropStatements()` drops it (CASCADE drops partitions).
- `schema_migrations` — tracks applied migration versions (`version INTEGER PRIMARY KEY`, `applied_at TIMESTAMPTZ`)

All data tables include a `version` column and an `attrs` JSONB column for the serialised entity.

**Insights** is opt-in (`WithInsightsEnabled`): a second pgxpool (`slackmgr-insights`, own `statement_timeout`, `synchronous_commit=off`) is created in `Connect` and closed in `Close`; the main pool is never used for insights. Tests need types with the insights contract (root workspace: `GOWORK=<repo>/go.work`) until the types release is bumped in `go.mod`.

**Adding a future migration:** append a new `migration{version: N, stmts: [...]}` entry to `migrations()` in `options.go`. Migrations are applied in order, each exactly once, under a `pg_advisory_xact_lock`.

**TTL:** Closed issues expire after 180 days; alerts after 30 days (both configurable). A background goroutine (started by `Init`, stopped by `Close`) deletes expired rows on a configurable interval (default 1 hour), in batches (`WithTTLCleanupBatchSize`, default 5000) using `FOR UPDATE SKIP LOCKED` so multiple instances don't block each other. Expired records are excluded from reads regardless of whether the cleanup goroutine runs.

## Dependencies

- `github.com/jackc/pgx/v5` — PostgreSQL driver with connection pooling
- `github.com/pashagolub/pgxmock/v4` — Mock PostgreSQL connections for unit tests
- `github.com/slackmgr/types` — shared Slack Manager types and interfaces
- `github.com/stretchr/testify` — test assertions
