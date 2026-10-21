# postgres

[![Go Reference](https://pkg.go.dev/badge/github.com/slackmgr/plugins/postgres.svg)](https://pkg.go.dev/github.com/slackmgr/plugins/postgres)
[![Go Report Card](https://goreportcard.com/badge/github.com/slackmgr/plugins/postgres)](https://goreportcard.com/report/github.com/slackmgr/plugins/postgres)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/slackmgr/plugins/workflows/CI/badge.svg)](https://github.com/slackmgr/plugins/actions)

A PostgreSQL storage backend for [Slack Manager](https://github.com/slackmgr/core). Implements the [`types.DB`](https://github.com/slackmgr/types) interface using `pgxpool` for connection pooling.

Part of the [slackmgr/plugins](https://github.com/slackmgr/plugins) monorepo. Versioned independently using the `postgres/vX.Y.Z` tag convention.

## Installation

```bash
go get github.com/slackmgr/plugins/postgres
```

Requires Go 1.26+ and PostgreSQL 13+.

## Usage

```go
import (
    postgres "github.com/slackmgr/plugins/postgres"
)

client := postgres.New(
    postgres.WithHost("localhost"),
    postgres.WithPort(5432),
    postgres.WithUser("myuser"),
    postgres.WithPassword("mypassword"),
    postgres.WithDatabase("mydb"),
    postgres.WithSSLMode(postgres.SSLModeRequire),
)

if err := client.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer client.Close(ctx)

if err := client.Init(ctx, false); err != nil {
    log.Fatal(err)
}
```

`Init` runs any pending schema migrations, then starts the background TTL cleanup goroutine (and, if insights is enabled, the insights partition maintenance, see [Insights](#insights)). It is idempotent — calling it multiple times on the same `Client` is safe.

## Configuration

All options are provided via `With*` constructor functions. Only `WithUser` and `WithDatabase` are required — everything else has a sensible default.

| Option | Default | Description |
|--------|---------|-------------|
| `WithHost(string)` | `"localhost"` | Database host |
| `WithPort(int)` | `5432` | Database port |
| `WithUser(string)` | *(required)* | Database user |
| `WithPassword(string)` | `""` | Database password |
| `WithDatabase(string)` | *(required)* | Database name |
| `WithSSLMode(SSLMode)` | `SSLModePrefer` | SSL connection mode |
| `WithSSLRootCert(string)` | `""` | Path to CA certificate file (for server verification) |
| `WithSSLCert(string)` | `""` | Path to client certificate file (for mutual TLS) |
| `WithSSLKey(string)` | `""` | Path to client private key file (for mutual TLS) |
| `WithIssuesTable(string)` | `"issues"` | Issues table name |
| `WithAlertsTable(string)` | `"alerts"` | Alerts table name |
| `WithMoveMappingsTable(string)` | `"move_mappings"` | Move mappings table name |
| `WithChannelProcessingStateTable(string)` | `"channel_processing_state"` | Channel processing state table name |
| `WithSchemaMigrationsTable(string)` | `"schema_migrations"` | Migration tracking table name |
| `WithAlertsTimeToLive(time.Duration)` | `30 days` | TTL applied to alert records. Must be greater than zero. |
| `WithIssuesTimeToLive(time.Duration)` | `180 days` | TTL applied to closed issue records. Open issues never expire. Must be greater than zero. |
| `WithTTLCleanupInterval(time.Duration)` | `1h` | How often the background goroutine physically deletes expired rows. Must be greater than zero. |
| `WithTTLCleanupBatchSize(int)` | `5000` | Maximum expired rows deleted per statement. Cleanup runs in batches, each its own short transaction, until no expired rows remain. Must be greater than zero. |
| `WithTTLCleanupDisabled()` | — | Disables the background TTL cleanup goroutine. Expired rows are excluded from reads but never physically deleted. |
| `WithInsightsEnabled()` | disabled | Enables the insights sink, the dedicated insights pool and partition maintenance on `Client`. See [Insights](#insights). |
| `WithInsightsEventsTable(string)` | `"insights_events"` | Insights events table name. Valid identifier, at most 40 characters (partition and index names must fit in 63). |
| `WithInsightsRetention(time.Duration)` | `30 days` | How long insights events are kept (day granularity, enforced by dropping partitions). Must be at least 24h. |
| `WithInsightsPoolMaxConnections(int32)` | `2` | Max connections of the insights pool (writer pool of `Client`, or the pool of `InsightsReader`). Must be at least 1. |
| `WithInsightsStatementTimeout(time.Duration)` | `5s` | `statement_timeout` of the insights writer pool. Must be greater than zero. |
| `WithInsightsReadStatementTimeout(time.Duration)` | `15s` | `statement_timeout` of the `InsightsReader` pool. Must be greater than zero. |
| `WithPoolMaxConnections(int32)` | pgx default | Max pool connections |
| `WithPoolMinConnections(int32)` | pgx default | Min pool connections |
| `WithPoolMinIdleConnections(int32)` | pgx default | Min idle connections |
| `WithPoolMaxConnectionLifetime(time.Duration)` | pgx default | Max connection lifetime |
| `WithPoolMaxConnectionLifetimeJitter(time.Duration)` | pgx default | Jitter added to max connection lifetime |
| `WithPoolMaxConnectionIdleTime(time.Duration)` | pgx default | Max connection idle time |
| `WithPoolHealthCheckPeriod(time.Duration)` | pgx default | Health check interval |

### SSL/TLS

```go
postgres.SSLModeDisable    // No SSL
postgres.SSLModeAllow      // Try non-SSL first, then SSL
postgres.SSLModePrefer     // Try SSL first, then non-SSL (default)
postgres.SSLModeRequire    // SSL only, no certificate verification
postgres.SSLModeVerifyCA   // SSL with CA certificate verification
postgres.SSLModeVerifyFull // SSL with CA and hostname verification
```

`sslmode` controls whether TLS is used and how strictly the server certificate is verified. Certificate files are additive — they provide the cryptographic material for the negotiated TLS connection.

**CA certificate** (`WithSSLRootCert`) — used to verify the server's certificate against a private CA. Only meaningful with `SSLModeVerifyCA` or `SSLModeVerifyFull`. When omitted, the system trust store is used.

**Client certificate + key** (`WithSSLCert` + `WithSSLKey`) — enables mutual TLS (mTLS). Both must be set together. Works with any SSL mode except `SSLModeDisable`.

Example — full mutual TLS with a private CA using certificate files:

```go
client := postgres.New(
    postgres.WithUser("appuser"),
    postgres.WithDatabase("appdb"),
    postgres.WithSSLMode(postgres.SSLModeVerifyFull),
    postgres.WithSSLRootCert("/etc/ssl/certs/ca.crt"),
    postgres.WithSSLCert("/etc/ssl/certs/client.crt"),
    postgres.WithSSLKey("/etc/ssl/private/client.key"),
)
```

### Custom Table Names

Custom table names are useful for multi-tenant setups or running integration tests alongside production data:

```go
client := postgres.New(
    postgres.WithUser("myuser"),
    postgres.WithDatabase("mydb"),
    postgres.WithIssuesTable("myapp_issues"),
    postgres.WithAlertsTable("myapp_alerts"),
    postgres.WithMoveMappingsTable("myapp_move_mappings"),
    postgres.WithChannelProcessingStateTable("myapp_channel_processing_state"),
)
```

Table names must be valid PostgreSQL unquoted identifiers (letters, digits, underscores; must not start with a digit).

## TTL (Time-To-Live)

TTL is always active. By default, alert records expire after **30 days** and closed issue records expire after **180 days**. Open issues never expire.

When a record's TTL expires it is immediately excluded from all read operations. A background goroutine (started by `Init`, stopped by `Close`) periodically runs a `DELETE` to physically remove expired rows. The cleanup interval defaults to 1 hour and is configurable via `WithTTLCleanupInterval`. The goroutine can be disabled entirely with `WithTTLCleanupDisabled` — expired rows will still be excluded from reads, but never physically deleted.

To override the defaults:

```go
client := postgres.New(
    postgres.WithUser("myuser"),
    postgres.WithDatabase("mydb"),
    postgres.WithAlertsTimeToLive(7 * 24 * time.Hour),   // 7 days
    postgres.WithIssuesTimeToLive(90 * 24 * time.Hour),  // 90 days
    postgres.WithTTLCleanupInterval(15 * time.Minute),
)
```

## Database Schema

`Init` manages schema creation and evolution automatically using a migrations table. On startup it runs any migrations that have not yet been applied and records them in a `schema_migrations` table. Migrations are applied atomically under a PostgreSQL advisory lock, so it is safe to call `Init` concurrently from multiple instances.

Six tables are created on first `Init`:

- **`issues`** — Slack issues with correlation IDs and open/closed state
- **`alerts`** — Alert records
- **`move_mappings`** — Issue move tracking between channels
- **`channel_processing_state`** — Per-channel processing state (timestamps, last activity, open issue count)
- **`insights_events`** — Insights events, range-partitioned by day on `occurred_at` (migration 4). Always created, but empty unless insights is enabled. Daily partitions are named `<table>_pYYYYMMDD`.
- **`schema_migrations`** — Migration tracking (version + applied timestamp)

All data tables include a `version` column for data-format versioning and an `attrs` JSONB column for flexible storage (nullable in `insights_events`). The `issues` and `alerts` tables additionally include an `expires_at` column used for TTL.

`DropAllData` also drops the insights table and all its partitions.

Use `WithSchemaMigrationsTable` to customise the migrations table name (useful when running multiple instances in the same database):

```go
client := postgres.New(
    postgres.WithUser("myuser"),
    postgres.WithDatabase("mydb"),
    postgres.WithSchemaMigrationsTable("myapp_schema_migrations"),
)
```

## Insights

Insights records structured events (alert outcomes, issue lifecycle, commands, Slack posts, ...) for dashboards and reporting. It requires `github.com/slackmgr/types` v0.8.0 or later. It is opt-in and isolated from the regular alert and issue storage.

### Writing events

```go
client := postgres.New(logger,
    postgres.WithUser("myuser"),
    postgres.WithDatabase("mydb"),
    postgres.WithInsightsEnabled(),
    postgres.WithInsightsRetention(30 * 24 * time.Hour),
)
```

With `WithInsightsEnabled`:

- `Connect` opens a second, small pool (default 2 connections) used only for insights, with `statement_timeout` (default 5s), `lock_timeout=1s`, `synchronous_commit=off` and `application_name=slackmgr-insights`. The main pool is never used for insights.
- `InsightsSink()` returns a `types.InsightsSink` (the client implements `types.InsightsSinkProvider`). `WriteInsightsEvents` writes a batch with a single `COPY`, outside any issue or alert transaction. Events are best-effort: an error loses the batch.
- `InsightsReader()` returns a `types.InsightsReader` over the insights pool (the client implements `types.InsightsReaderProvider`). Both return `nil` when insights is disabled.

Without `WithInsightsEnabled` the client opens no extra pool, and both providers return `nil`.

### Partitions and retention

The events table is partitioned by day (UTC). There is no default partition, so a missing partition makes the `COPY` fail (the batch is lost) — the maintenance below prevents that.

When insights is enabled, `Init` runs partition maintenance once, then a background goroutine repeats it hourly until `Close`. Maintenance errors are logged and never fail `Init`. Each step runs in its own transaction on the insights pool, under a `pg_try_advisory_xact_lock`, so only one instance at a time does the work (others skip), with `lock_timeout = 2s`:

1. **Create** the partitions for yesterday through today + 3 days (`CREATE TABLE IF NOT EXISTS <table>_pYYYYMMDD PARTITION OF <table> ...`).
2. **Drop** partitions whose whole day is older than the retention (`WithInsightsRetention`, default 30 days, minimum 24h). Only tables named `<table>_pYYYYMMDD` are considered. Dropping a partition is O(1) and creates no bloat. If a long-running reader blocks the drop, it is retried an hour later.

Creating a partition for a new day briefly takes an `ACCESS EXCLUSIVE` lock on the parent table (once per day, because partitions are pre-created). Existing partitions take no lock.

The writer user must own the events table (it runs the DDL). Because retention is day-granular, up to one extra day of events can remain beyond the configured retention.

### Event storage (`attrs`)

Frequently filtered fields are columns (`type`, `reason`, `channel_id`, `related_channel_id`, `issue_id`, `correlation_id`, `severity`, `route_key`, `actor_id`). The rest is stored in the nullable `attrs` JSONB column, with the keys `prevSeverity`, `alertType`, `host`, `author`, `actorName`, `count`, `detail` and `values` (an object of numbers). `attrs` is `NULL` when all of them are empty. The row `version` is `InsightsEventModelVersion`.

Events contain some personal data: `actor_id` and `actorName` (the Slack user ID and real name of users who run commands), and the alert `host` and `author`. They are kept for the insights retention (`WithInsightsRetention`, default 30 days).

### Read-only access

Dashboards and other read-only consumers should use `InsightsReader`, not `Client`. **Do not call `Client.Init` with a read-only user**: it runs DDL (`CREATE TABLE IF NOT EXISTS ...`) and fails.

```go
reader := postgres.NewInsightsReader(logger,
    postgres.WithHost("db"),
    postgres.WithUser("insights_reader"),
    postgres.WithPassword("..."),
    postgres.WithDatabase("mydb"),
)

if err := reader.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer reader.Close()

if err := reader.CheckSchema(ctx); err != nil {
    log.Fatal(err) // schema not initialised by a privileged user
}

rows, err := reader.CountInsightsEvents(ctx, &types.InsightsCountQuery{...})
```

`NewInsightsReader` accepts the same options as `New`; the connection options, table names (`WithIssuesTable`, `WithSchemaMigrationsTable`, `WithInsightsEventsTable`), `WithInsightsPoolMaxConnections` and `WithInsightsReadStatementTimeout` (default 15s) are used. It:

- opens a single pool with `default_transaction_read_only=on` (writes are rejected by the server),
- never runs migrations or DDL and never starts goroutines,
- provides `CheckSchema`, which reads `MAX(version)` from the migrations table and fails if it is below 4 ("run Init with a privileged user first").

Required grants (the partitions are read through the parent table, so new partitions need **no** additional grants; this is verified by the integration tests):

```sql
GRANT SELECT ON issues, insights_events, schema_migrations TO insights_reader;
```

(Use your configured table names.) `issues` is needed by `LoadOpenIssues`, `schema_migrations` by `CheckSchema`.

Count and stats queries fetch one row more than the limit. Without an explicit `Limit`, a query matching more than `types.MaxInsightsRows` groups fails with `types.ErrInsightsTooManyRows`; with one, the extra rows are dropped.

Queries use whitelisted SQL expressions only; all values, including the value key and bucket size, are bind parameters. Every query is validated with its `Validate` method first.

## License

This project is licensed under the MIT License — see the [LICENSE](../LICENSE) file for details.

Copyright (c) 2026 Peter Aglen
