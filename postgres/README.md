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

Requires Go 1.25+ and PostgreSQL 13+.

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

`Init` creates the required tables if they do not exist, validates the schema, and starts the background TTL cleanup goroutine. It is idempotent — calling it multiple times on the same `Client` is safe. Pass `skipSchemaValidation: true` to skip the schema validation step.

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
| `WithIssuesTable(string)` | `"issues"` | Issues table name |
| `WithAlertsTable(string)` | `"alerts"` | Alerts table name |
| `WithMoveMappingsTable(string)` | `"move_mappings"` | Move mappings table name |
| `WithChannelProcessingStateTable(string)` | `"channel_processing_state"` | Channel processing state table name |
| `WithAlertsTimeToLive(time.Duration)` | `30 days` | TTL applied to alert records. Must be greater than zero. |
| `WithIssuesTimeToLive(time.Duration)` | `180 days` | TTL applied to closed issue records. Open issues never expire. Must be greater than zero. |
| `WithTTLCleanupInterval(time.Duration)` | `1h` | How often the background goroutine physically deletes expired rows. Must be greater than zero. |
| `WithTTLCleanupDisabled()` | — | Disables the background TTL cleanup goroutine. Expired rows are excluded from reads but never physically deleted. |
| `WithPoolMaxConnections(int32)` | pgx default | Max pool connections |
| `WithPoolMinConnections(int32)` | pgx default | Min pool connections |
| `WithPoolMinIdleConnections(int32)` | pgx default | Min idle connections |
| `WithPoolMaxConnectionLifetime(time.Duration)` | pgx default | Max connection lifetime |
| `WithPoolMaxConnectionLifetimeJitter(time.Duration)` | pgx default | Jitter added to max connection lifetime |
| `WithPoolMaxConnectionIdleTime(time.Duration)` | pgx default | Max connection idle time |
| `WithPoolHealthCheckPeriod(time.Duration)` | pgx default | Health check interval |

### SSL Modes

```go
postgres.SSLModeDisable    // No SSL
postgres.SSLModeAllow      // Try non-SSL first, then SSL
postgres.SSLModePrefer     // Try SSL first, then non-SSL (default)
postgres.SSLModeRequire    // SSL only, no certificate verification
postgres.SSLModeVerifyCA   // SSL with CA certificate verification
postgres.SSLModeVerifyFull // SSL with CA and hostname verification
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

Four tables are created by `Init`:

- **`issues`** — Slack issues with correlation IDs and open/closed state
- **`alerts`** — Alert records
- **`move_mappings`** — Issue move tracking between channels
- **`channel_processing_state`** — Per-channel processing timestamps

All tables include a `version` column for schema evolution and an `attrs` JSONB column for flexible storage. The `issues` and `alerts` tables additionally include an `expires_at` column used for TTL.

## License

This project is licensed under the MIT License — see the [LICENSE](../LICENSE) file for details.

Copyright (c) 2026 Peter Aglen
