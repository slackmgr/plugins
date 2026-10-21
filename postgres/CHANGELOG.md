# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `postgres/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

For history prior to this monorepo migration, see the
[slack-manager-postgres-plugin](https://github.com/slackmgr/slack-manager-postgres-plugin) repository.

## [Unreleased]

### Fixed

- Persist the full channel processing state. `SaveChannelProcessingState` and `FindChannelProcessingState` previously ignored `OpenIssues` and `LastChannelActivity`, so `OpenIssues` always read back as 0 on a database cache miss. This disabled alert-driven Slack throttling and could pause issue processing in multi-instance deployments. Schema migration 3 adds the `open_issues` and `last_channel_activity` columns
- `FindChannelProcessingState` returns timestamps in UTC
- Integration tests now run the full shared `dbtests` suite
- Background TTL cleanup deletes expired rows in bounded batches (each its own short transaction, 30s statement deadline, 10 minute run deadline) instead of one unbounded `DELETE` per table. Batches use `FOR UPDATE SKIP LOCKED`, so concurrent cleanup runs from multiple instances no longer block each other

### Added

- `WithTTLCleanupBatchSize(int)` option (default 5000)
- Insights support (requires `github.com/slackmgr/types` v0.8.0):
  - Schema migration 4 creates the `insights_events` table, partitioned by day on `occurred_at` (always created, empty unless insights is enabled). `DropAllData` also drops it
  - `WithInsightsEnabled()` (opt-in) opens a dedicated, small pool (`statement_timeout`, `lock_timeout=1s`, `synchronous_commit=off`) and enables `Client.InsightsSink()` (`types.InsightsSinkProvider`, batch writes with `COPY`) and `Client.InsightsReader()` (`types.InsightsReaderProvider`)
  - Partition maintenance: `Init` and then an hourly background goroutine (stopped by `Close`) pre-create daily partitions (yesterday to today + 3) and drop partitions older than the retention, guarded by an advisory lock. Errors are logged and never fail `Init`
  - `NewInsightsReader` / `InsightsReader`: standalone read-only `types.InsightsReader` (`default_transaction_read_only=on`, never runs migrations or DDL) with `CheckSchema`. Needs `SELECT` on the issues, insights and migrations tables only
  - Options `WithInsightsEventsTable`, `WithInsightsRetention`, `WithInsightsPoolMaxConnections`, `WithInsightsStatementTimeout`, `WithInsightsReadStatementTimeout`
  - `InsightsEventModelVersion` constant
  - Count and stats queries without an explicit limit fail with `types.ErrInsightsTooManyRows` when more than `types.MaxInsightsRows` groups match, instead of returning a truncated result (one extra row is fetched to detect this)

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.8.0
- The internal `pool` interface gained `CopyFrom`

## [0.6.0] - 2026-10-21

### Changed

- Minimum Go version is now 1.26
- Bump `github.com/slackmgr/types` dependency to v0.7.1
- Update direct dependencies (`github.com/jackc/pgx/v5` v5.10.0, `github.com/stretchr/testify` v1.12.1). `pgx` v5.11.0 is held back because the test mock `github.com/pashagolub/pgxmock/v4` does not support its new `Rows.TypeMap` method (only a v6 beta does)
- Bump `golang.org/x/text` to v0.43.0 (fixes GO-2026-6629 and GO-2026-5970)
- CI and code scanning workflows read the Go version (major.minor) from `go.mod`/`go.work` and install the latest patch release (`check-latest: true`), so all jobs, including gosec and both govulncheck runs, use the same Go version. Workflows also run when `go.work` changes
- Update GitHub Actions to latest versions (`actions/checkout` v7.0.1, `actions/setup-go` v7.0.0, `github/codeql-action` v4.39.0, `golangci/golangci-lint-action` v9.3.0)
- Disable the `exhaustruct_v5` linter (renamed in golangci-lint 2.15), matching the existing `exhaustruct` exclusion
- README: Go requirement updated to 1.26+

## [0.5.5] - 2026-05-11

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.6.1

## [0.5.4] - 2026-05-08

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.2

## [0.5.3] - 2026-05-07

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.1

## [0.5.2] - 2026-04-14

### Fixed

- `slack_post_id` is now stored as `NULL` (not empty string) when an issue has no Slack post yet — prevents migration 2 from failing on upgrade when existing rows have `slack_post_id = ''`

## [0.5.1] - 2026-04-14

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.4.1

## [0.5.0] - 2026-04-14

### Added

- `New()` now requires a `types.Logger` as its first parameter; errors from the background TTL cleanup goroutine are logged instead of silently discarded — mirrors the SQS plugin pattern (**breaking change**)
- Schema migration v2: partial unique indexes enforcing one open issue per `(channel_id, correlation_id)`, one issue per `(channel_id, slack_post_id)`, and one move mapping per `(channel_id, correlation_id)`

### Changed

- `FindActiveChannels` returns an empty slice instead of nil when no open issues exist

### Fixed

- `SaveAlert` now validates that `alert.SlackChannelID` is non-empty
- `MoveIssue` now validates that `sourceChannelID` is non-empty and differs from `targetChannelID`
- Connection pool not closed when `Connect()` fails at the `Ping` step
- Race condition where concurrent `Init()` calls could spawn multiple TTL cleanup goroutines
- `Close()` now waits for the TTL cleanup goroutine to finish before closing the pool
- Database name was not URL-encoded in the connection string

## [0.4.0] - 2026-03-31

### Added

- `WithSSLRootCert`, `WithSSLCert`, `WithSSLKey` options for passing TLS certificate file paths to the PostgreSQL connection
- Schema migrations table: `Init` now applies versioned migrations automatically using a `schema_migrations` table, protected by a `pg_advisory_xact_lock` for safe concurrent startup
- `WithSchemaMigrationsTable` option to customise the migrations table name (default: `"schema_migrations"`)

### Removed

- `createStatements()` and `verifyCurrentDatabaseVersion()` replaced by the migrations system

## [0.3.5] - 2026-02-26

### Changed

- CI: added govulncheck to per-plugin CI and code scanning workflow
- Updated Go module dependencies (slackmgr/types v0.4.0)

## [0.3.4] - 2026-02-22

### Changed

- Updated Go module dependencies
- CI: replaced shared matrix workflow with a dedicated per-plugin path-filtered workflow (`ci-postgres.yml`)
- CI: refactored Security job to install gosec via `go install` using the host Go toolchain, run from the module directory, use stable Go, and enforce failure on findings; gosec removed from golangci-lint
- CI: fixed `setup-go` `cache-dependency-path` for subdirectory modules
- Lint: updated golangci-lint config — disabled `godox`, enabled `sqlclosecheck`, disabled `testpackage`

## [0.3.3] - 2026-02-20

_Initial version in the slackmgr/plugins monorepo. For prior history see the
[slack-manager-postgres-plugin](https://github.com/slackmgr/slack-manager-postgres-plugin) repository._

### Changed

- Module path updated from `github.com/slackmgr/slack-manager-postgres-plugin` to `github.com/slackmgr/plugins/postgres`

[Unreleased]: https://github.com/slackmgr/plugins/compare/postgres/v0.6.0...HEAD
[0.6.0]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.5...postgres/v0.6.0
[0.5.5]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.4...postgres/v0.5.5
[0.5.4]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.3...postgres/v0.5.4
[0.5.3]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.2...postgres/v0.5.3
[0.5.2]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.1...postgres/v0.5.2
[0.5.1]: https://github.com/slackmgr/plugins/compare/postgres/v0.5.0...postgres/v0.5.1
[0.5.0]: https://github.com/slackmgr/plugins/compare/postgres/v0.4.0...postgres/v0.5.0
[0.4.0]: https://github.com/slackmgr/plugins/compare/postgres/v0.3.5...postgres/v0.4.0
[0.3.5]: https://github.com/slackmgr/plugins/compare/postgres/v0.3.4...postgres/v0.3.5
[0.3.4]: https://github.com/slackmgr/plugins/compare/postgres/v0.3.3...postgres/v0.3.4
[0.3.3]: https://github.com/slackmgr/plugins/releases/tag/postgres/v0.3.3
