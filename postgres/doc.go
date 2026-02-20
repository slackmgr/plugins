// Package postgres provides a PostgreSQL-backed implementation of the
// types.DB interface from github.com/slackmgr/types.
//
// It uses pgx v5 with connection pooling (pgxpool) and stores all entities
// as JSONB with indexed columns for efficient querying.
//
// # Usage
//
// Create a client using [New] with functional options, call [Client.Connect]
// to establish the connection pool, and then [Client.Init] to create the
// database schema:
//
//	client := postgres.New(
//	    postgres.WithHost("localhost"),
//	    postgres.WithPort(5432),
//	    postgres.WithUser("postgres"),
//	    postgres.WithPassword("secret"),
//	    postgres.WithDatabase("slack_manager"),
//	)
//
//	if err := client.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close(ctx)
//
//	if err := client.Init(ctx, false); err != nil {
//	    log.Fatal(err)
//	}
//
// # Database Tables
//
// Four tables are created automatically by [Client.Init] (table names are
// configurable via the corresponding With* options):
//
//   - issues — Slack issues with correlation IDs, open/closed state, and TTL
//   - alerts — Alert records with TTL
//   - move_mappings — Issue move tracking between channels
//   - channel_processing_state — Per-channel processing timestamps
//
// Each table includes a version column and an attrs JSONB column that holds
// the full serialised entity.
//
// # Connection Pool
//
// The underlying pgxpool can be tuned with the pool-specific options:
// [WithPoolMaxConnections], [WithPoolMinConnections],
// [WithPoolMinIdleConnections], [WithPoolMaxConnectionLifetime],
// [WithPoolMaxConnectionIdleTime], [WithPoolHealthCheckPeriod], and
// [WithPoolMaxConnectionLifetimeJitter].
//
// # TTL and Cleanup
//
// Closed issues and alerts are assigned an expiry timestamp on write.
// Default TTLs are 180 days for issues and 30 days for alerts; both are
// configurable via [WithIssuesTimeToLive] and [WithAlertsTimeToLive].
//
// A background goroutine periodically deletes expired rows. Its interval
// defaults to 1 hour and can be changed with [WithTTLCleanupInterval] or
// disabled entirely with [WithTTLCleanupDisabled].
//
// # Schema Validation
//
// When [Client.Init] is called with skipSchemaValidation set to false, it
// queries information_schema.columns and verifies that every expected column
// exists with the correct data type and nullability. Pass true to skip this
// check in environments where the schema is managed externally.
//
// # SSL
//
// SSL behaviour is controlled by [WithSSLMode] using the [SSLMode] constants
// ([SSLModeDisable], [SSLModeAllow], [SSLModePrefer], [SSLModeRequire],
// [SSLModeVerifyCA], [SSLModeVerifyFull]). The default is [SSLModePrefer].
package postgres
