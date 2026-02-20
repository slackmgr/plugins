// Package dynamodb provides a DynamoDB-backed implementation of the
// [github.com/slackmgr/types.DB] interface for Slack Manager.
//
// # Overview
//
// The package uses a single-table DynamoDB design. Every record is keyed by
// a Slack channel ID (partition key, "pk") combined with a type-prefixed
// composite sort key ("sk"):
//
//   - Alerts:           ALERT#<timestamp>#<unique_id>
//   - Issues:           ISSUE#<channel>#<base64_correlation_id>#<unique_id>
//   - Move mappings:    MOVEMAPPING#<channel>#<base64_correlation_id>
//   - Processing state: PROCESSINGSTATE#<channel>
//
// Two Global Secondary Indexes support cross-channel queries:
//
//   - [GSIPostID] — look up an issue by its Slack post ID.
//   - [GSIIsOpen] — enumerate all currently open issues.
//
// # Getting Started
//
// Create a [Client] with [New], supplying an AWS config, the DynamoDB table
// name, and any [Option] values you need:
//
//	client := dynamodb.New(
//	    &awsCfg,
//	    tableName,
//	    dynamodb.WithAlertsTimeToLive(30*24*time.Hour),
//	    dynamodb.WithIssuesTimeToLive(180*24*time.Hour),
//	)
//
// By default, [New] creates an AWS SDK v2 DynamoDB client from the supplied
// [aws.Config]. Supply [WithAPI] to inject a custom or mock implementation.
//
// # TTL Behaviour
//
// Alert records expire after 30 days by default; closed issue records expire
// after 180 days. Both windows are configurable via [WithAlertsTimeToLive]
// and [WithIssuesTimeToLive]. TTL values are stored as Unix timestamps and
// rely on DynamoDB's built-in TTL feature for automatic deletion.
//
// # Concurrency
//
// [Client] is safe for concurrent use by multiple goroutines.
package dynamodb
