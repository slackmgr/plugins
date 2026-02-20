# CLAUDE.md

This file provides guidance to Claude Code when working with the `dynamodb` plugin. See also the root `CLAUDE.md` for monorepo-wide conventions.

## Module Overview

`github.com/slackmgr/plugins/dynamodb` is a Go plugin that implements the `types.DB` interface from `github.com/slackmgr/types`. It provides DynamoDB storage for Slack Manager using a single-table design with AWS SDK v2.

## Build Commands

Standard targets (`make test`, `make lint`, etc.) are documented in the root `CLAUDE.md`. Additional dynamodb-specific targets:

```bash
make test-integration  # Run integration tests (requires AWS credentials)
make open-cover-report # View HTML coverage report
make bump-common-lib   # Update github.com/slackmgr/types to latest
```

Integration tests require environment variables: `AWS_REGION`, `DYNAMODB_TABLE_NAME`

For running a single integration test:
```bash
go test -tags=integration -run TestName ./...
```

## Architecture

**Core components:**

- **`client.go`** — Core DynamoDB client implementing `types.DB`. Init flow: `New()` → `Connect()` → `Init(ctx, skipSchemaValidation bool)`. `Connect` initializes the AWS SDK client; `Init` validates the table schema.

- **`options.go`** — Functional options: `WithAlertsTimeToLive`, `WithIssuesTimeToLive`, `WithAPI`, `WithClock`.

- **`dynamodb_api.go`** — `API` interface for DynamoDB operations (dependency injection / testing).

- **`client_test.go`** — Unit tests using an in-package `mockAPI` struct.

- **`client_integration_test.go`** — Integration tests (build tag `integration`) delegating to the shared `dbtests` package from `github.com/slackmgr/types`.

**DynamoDB schema:**

Single-table design with composite keys:
- **Partition Key (`pk`)**: Slack Channel ID
- **Sort Key (`sk`)**: Composite identifier with type prefix

Sort key patterns:
- Alerts: `ALERT#<timestamp>#<unique_id>`
- Issues: `ISSUE#<channel>#<base64_correlation_id>#<unique_id>`
- Move Mappings: `MOVEMAPPING#<channel>#<base64_correlation_id>`
- Processing State: `PROCESSINGSTATE#<channel>`

**Global Secondary Indexes:**
- `GSIPostID`: Query issues by Slack post ID (pk + post_id → sk)
- `GSIIsOpen`: Query all open issues (is_open + sk → body)

## Dependencies

- `github.com/aws/aws-sdk-go-v2` — AWS SDK v2
- `github.com/aws/aws-sdk-go-v2/config` — AWS config loading
- `github.com/aws/aws-sdk-go-v2/service/dynamodb` — DynamoDB service client
- `github.com/slackmgr/types` — shared Slack Manager types and interfaces
