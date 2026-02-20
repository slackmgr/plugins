# dynamodb

[![Go Reference](https://pkg.go.dev/badge/github.com/slackmgr/plugins/dynamodb.svg)](https://pkg.go.dev/github.com/slackmgr/plugins/dynamodb)
[![Go Report Card](https://goreportcard.com/badge/github.com/slackmgr/plugins/dynamodb)](https://goreportcard.com/report/github.com/slackmgr/plugins/dynamodb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/slackmgr/plugins/workflows/CI/badge.svg)](https://github.com/slackmgr/plugins/actions)

A DynamoDB storage backend for [Slack Manager](https://github.com/slackmgr/slack-manager). Implements the [`types.DB`](https://github.com/slackmgr/types) interface using a single-table design with AWS SDK v2.

Part of the [slackmgr/plugins](https://github.com/slackmgr/plugins) monorepo. Versioned independently using the `dynamodb/vX.Y.Z` tag convention.

## Installation

```bash
go get github.com/slackmgr/plugins/dynamodb
```

Requires Go 1.25+.

## Usage

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    dynamoplugin "github.com/slackmgr/plugins/dynamodb"
)

awsCfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
    log.Fatal(err)
}

client := dynamoplugin.New(&awsCfg, "my-table",
    dynamoplugin.WithAlertsTimeToLive(30*24*time.Hour),
    dynamoplugin.WithIssuesTimeToLive(180*24*time.Hour),
)

if err := client.Connect(); err != nil {
    log.Fatal(err)
}

if err := client.Init(ctx, false); err != nil {
    log.Fatal(err)
}
```

`Connect` initializes the underlying DynamoDB client and must be called before any other methods. `Init` validates that the table exists with the expected schema — pass `skipSchemaValidation: true` to skip validation.

## Configuration

All options are provided via `With*` constructor functions.

| Option | Default | Description |
|--------|---------|-------------|
| `WithAlertsTimeToLive(time.Duration)` | `30 days` | TTL applied to alert records |
| `WithIssuesTimeToLive(time.Duration)` | `180 days` | TTL applied to closed issues |
| `WithAPI(API)` | *(AWS default)* | Custom DynamoDB API implementation (useful for testing) |
| `WithClock(func() time.Time)` | `time.Now` | Custom clock function for time-dependent behaviour (useful for testing) |

## DynamoDB Table Setup

The table must be created before use — this plugin does not create tables. `Init` validates the schema and returns an error if the table is missing or misconfigured.

### Required configuration

| Property | Value |
|----------|-------|
| Partition key | `pk` (String) |
| Sort key | `sk` (String) |
| TTL attribute | `ttl` |

### Required Global Secondary Indexes

| Index | Partition key | Sort key | Projected attributes |
|-------|--------------|----------|----------------------|
| `GSIPostID` | `pk` | `post_id` | `sk` |
| `GSIIsOpen` | `is_open` | `sk` | `body` |

Both indexes must use `INCLUDE` projection type and be in `ACTIVE` status.

### Sort key patterns

The table uses a single-table design where the sort key encodes the record type:

| Record type | Sort key format |
|-------------|----------------|
| Alert | `ALERT#<timestamp>#<unique_id>` |
| Issue | `ISSUE#<channel>#<base64_correlation_id>#<unique_id>` |
| Move mapping | `MOVEMAPPING#<channel>#<base64_correlation_id>` |
| Processing state | `PROCESSINGSTATE#<channel>` |

Correlation IDs are base64 URL-encoded in sort keys.

## License

MIT — see [LICENSE](../LICENSE) for details.

Copyright (c) 2026 Peter Aglen
