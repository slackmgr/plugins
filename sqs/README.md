# sqs plugin

[![Go Reference](https://pkg.go.dev/badge/github.com/slackmgr/plugins/sqs.svg)](https://pkg.go.dev/github.com/slackmgr/plugins/sqs)
[![Go Report Card](https://goreportcard.com/badge/github.com/slackmgr/plugins/sqs)](https://goreportcard.com/report/github.com/slackmgr/plugins/sqs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/slackmgr/plugins/workflows/CI/badge.svg)](https://github.com/slackmgr/plugins/actions)

An AWS SQS queue backend for [Slack Manager](https://github.com/slackmgr/core). Provides FIFO queue consumption with automatic visibility timeout extension and a webhook handler for forwarding HTTP callbacks to SQS queues. Uses AWS SDK v2.

This module lives in the [`slackmgr/plugins`](https://github.com/slackmgr/plugins) monorepo under the `sqs/` subdirectory. Versions are tagged using the `sqs/vX.Y.Z` convention (e.g. `sqs/v0.2.0`).

## Installation

```bash
go get github.com/slackmgr/plugins/sqs
```

Requires Go 1.25+.

## Usage

### Client

The `Client` reads messages from an SQS FIFO queue and delivers them to a sink channel. Each message is wrapped with `Ack` and `Nack` callbacks, and its visibility timeout is automatically extended in the background while processing is ongoing.

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    sqsplugin "github.com/slackmgr/plugins/sqs"
    "github.com/slackmgr/types"
)

awsCfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
    log.Fatal(err)
}

client, err := sqsplugin.New(&awsCfg, "my-queue.fifo", logger,
    sqsplugin.WithSqsVisibilityTimeout(30),
    sqsplugin.WithMaxMessageExtension(10*time.Minute),
).Init(ctx)
if err != nil {
    log.Fatal(err)
}

sinkCh := make(chan *types.FifoQueueItem)

go func() {
    if err := client.Receive(ctx, sinkCh); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatal(err)
    }
}()

for item := range sinkCh {
    // process item...
    item.Ack()
}
```

`Init` validates options, resolves the queue URL, and starts the background visibility extender. `Receive` is a blocking loop that must be run in a goroutine; it closes `sinkCh` when it returns.

Call `Ack()` on a message to delete it from the queue after successful processing. Call `Nack()` to abandon it — the message will become visible again in SQS after the visibility timeout expires.

### Webhook Handler

`WebhookHandler` converts incoming HTTP webhook callbacks into SQS messages. It implements the `types.WebhookHandler` interface and supports both FIFO and standard queues. For FIFO queues, it uses the channel ID as the message group and a SHA256 hash of the callback fields as the deduplication ID.

```go
handler, err := sqsplugin.NewWebhookHandler(&awsCfg).Init(ctx)
if err != nil {
    log.Fatal(err)
}

// ShouldHandleWebhook returns true for targets starting with "https://sqs."
if handler.ShouldHandleWebhook(ctx, target) {
    if err := handler.HandleWebhook(ctx, queueURL, webhookData, logger); err != nil {
        log.Printf("webhook error: %v", err)
    }
}
```

## Configuration

All options are provided via `With*` constructor functions passed to `New`.

| Option | Default | Description |
|--------|---------|-------------|
| `WithSqsVisibilityTimeout(int32)` | `30` seconds | Visibility timeout applied to received messages |
| `WithSqsReceiveMaxNumberOfMessages(int32)` | `1` | Maximum messages per receive call (1–10) |
| `WithSqsReceiveWaitTimeSeconds(int32)` | `20` seconds | Long-poll wait time per receive call (3–20) |
| `WithSqsAPIMaxRetryAttempts(int)` | `5` | Maximum SQS API retry attempts (0–10) |
| `WithSqsAPIMaxRetryBackoffDelay(time.Duration)` | `10s` | Maximum backoff delay between retries (1s–30s) |
| `WithMaxMessageExtension(time.Duration)` | `10m` | Maximum total visibility extension per message (1m–1h) |
| `WithMaxOutstandingMessages(int)` | `100` | Maximum number of in-flight messages before pausing reads |
| `WithMaxOutstandingBytes(int)` | `1048576` (1 MB) | Maximum total in-flight message bytes before pausing reads |
| `WithSQSClient(sqsClient)` | *(AWS default)* | Custom SQS client implementation (useful for testing) |

## SQS Queue Setup

### Client requirements

The `Client` requires an SQS **FIFO queue**. `Init` returns an error if the queue name does not end with `.fifo`.

| Property | Value |
|----------|-------|
| Queue type | FIFO (name must end with `.fifo`) |
| Content-based deduplication | Optional — the client always supplies an explicit deduplication ID |

Messages are grouped by Slack channel ID using the SQS `MessageGroupId` attribute.

### Visibility timeout extension

The background extender checks in-flight messages at an interval of one-third of the configured visibility timeout (minimum 5 seconds). It extends the timeout for any message that has consumed more than half of its current visibility window. Extension is best-effort — if an extension fails, the message is removed from tracking and may be redelivered. Downstream processing should be **idempotent**.

A message will not be extended beyond the duration set by `WithMaxMessageExtension`. After that limit is reached, the message is dropped from tracking and will become visible in SQS again.

### Webhook handler requirements

The `WebhookHandler` supports both FIFO and standard queues. For FIFO queues, the target URL must end with `.fifo`. For standard queues, no deduplication ID is used.

## License

This project is licensed under the MIT License — see the [LICENSE](../LICENSE) file for details.

Copyright (c) 2026 Peter Aglen
