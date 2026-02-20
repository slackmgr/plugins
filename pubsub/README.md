# pubsub

[![Go Reference](https://pkg.go.dev/badge/github.com/slackmgr/plugins/pubsub.svg)](https://pkg.go.dev/github.com/slackmgr/plugins/pubsub)
[![Go Report Card](https://goreportcard.com/badge/github.com/slackmgr/plugins/pubsub)](https://goreportcard.com/report/github.com/slackmgr/plugins/pubsub)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/slackmgr/plugins/workflows/CI%20(pubsub)/badge.svg)](https://github.com/slackmgr/plugins/actions)

A Google Cloud Pub/Sub queue backend for [Slack Manager](https://github.com/slackmgr/core). Provides FIFO-ordered message publishing and subscription with automatic visibility extension, and a webhook handler for forwarding HTTP callbacks to Pub/Sub topics.

Part of the [slackmgr/plugins](https://github.com/slackmgr/plugins) monorepo. Versioned independently using the `pubsub/vX.Y.Z` tag convention.

## Installation

```bash
go get github.com/slackmgr/plugins/pubsub
```

Requires Go 1.25+.

## Usage

### Client

The `Client` publishes messages to a Pub/Sub topic and optionally receives messages from a subscription. Messages are ordered by `OrderingKey` (Slack channel ID) and deduplicated via a `dedup_id` message attribute.

```go
import (
    "cloud.google.com/go/pubsub/v2"
    pubsubplugin "github.com/slackmgr/plugins/pubsub"
    "github.com/slackmgr/types"
)

gcpClient, err := pubsub.NewClient(ctx, projectID)
if err != nil {
    log.Fatal(err)
}

client, err := pubsubplugin.New(gcpClient, "projects/my-project/topics/my-topic", "my-subscription", logger)
if err != nil {
    log.Fatal(err)
}

client, err = client.Init()
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Publish a message
err = client.Send(ctx, channelID, dedupID, body)

// Receive messages
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

`Init` validates options and configures the publisher and subscriber. `Receive` is a blocking loop that must be run in a goroutine; it closes `sinkCh` when it returns.

Call `Ack()` on a message to acknowledge it. Call `Nack()` to abandon it — the message will be redelivered by Pub/Sub.

### Webhook Handler

`WebhookHandler` converts incoming HTTP webhook callbacks into Pub/Sub messages. It caches publishers per topic and validates topic names against the GCP resource name format.

```go
handler := pubsubplugin.NewWebhookHandler(gcpClient, true /* isOrdered */)

handler, err = handler.Init(ctx)
if err != nil {
    log.Fatal(err)
}
defer handler.Close()

// ShouldHandleWebhook returns true for valid GCP topic resource names
// e.g. "projects/my-project/topics/my-topic"
if handler.ShouldHandleWebhook(ctx, target) {
    if err := handler.HandleWebhook(ctx, target, webhookData, logger); err != nil {
        log.Printf("webhook error: %v", err)
    }
}
```

When `isOrdered` is true, the Slack channel ID is used as the `OrderingKey`. When false, messages are published without ordering.

## Configuration

All options are provided via `With*` constructor functions passed to `New` or `NewWebhookHandler`.

### Publisher options

| Option | Default | Description |
|--------|---------|-------------|
| `WithPublisherDelayThreshold(time.Duration)` | `10ms` | Maximum delay before flushing a batch |
| `WithPublisherCountThreshold(int)` | `100` | Maximum number of messages per batch |
| `WithPublisherByteThreshold(int)` | `1048576` (1 MB) | Maximum total bytes per batch |

### Subscriber options

| Option | Default | Description |
|--------|---------|-------------|
| `WithSubscriberMaxExtension(time.Duration)` | `10m` | Maximum total ack deadline extension per message (1m–1h) |
| `WithSubscriberMaxDurationPerAckExtension(time.Duration)` | `1m` | Maximum single ack deadline extension (10s–10m) |
| `WithSubscriberMinDurationPerAckExtension(time.Duration)` | `10s` | Minimum single ack deadline extension (10s–10m) |
| `WithSubscriberMaxOutstandingMessages(int)` | `100` | Maximum number of unprocessed messages |
| `WithSubscriberMaxOutstandingBytes(int)` | `1048576` (1 MB) | Maximum total bytes of unprocessed messages |
| `WithSubscriberShutdownTimeout(time.Duration)` | `1s` | Timeout for graceful subscriber shutdown |

### Testing option

| Option | Description |
|--------|-------------|
| `WithPubSubClient(pubsubClient)` | Custom Pub/Sub client implementation (useful for testing) |

## Topic name format

`ShouldHandleWebhook` validates topic names against the GCP Pub/Sub resource name format:

```
projects/<project-id>/topics/<topic-name>
```

- Project ID: 6–30 lowercase alphanumeric characters, hyphens, or colons (domain-prefixed)
- Topic name: 3–255 characters starting with a letter, followed by letters, digits, dots, underscores, or hyphens

## License

This project is licensed under the MIT License — see the [LICENSE](../LICENSE) file for details.

Copyright (c) 2026 Peter Aglen
