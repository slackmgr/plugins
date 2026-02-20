# CLAUDE.md

This file provides guidance to Claude Code when working with the `pubsub` plugin. See also the root `CLAUDE.md` for monorepo-wide conventions.

## Module Overview

`github.com/slackmgr/plugins/pubsub` is a Go plugin that provides Google Cloud Pub/Sub integration for Slack Manager. It implements a publisher/subscriber client with ordered message delivery and a webhook handler for forwarding HTTP callbacks to Pub/Sub topics.

## Build Commands

Standard targets (`make test`, `make lint`, etc.) are documented in the root `CLAUDE.md`.

For running a single test:
```bash
go test -v -run TestName ./...
```

## Architecture

**Core components:**

- **`client.go`** — Main Pub/Sub client. Init flow: `New(gcpClient, topic, subscription, logger, opts...)` → `Init()`. Subscription is optional (publisher-only mode). `Send` publishes with ordered key and dedup attribute. `Receive` is a blocking loop that sends incoming messages to a sink channel; it closes the channel on return. `Close` flushes the publisher.

- **`webhook_handler.go`** — `WebhookHandler` for dynamic topic publishing. Caches publishers per topic (unbounded, assumes small finite set). Validates topic names via `topicNameRegex`. `HandleWebhook` marshals `types.WebhookCallback` to JSON and publishes to the given topic.

- **`options.go`** — Functional options: publisher thresholds (`WithPublisher*`) and subscriber receive settings (`WithSubscriber*`). Both publisher and subscriber options are validated in `Init`.

- **`pubsub_interfaces.go`** — Internal interfaces (`pubsubClient`, `pubsubPublisher`, `pubsubSubscriber`, `pubsubPublishResult`) for dependency injection and testing.

- **`pubsub_adapters.go`** — Concrete adapter types wrapping the real GCP Pub/Sub SDK types to implement the internal interfaces.

**Message ordering:**
- `Client.Send`: always uses `OrderingKey = groupID` and `Attributes["dedup_id"] = dedupID`
- `WebhookHandler.HandleWebhook`: uses `OrderingKey = data.ChannelID` when `isOrdered=true`, no ordering key otherwise

## Dependencies

- `cloud.google.com/go/pubsub/v2` — Google Cloud Pub/Sub client
- `github.com/slackmgr/types` — shared Slack Manager types and interfaces
