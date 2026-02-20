# CLAUDE.md

This file provides guidance to Claude Code when working with the `sqs` plugin. See also the root `CLAUDE.md` for monorepo-wide conventions.

## Module Overview

`github.com/slackmgr/plugins/sqs` is a Go plugin that integrates AWS SQS with the Slack Manager ecosystem. It provides:
- FIFO queue consumption with automatic visibility timeout extension
- A webhook handler that converts HTTP callbacks into SQS messages

## Architecture

**Core components:**

- **`client.go`** — Main SQS client wrapping AWS SDK v2. Functional options pattern. Key methods: `New()`, `Init()`, `Send()`, `Receive()`.

- **`message_extender.go`** — Background goroutine that manages in-flight message visibility extensions. Checks at one-third of the visibility timeout interval (minimum 5 s); extends messages past the halfway point of their current window.

- **`extendable_message.go`** — Mutex-protected message wrapper. `Ack()` triggers message deletion using `context.Background()` with a 2 s timeout (deletion must complete regardless of the caller's context state). `Nack()` is a no-op that only clears callbacks — it makes no AWS call.

- **`options.go`** — Functional options with validation. All `With*` functions.

- **`webhook_handler.go`** — Converts incoming HTTP webhooks to SQS messages. Generates FIFO deduplication IDs via SHA-256 hash of `(channelID, messageID, callbackID, timestamp)` with null-byte delimiters to prevent collisions.

- **`sqs_client.go`** — Internal `sqsClient` interface for dependency injection / testing.

**Initialization flow:**
1. `New(awsCfg, queueName, logger, opts...)` — create client, no AWS calls
2. `Init(ctx)` — validate options, resolve queue URL, start message extender goroutine
3. `Receive(ctx, sinkCh)` — blocking loop; run in a goroutine

**Key constraints:**
- Queue name must end with `.fifo`
- `Init` is idempotent; subsequent calls are no-ops
- Visibility extension is best-effort: on failure the message is dropped from tracking and may be redelivered — downstream processing must be idempotent

## Dependencies

- `github.com/aws/aws-sdk-go-v2` — AWS SDK v2
- `github.com/slackmgr/types` — shared Slack Manager types and interfaces
- `golang.org/x/sync` — semaphore for bounded concurrent extension
