// Package sqs provides an AWS SQS queue backend for the Slack Manager
// ecosystem. It implements FIFO queue consumption with automatic visibility
// timeout extension and a webhook handler that forwards HTTP callbacks to
// SQS queues.
//
// # Client
//
// [Client] reads messages from an SQS FIFO queue and delivers them to a
// caller-supplied channel as [github.com/slackmgr/types.FifoQueueItem]
// values. While a message is in flight, a background goroutine periodically
// extends its visibility timeout so it is not redelivered before processing
// completes. Callers signal completion by invoking the Ack or Nack callback
// on each item.
//
// Create a client with [New] and initialise it with [Client.Init]:
//
//	client, err := sqs.New(&awsCfg, "events.fifo", logger,
//	    sqs.WithSqsVisibilityTimeout(60),
//	).Init(ctx)
//
// Then start consuming:
//
//	sinkCh := make(chan *types.FifoQueueItem)
//	go client.Receive(ctx, sinkCh)
//	for item := range sinkCh {
//	    process(item)
//	    item.Ack()
//	}
//
// # WebhookHandler
//
// [WebhookHandler] converts incoming HTTP webhook callbacks into SQS messages.
// It implements the [github.com/slackmgr/types.WebhookHandler] interface and supports both FIFO
// and standard queues. For FIFO queues the handler derives a per-message
// deduplication ID from a SHA-256 hash of the callback fields, preventing
// duplicate messages caused by webhook retries.
//
//	handler, err := sqs.NewWebhookHandler(&awsCfg).Init(ctx)
//	if handler.ShouldHandleWebhook(ctx, target) {
//	    if err := handler.HandleWebhook(ctx, target, data, logger); err != nil {
//	        log.Printf("webhook error: %v", err)
//	    }
//	}
//
// # Configuration
//
// Both [Client] and [WebhookHandler] accept functional options that are
// passed to the constructor and take effect before [Client.Init] or
// [WebhookHandler.Init] is called. See the With* functions for available
// settings and their defaults.
//
// # Visibility Extension
//
// Visibility extension is best-effort. If an extension call fails (for
// example due to a transient network error or SQS throttling), the message
// is removed from extension tracking and will become visible in SQS again
// after the current timeout expires. Downstream processing should therefore
// be idempotent to handle potential duplicate deliveries gracefully.
//
// A message is never extended beyond the duration set by
// [WithMaxMessageExtension] (default: 10 minutes). Once that limit is
// reached the message is dropped from tracking regardless of whether Ack or
// Nack has been called.
package sqs
