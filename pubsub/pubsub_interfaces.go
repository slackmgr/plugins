package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

// pubsubClient abstracts *pubsub.Client for testing.
type pubsubClient interface {
	Publisher(topic string) pubsubPublisher
	Subscriber(subscription string) pubsubSubscriber
}

// pubsubPublisher abstracts *pubsub.Publisher for testing.
type pubsubPublisher interface {
	Publish(ctx context.Context, msg *pubsub.Message) pubsubPublishResult
	Stop()
	SetEnableMessageOrdering(enabled bool)
	SetDelayThreshold(d time.Duration)
	SetCountThreshold(n int)
	SetByteThreshold(n int)
}

// pubsubSubscriber abstracts *pubsub.Subscriber for testing.
type pubsubSubscriber interface {
	Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error
	SetMaxExtension(d time.Duration)
	SetMaxDurationPerAckExtension(d time.Duration)
	SetMinDurationPerAckExtension(d time.Duration)
	SetMaxOutstandingMessages(n int)
	SetMaxOutstandingBytes(n int)
	SetShutdownOptions(opts *pubsub.ShutdownOptions)
}

// pubsubPublishResult abstracts *pubsub.PublishResult for testing.
type pubsubPublishResult interface {
	Get(ctx context.Context) (serverID string, err error)
}
