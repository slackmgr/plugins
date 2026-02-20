package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

// realPubSubClient wraps *pubsub.Client to implement pubsubClient.
type realPubSubClient struct {
	client *pubsub.Client
}

//nolint:ireturn // Returns interface for dependency injection pattern
func newRealPubSubClient(client *pubsub.Client) pubsubClient {
	return &realPubSubClient{client: client}
}

//nolint:ireturn // Interface required by pubsubClient interface
func (r *realPubSubClient) Publisher(topic string) pubsubPublisher {
	return newRealPublisher(r.client.Publisher(topic))
}

//nolint:ireturn // Interface required by pubsubClient interface
func (r *realPubSubClient) Subscriber(subscription string) pubsubSubscriber {
	return newRealSubscriber(r.client.Subscriber(subscription))
}

// realPublisher wraps *pubsub.Publisher to implement pubsubPublisher.
type realPublisher struct {
	publisher *pubsub.Publisher
}

//nolint:ireturn // Returns interface for dependency injection pattern
func newRealPublisher(publisher *pubsub.Publisher) pubsubPublisher {
	return &realPublisher{publisher: publisher}
}

//nolint:ireturn // Interface required by pubsubPublisher interface
func (r *realPublisher) Publish(ctx context.Context, msg *pubsub.Message) pubsubPublishResult {
	return r.publisher.Publish(ctx, msg)
}

func (r *realPublisher) Stop() {
	r.publisher.Stop()
}

func (r *realPublisher) SetEnableMessageOrdering(enabled bool) {
	r.publisher.EnableMessageOrdering = enabled
}

func (r *realPublisher) SetDelayThreshold(d time.Duration) {
	r.publisher.PublishSettings.DelayThreshold = d
}

func (r *realPublisher) SetCountThreshold(n int) {
	r.publisher.PublishSettings.CountThreshold = n
}

func (r *realPublisher) SetByteThreshold(n int) {
	r.publisher.PublishSettings.ByteThreshold = n
}

// realSubscriber wraps *pubsub.Subscriber to implement pubsubSubscriber.
type realSubscriber struct {
	subscriber *pubsub.Subscriber
}

//nolint:ireturn // Returns interface for dependency injection pattern
func newRealSubscriber(subscriber *pubsub.Subscriber) pubsubSubscriber {
	return &realSubscriber{subscriber: subscriber}
}

func (r *realSubscriber) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
	return r.subscriber.Receive(ctx, f)
}

func (r *realSubscriber) SetMaxExtension(d time.Duration) {
	r.subscriber.ReceiveSettings.MaxExtension = d
}

func (r *realSubscriber) SetMaxDurationPerAckExtension(d time.Duration) {
	r.subscriber.ReceiveSettings.MaxDurationPerAckExtension = d
}

func (r *realSubscriber) SetMinDurationPerAckExtension(d time.Duration) {
	r.subscriber.ReceiveSettings.MinDurationPerAckExtension = d
}

func (r *realSubscriber) SetMaxOutstandingMessages(n int) {
	r.subscriber.ReceiveSettings.MaxOutstandingMessages = n
}

func (r *realSubscriber) SetMaxOutstandingBytes(n int) {
	r.subscriber.ReceiveSettings.MaxOutstandingBytes = n
}

func (r *realSubscriber) SetShutdownOptions(opts *pubsub.ShutdownOptions) {
	r.subscriber.ReceiveSettings.ShutdownOptions = opts
}
