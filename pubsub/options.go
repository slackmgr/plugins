package pubsub

import (
	"errors"
	"time"
)

type Option func(*Options)

type Options struct {
	publisherDelayThreshold              time.Duration
	publisherCountThreshold              int
	publisherByteThreshold               int
	subscriberMaxExtension               time.Duration
	subscriberMaxDurationPerAckExtension time.Duration
	subscriberMinDurationPerAckExtension time.Duration
	subscriberMaxOutstandingMessages     int
	subscriberMaxOutstandingBytes        int
	subscriberShutdownTimeout            time.Duration
	pubsubClient                         pubsubClient
}

func newOptions() *Options {
	return &Options{
		publisherDelayThreshold:              10 * time.Millisecond,
		publisherCountThreshold:              100,
		publisherByteThreshold:               1e6, // 1 MB
		subscriberMaxExtension:               10 * time.Minute,
		subscriberMaxDurationPerAckExtension: time.Minute,
		subscriberMinDurationPerAckExtension: 10 * time.Second,
		subscriberMaxOutstandingMessages:     100,
		subscriberMaxOutstandingBytes:        1e6, // 1 MB
		subscriberShutdownTimeout:            time.Second,
	}
}

func (o *Options) validatePublisher() error {
	if o.publisherDelayThreshold < 0 {
		return errors.New("publisher delay threshold must be non-negative")
	}

	if o.publisherCountThreshold <= 0 {
		return errors.New("publisher count threshold must be greater than zero")
	}

	if o.publisherByteThreshold <= 0 {
		return errors.New("publisher byte threshold must be greater than zero")
	}

	return nil
}

func (o *Options) validateSubscriber() error {
	if o.subscriberMaxExtension < time.Minute || o.subscriberMaxExtension > time.Hour {
		return errors.New("subscriber max extension must be between 1 minute and 1 hour")
	}

	if o.subscriberMaxDurationPerAckExtension < 10*time.Second || o.subscriberMaxDurationPerAckExtension > 600*time.Second {
		return errors.New("subscriber max duration per ack extension must be between 10 seconds and 10 minutes")
	}

	if o.subscriberMinDurationPerAckExtension < 10*time.Second || o.subscriberMinDurationPerAckExtension > 600*time.Second {
		return errors.New("subscriber min duration per ack extension must be between 10 seconds and 10 minutes")
	}

	if o.subscriberMinDurationPerAckExtension >= o.subscriberMaxDurationPerAckExtension {
		return errors.New("subscriber min duration per ack extension must be less than max duration per ack extension")
	}

	if o.subscriberMaxOutstandingMessages < 1 {
		return errors.New("subscriber max outstanding messages must be greater than or equal to 1")
	}

	if o.subscriberMaxOutstandingBytes < 1e4 {
		return errors.New("subscriber max outstanding bytes must be greater than or equal to 10 KB")
	}

	if o.subscriberShutdownTimeout <= 0 {
		return errors.New("subscriber shutdown timeout must be greater than zero")
	}

	return nil
}

func WithPublisherDelayThreshold(d time.Duration) Option {
	return func(o *Options) {
		o.publisherDelayThreshold = d
	}
}

func WithPublisherCountThreshold(n int) Option {
	return func(o *Options) {
		o.publisherCountThreshold = n
	}
}

func WithPublisherByteThreshold(n int) Option {
	return func(o *Options) {
		o.publisherByteThreshold = n
	}
}

func WithSubscriberMaxExtension(d time.Duration) Option {
	return func(o *Options) {
		o.subscriberMaxExtension = d
	}
}

func WithSubscriberMaxDurationPerAckExtension(d time.Duration) Option {
	return func(o *Options) {
		o.subscriberMaxDurationPerAckExtension = d
	}
}

func WithSubscriberMinDurationPerAckExtension(d time.Duration) Option {
	return func(o *Options) {
		o.subscriberMinDurationPerAckExtension = d
	}
}

func WithSubscriberMaxOutstandingMessages(n int) Option {
	return func(o *Options) {
		o.subscriberMaxOutstandingMessages = n
	}
}

func WithSubscriberMaxOutstandingBytes(n int) Option {
	return func(o *Options) {
		o.subscriberMaxOutstandingBytes = n
	}
}

func WithSubscriberShutdownTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.subscriberShutdownTimeout = d
	}
}

// WithPubSubClient sets a custom pubsubClient implementation for testing.
func WithPubSubClient(client pubsubClient) Option {
	return func(o *Options) {
		o.pubsubClient = client
	}
}
