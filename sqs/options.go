package sqs

import (
	"errors"
	"time"
)

// Option is a functional option for configuring a [Client].
// Options are passed to [New] and applied before [Client.Init] is called.
type Option func(*Options)

// Options holds the resolved configuration for a [Client].
// All fields are set to sensible defaults by [New]; use With* functions to
// override individual values.
type Options struct {
	sqsVisibilityTimeoutSeconds   int32
	sqsReceiveMaxNumberOfMessages int32
	sqsReceiveWaitTimeSeconds     int32
	sqsAPIMaxRetryAttempts        int
	sqsAPIMaxRetryBackoffDelay    time.Duration
	maxMessageExtension           time.Duration
	maxOutstandingMessages        int
	maxOutstandingBytes           int
	sqsClient                     sqsClient // Optional: injected SQS client for testing
}

func newOptions() *Options {
	return &Options{
		sqsVisibilityTimeoutSeconds:   30,
		sqsReceiveMaxNumberOfMessages: 1,
		sqsReceiveWaitTimeSeconds:     20,
		sqsAPIMaxRetryAttempts:        5,
		sqsAPIMaxRetryBackoffDelay:    10 * time.Second,
		maxMessageExtension:           10 * time.Minute,
		maxOutstandingMessages:        100,
		maxOutstandingBytes:           1e6, // 1 MB
	}
}

func (o *Options) validate() error {
	if o.sqsVisibilityTimeoutSeconds < 10 || o.sqsVisibilityTimeoutSeconds > 3600 {
		return errors.New("SQS message visibility timeout must be between 10 seconds and 1 hour")
	}

	if o.sqsReceiveMaxNumberOfMessages < 1 || o.sqsReceiveMaxNumberOfMessages > 10 {
		return errors.New("max number of messages per SQS receive must be between 1 and 10")
	}

	if o.sqsReceiveWaitTimeSeconds < 3 || o.sqsReceiveWaitTimeSeconds > 20 {
		return errors.New("SQS receive wait time must be between 3 and 20 seconds")
	}

	if o.sqsAPIMaxRetryAttempts < 0 || o.sqsAPIMaxRetryAttempts > 10 {
		return errors.New("max SQS API retry attempts must be between 0 and 10")
	}

	if o.sqsAPIMaxRetryBackoffDelay < 1*time.Second || o.sqsAPIMaxRetryBackoffDelay > 30*time.Second {
		return errors.New("max SQS API retry backoff delay must be between 1 and 30 seconds")
	}

	if o.maxMessageExtension < 1*time.Minute || o.maxMessageExtension > time.Hour {
		return errors.New("max message extension must be between 1 minute and 1 hour")
	}

	if o.maxOutstandingMessages < 1 {
		return errors.New("max outstanding messages must be greater than or equal to 1")
	}

	if o.maxOutstandingBytes < 1e4 {
		return errors.New("max outstanding bytes must be greater than or equal to 10 KB")
	}

	return nil
}

// WithSqsVisibilityTimeout sets the visibility timeout applied to each
// received message. While a message is hidden from other consumers its
// timeout is extended automatically by the background goroutine.
// Must be between 10 and 3600 seconds. Default: 30.
func WithSqsVisibilityTimeout(seconds int32) Option {
	return func(o *Options) {
		o.sqsVisibilityTimeoutSeconds = seconds
	}
}

// WithSqsReceiveMaxNumberOfMessages sets the maximum number of messages
// returned by a single ReceiveMessage API call. Must be between 1 and 10.
// Default: 1.
func WithSqsReceiveMaxNumberOfMessages(n int32) Option {
	return func(o *Options) {
		o.sqsReceiveMaxNumberOfMessages = n
	}
}

// WithSqsReceiveWaitTimeSeconds sets the long-poll wait duration for each
// ReceiveMessage API call. Longer values reduce empty responses and API costs.
// Must be between 3 and 20 seconds. Default: 20.
func WithSqsReceiveWaitTimeSeconds(seconds int32) Option {
	return func(o *Options) {
		o.sqsReceiveWaitTimeSeconds = seconds
	}
}

// WithSqsAPIMaxRetryAttempts sets the maximum number of retry attempts for
// failed SQS API calls. Must be between 0 and 10. Default: 5.
func WithSqsAPIMaxRetryAttempts(n int) Option {
	return func(o *Options) {
		o.sqsAPIMaxRetryAttempts = n
	}
}

// WithSqsAPIMaxRetryBackoffDelay sets the maximum backoff delay between
// consecutive SQS API retry attempts. Must be between 1 second and 30 seconds.
// Default: 10 seconds.
func WithSqsAPIMaxRetryBackoffDelay(d time.Duration) Option {
	return func(o *Options) {
		o.sqsAPIMaxRetryBackoffDelay = d
	}
}

// WithMaxMessageExtension sets the maximum total duration for which a
// message's visibility timeout may be extended after it was first received.
// Once a message reaches this age it is dropped from extension tracking and
// will become visible in SQS again after the current timeout expires.
// Must be between 1 minute and 1 hour. Default: 10 minutes.
func WithMaxMessageExtension(d time.Duration) Option {
	return func(o *Options) {
		o.maxMessageExtension = d
	}
}

// WithMaxOutstandingMessages sets the maximum number of in-flight messages
// before [Client.Receive] pauses reading from the queue. This prevents
// unbounded memory growth when processing is slower than delivery.
// Must be at least 1. Default: 100.
func WithMaxOutstandingMessages(n int) Option {
	return func(o *Options) {
		o.maxOutstandingMessages = n
	}
}

// WithMaxOutstandingBytes sets the maximum total byte size of in-flight
// messages before [Client.Receive] pauses reading from the queue.
// Must be at least 10 KB (10240 bytes). Default: 1 MB (1048576 bytes).
func WithMaxOutstandingBytes(n int) Option {
	return func(o *Options) {
		o.maxOutstandingBytes = n
	}
}

// WithSQSClient replaces the default AWS SQS client with a custom
// implementation of the internal sqsClient interface. This option is
// intended for testing with mock or stub clients.
func WithSQSClient(client sqsClient) Option {
	return func(o *Options) {
		o.sqsClient = client
	}
}
