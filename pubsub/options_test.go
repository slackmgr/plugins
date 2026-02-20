//nolint:paralleltest // Tests need access to unexported functions
package pubsub

import (
	"testing"
	"time"
)

func TestNewOptions_Defaults(t *testing.T) {
	opts := newOptions()

	if opts.publisherDelayThreshold != 10*time.Millisecond {
		t.Errorf("expected publisherDelayThreshold to be 10ms, got %v", opts.publisherDelayThreshold)
	}

	if opts.publisherCountThreshold != 100 {
		t.Errorf("expected publisherCountThreshold to be 100, got %d", opts.publisherCountThreshold)
	}

	if opts.publisherByteThreshold != 1e6 {
		t.Errorf("expected publisherByteThreshold to be 1MB, got %d", opts.publisherByteThreshold)
	}

	if opts.subscriberMaxExtension != 10*time.Minute {
		t.Errorf("expected subscriberMaxExtension to be 10m, got %v", opts.subscriberMaxExtension)
	}

	if opts.subscriberMaxDurationPerAckExtension != time.Minute {
		t.Errorf("expected subscriberMaxDurationPerAckExtension to be 1m, got %v", opts.subscriberMaxDurationPerAckExtension)
	}

	if opts.subscriberMinDurationPerAckExtension != 10*time.Second {
		t.Errorf("expected subscriberMinDurationPerAckExtension to be 10s, got %v", opts.subscriberMinDurationPerAckExtension)
	}

	if opts.subscriberMaxOutstandingMessages != 100 {
		t.Errorf("expected subscriberMaxOutstandingMessages to be 100, got %d", opts.subscriberMaxOutstandingMessages)
	}

	if opts.subscriberMaxOutstandingBytes != 1e6 {
		t.Errorf("expected subscriberMaxOutstandingBytes to be 1MB, got %d", opts.subscriberMaxOutstandingBytes)
	}

	if opts.subscriberShutdownTimeout != time.Second {
		t.Errorf("expected subscriberShutdownTimeout to be 1s, got %v", opts.subscriberShutdownTimeout)
	}

	if opts.pubsubClient != nil {
		t.Error("expected pubsubClient to be nil by default")
	}
}

func TestValidatePublisher_Success(t *testing.T) {
	opts := newOptions()

	if err := opts.validatePublisher(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestValidatePublisher_NegativeDelayThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherDelayThreshold = -1

	err := opts.validatePublisher()

	if err == nil {
		t.Error("expected error for negative delay threshold")
	}

	expected := "publisher delay threshold must be non-negative"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidatePublisher_ZeroDelayThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherDelayThreshold = 0

	if err := opts.validatePublisher(); err != nil {
		t.Errorf("expected no error for zero delay threshold, got %v", err)
	}
}

func TestValidatePublisher_ZeroCountThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherCountThreshold = 0

	err := opts.validatePublisher()

	if err == nil {
		t.Error("expected error for zero count threshold")
	}

	expected := "publisher count threshold must be greater than zero"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidatePublisher_NegativeCountThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherCountThreshold = -1

	err := opts.validatePublisher()

	if err == nil {
		t.Error("expected error for negative count threshold")
	}
}

func TestValidatePublisher_ZeroByteThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherByteThreshold = 0

	err := opts.validatePublisher()

	if err == nil {
		t.Error("expected error for zero byte threshold")
	}

	expected := "publisher byte threshold must be greater than zero"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidatePublisher_NegativeByteThreshold(t *testing.T) {
	opts := newOptions()
	opts.publisherByteThreshold = -1

	err := opts.validatePublisher()

	if err == nil {
		t.Error("expected error for negative byte threshold")
	}
}

func TestValidateSubscriber_Success(t *testing.T) {
	opts := newOptions()

	if err := opts.validateSubscriber(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestValidateSubscriber_MaxExtensionTooLow(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxExtension = 30 * time.Second

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for max extension below 1 minute")
	}

	expected := "subscriber max extension must be between 1 minute and 1 hour"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_MaxExtensionTooHigh(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxExtension = 2 * time.Hour

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for max extension above 1 hour")
	}
}

func TestValidateSubscriber_MaxDurationPerAckExtensionTooLow(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxDurationPerAckExtension = 5 * time.Second

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for max duration per ack extension below 10 seconds")
	}

	expected := "subscriber max duration per ack extension must be between 10 seconds and 10 minutes"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_MaxDurationPerAckExtensionTooHigh(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxDurationPerAckExtension = 15 * time.Minute

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for max duration per ack extension above 10 minutes")
	}
}

func TestValidateSubscriber_MinDurationPerAckExtensionTooLow(t *testing.T) {
	opts := newOptions()
	opts.subscriberMinDurationPerAckExtension = 5 * time.Second

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for min duration per ack extension below 10 seconds")
	}

	expected := "subscriber min duration per ack extension must be between 10 seconds and 10 minutes"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_MinDurationPerAckExtensionTooHigh(t *testing.T) {
	opts := newOptions()
	opts.subscriberMinDurationPerAckExtension = 15 * time.Minute

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for min duration per ack extension above 10 minutes")
	}
}

func TestValidateSubscriber_MinGreaterThanOrEqualToMax(t *testing.T) {
	opts := newOptions()
	opts.subscriberMinDurationPerAckExtension = 60 * time.Second
	opts.subscriberMaxDurationPerAckExtension = 60 * time.Second

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error when min >= max duration per ack extension")
	}

	expected := "subscriber min duration per ack extension must be less than max duration per ack extension"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_MinGreaterThanMax(t *testing.T) {
	opts := newOptions()
	opts.subscriberMinDurationPerAckExtension = 90 * time.Second
	opts.subscriberMaxDurationPerAckExtension = 60 * time.Second

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error when min > max duration per ack extension")
	}
}

func TestValidateSubscriber_MaxOutstandingMessagesZero(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxOutstandingMessages = 0

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for zero max outstanding messages")
	}

	expected := "subscriber max outstanding messages must be greater than or equal to 1"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_MaxOutstandingMessagesNegative(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxOutstandingMessages = -1

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for negative max outstanding messages")
	}
}

func TestValidateSubscriber_MaxOutstandingBytesTooLow(t *testing.T) {
	opts := newOptions()
	opts.subscriberMaxOutstandingBytes = 1000 // less than 10KB

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for max outstanding bytes below 10KB")
	}

	expected := "subscriber max outstanding bytes must be greater than or equal to 10 KB"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_ShutdownTimeoutZero(t *testing.T) {
	opts := newOptions()
	opts.subscriberShutdownTimeout = 0

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for zero shutdown timeout")
	}

	expected := "subscriber shutdown timeout must be greater than zero"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidateSubscriber_ShutdownTimeoutNegative(t *testing.T) {
	opts := newOptions()
	opts.subscriberShutdownTimeout = -1

	err := opts.validateSubscriber()

	if err == nil {
		t.Error("expected error for negative shutdown timeout")
	}
}

func TestWithPublisherDelayThreshold(t *testing.T) {
	opts := newOptions()
	expected := 50 * time.Millisecond

	WithPublisherDelayThreshold(expected)(opts)

	if opts.publisherDelayThreshold != expected {
		t.Errorf("expected %v, got %v", expected, opts.publisherDelayThreshold)
	}
}

func TestWithPublisherCountThreshold(t *testing.T) {
	opts := newOptions()
	expected := 200

	WithPublisherCountThreshold(expected)(opts)

	if opts.publisherCountThreshold != expected {
		t.Errorf("expected %d, got %d", expected, opts.publisherCountThreshold)
	}
}

func TestWithPublisherByteThreshold(t *testing.T) {
	opts := newOptions()
	expected := 2000000

	WithPublisherByteThreshold(expected)(opts)

	if opts.publisherByteThreshold != expected {
		t.Errorf("expected %d, got %d", expected, opts.publisherByteThreshold)
	}
}

func TestWithSubscriberMaxExtension(t *testing.T) {
	opts := newOptions()
	expected := 15 * time.Minute

	WithSubscriberMaxExtension(expected)(opts)

	if opts.subscriberMaxExtension != expected {
		t.Errorf("expected %v, got %v", expected, opts.subscriberMaxExtension)
	}
}

func TestWithSubscriberMaxDurationPerAckExtension(t *testing.T) {
	opts := newOptions()
	expected := 2 * time.Minute

	WithSubscriberMaxDurationPerAckExtension(expected)(opts)

	if opts.subscriberMaxDurationPerAckExtension != expected {
		t.Errorf("expected %v, got %v", expected, opts.subscriberMaxDurationPerAckExtension)
	}
}

func TestWithSubscriberMinDurationPerAckExtension(t *testing.T) {
	opts := newOptions()
	expected := 20 * time.Second

	WithSubscriberMinDurationPerAckExtension(expected)(opts)

	if opts.subscriberMinDurationPerAckExtension != expected {
		t.Errorf("expected %v, got %v", expected, opts.subscriberMinDurationPerAckExtension)
	}
}

func TestWithSubscriberMaxOutstandingMessages(t *testing.T) {
	opts := newOptions()
	expected := 200

	WithSubscriberMaxOutstandingMessages(expected)(opts)

	if opts.subscriberMaxOutstandingMessages != expected {
		t.Errorf("expected %d, got %d", expected, opts.subscriberMaxOutstandingMessages)
	}
}

func TestWithSubscriberMaxOutstandingBytes(t *testing.T) {
	opts := newOptions()
	expected := 2000000

	WithSubscriberMaxOutstandingBytes(expected)(opts)

	if opts.subscriberMaxOutstandingBytes != expected {
		t.Errorf("expected %d, got %d", expected, opts.subscriberMaxOutstandingBytes)
	}
}

func TestWithSubscriberShutdownTimeout(t *testing.T) {
	opts := newOptions()
	expected := 5 * time.Second

	WithSubscriberShutdownTimeout(expected)(opts)

	if opts.subscriberShutdownTimeout != expected {
		t.Errorf("expected %v, got %v", expected, opts.subscriberShutdownTimeout)
	}
}

func TestWithPubSubClient(t *testing.T) {
	opts := newOptions()
	mockClient := newMockPubSubClient()

	WithPubSubClient(mockClient)(opts)

	if opts.pubsubClient != mockClient {
		t.Error("expected pubsubClient to be set to mockClient")
	}
}
