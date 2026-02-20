package pubsub

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/slackmgr/types"
)

// mockPubSubClient implements pubsubClient for testing.
type mockPubSubClient struct {
	publisherFunc   func(topic string) pubsubPublisher
	subscriberFunc  func(subscription string) pubsubSubscriber
	defaultPub      pubsubPublisher
	defaultSub      pubsubSubscriber
	publisherCalls  []string
	subscriberCalls []string
	mu              sync.Mutex
}

func newMockPubSubClient() *mockPubSubClient {
	return &mockPubSubClient{}
}

//nolint:ireturn // Returns interface required by pubsubClient interface
func (m *mockPubSubClient) Publisher(topic string) pubsubPublisher {
	m.mu.Lock()
	m.publisherCalls = append(m.publisherCalls, topic)
	m.mu.Unlock()

	if m.publisherFunc != nil {
		return m.publisherFunc(topic)
	}
	return m.defaultPub
}

//nolint:ireturn // Returns interface required by pubsubClient interface
func (m *mockPubSubClient) Subscriber(subscription string) pubsubSubscriber {
	m.mu.Lock()
	m.subscriberCalls = append(m.subscriberCalls, subscription)
	m.mu.Unlock()

	if m.subscriberFunc != nil {
		return m.subscriberFunc(subscription)
	}
	return m.defaultSub
}

// mockPublisher implements pubsubPublisher for testing.
type mockPublisher struct {
	publishFunc           func(ctx context.Context, msg *pubsub.Message) pubsubPublishResult
	stopCalled            atomic.Bool
	enableMessageOrdering bool
	delayThreshold        time.Duration
	countThreshold        int
	byteThreshold         int
	publishedMessages     []*pubsub.Message
	mu                    sync.Mutex
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{}
}

//nolint:ireturn // Returns interface required by pubsubPublisher interface
func (m *mockPublisher) Publish(ctx context.Context, msg *pubsub.Message) pubsubPublishResult {
	m.mu.Lock()
	m.publishedMessages = append(m.publishedMessages, msg)
	m.mu.Unlock()

	if m.publishFunc != nil {
		return m.publishFunc(ctx, msg)
	}
	return &mockPublishResult{}
}

func (m *mockPublisher) Stop() {
	m.stopCalled.Store(true)
}

func (m *mockPublisher) SetEnableMessageOrdering(enabled bool) {
	m.mu.Lock()
	m.enableMessageOrdering = enabled
	m.mu.Unlock()
}

func (m *mockPublisher) SetDelayThreshold(d time.Duration) {
	m.mu.Lock()
	m.delayThreshold = d
	m.mu.Unlock()
}

func (m *mockPublisher) SetCountThreshold(n int) {
	m.mu.Lock()
	m.countThreshold = n
	m.mu.Unlock()
}

func (m *mockPublisher) SetByteThreshold(n int) {
	m.mu.Lock()
	m.byteThreshold = n
	m.mu.Unlock()
}

// mockSubscriber implements pubsubSubscriber for testing.
type mockSubscriber struct {
	receiveFunc                func(ctx context.Context, f func(context.Context, *pubsub.Message)) error
	maxExtension               time.Duration
	maxDurationPerAckExtension time.Duration
	minDurationPerAckExtension time.Duration
	maxOutstandingMessages     int
	maxOutstandingBytes        int
	shutdownOptions            *pubsub.ShutdownOptions
	receiveCalled              atomic.Bool
	mu                         sync.Mutex
}

func newMockSubscriber() *mockSubscriber {
	return &mockSubscriber{}
}

func (m *mockSubscriber) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
	m.receiveCalled.Store(true)
	if m.receiveFunc != nil {
		return m.receiveFunc(ctx, f)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockSubscriber) SetMaxExtension(d time.Duration) {
	m.mu.Lock()
	m.maxExtension = d
	m.mu.Unlock()
}

func (m *mockSubscriber) SetMaxDurationPerAckExtension(d time.Duration) {
	m.mu.Lock()
	m.maxDurationPerAckExtension = d
	m.mu.Unlock()
}

func (m *mockSubscriber) SetMinDurationPerAckExtension(d time.Duration) {
	m.mu.Lock()
	m.minDurationPerAckExtension = d
	m.mu.Unlock()
}

func (m *mockSubscriber) SetMaxOutstandingMessages(n int) {
	m.mu.Lock()
	m.maxOutstandingMessages = n
	m.mu.Unlock()
}

func (m *mockSubscriber) SetMaxOutstandingBytes(n int) {
	m.mu.Lock()
	m.maxOutstandingBytes = n
	m.mu.Unlock()
}

func (m *mockSubscriber) SetShutdownOptions(opts *pubsub.ShutdownOptions) {
	m.mu.Lock()
	m.shutdownOptions = opts
	m.mu.Unlock()
}

// mockPublishResult implements pubsubPublishResult for testing.
type mockPublishResult struct {
	serverID string
	err      error
}

func (m *mockPublishResult) Get(_ context.Context) (string, error) {
	return m.serverID, m.err
}

// mockLogger implements types.Logger for testing.
type mockLogger struct {
	debugLogs []string
	errorLogs []string
	fields    map[string]any
	mu        sync.Mutex
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		fields: make(map[string]any),
	}
}

func (m *mockLogger) Debug(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.debugLogs = append(m.debugLogs, msg)
}

func (m *mockLogger) Debugf(format string, _ ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.debugLogs = append(m.debugLogs, format)
}

func (m *mockLogger) Info(_ string) {}

func (m *mockLogger) Infof(_ string, _ ...any) {}

func (m *mockLogger) Warn(_ string) {}

func (m *mockLogger) Warnf(_ string, _ ...any) {}

func (m *mockLogger) Error(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorLogs = append(m.errorLogs, msg)
}

func (m *mockLogger) Errorf(format string, _ ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorLogs = append(m.errorLogs, format)
}

func (m *mockLogger) Fatal(_ string) {}

func (m *mockLogger) Fatalf(_ string, _ ...any) {}

//nolint:ireturn // Must return interface to implement types.Logger
func (m *mockLogger) WithField(key string, value any) types.Logger {
	m.mu.Lock()
	defer m.mu.Unlock()
	newLogger := newMockLogger()
	maps.Copy(newLogger.fields, m.fields)
	newLogger.fields[key] = value
	return newLogger
}

//nolint:ireturn // Must return interface to implement types.Logger
func (m *mockLogger) WithFields(fields map[string]any) types.Logger {
	m.mu.Lock()
	defer m.mu.Unlock()
	newLogger := newMockLogger()
	maps.Copy(newLogger.fields, m.fields)
	maps.Copy(newLogger.fields, fields)
	return newLogger
}
