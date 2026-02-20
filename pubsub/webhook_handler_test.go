//nolint:paralleltest // Tests need access to unexported functions
package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/slackmgr/types"
)

func TestNewWebhookHandler(t *testing.T) {
	gcpClient := &pubsub.Client{}

	handler := NewWebhookHandler(gcpClient, true)

	if handler == nil {
		t.Fatal("expected handler to be non-nil")
	}

	if !handler.isOrdered {
		t.Error("expected isOrdered to be true")
	}

	if handler.publishers == nil {
		t.Error("expected publishers map to be initialized")
	}
}

func TestNewWebhookHandler_NotOrdered(t *testing.T) {
	gcpClient := &pubsub.Client{}

	handler := NewWebhookHandler(gcpClient, false)

	if handler.isOrdered {
		t.Error("expected isOrdered to be false")
	}
}

func TestNewWebhookHandler_WithOptions(t *testing.T) {
	gcpClient := &pubsub.Client{}

	handler := NewWebhookHandler(gcpClient, true,
		WithPublisherDelayThreshold(50*time.Millisecond),
		WithPublisherCountThreshold(200),
	)

	if handler.opts.publisherDelayThreshold != 50*time.Millisecond {
		t.Errorf("expected delay threshold 50ms, got %v", handler.opts.publisherDelayThreshold)
	}

	if handler.opts.publisherCountThreshold != 200 {
		t.Errorf("expected count threshold 200, got %d", handler.opts.publisherCountThreshold)
	}
}

func TestWebhookHandler_Init_Success(t *testing.T) {
	mockClient := newMockPubSubClient()

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))

	result, err := handler.Init(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != handler {
		t.Error("expected Init to return same handler instance")
	}

	if !handler.initialized.Load() {
		t.Error("expected handler to be initialized")
	}
}

func TestWebhookHandler_Init_AlreadyInitialized(t *testing.T) {
	mockClient := newMockPubSubClient()

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	// Init again
	result, err := handler.Init(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != handler {
		t.Error("expected Init to return same handler instance")
	}
}

func TestWebhookHandler_Init_InvalidOptions(t *testing.T) {
	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true,
		WithPublisherCountThreshold(0), // Invalid
	)

	_, err := handler.Init(context.Background())

	if err == nil {
		t.Fatal("expected error for invalid options")
	}
}

func TestWebhookHandler_Close(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub1 := newMockPublisher()
	mockPub2 := newMockPublisher()

	topicPublishers := map[string]pubsubPublisher{
		"topic1": mockPub1,
		"topic2": mockPub2,
	}
	mockClient.publisherFunc = func(topic string) pubsubPublisher {
		return topicPublishers[topic]
	}

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	// Simulate cached publishers
	handler.publishers["topic1"] = mockPub1
	handler.publishers["topic2"] = mockPub2

	handler.Close()

	if !mockPub1.stopCalled.Load() {
		t.Error("expected Stop to be called on publisher 1")
	}

	if !mockPub2.stopCalled.Load() {
		t.Error("expected Stop to be called on publisher 2")
	}
}

func TestShouldHandleWebhook_ValidTopicNames(t *testing.T) {
	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true)

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "standard topic",
			target: "projects/my-project/topics/my-topic",
			want:   true,
		},
		{
			name:   "domain prefixed project",
			target: "projects/google.com:my-project/topics/my-topic",
			want:   true,
		},
		{
			name:   "topic with dots",
			target: "projects/my-project/topics/my.topic.name",
			want:   true,
		},
		{
			name:   "topic with underscores",
			target: "projects/my-project/topics/my_topic_name",
			want:   true,
		},
		{
			name:   "topic with hyphens",
			target: "projects/my-project/topics/my-topic-name",
			want:   true,
		},
		{
			name:   "project with hyphens",
			target: "projects/my-project-123/topics/topic",
			want:   true,
		},
		{
			name:   "minimum length topic",
			target: "projects/abcdef/topics/abc",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handler.ShouldHandleWebhook(context.Background(), tt.target)
			if got != tt.want {
				t.Errorf("ShouldHandleWebhook(%q) = %v, want %v", tt.target, got, tt.want)
			}
		})
	}
}

func TestShouldHandleWebhook_InvalidTopicNames(t *testing.T) {
	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true)

	tests := []struct {
		name   string
		target string
	}{
		{
			name:   "empty string",
			target: "",
		},
		{
			name:   "URL",
			target: "https://example.com/webhook",
		},
		{
			name:   "short project ID",
			target: "projects/abc/topics/my-topic",
		},
		{
			name:   "missing topics",
			target: "projects/my-project/my-topic",
		},
		{
			name:   "missing project prefix",
			target: "my-project/topics/my-topic",
		},
		{
			name:   "topic starts with number",
			target: "projects/my-project/topics/1topic",
		},
		{
			name:   "topic too short",
			target: "projects/my-project/topics/ab",
		},
		{
			name:   "uppercase project ID",
			target: "projects/My-Project/topics/my-topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handler.ShouldHandleWebhook(context.Background(), tt.target)
			if got {
				t.Errorf("ShouldHandleWebhook(%q) = true, want false", tt.target)
			}
		})
	}
}

func TestHandleWebhook_Success_Ordered(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockPub.publishFunc = func(_ context.Context, msg *pubsub.Message) pubsubPublishResult {
		return &mockPublishResult{serverID: "server-123"}
	}
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	data := &types.WebhookCallback{
		ChannelID: "C12345",
		UserID:    "U12345",
	}
	logger := newMockLogger()

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", data, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mockPub.publishedMessages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mockPub.publishedMessages))
	}

	msg := mockPub.publishedMessages[0]

	// Verify ordering key is set for ordered handler
	if msg.OrderingKey != "C12345" {
		t.Errorf("expected ordering key 'C12345', got %q", msg.OrderingKey)
	}

	// Verify message body is JSON
	var decoded types.WebhookCallback
	if err := json.Unmarshal(msg.Data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if decoded.ChannelID != "C12345" {
		t.Errorf("expected ChannelID 'C12345', got %q", decoded.ChannelID)
	}
}

func TestHandleWebhook_Success_NotOrdered(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockPub.publishFunc = func(_ context.Context, msg *pubsub.Message) pubsubPublishResult {
		return &mockPublishResult{serverID: "server-123"}
	}
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, false, WithPubSubClient(mockClient)) // Not ordered
	_, _ = handler.Init(context.Background())

	data := &types.WebhookCallback{
		ChannelID: "C12345",
		UserID:    "U12345",
	}
	logger := newMockLogger()

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", data, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := mockPub.publishedMessages[0]

	// Verify ordering key is NOT set for unordered handler
	if msg.OrderingKey != "" {
		t.Errorf("expected empty ordering key, got %q", msg.OrderingKey)
	}
}

func TestHandleWebhook_NotInitialized(t *testing.T) {
	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true)

	data := &types.WebhookCallback{ChannelID: "C12345"}
	logger := newMockLogger()

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", data, logger)

	if err == nil {
		t.Fatal("expected error for uninitialized handler")
	}

	expected := "pub/sub webhook handler not initialized"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestHandleWebhook_NilData(t *testing.T) {
	mockClient := newMockPubSubClient()

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	logger := newMockLogger()

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", nil, logger)

	if err == nil {
		t.Fatal("expected error for nil data")
	}

	expected := "webhook callback data cannot be nil"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestHandleWebhook_NilLogger(t *testing.T) {
	mockClient := newMockPubSubClient()

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	data := &types.WebhookCallback{ChannelID: "C12345"}

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", data, nil)

	if err == nil {
		t.Fatal("expected error for nil logger")
	}

	expected := "logger cannot be nil"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestHandleWebhook_PublishError(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockPub.publishFunc = func(_ context.Context, _ *pubsub.Message) pubsubPublishResult {
		return &mockPublishResult{err: errors.New("publish failed")}
	}
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	data := &types.WebhookCallback{ChannelID: "C12345"}
	logger := newMockLogger()

	err := handler.HandleWebhook(context.Background(), "projects/my-project/topics/my-topic", data, logger)

	if err == nil {
		t.Fatal("expected error for publish failure")
	}
}

func TestGetPublisher_NewPublisher(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	publisher := handler.getPublisher("projects/my-project/topics/my-topic")

	if publisher != mockPub {
		t.Error("expected publisher to be the mock")
	}

	if len(mockClient.publisherCalls) != 1 {
		t.Errorf("expected 1 publisher call, got %d", len(mockClient.publisherCalls))
	}
}

func TestGetPublisher_Cached(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	topic := "projects/my-project/topics/my-topic"

	// First call
	_ = handler.getPublisher(topic)
	// Second call - should use cache
	_ = handler.getPublisher(topic)

	if len(mockClient.publisherCalls) != 1 {
		t.Errorf("expected 1 publisher call (cached), got %d", len(mockClient.publisherCalls))
	}
}

func TestGetPublisher_ConcurrentAccess(t *testing.T) {
	mockClient := newMockPubSubClient()
	callCount := 0
	var mu sync.Mutex
	mockClient.publisherFunc = func(_ string) pubsubPublisher {
		mu.Lock()
		callCount++
		mu.Unlock()
		return newMockPublisher()
	}

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true, WithPubSubClient(mockClient))
	_, _ = handler.Init(context.Background())

	topic := "projects/my-project/topics/my-topic"

	// Launch multiple goroutines to access the same topic concurrently
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			_ = handler.getPublisher(topic)
		})
	}
	wg.Wait()

	// Due to double-check locking, there should only be 1 publisher created
	mu.Lock()
	finalCount := callCount
	mu.Unlock()

	if finalCount != 1 {
		t.Errorf("expected 1 publisher call with concurrent access, got %d", finalCount)
	}
}

func TestGetPublisher_SettingsApplied(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, true,
		WithPubSubClient(mockClient),
		WithPublisherDelayThreshold(100*time.Millisecond),
		WithPublisherCountThreshold(50),
		WithPublisherByteThreshold(500000),
	)
	_, _ = handler.Init(context.Background())

	_ = handler.getPublisher("projects/my-project/topics/my-topic")

	if !mockPub.enableMessageOrdering {
		t.Error("expected message ordering to be enabled for ordered handler")
	}

	if mockPub.delayThreshold != 100*time.Millisecond {
		t.Errorf("expected delay threshold 100ms, got %v", mockPub.delayThreshold)
	}

	if mockPub.countThreshold != 50 {
		t.Errorf("expected count threshold 50, got %d", mockPub.countThreshold)
	}

	if mockPub.byteThreshold != 500000 {
		t.Errorf("expected byte threshold 500000, got %d", mockPub.byteThreshold)
	}
}

func TestGetPublisher_NotOrdered_MessageOrderingDisabled(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	handler := NewWebhookHandler(gcpClient, false, WithPubSubClient(mockClient)) // Not ordered
	_, _ = handler.Init(context.Background())

	_ = handler.getPublisher("projects/my-project/topics/my-topic")

	if mockPub.enableMessageOrdering {
		t.Error("expected message ordering to be disabled for unordered handler")
	}
}
