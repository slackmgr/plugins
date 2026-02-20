//nolint:paralleltest // Tests need access to unexported functions
package pubsub

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/slackmgr/types"
)

func TestNew_Success(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "test-topic", "test-subscription", logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client == nil {
		t.Fatal("expected client to be non-nil")
	}

	if client.topic != "test-topic" {
		t.Errorf("expected topic to be 'test-topic', got %q", client.topic)
	}

	if client.subscription != "test-subscription" {
		t.Errorf("expected subscription to be 'test-subscription', got %q", client.subscription)
	}
}

func TestNew_NilClient(t *testing.T) {
	logger := newMockLogger()

	client, err := New(nil, "test-topic", "", logger)

	if err == nil {
		t.Fatal("expected error for nil client")
	}

	if client != nil {
		t.Error("expected client to be nil when error occurs")
	}

	expected := "pub/sub client cannot be nil"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestNew_NilLogger(t *testing.T) {
	gcpClient := &pubsub.Client{}

	client, err := New(gcpClient, "test-topic", "", nil)

	if err == nil {
		t.Fatal("expected error for nil logger")
	}

	if client != nil {
		t.Error("expected client to be nil when error occurs")
	}

	expected := "logger cannot be nil"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestNew_WithOptions(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "test-topic", "", logger,
		WithPublisherDelayThreshold(50*time.Millisecond),
		WithPublisherCountThreshold(200),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.opts.publisherDelayThreshold != 50*time.Millisecond {
		t.Errorf("expected publisherDelayThreshold to be 50ms, got %v", client.opts.publisherDelayThreshold)
	}

	if client.opts.publisherCountThreshold != 200 {
		t.Errorf("expected publisherCountThreshold to be 200, got %d", client.opts.publisherCountThreshold)
	}
}

func TestNew_EmptySubscription(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "test-topic", "", logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.subscription != "" {
		t.Errorf("expected empty subscription, got %q", client.subscription)
	}
}

func TestInit_Success(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := client.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != client {
		t.Error("expected Init to return same client instance")
	}

	if !client.initialized.Load() {
		t.Error("expected client to be initialized")
	}
}

func TestInit_AlreadyInitialized(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	// Call Init again
	result, err := client.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result != client {
		t.Error("expected Init to return same client instance")
	}

	// Should only have called Publisher once
	if len(mockClient.publisherCalls) != 1 {
		t.Errorf("expected 1 publisher call, got %d", len(mockClient.publisherCalls))
	}
}

func TestInit_EmptyTopic(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "", "", logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Manually set topic to empty to bypass New validation
	client.topic = ""

	_, err = client.Init()

	if err == nil {
		t.Fatal("expected error for empty topic")
	}

	expected := "pub/sub topic cannot be empty"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestInit_InvalidPublisherOptions(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, err := New(gcpClient, "test-topic", "", logger,
		WithPublisherCountThreshold(0), // Invalid
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = client.Init()

	if err == nil {
		t.Fatal("expected error for invalid publisher options")
	}

	if !errors.Is(err, errors.Unwrap(err)) && err.Error() == "" {
		t.Error("expected wrapped error message")
	}
}

func TestInit_WithSubscription(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()
	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))

	_, err := client.Init()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mockClient.subscriberCalls) != 1 {
		t.Errorf("expected 1 subscriber call, got %d", len(mockClient.subscriberCalls))
	}

	if mockClient.subscriberCalls[0] != "test-subscription" {
		t.Errorf("expected subscriber call with 'test-subscription', got %q", mockClient.subscriberCalls[0])
	}
}

func TestInit_InvalidSubscriberOptions(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger,
		WithPubSubClient(mockClient),
		WithSubscriberShutdownTimeout(0), // Invalid
	)

	_, err := client.Init()

	if err == nil {
		t.Fatal("expected error for invalid subscriber options")
	}
}

func TestInit_PublisherSettingsApplied(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger,
		WithPubSubClient(mockClient),
		WithPublisherDelayThreshold(100*time.Millisecond),
		WithPublisherCountThreshold(50),
		WithPublisherByteThreshold(500000),
	)

	_, _ = client.Init()

	if !mockPub.enableMessageOrdering {
		t.Error("expected message ordering to be enabled")
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

func TestInit_SubscriberSettingsApplied(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()
	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger,
		WithPubSubClient(mockClient),
		WithSubscriberMaxExtension(15*time.Minute),
		WithSubscriberMaxDurationPerAckExtension(2*time.Minute),
		WithSubscriberMinDurationPerAckExtension(30*time.Second),
		WithSubscriberMaxOutstandingMessages(50),
		WithSubscriberMaxOutstandingBytes(500000),
		WithSubscriberShutdownTimeout(5*time.Second),
	)

	_, _ = client.Init()

	if mockSub.maxExtension != 15*time.Minute {
		t.Errorf("expected max extension 15m, got %v", mockSub.maxExtension)
	}

	if mockSub.maxDurationPerAckExtension != 2*time.Minute {
		t.Errorf("expected max duration per ack extension 2m, got %v", mockSub.maxDurationPerAckExtension)
	}

	if mockSub.minDurationPerAckExtension != 30*time.Second {
		t.Errorf("expected min duration per ack extension 30s, got %v", mockSub.minDurationPerAckExtension)
	}

	if mockSub.maxOutstandingMessages != 50 {
		t.Errorf("expected max outstanding messages 50, got %d", mockSub.maxOutstandingMessages)
	}

	if mockSub.maxOutstandingBytes != 500000 {
		t.Errorf("expected max outstanding bytes 500000, got %d", mockSub.maxOutstandingBytes)
	}

	if mockSub.shutdownOptions == nil {
		t.Fatal("expected shutdown options to be set")
	}

	if mockSub.shutdownOptions.Timeout != 5*time.Second {
		t.Errorf("expected shutdown timeout 5s, got %v", mockSub.shutdownOptions.Timeout)
	}
}

func TestName(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "my-topic", "", logger)

	if client.Name() != "my-topic" {
		t.Errorf("expected Name() to return 'my-topic', got %q", client.Name())
	}
}

func TestClose(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	client.Close()

	if !mockPub.stopCalled.Load() {
		t.Error("expected publisher Stop to be called")
	}
}

func TestClose_NilPublisher(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger)

	// Close without Init - should not panic
	client.Close()
}

func TestSend_Success(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockPub.publishFunc = func(_ context.Context, msg *pubsub.Message) pubsubPublishResult {
		return &mockPublishResult{serverID: "server-123"}
	}
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	err := client.Send(context.Background(), "group-1", "dedup-1", "test-body")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mockPub.publishedMessages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(mockPub.publishedMessages))
	}

	msg := mockPub.publishedMessages[0]

	if string(msg.Data) != "test-body" {
		t.Errorf("expected body 'test-body', got %q", string(msg.Data))
	}

	if msg.OrderingKey != "group-1" {
		t.Errorf("expected ordering key 'group-1', got %q", msg.OrderingKey)
	}

	if msg.Attributes["dedup_id"] != "dedup-1" {
		t.Errorf("expected dedup_id 'dedup-1', got %q", msg.Attributes["dedup_id"])
	}
}

func TestSend_NotInitialized(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger)

	err := client.Send(context.Background(), "group-1", "dedup-1", "test-body")

	if err == nil {
		t.Fatal("expected error for uninitialized client")
	}

	expected := "pub/sub client not initialized"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestSend_EmptyGroupID(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	err := client.Send(context.Background(), "", "dedup-1", "test-body")

	if err == nil {
		t.Fatal("expected error for empty groupID")
	}

	expected := "groupID cannot be empty"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestSend_EmptyDedupID(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	err := client.Send(context.Background(), "group-1", "", "test-body")

	if err == nil {
		t.Fatal("expected error for empty dedupID")
	}

	expected := "dedupID cannot be empty"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestSend_EmptyBody(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	err := client.Send(context.Background(), "group-1", "dedup-1", "")

	if err == nil {
		t.Fatal("expected error for empty body")
	}

	expected := "body cannot be empty"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestSend_PublishError(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockPub.publishFunc = func(_ context.Context, _ *pubsub.Message) pubsubPublishResult {
		return &mockPublishResult{err: errors.New("publish failed")}
	}
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	err := client.Send(context.Background(), "group-1", "dedup-1", "test-body")

	if err == nil {
		t.Fatal("expected error for publish failure")
	}

	if !errors.Is(errors.Unwrap(err), errors.New("")) {
		// Just check that error message contains useful info
		if err.Error() == "" {
			t.Error("expected non-empty error message")
		}
	}
}

func TestReceive_Success(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()

	var handlerCalled atomic.Bool
	mockSub.receiveFunc = func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		handlerCalled.Store(true)
		// Simulate receiving a message
		msg := &pubsub.Message{
			ID:          "msg-123",
			Data:        []byte("test-data"),
			OrderingKey: "channel-1",
		}
		f(ctx, msg)
		return nil
	}

	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	sinkCh := make(chan *types.FifoQueueItem, 1)

	err := client.Receive(context.Background(), sinkCh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handlerCalled.Load() {
		t.Error("expected receive handler to be called")
	}

	// Channel should be closed
	_, open := <-sinkCh
	if open {
		// There might be one item, consume it and check again
		_, stillOpen := <-sinkCh
		if stillOpen {
			t.Error("expected sink channel to be closed")
		}
	}
}

func TestReceive_NotInitialized(t *testing.T) {
	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger)

	sinkCh := make(chan *types.FifoQueueItem)

	err := client.Receive(context.Background(), sinkCh)

	if err == nil {
		t.Fatal("expected error for uninitialized client")
	}

	expected := "pub/sub client not initialized"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}

	// Channel should be closed
	_, open := <-sinkCh
	if open {
		t.Error("expected sink channel to be closed")
	}
}

func TestReceive_NoSubscriber(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockClient.defaultPub = mockPub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	// No subscription
	client, _ := New(gcpClient, "test-topic", "", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	sinkCh := make(chan *types.FifoQueueItem)

	err := client.Receive(context.Background(), sinkCh)

	if err == nil {
		t.Fatal("expected error for missing subscriber")
	}

	expected := "pub/sub client subscriber is not configured"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestReceive_AlreadyReceiving(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()

	// Simulate a long-running receive that blocks
	mockSub.receiveFunc = func(ctx context.Context, _ func(context.Context, *pubsub.Message)) error {
		<-ctx.Done()
		return ctx.Err()
	}

	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start first receive in goroutine
	sinkCh1 := make(chan *types.FifoQueueItem)
	go func() {
		_ = client.Receive(ctx, sinkCh1)
	}()

	// Wait for first receive to start
	time.Sleep(50 * time.Millisecond)

	// Try second receive
	sinkCh2 := make(chan *types.FifoQueueItem)
	err := client.Receive(context.Background(), sinkCh2)

	if err == nil {
		t.Fatal("expected error for already receiving")
	}

	expected := "pub/sub client is already receiving messages"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}

	cancel()
}

func TestReceive_ContextCancellation(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()

	mockSub.receiveFunc = func(ctx context.Context, _ func(context.Context, *pubsub.Message)) error {
		<-ctx.Done()
		return ctx.Err()
	}

	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	ctx, cancel := context.WithCancel(context.Background())
	sinkCh := make(chan *types.FifoQueueItem)

	errCh := make(chan error)
	go func() {
		errCh <- client.Receive(ctx, sinkCh)
	}()

	// Cancel context
	cancel()

	err := <-errCh

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestReceiveHandler_MessageConversion(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()

	var receivedItem *types.FifoQueueItem
	mockSub.receiveFunc = func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		msg := &pubsub.Message{
			ID:          "msg-456",
			Data:        []byte("message-body"),
			OrderingKey: "channel-abc",
		}
		f(ctx, msg)
		return nil
	}

	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	sinkCh := make(chan *types.FifoQueueItem, 1)

	_ = client.Receive(context.Background(), sinkCh)

	select {
	case item := <-sinkCh:
		receivedItem = item
	default:
	}

	if receivedItem == nil {
		t.Fatal("expected to receive an item")
	}

	if receivedItem.MessageID != "msg-456" {
		t.Errorf("expected MessageID 'msg-456', got %q", receivedItem.MessageID)
	}

	if receivedItem.SlackChannelID != "channel-abc" {
		t.Errorf("expected SlackChannelID 'channel-abc', got %q", receivedItem.SlackChannelID)
	}

	if receivedItem.Body != "message-body" {
		t.Errorf("expected Body 'message-body', got %q", receivedItem.Body)
	}

	if receivedItem.Ack == nil {
		t.Error("expected Ack function to be set")
	}

	if receivedItem.Nack == nil {
		t.Error("expected Nack function to be set")
	}
}

func TestTrySend_Success(t *testing.T) {
	ctx := context.Background()
	item := &types.FifoQueueItem{MessageID: "test"}
	sinkCh := make(chan *types.FifoQueueItem, 1)

	err := trySend(ctx, item, sinkCh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	received := <-sinkCh
	if received.MessageID != "test" {
		t.Error("expected to receive the same item")
	}
}

func TestTrySend_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	item := &types.FifoQueueItem{MessageID: "test"}
	sinkCh := make(chan *types.FifoQueueItem) // unbuffered, will block

	err := trySend(ctx, item, sinkCh)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestTrySend_ContextDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(time.Millisecond) // Ensure timeout

	item := &types.FifoQueueItem{MessageID: "test"}
	sinkCh := make(chan *types.FifoQueueItem) // unbuffered, will block

	err := trySend(ctx, item, sinkCh)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded error, got %v", err)
	}
}

func TestReceiveHandler_ContextCancellation_Path(t *testing.T) {
	mockClient := newMockPubSubClient()
	mockPub := newMockPublisher()
	mockSub := newMockSubscriber()

	var handlerCalled atomic.Bool
	mockSub.receiveFunc = func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
		// Create a cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		msg := &pubsub.Message{
			ID:          "msg-789",
			Data:        []byte("test"),
			OrderingKey: "channel",
		}
		handlerCalled.Store(true)
		f(cancelledCtx, msg)
		return nil
	}

	mockClient.defaultPub = mockPub
	mockClient.defaultSub = mockSub

	gcpClient := &pubsub.Client{}
	logger := newMockLogger()

	client, _ := New(gcpClient, "test-topic", "test-subscription", logger, WithPubSubClient(mockClient))
	_, _ = client.Init()

	// Unbuffered channel - will block, causing context cancellation path
	sinkCh := make(chan *types.FifoQueueItem)

	err := client.Receive(context.Background(), sinkCh)
	// Receive should complete without error (it returns the subscriber's error, which is nil here)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !handlerCalled.Load() {
		t.Error("expected receive handler to be called")
	}

	// The message should not have been sent to sinkCh due to cancelled context
	// Channel should be closed after Receive returns
	_, open := <-sinkCh
	if open {
		t.Error("expected sink channel to be closed with no items")
	}
}
