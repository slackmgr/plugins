//nolint:paralleltest,testpackage // Tests use shared resources and need access to unexported functions
package sqs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewMessageExtender(t *testing.T) {
	opts := newOptions()
	logger := newMockLogger()

	ext := newMessageExtender(opts, logger)

	if ext == nil {
		t.Fatal("expected non-nil message extender")
	}

	if ext.inFlightMessages == nil {
		t.Error("expected inFlightMessages map to be initialized")
	}

	if ext.opts != opts {
		t.Error("expected opts to be set")
	}

	if ext.logger != logger {
		t.Error("expected logger to be set")
	}
}

func TestAllowMoreMessages_UnderLimit(t *testing.T) {
	opts := newOptions()
	opts.maxOutstandingMessages = 10
	opts.maxOutstandingBytes = 100000

	ext := newMessageExtender(opts, newMockLogger())

	if !ext.AllowMoreMessages() {
		t.Error("expected AllowMoreMessages to return true when under both limits")
	}
}

func TestAllowMoreMessages_AtCountLimit(t *testing.T) {
	opts := newOptions()
	opts.maxOutstandingMessages = 10
	opts.maxOutstandingBytes = 100000

	ext := newMessageExtender(opts, newMockLogger())
	ext.inFlightMsgCount.Store(10)

	if ext.AllowMoreMessages() {
		t.Error("expected AllowMoreMessages to return false at message count limit")
	}
}

func TestAllowMoreMessages_AtBytesLimit(t *testing.T) {
	opts := newOptions()
	opts.maxOutstandingMessages = 10
	opts.maxOutstandingBytes = 100000

	ext := newMessageExtender(opts, newMockLogger())
	ext.inFlightMsgBytes.Store(100000)

	if ext.AllowMoreMessages() {
		t.Error("expected AllowMoreMessages to return false at bytes limit")
	}
}

func TestAllowMoreMessages_OverCountLimit(t *testing.T) {
	opts := newOptions()
	opts.maxOutstandingMessages = 10
	opts.maxOutstandingBytes = 100000

	ext := newMessageExtender(opts, newMockLogger())
	ext.inFlightMsgCount.Store(15)

	if ext.AllowMoreMessages() {
		t.Error("expected AllowMoreMessages to return false over message count limit")
	}
}

func TestAllowMoreMessages_OverBytesLimit(t *testing.T) {
	opts := newOptions()
	opts.maxOutstandingMessages = 10
	opts.maxOutstandingBytes = 100000

	ext := newMessageExtender(opts, newMockLogger())
	ext.inFlightMsgBytes.Store(150000)

	if ext.AllowMoreMessages() {
		t.Error("expected AllowMoreMessages to return false over bytes limit")
	}
}

func TestRun_ProcessesMessages(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	ctx, cancel := context.WithCancel(context.Background())
	sourceCh := make(chan *extendableMessage, 10)

	go ext.run(ctx, sourceCh)

	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	sourceCh <- msg

	// Give time for message to be processed
	time.Sleep(50 * time.Millisecond)

	// Use atomic counters which are safe to read from another goroutine
	if ext.inFlightMsgCount.Load() != 1 {
		t.Errorf("expected 1 in-flight message, got %d", ext.inFlightMsgCount.Load())
	}

	if ext.inFlightMsgBytes.Load() != 100 {
		t.Errorf("expected 100 in-flight bytes, got %d", ext.inFlightMsgBytes.Load())
	}

	// Don't access inFlightMessages map directly as it's not thread-safe
	// The atomic counters above verify the message was added

	cancel()
}

func TestRun_ContextCancellation(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	ctx, cancel := context.WithCancel(context.Background())
	sourceCh := make(chan *extendableMessage, 10)

	done := make(chan struct{})
	go func() {
		ext.run(ctx, sourceCh)
		close(done)
	}()

	// Cancel context should cause run to exit
	cancel()

	select {
	case <-done:
		// Success - run exited
	case <-time.After(time.Second):
		t.Error("run did not exit after context cancellation")
	}
}

func TestRun_ChannelClose(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	ctx := context.Background()
	sourceCh := make(chan *extendableMessage, 10)

	done := make(chan struct{})
	go func() {
		ext.run(ctx, sourceCh)
		close(done)
	}()

	// Closing channel should cause run to exit
	close(sourceCh)

	select {
	case <-done:
		// Success - run exited
	case <-time.After(time.Second):
		t.Error("run did not exit after channel close")
	}
}

func TestProcessInFlightMessages_RemovesAcked(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	ext.addMessage(msg)

	// Ack the message
	msg.Ack()

	ext.processInFlightMessages(context.Background())

	if ext.inFlightMsgCount.Load() != 0 {
		t.Errorf("expected 0 in-flight messages after ack, got %d", ext.inFlightMsgCount.Load())
	}

	if _, ok := ext.inFlightMessages["msg-1"]; ok {
		t.Error("expected acked message to be removed from map")
	}
}

func TestProcessInFlightMessages_RemovesNacked(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	ext.addMessage(msg)

	// Nack the message
	msg.Nack()

	ext.processInFlightMessages(context.Background())

	if ext.inFlightMsgCount.Load() != 0 {
		t.Errorf("expected 0 in-flight messages after nack, got %d", ext.inFlightMsgCount.Load())
	}

	if _, ok := ext.inFlightMessages["msg-1"]; ok {
		t.Error("expected nacked message to be removed from map")
	}
}

func TestProcessInFlightMessages_RemovesExpired(t *testing.T) {
	opts := newOptions()
	opts.sqsVisibilityTimeoutSeconds = 30
	opts.maxMessageExtension = 1 * time.Minute

	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error { return nil })

	// Set original receive time to be past the max extension limit
	msg.originalReceiveTimestamp = time.Now().Add(-2 * time.Minute)

	ext.addMessage(msg)

	ext.processInFlightMessages(context.Background())

	if ext.inFlightMsgCount.Load() != 0 {
		t.Errorf("expected 0 in-flight messages after expiry, got %d", ext.inFlightMsgCount.Load())
	}

	if _, ok := ext.inFlightMessages["msg-1"]; ok {
		t.Error("expected expired message to be removed from map")
	}
}

func TestProcessInFlightMessages_ExtendsNeeded(t *testing.T) {
	opts := newOptions()
	opts.sqsVisibilityTimeoutSeconds = 30

	ext := newMessageExtender(opts, newMockLogger())

	var extendCalled atomic.Bool
	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		extendCalled.Store(true)
		return nil
	})

	// Set last extended time to be more than half the timeout ago
	msg.lastExtendedAt = time.Now().Add(-20 * time.Second)

	ext.addMessage(msg)

	ext.processInFlightMessages(context.Background())

	if !extendCalled.Load() {
		t.Error("expected ExtendVisibility to be called")
	}
}

func TestProcessInFlightMessages_NoExtensionNeeded(t *testing.T) {
	opts := newOptions()
	opts.sqsVisibilityTimeoutSeconds = 30

	ext := newMessageExtender(opts, newMockLogger())

	var extendCalled atomic.Bool
	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		extendCalled.Store(true)
		return nil
	})

	// lastExtendedAt is recent (set by constructor), so no extension needed

	ext.addMessage(msg)

	ext.processInFlightMessages(context.Background())

	if extendCalled.Load() {
		t.Error("expected ExtendVisibility not to be called when not needed")
	}
}

func TestProcessInFlightMessages_ConcurrentExtension(t *testing.T) {
	opts := newOptions()
	opts.sqsVisibilityTimeoutSeconds = 30

	ext := newMessageExtender(opts, newMockLogger())

	var extendCount atomic.Int32
	// Add more than 3 messages that need extension to trigger concurrent path
	for i := range 5 {
		msg := newExtendableMessage("msg-"+string(rune('1'+i)), 30, 100)
		msg.SetAckFunc(func() {})
		msg.SetExtendVisibilityFunc(func(_ context.Context) error {
			extendCount.Add(1)
			time.Sleep(10 * time.Millisecond) // Small delay to test concurrency
			return nil
		})
		msg.lastExtendedAt = time.Now().Add(-20 * time.Second)
		ext.addMessage(msg)
	}

	ext.processInFlightMessages(context.Background())

	if extendCount.Load() != 5 {
		t.Errorf("expected 5 extend calls, got %d", extendCount.Load())
	}
}

func TestProcessInFlightMessages_ExtensionFailure(t *testing.T) {
	opts := newOptions()
	opts.sqsVisibilityTimeoutSeconds = 30

	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 100)
	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		return errors.New("simulated extension failure")
	})

	// Set last extended time to trigger extension
	msg.lastExtendedAt = time.Now().Add(-20 * time.Second)

	ext.addMessage(msg)

	if ext.inFlightMsgCount.Load() != 1 {
		t.Fatalf("expected 1 in-flight message before processing, got %d", ext.inFlightMsgCount.Load())
	}

	ext.processInFlightMessages(context.Background())

	// Message should be removed due to extension failure
	if ext.inFlightMsgCount.Load() != 0 {
		t.Errorf("expected 0 in-flight messages after extension failure, got %d", ext.inFlightMsgCount.Load())
	}

	if _, ok := ext.inFlightMessages["msg-1"]; ok {
		t.Error("expected message to be removed from map after extension failure")
	}
}

func TestProcessInFlightMessages_Empty(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	// Should not panic with empty message list
	ext.processInFlightMessages(context.Background())
}

func TestAddMessage(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 500)

	ext.addMessage(msg)

	if ext.inFlightMsgCount.Load() != 1 {
		t.Errorf("expected count 1, got %d", ext.inFlightMsgCount.Load())
	}

	if ext.inFlightMsgBytes.Load() != 500 {
		t.Errorf("expected bytes 500, got %d", ext.inFlightMsgBytes.Load())
	}

	if stored, ok := ext.inFlightMessages["msg-1"]; !ok || stored != msg {
		t.Error("expected message to be stored in map")
	}
}

func TestRemoveMessage(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	msg := newExtendableMessage("msg-1", 30, 500)
	ext.addMessage(msg)

	ext.removeMessage(msg)

	if ext.inFlightMsgCount.Load() != 0 {
		t.Errorf("expected count 0, got %d", ext.inFlightMsgCount.Load())
	}

	if ext.inFlightMsgBytes.Load() != 0 {
		t.Errorf("expected bytes 0, got %d", ext.inFlightMsgBytes.Load())
	}

	if _, ok := ext.inFlightMessages["msg-1"]; ok {
		t.Error("expected message to be removed from map")
	}
}

func TestMultipleMessages(t *testing.T) {
	opts := newOptions()
	ext := newMessageExtender(opts, newMockLogger())

	msg1 := newExtendableMessage("msg-1", 30, 100)
	msg2 := newExtendableMessage("msg-2", 30, 200)
	msg3 := newExtendableMessage("msg-3", 30, 300)

	ext.addMessage(msg1)
	ext.addMessage(msg2)
	ext.addMessage(msg3)

	if ext.inFlightMsgCount.Load() != 3 {
		t.Errorf("expected count 3, got %d", ext.inFlightMsgCount.Load())
	}

	if ext.inFlightMsgBytes.Load() != 600 {
		t.Errorf("expected bytes 600, got %d", ext.inFlightMsgBytes.Load())
	}

	ext.removeMessage(msg2)

	if ext.inFlightMsgCount.Load() != 2 {
		t.Errorf("expected count 2, got %d", ext.inFlightMsgCount.Load())
	}

	if ext.inFlightMsgBytes.Load() != 400 {
		t.Errorf("expected bytes 400, got %d", ext.inFlightMsgBytes.Load())
	}
}
