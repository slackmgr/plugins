//nolint:paralleltest,testpackage // Tests use shared resources and need access to unexported functions
package sqs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewExtendableMessage(t *testing.T) {
	before := time.Now()
	msg := newExtendableMessage("msg-123", 30, 1024)
	after := time.Now()

	if msg.messageID != "msg-123" {
		t.Errorf("expected messageID 'msg-123', got %q", msg.messageID)
	}

	if msg.visibilityTimeout != 30*time.Second {
		t.Errorf("expected visibilityTimeout 30s, got %v", msg.visibilityTimeout)
	}

	if msg.msgSize != 1024 {
		t.Errorf("expected msgSize 1024, got %d", msg.msgSize)
	}

	if msg.originalReceiveTimestamp.Before(before) || msg.originalReceiveTimestamp.After(after) {
		t.Errorf("originalReceiveTimestamp %v not between %v and %v", msg.originalReceiveTimestamp, before, after)
	}

	if msg.lastExtendedAt.Before(before) || msg.lastExtendedAt.After(after) {
		t.Errorf("lastExtendedAt %v not between %v and %v", msg.lastExtendedAt, before, after)
	}

	if msg.processingLock == nil {
		t.Error("processingLock should not be nil")
	}
}

func TestMessageID(t *testing.T) {
	msg := newExtendableMessage("test-msg-id", 30, 100)

	if msg.MessageID() != "test-msg-id" {
		t.Errorf("expected MessageID 'test-msg-id', got %q", msg.MessageID())
	}
}

func TestOriginalReceiveTimestamp(t *testing.T) {
	before := time.Now()
	msg := newExtendableMessage("msg-123", 30, 100)
	after := time.Now()

	ts := msg.OriginalReceiveTimestamp()

	if ts.Before(before) || ts.After(after) {
		t.Errorf("OriginalReceiveTimestamp %v not between %v and %v", ts, before, after)
	}
}

func TestSize(t *testing.T) {
	tests := []struct {
		name     string
		msgSize  int
		expected int64
	}{
		{"small message", 100, 100},
		{"large message", 256000, 256000},
		{"zero size", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := newExtendableMessage("msg", 30, tt.msgSize)
			if msg.Size() != tt.expected {
				t.Errorf("expected Size() %d, got %d", tt.expected, msg.Size())
			}
		})
	}
}

func TestAck_CallsAckFunc(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	called := false
	msg.SetAckFunc(func() {
		called = true
	})

	msg.Ack()

	if !called {
		t.Error("expected ack function to be called")
	}
}

func TestAck_ClearsCallbacks(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error { return nil })

	msg.Ack()

	if msg.ackFunc != nil {
		t.Error("expected ackFunc to be nil after Ack()")
	}

	if msg.extendVisibilityFunc != nil {
		t.Error("expected extendVisibilityFunc to be nil after Ack()")
	}
}

func TestAck_OnlyOnce(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	callCount := 0
	msg.SetAckFunc(func() {
		callCount++
	})

	msg.Ack()
	msg.Ack()
	msg.Ack()

	if callCount != 1 {
		t.Errorf("expected ack function to be called once, got %d calls", callCount)
	}
}

func TestAck_NoAckFuncSet(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	// Should not panic when ackFunc is not set
	msg.Ack()
}

func TestNack_ClearsCallbacks(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	ackCalled := false
	msg.SetAckFunc(func() {
		ackCalled = true
	})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error { return nil })

	msg.Nack()

	if ackCalled {
		t.Error("ack function should not be called on Nack()")
	}

	if msg.ackFunc != nil {
		t.Error("expected ackFunc to be nil after Nack()")
	}

	if msg.extendVisibilityFunc != nil {
		t.Error("expected extendVisibilityFunc to be nil after Nack()")
	}
}

func TestIsAckedOrNacked_AfterAck(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)
	msg.SetAckFunc(func() {})

	if msg.IsAckedOrNacked() {
		t.Error("expected IsAckedOrNacked() to be false before ack")
	}

	msg.Ack()

	if !msg.IsAckedOrNacked() {
		t.Error("expected IsAckedOrNacked() to be true after ack")
	}
}

func TestIsAckedOrNacked_AfterNack(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)
	msg.SetAckFunc(func() {})

	if msg.IsAckedOrNacked() {
		t.Error("expected IsAckedOrNacked() to be false before nack")
	}

	msg.Nack()

	if !msg.IsAckedOrNacked() {
		t.Error("expected IsAckedOrNacked() to be true after nack")
	}
}

func TestNeedsExtensionNow_BeforeHalfTimeout(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)
	msg.SetExtendVisibilityFunc(func(_ context.Context) error { return nil })

	// Immediately after creation, should not need extension
	if msg.NeedsExtensionNow() {
		t.Error("should not need extension immediately after creation")
	}
}

func TestNeedsExtensionNow_AfterHalfTimeout(t *testing.T) {
	msg := newExtendableMessage("msg-123", 4, 100) // 4 second timeout for faster test
	msg.SetExtendVisibilityFunc(func(_ context.Context) error { return nil })

	// Set lastExtendedAt to more than half the timeout ago
	msg.lastExtendedAt = time.Now().Add(-3 * time.Second)

	if !msg.NeedsExtensionNow() {
		t.Error("should need extension after half timeout has passed")
	}
}

func TestNeedsExtensionNow_NoExtendFunc(t *testing.T) {
	msg := newExtendableMessage("msg-123", 4, 100)

	// Set lastExtendedAt to more than half the timeout ago
	msg.lastExtendedAt = time.Now().Add(-3 * time.Second)

	// Should return false if no extend function is set
	if msg.NeedsExtensionNow() {
		t.Error("should not need extension when no extend function is set")
	}
}

func TestExtendVisibility(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	called := false
	var receivedCtx context.Context
	msg.SetExtendVisibilityFunc(func(ctx context.Context) error {
		called = true
		receivedCtx = ctx //nolint:fatcontext // Test needs to capture context for verification
		return nil
	})

	ctx := context.Background()
	before := time.Now()
	err := msg.ExtendVisibility(ctx)
	after := time.Now()

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !called {
		t.Error("expected extend visibility function to be called")
	}

	if receivedCtx != ctx {
		t.Error("expected context to be passed to extend function")
	}

	if msg.lastExtendedAt.Before(before) || msg.lastExtendedAt.After(after) {
		t.Errorf("lastExtendedAt %v not between %v and %v", msg.lastExtendedAt, before, after)
	}
}

func TestExtendVisibility_NoFunc(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	// Should not panic when extendVisibilityFunc is not set, and should return nil
	err := msg.ExtendVisibility(context.Background())
	if err != nil {
		t.Errorf("expected nil error when no func is set, got %v", err)
	}
}

func TestExtendVisibility_ReturnsError(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	expectedErr := errors.New("simulated extension failure")
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		return expectedErr
	})

	before := msg.lastExtendedAt
	err := msg.ExtendVisibility(context.Background())

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	// lastExtendedAt should NOT be updated on failure
	if msg.lastExtendedAt != before {
		t.Error("lastExtendedAt should not be updated when extension fails")
	}
}

func TestExtendVisibility_AfterAck(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	extendCalled := false
	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		extendCalled = true
		return nil
	})

	msg.Ack()
	_ = msg.ExtendVisibility(context.Background())

	if extendCalled {
		t.Error("extend visibility should not be called after ack")
	}
}

func TestConcurrentAckAndExtend(t *testing.T) {
	// This test verifies thread safety of concurrent operations
	msg := newExtendableMessage("msg-123", 30, 100)

	var ackCount atomic.Int32
	var extendCount atomic.Int32

	msg.SetAckFunc(func() {
		ackCount.Add(1)
	})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		extendCount.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	const goroutines = 100

	// Half try to Ack, half try to ExtendVisibility
	for i := range goroutines {
		wg.Add(1)
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				msg.Ack()
			}()
		} else {
			go func() {
				defer wg.Done()
				_ = msg.ExtendVisibility(context.Background())
			}()
		}
	}

	wg.Wait()

	// Ack should only be called once
	if ackCount.Load() != 1 {
		t.Errorf("expected ack to be called once, got %d", ackCount.Load())
	}

	// Some extend calls may or may not succeed depending on race ordering
	// The important thing is no panics or data races occur (checked by -race flag)
}

func TestConcurrentNackAndExtend(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	var extendCount atomic.Int32

	msg.SetAckFunc(func() {})
	msg.SetExtendVisibilityFunc(func(_ context.Context) error {
		extendCount.Add(1)
		return nil
	})

	var wg sync.WaitGroup
	const goroutines = 100

	for i := range goroutines {
		wg.Add(1)
		if i%2 == 0 {
			go func() {
				defer wg.Done()
				msg.Nack()
			}()
		} else {
			go func() {
				defer wg.Done()
				_ = msg.ExtendVisibility(context.Background())
			}()
		}
	}

	wg.Wait()

	// The important thing is no panics or data races occur
}

func TestSetAckFunc(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	called := false
	f := func() { called = true }

	msg.SetAckFunc(f)
	msg.ackFunc()

	if !called {
		t.Error("expected SetAckFunc to set the ack function")
	}
}

func TestSetExtendVisibilityFunc(t *testing.T) {
	msg := newExtendableMessage("msg-123", 30, 100)

	called := false
	f := func(_ context.Context) error {
		called = true
		return nil
	}

	msg.SetExtendVisibilityFunc(f)
	_ = msg.extendVisibilityFunc(context.Background())

	if !called {
		t.Error("expected SetExtendVisibilityFunc to set the extend function")
	}
}
