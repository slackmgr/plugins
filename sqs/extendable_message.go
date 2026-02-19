package sqs

import (
	"context"
	"sync"
	"time"
)

type extendableMessage struct {
	messageID                string
	originalReceiveTimestamp time.Time
	lastExtendedAt           time.Time
	visibilityTimeout        time.Duration
	msgSize                  int64
	ackFunc                  func()
	extendVisibilityFunc     func(ctx context.Context) error
	processingLock           *sync.Mutex
}

func newExtendableMessage(messageID string, visibilityTimeoutSeconds int32, msgSize int) *extendableMessage {
	visibilityTimeout := time.Duration(visibilityTimeoutSeconds) * time.Second
	now := time.Now()

	return &extendableMessage{
		messageID:                messageID,
		originalReceiveTimestamp: now,
		lastExtendedAt:           now,
		visibilityTimeout:        visibilityTimeout,
		msgSize:                  int64(msgSize),
		processingLock:           &sync.Mutex{},
	}
}

func (m *extendableMessage) MessageID() string {
	return m.messageID
}

func (m *extendableMessage) OriginalReceiveTimestamp() time.Time {
	return m.originalReceiveTimestamp
}

func (m *extendableMessage) Size() int64 {
	return m.msgSize
}

// SetAckFunc sets the function to acknowledge successful processing of the message.
func (m *extendableMessage) SetAckFunc(f func()) {
	m.ackFunc = f
}

// SetExtendVisibilityFunc sets the function to extend the message visibility timeout.
func (m *extendableMessage) SetExtendVisibilityFunc(f func(ctx context.Context) error) {
	m.extendVisibilityFunc = f
}

// Ack acknowledges successful processing of the message. For SQS messages, this means deleting
// the message from the queue.
func (m *extendableMessage) Ack() {
	m.processingLock.Lock()
	defer m.processingLock.Unlock()

	if m.ackFunc == nil {
		return
	}

	m.ackFunc()

	// Clear the ack and extend functions to prevent them from being called again.
	m.ackFunc = nil
	m.extendVisibilityFunc = nil
}

// Nack is a no-op for SQS messages, since there is no explicit negative acknowledgment in SQS.
func (m *extendableMessage) Nack() {
	m.processingLock.Lock()
	defer m.processingLock.Unlock()

	// Clear the ack and extend functions to prevent them from being called again.
	m.ackFunc = nil
	m.extendVisibilityFunc = nil
}

// IsAckedOrNacked returns true if the message has been acknowledged or negatively acknowledged.
func (m *extendableMessage) IsAckedOrNacked() bool {
	m.processingLock.Lock()
	defer m.processingLock.Unlock()

	return m.ackFunc == nil
}

// NeedsExtensionNow returns true if the message needs its visibility timeout to be extended now.
func (m *extendableMessage) NeedsExtensionNow() bool {
	m.processingLock.Lock()
	defer m.processingLock.Unlock()

	return m.extendVisibilityFunc != nil && time.Since(m.lastExtendedAt) > m.visibilityTimeout/2
}

// ExtendVisibility extends the message visibility timeout.
// Returns an error if the extension fails. On success, updates lastExtendedAt.
// Note: Message visibility extension is best-effort. If extension fails, the message
// will be removed from further extension attempts, and downstream processing should
// be idempotent to handle potential duplicate deliveries.
func (m *extendableMessage) ExtendVisibility(ctx context.Context) error {
	m.processingLock.Lock()
	defer m.processingLock.Unlock()

	// Check that the extend function is still set after we acquired the lock.
	// If it is not, it means that the alert has already been acked (or that we have given up).
	if m.extendVisibilityFunc == nil {
		return nil
	}

	if err := m.extendVisibilityFunc(ctx); err != nil {
		return err
	}

	// Update lastExtendedAt only after successful extension.
	m.lastExtendedAt = time.Now()

	return nil
}
