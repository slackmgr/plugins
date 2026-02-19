package sqs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/slackmgr/types"
	"golang.org/x/sync/semaphore"
)

// messageExtender tracks in-flight messages and extends their visibility timeout
// to prevent them from being redelivered while still being processed.
//
// Message extension is best-effort: if an extension fails (e.g., due to network
// errors or SQS throttling), the message is removed from tracking and will not
// be extended again. This means the message may become visible in SQS and be
// redelivered to another consumer. Downstream processing should be idempotent
// to handle potential duplicate deliveries gracefully.
type messageExtender struct {
	inFlightMessages map[string]*extendableMessage
	inFlightMsgCount atomic.Int64
	inFlightMsgBytes atomic.Int64
	opts             *Options
	logger           types.Logger
}

func newMessageExtender(opts *Options, logger types.Logger) *messageExtender {
	return &messageExtender{
		inFlightMessages: make(map[string]*extendableMessage),
		inFlightMsgCount: atomic.Int64{},
		inFlightMsgBytes: atomic.Int64{},
		opts:             opts,
		logger:           logger,
	}
}

func (m *messageExtender) AllowMoreMessages() bool {
	return m.inFlightMsgCount.Load() < int64(m.opts.maxOutstandingMessages) &&
		m.inFlightMsgBytes.Load() < int64(m.opts.maxOutstandingBytes)
}

func (m *messageExtender) run(ctx context.Context, sourceCh <-chan *extendableMessage) {
	m.logger.Info("SQS message extender started")
	defer m.logger.Info("SQS message extender exited")

	checkInterval := max(time.Duration(m.opts.sqsVisibilityTimeoutSeconds/3)*time.Second, 5*time.Second)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.processInFlightMessages(ctx)
		case msg, ok := <-sourceCh:
			if !ok {
				return
			}

			m.addMessage(msg)
		}
	}
}

func (m *messageExtender) processInFlightMessages(ctx context.Context) {
	if len(m.inFlightMessages) == 0 {
		m.logger.Debug("No SQS in-flight messages to process")
		return
	}

	m.logger.WithField("count", len(m.inFlightMessages)).Debug("Starting SQS in-flight message processing")

	inNeedOfExtension := []*extendableMessage{}

	for _, msg := range m.inFlightMessages {
		if msg.IsAckedOrNacked() {
			m.logger.WithField("message_id", msg.MessageID()).Debug("Removing acked/nacked message from list of SQS in-flight messages")
			m.removeMessage(msg)
			continue
		}

		if (time.Since(msg.OriginalReceiveTimestamp()) + time.Duration(m.opts.sqsVisibilityTimeoutSeconds)*time.Second) >= m.opts.maxMessageExtension {
			m.logger.WithField("message_id", msg.MessageID()).Error("SQS message has reached maximum visibility timeout extension limit, removing from list of in-flight messages")
			m.removeMessage(msg)
			continue
		}

		if msg.NeedsExtensionNow() {
			inNeedOfExtension = append(inNeedOfExtension, msg)
		}
	}

	if len(inNeedOfExtension) == 0 {
		return
	}

	// If 1-3 messages need to be extended, do it sequentially. Otherwise, do it concurrently.
	//
	// Note: Message visibility extension is best-effort. If extension fails for a message,
	// it is removed from further extension attempts. Downstream processing should be
	// idempotent to handle potential duplicate deliveries.
	if len(inNeedOfExtension) < 3 {
		m.extendMessagesSync(ctx, inNeedOfExtension)
	} else {
		m.extendMessagesAsync(ctx, inNeedOfExtension)
	}
}

func (m *messageExtender) extendMessagesSync(ctx context.Context, inNeedOfExtension []*extendableMessage) {
	started := time.Now()

	for _, msg := range inNeedOfExtension {
		if err := msg.ExtendVisibility(ctx); err != nil {
			if ctx.Err() != nil {
				return // Context was cancelled or timed out; skip further processing.
			}

			m.logger.WithField("message_id", msg.MessageID()).Errorf("Failed to extend message visibility, removing from in-flight tracking: %v", err)

			m.removeMessage(msg)
		}
	}

	m.logger.WithField("elapsed", time.Since(started)).Debug("Completed SQS in-flight message processing")
}

func (m *messageExtender) extendMessagesAsync(ctx context.Context, inNeedOfExtension []*extendableMessage) {
	started := time.Now()

	wg := sync.WaitGroup{}
	sem := semaphore.NewWeighted(3)
	var mu sync.Mutex
	toRemove := []*extendableMessage{}

	for _, msg := range inNeedOfExtension {
		wg.Go(func() {
			if err := sem.Acquire(ctx, 1); err != nil {
				return
			}
			defer sem.Release(1)

			if ctx.Err() != nil {
				return // Context was cancelled or timed out; skip further processing.
			}

			if err := msg.ExtendVisibility(ctx); err != nil {
				if ctx.Err() != nil {
					return // Context was cancelled or timed out; skip further processing.
				}

				m.logger.WithField("message_id", msg.MessageID()).Errorf("Failed to extend message visibility, removing from in-flight tracking: %v", err)

				mu.Lock()
				toRemove = append(toRemove, msg)
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if ctx.Err() != nil {
		return // Context was cancelled or timed out; skip further processing.
	}

	// Remove failed messages after all extensions complete
	for _, msg := range toRemove {
		m.removeMessage(msg)
	}

	m.logger.WithField("elapsed", time.Since(started)).Debug("Completed SQS in-flight message processing")
}

func (m *messageExtender) addMessage(msg *extendableMessage) {
	m.inFlightMsgCount.Add(1)
	m.inFlightMsgBytes.Add(msg.Size())

	m.inFlightMessages[msg.MessageID()] = msg
}

func (m *messageExtender) removeMessage(msg *extendableMessage) {
	m.inFlightMsgCount.Add(-1)
	m.inFlightMsgBytes.Add(-msg.Size())

	delete(m.inFlightMessages, msg.MessageID())
}
