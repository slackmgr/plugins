package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/slackmgr/types"
)

// Client is an SQS queue consumer for the Slack Manager ecosystem.
// It reads messages from a FIFO SQS queue, extends their visibility timeouts
// automatically while processing is in progress, and delivers each message to
// a caller-supplied sink channel as a [github.com/slackmgr/types.FifoQueueItem].
//
// Create a Client with [New], then call [Client.Init] once before any other
// method. Init is not thread-safe; all other methods are safe for concurrent
// use after Init returns.
type Client struct {
	client      sqsClient
	queueName   string
	queueURL    string
	awsCfg      *aws.Config
	opts        *Options
	extender    *messageExtender
	extenderCh  chan *extendableMessage
	logger      types.Logger
	initialized bool
}

// New creates a Client configured to consume from the named SQS FIFO queue.
// The queue name must end with ".fifo"; this constraint is enforced by
// [Client.Init].
//
// Functional options may be passed to override defaults (see With* functions).
// The logger is automatically enriched with "plugin" and "queue_name" fields.
//
// New does not connect to AWS. Call [Client.Init] to resolve the queue URL
// and start the background visibility-extension goroutine.
func New(awsCfg *aws.Config, queueName string, logger types.Logger, opts ...Option) *Client {
	options := newOptions()

	for _, o := range opts {
		o(options)
	}

	logger = logger.
		WithField("plugin", "sqs").
		WithField("queue_name", queueName)

	return &Client{
		awsCfg:     awsCfg,
		queueName:  queueName,
		opts:       options,
		extenderCh: make(chan *extendableMessage, 1000),
		logger:     logger,
	}
}

// Init initializes the Client: validates options, resolves the queue URL via
// GetQueueUrl, and starts the background visibility-extension goroutine.
// It returns the receiver so that initialization can be chained with [New]:
//
//	client, err := sqs.New(&awsCfg, "events.fifo", logger).Init(ctx)
//
// The provided context governs the background visibility-extension goroutine;
// cancelling it shuts the goroutine down cleanly.
//
// Init is idempotent — subsequent calls on an already-initialized Client are
// no-ops. It is not thread-safe and must be called once during application
// startup before any concurrent access.
func (c *Client) Init(ctx context.Context) (*Client, error) {
	if c.initialized {
		return c, nil
	}

	if !strings.HasSuffix(c.queueName, ".fifo") {
		return nil, errors.New("the SQS queue must be a FIFO queue (the name must end with .fifo)")
	}

	if err := c.opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid SQS options: %w", err)
	}

	// Use injected client if provided (for testing), otherwise create real client
	if c.opts.sqsClient != nil {
		c.client = c.opts.sqsClient
	} else {
		c.client = sqs.NewFromConfig(*c.awsCfg, func(o *sqs.Options) {
			o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, c.opts.sqsAPIMaxRetryBackoffDelay)
			o.Retryer = retry.AddWithMaxAttempts(o.Retryer, c.opts.sqsAPIMaxRetryAttempts)
		})
	}

	resp, err := c.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(c.queueName)})
	if err != nil {
		return nil, fmt.Errorf("failed to get SQS queue URL for %s: %w", c.queueName, err)
	}

	c.queueURL = aws.ToString(resp.QueueUrl)

	c.extender = newMessageExtender(c.opts, c.logger)

	// Start the message extender. This will run until the context is cancelled or the extender channel is closed.
	go c.extender.run(ctx, c.extenderCh)

	c.initialized = true

	return c, nil
}

// Send publishes a single message to the FIFO queue.
//
// groupID is used as the SQS MessageGroupId, which determines message
// ordering within the queue. dedupID is used as the SQS
// MessageDeduplicationId; SQS will silently discard messages with a
// duplicate ID within the 5-minute deduplication window. Both fields are
// required and must be non-empty.
//
// Send requires [Client.Init] to have been called successfully.
func (c *Client) Send(ctx context.Context, groupID, dedupID, body string) error {
	if !c.initialized {
		return errors.New("SQS client not initialized")
	}

	if groupID == "" {
		return errors.New("groupID cannot be empty")
	}

	if dedupID == "" {
		return errors.New("dedupID cannot be empty")
	}

	if body == "" {
		return errors.New("body cannot be empty")
	}

	input := &sqs.SendMessageInput{
		QueueUrl:               &c.queueURL,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &dedupID,
		MessageBody:            &body,
	}

	if _, err := c.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("failed to send SQS message: %w", err)
	}

	return nil
}

// Name returns the SQS queue name supplied to [New].
func (c *Client) Name() string {
	return c.queueName
}

// Receive reads messages from the queue in a loop and sends each one to
// sinkCh as a [github.com/slackmgr/types.FifoQueueItem].
// It closes sinkCh before returning.
//
// Each delivered item exposes two callbacks:
//   - Ack deletes the message from SQS, signalling successful processing.
//   - Nack abandons the message without deleting it; SQS will redeliver it
//     after the visibility timeout expires.
//
// Receive pauses reads automatically when the number or total byte size of
// in-flight messages reaches the limits configured by
// [WithMaxOutstandingMessages] or [WithMaxOutstandingBytes]. On a transient
// receive error it logs the failure and retries after a 5-second backoff.
//
// Receive blocks until ctx is cancelled, at which point it returns ctx.Err().
// It must be called in its own goroutine. [Client.Init] must have been called
// successfully before Receive is invoked.
func (c *Client) Receive(ctx context.Context, sinkCh chan<- *types.FifoQueueItem) error {
	defer close(sinkCh)

	if !c.initialized {
		return errors.New("SQS client not initialized")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			c.logger.WithField("wait_time", c.opts.sqsReceiveWaitTimeSeconds).Debug("Reading SQS queue")

			err := c.read(ctx, sinkCh)

			// No error means we keep reading
			if err == nil {
				continue
			}

			// If the context was cancelled, return without logging an error
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Otherwise log the error and continue reading after a short delay
			// The delay prevents hammering the SQS API (and excessive logging) in case of persistent errors
			c.logger.Errorf("Error reading SQS queue %s: %v", c.queueName, err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (c *Client) read(ctx context.Context, sinkCh chan<- *types.FifoQueueItem) error {
	// First check if we are allowed to read more messages, based on the extender's capacity
	for !c.extender.AllowMoreMessages() {
		c.logger.Debug("SQS message extender is at capacity, waiting to read more messages")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &c.queueURL,
		MaxNumberOfMessages: c.opts.sqsReceiveMaxNumberOfMessages,
		VisibilityTimeout:   c.opts.sqsVisibilityTimeoutSeconds,
		WaitTimeSeconds:     c.opts.sqsReceiveWaitTimeSeconds,
		AttributeNames:      []sqstypes.QueueAttributeName{sqstypes.QueueAttributeName(sqstypes.MessageSystemAttributeNameMessageGroupId)},
	}

	output, err := c.client.ReceiveMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to receive SQS messages: %w", err)
	}

	now := time.Now()

	for _, m := range output.Messages {
		msgID := aws.ToString(m.MessageId)
		receiptHandle := aws.ToString(m.ReceiptHandle)
		body := aws.ToString(m.Body)

		ack := func() { //nolint:contextcheck // ack must complete regardless of caller's context state
			c.deleteMessage(msgID, receiptHandle)
		}

		extendVisibilty := func(ctx context.Context) error {
			return c.changeMessageVisibility(ctx, msgID, receiptHandle)
		}

		extendableMsg := newExtendableMessage(msgID, c.opts.sqsVisibilityTimeoutSeconds, len(body))

		extendableMsg.SetAckFunc(ack)
		extendableMsg.SetExtendVisibilityFunc(extendVisibilty)

		if err := trySend(ctx, extendableMsg, c.extenderCh); err != nil {
			return err
		}

		queueItem := &types.FifoQueueItem{
			MessageID:        msgID,
			SlackChannelID:   m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)],
			ReceiveTimestamp: now,
			Body:             body,
			Ack:              extendableMsg.Ack,
			Nack:             extendableMsg.Nack,
		}

		if err := trySend(ctx, queueItem, sinkCh); err != nil {
			return err
		}

		c.logger.WithField("message_id", msgID).Debug("SQS message received")
	}

	return nil
}

// deleteMessage deletes the SQS message with the given receipt handle.
// These functions use context.Background() with a short timeout because it
// must complete regardless of the caller's context state.
func (c *Client) deleteMessage(messageID, receiptHandle string) {
	logger := c.logger.WithField("message_id", messageID)

	input := &sqs.DeleteMessageInput{
		QueueUrl:      &c.queueURL,
		ReceiptHandle: &receiptHandle,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := c.client.DeleteMessage(ctx, input); err != nil {
		logger.Errorf("Failed to delete SQS message: %v", err)
		return
	}

	logger.Debug("SQS message deleted")
}

// changeMessageVisibility extends the visibility timeout of the SQS message with the given receipt handle.
// Returns an error if the visibility extension fails.
func (c *Client) changeMessageVisibility(ctx context.Context, messageID, receiptHandle string) error {
	logger := c.logger.WithField("message_id", messageID).WithField("visibility_timeout_seconds", c.opts.sqsVisibilityTimeoutSeconds)

	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &c.queueURL,
		ReceiptHandle:     &receiptHandle,
		VisibilityTimeout: c.opts.sqsVisibilityTimeoutSeconds,
	}

	if _, err := c.client.ChangeMessageVisibility(ctx, input); err != nil {
		return fmt.Errorf("failed to extend SQS message visibility: %w", err)
	}

	logger.Debug("SQS message visibility extended")

	return nil
}

func trySend[T any](ctx context.Context, msg T, sinkCh chan<- T) error {
	select {
	case sinkCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
