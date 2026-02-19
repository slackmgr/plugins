package sqs

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/slackmgr/types"
)

// WebhookHandler converts incoming HTTP webhook callbacks into SQS messages.
// It implements the types.WebhookHandler interface and routes each callback
// to either a FIFO or a standard SQS queue based on the target URL.
//
// For FIFO queues the handler sets the message group ID to the Slack channel
// ID and derives a deduplication ID from a SHA-256 hash of the callback
// fields, preventing duplicate messages caused by webhook retries.
//
// Create a WebhookHandler with [NewWebhookHandler] and call
// [WebhookHandler.Init] once before handling any requests. Init is not
// thread-safe; all other methods are safe for concurrent use after Init
// returns.
type WebhookHandler struct {
	client      sqsClient
	awsCfg      *aws.Config
	initialized bool
}

// NewWebhookHandler creates a WebhookHandler that uses awsCfg to construct
// its SQS client during [WebhookHandler.Init].
//
// NewWebhookHandler does not connect to AWS. Call [WebhookHandler.Init] before
// handling any requests.
func NewWebhookHandler(awsCfg *aws.Config) *WebhookHandler {
	return &WebhookHandler{
		awsCfg: awsCfg,
	}
}

// newWebhookHandlerWithClient creates a WebhookHandler with a custom SQS client.
// This constructor is intended for testing purposes.
func newWebhookHandlerWithClient(client sqsClient) *WebhookHandler {
	return &WebhookHandler{
		client:      client,
		initialized: true,
	}
}

// Init initializes the WebhookHandler by constructing the underlying SQS
// client from the AWS configuration supplied to [NewWebhookHandler].
// It returns the receiver so that initialization can be chained:
//
//	handler, err := sqs.NewWebhookHandler(&awsCfg).Init(ctx)
//
// Init is idempotent — subsequent calls on an already-initialized handler are
// no-ops. It is not thread-safe and must be called once during application
// startup before any concurrent access.
func (c *WebhookHandler) Init(_ context.Context) (*WebhookHandler, error) {
	if c.initialized {
		return c, nil
	}

	c.client = sqs.NewFromConfig(*c.awsCfg)
	c.initialized = true

	return c, nil
}

// ShouldHandleWebhook reports whether the handler should process a webhook
// with the given target URL. It returns true when target begins with
// "https://sqs.", matching any AWS SQS queue endpoint URL.
func (c *WebhookHandler) ShouldHandleWebhook(_ context.Context, target string) bool {
	return strings.HasPrefix(target, "https://sqs.")
}

// HandleWebhook marshals data as JSON and sends it to the SQS queue at
// queueURL. The queue type is detected from the URL suffix:
//   - FIFO queues (URL ends with ".fifo"): the message group ID is set to
//     data.ChannelID and a deduplication ID is derived from a SHA-256 hash
//     of the channel ID, message ID, callback ID, and nanosecond timestamp.
//   - Standard queues: the message is sent without a group or dedup ID.
//
// HandleWebhook requires [WebhookHandler.Init] to have been called
// successfully.
func (c *WebhookHandler) HandleWebhook(ctx context.Context, queueURL string, data *types.WebhookCallback, logger types.Logger) error {
	if !c.initialized {
		return errors.New("SQS webhook handler not initialized")
	}

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook callback data: %w", err)
	}

	if strings.HasSuffix(queueURL, ".fifo") {
		groupID := data.ChannelID
		dedupID := hash(data.ChannelID, data.MessageID, data.ID, data.Timestamp.UTC().Format(time.RFC3339Nano))

		return c.sendToFifoQueue(ctx, queueURL, groupID, dedupID, string(body), logger)
	}

	return c.sendToStdQueue(ctx, queueURL, string(body), logger)
}

func (c *WebhookHandler) sendToFifoQueue(ctx context.Context, queueURL, groupID, dedupID, body string, logger types.Logger) error {
	input := &sqs.SendMessageInput{
		QueueUrl:               &queueURL,
		MessageGroupId:         &groupID,
		MessageDeduplicationId: &dedupID,
		MessageBody:            &body,
	}

	if _, err := c.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("failed to send SQS message: %w", err)
	}

	logger.Debugf("Webhook body sent to FIFO SQS queue %s with group ID %s and dedup ID %s", queueURL, groupID, dedupID)

	return nil
}

func (c *WebhookHandler) sendToStdQueue(ctx context.Context, queueURL, body string, logger types.Logger) error {
	input := &sqs.SendMessageInput{
		QueueUrl:    &queueURL,
		MessageBody: &body,
	}

	if _, err := c.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("failed to send SQS message: %w", err)
	}

	logger.Debugf("Webhook body sent to standard SQS queue %s", queueURL)

	return nil
}

func hash(input ...string) string {
	h := sha256.New()

	for _, s := range input {
		h.Write([]byte(s))
		h.Write([]byte{0}) // null byte delimiter to prevent hash collisions
	}

	bs := h.Sum(nil)

	return base64.URLEncoding.EncodeToString(bs)
}
