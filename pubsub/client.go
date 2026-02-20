package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/slackmgr/types"
)

type Client struct {
	gcpClient     *pubsub.Client
	client        pubsubClient
	publisher     pubsubPublisher
	subscriber    pubsubSubscriber
	topic         string
	subscription  string
	opts          *Options
	logger        types.Logger
	initialized   atomic.Bool
	isReceiving   atomic.Bool
	receiveSinkCh chan<- *types.FifoQueueItem
}

func New(c *pubsub.Client, topic string, subscription string, logger types.Logger, opts ...Option) (*Client, error) {
	if c == nil {
		return nil, errors.New("pub/sub client cannot be nil")
	}

	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	options := newOptions()

	for _, o := range opts {
		o(options)
	}

	logger = logger.WithField("plugin", "pubsub").WithField("topic", topic)

	if subscription != "" {
		logger = logger.WithField("subscription", subscription)
	}

	return &Client{
		gcpClient:    c,
		topic:        topic,
		subscription: subscription,
		opts:         options,
		logger:       logger,
	}, nil
}

func (c *Client) Init() (*Client, error) {
	if c.initialized.Load() {
		return c, nil
	}

	if c.topic == "" {
		return nil, errors.New("pub/sub topic cannot be empty")
	}

	if err := c.opts.validatePublisher(); err != nil {
		return nil, fmt.Errorf("invalid pub/sub publisher options: %w", err)
	}

	// Use injected client for testing, otherwise wrap the real GCP client.
	if c.opts.pubsubClient != nil {
		c.client = c.opts.pubsubClient
	} else {
		c.client = newRealPubSubClient(c.gcpClient)
	}

	c.publisher = c.client.Publisher(c.topic)

	c.publisher.SetEnableMessageOrdering(true)
	c.publisher.SetDelayThreshold(c.opts.publisherDelayThreshold)
	c.publisher.SetCountThreshold(c.opts.publisherCountThreshold)
	c.publisher.SetByteThreshold(c.opts.publisherByteThreshold)

	if c.subscription != "" {
		if err := c.opts.validateSubscriber(); err != nil {
			return nil, fmt.Errorf("invalid pub/sub subscriber options: %w", err)
		}

		c.subscriber = c.client.Subscriber(c.subscription)

		c.subscriber.SetMaxExtension(c.opts.subscriberMaxExtension)
		c.subscriber.SetMaxDurationPerAckExtension(c.opts.subscriberMaxDurationPerAckExtension)
		c.subscriber.SetMinDurationPerAckExtension(c.opts.subscriberMinDurationPerAckExtension)
		c.subscriber.SetMaxOutstandingMessages(c.opts.subscriberMaxOutstandingMessages)
		c.subscriber.SetMaxOutstandingBytes(c.opts.subscriberMaxOutstandingBytes)

		c.subscriber.SetShutdownOptions(&pubsub.ShutdownOptions{
			Behavior: pubsub.ShutdownBehaviorNackImmediately,
			Timeout:  c.opts.subscriberShutdownTimeout,
		})
	}

	c.initialized.Store(true)

	return c, nil
}

func (c *Client) Name() string {
	return c.topic
}

// Close stops the publisher, flushing any pending messages.
func (c *Client) Close() {
	if c.publisher != nil {
		c.publisher.Stop()
	}
}

func (c *Client) Send(ctx context.Context, groupID, dedupID, body string) error {
	if !c.initialized.Load() {
		return errors.New("pub/sub client not initialized")
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

	msg := &pubsub.Message{
		Data:        []byte(body),
		OrderingKey: groupID,
		Attributes:  map[string]string{"dedup_id": dedupID},
	}

	if _, err := c.publisher.Publish(ctx, msg).Get(ctx); err != nil {
		return fmt.Errorf("failed to publish message to pub/sub topic %s with ordering key %s: %w", c.topic, groupID, err)
	}

	return nil
}

// Receive starts receiving messages from the configured subscription and sends them to the sink channel.
// The sink channel is always closed when this method returns, including on validation errors.
func (c *Client) Receive(ctx context.Context, sinkCh chan<- *types.FifoQueueItem) error {
	defer close(sinkCh)

	if !c.initialized.Load() {
		return errors.New("pub/sub client not initialized")
	}

	if c.subscriber == nil {
		return errors.New("pub/sub client subscriber is not configured")
	}

	if c.isReceiving.Load() {
		return errors.New("pub/sub client is already receiving messages")
	}

	c.isReceiving.Store(true)
	c.receiveSinkCh = sinkCh

	defer func() {
		c.isReceiving.Store(false)
		c.receiveSinkCh = nil
		c.logger.Debug("Stopped receiving pub/sub messages")
	}()

	c.logger.Debug("Started receiving pub/sub messages")

	return c.subscriber.Receive(ctx, c.receiveHandler)
}

func (c *Client) receiveHandler(ctx context.Context, msg *pubsub.Message) {
	item := &types.FifoQueueItem{
		MessageID:        msg.ID,
		SlackChannelID:   msg.OrderingKey,
		ReceiveTimestamp: time.Now(),
		Body:             string(msg.Data),
		Ack:              msg.Ack,
		Nack:             msg.Nack,
	}

	if err := trySend(ctx, item, c.receiveSinkCh); err != nil {
		msg.Nack()

		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			c.logger.Errorf("Failed to send pub/sub message %s to sink channel: %v", msg.ID, err)
		}

		return
	}

	c.logger.WithField("message_id", msg.ID).Debug("Pub/Sub message sent to sink channel")
}

func trySend(ctx context.Context, msg *types.FifoQueueItem, sinkCh chan<- *types.FifoQueueItem) error {
	select {
	case sinkCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
