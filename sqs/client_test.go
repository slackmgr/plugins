//nolint:paralleltest,testpackage // Tests use shared resources and need access to unexported functions
package sqs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/slackmgr/types"
)

func TestNew(t *testing.T) {
	awsCfg := &aws.Config{}
	logger := newMockLogger()

	client := New(awsCfg, "test-queue.fifo", logger)

	if client == nil {
		t.Fatal("expected non-nil client")
	}

	if client.queueName != "test-queue.fifo" {
		t.Errorf("expected queueName 'test-queue.fifo', got %q", client.queueName)
	}

	if client.awsCfg != awsCfg {
		t.Error("expected awsCfg to be set")
	}

	if client.extenderCh == nil {
		t.Error("expected extenderCh to be initialized")
	}

	if client.initialized {
		t.Error("expected initialized to be false before Init()")
	}
}

func TestNew_WithOptions(t *testing.T) {
	awsCfg := &aws.Config{}
	logger := newMockLogger()

	client := New(awsCfg, "test-queue.fifo", logger,
		WithSqsVisibilityTimeout(60),
		WithSqsReceiveMaxNumberOfMessages(5),
	)

	if client.opts.sqsVisibilityTimeoutSeconds != 60 {
		t.Errorf("expected visibility timeout 60, got %d", client.opts.sqsVisibilityTimeoutSeconds)
	}

	if client.opts.sqsReceiveMaxNumberOfMessages != 5 {
		t.Errorf("expected max messages 5, got %d", client.opts.sqsReceiveMaxNumberOfMessages)
	}
}

func TestInit_Success(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, input *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			if *input.QueueName != "test-queue.fifo" {
				t.Errorf("expected queue name 'test-queue.fifo', got %q", *input.QueueName)
			}
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo"),
			}, nil
		},
	}

	awsCfg := &aws.Config{}
	client := New(awsCfg, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	result, err := client.Init(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != client {
		t.Error("expected Init to return the same client")
	}

	if !client.initialized {
		t.Error("expected initialized to be true after Init()")
	}

	if client.queueURL != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo" {
		t.Errorf("expected queue URL to be set, got %q", client.queueURL)
	}

	if client.extender == nil {
		t.Error("expected extender to be set")
	}
}

func TestInit_AlreadyInitialized(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo"),
			}, nil
		},
	}

	awsCfg := &aws.Config{}
	client := New(awsCfg, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, err := client.Init(ctx)
	if err != nil {
		t.Fatalf("first Init failed: %v", err)
	}

	// Second Init should return immediately without error
	result, err := client.Init(ctx)
	if err != nil {
		t.Fatalf("second Init failed: %v", err)
	}

	if result != client {
		t.Error("expected Init to return the same client")
	}
}

func TestInit_InvalidQueueName(t *testing.T) {
	awsCfg := &aws.Config{}
	client := New(awsCfg, "test-queue-without-fifo", newMockLogger())

	_, err := client.Init(context.Background())

	if err == nil {
		t.Fatal("expected error for non-FIFO queue name")
	}

	if client.initialized {
		t.Error("expected initialized to remain false after error")
	}
}

func TestInit_InvalidOptions(t *testing.T) {
	awsCfg := &aws.Config{}
	client := New(awsCfg, "test-queue.fifo", newMockLogger(),
		WithSqsVisibilityTimeout(5), // Invalid: less than 10
	)

	_, err := client.Init(context.Background())

	if err == nil {
		t.Fatal("expected error for invalid options")
	}

	if client.initialized {
		t.Error("expected initialized to remain false after error")
	}
}

func TestInit_GetQueueUrlError(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return nil, errors.New("queue not found")
		},
	}

	awsCfg := &aws.Config{}
	client := New(awsCfg, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	_, err := client.Init(context.Background())

	if err == nil {
		t.Fatal("expected error when GetQueueUrl fails")
	}

	if client.initialized {
		t.Error("expected initialized to remain false after error")
	}
}

func TestName(t *testing.T) {
	client := New(&aws.Config{}, "my-queue.fifo", newMockLogger())

	if client.Name() != "my-queue.fifo" {
		t.Errorf("expected 'my-queue.fifo', got %q", client.Name())
	}
}

func TestSend_Success(t *testing.T) {
	var capturedInput *sqs.SendMessageInput

	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{
				QueueUrl: aws.String("https://sqs.example.com/queue.fifo"),
			}, nil
		},
		sendMessageFunc: func(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedInput = input
			return &sqs.SendMessageOutput{
				MessageId: aws.String("msg-123"),
			}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, err := client.Init(ctx)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	err = client.Send(ctx, "group-1", "dedup-1", "test body")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if capturedInput == nil {
		t.Fatal("expected SendMessage to be called")
	}

	if *capturedInput.MessageGroupId != "group-1" {
		t.Errorf("expected group ID 'group-1', got %q", *capturedInput.MessageGroupId)
	}

	if *capturedInput.MessageDeduplicationId != "dedup-1" {
		t.Errorf("expected dedup ID 'dedup-1', got %q", *capturedInput.MessageDeduplicationId)
	}

	if *capturedInput.MessageBody != "test body" {
		t.Errorf("expected body 'test body', got %q", *capturedInput.MessageBody)
	}
}

func TestSend_NotInitialized(t *testing.T) {
	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger())

	err := client.Send(context.Background(), "group", "dedup", "body")

	if err == nil {
		t.Fatal("expected error when not initialized")
	}
}

func TestSend_EmptyGroupID(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	err := client.Send(ctx, "", "dedup", "body")

	if err == nil {
		t.Fatal("expected error for empty groupID")
	}
}

func TestSend_EmptyDedupID(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	err := client.Send(ctx, "group", "", "body")

	if err == nil {
		t.Fatal("expected error for empty dedupID")
	}
}

func TestSend_EmptyBody(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	err := client.Send(ctx, "group", "dedup", "")

	if err == nil {
		t.Fatal("expected error for empty body")
	}
}

func TestSend_Error(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		sendMessageFunc: func(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("SQS send failed")
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	err := client.Send(ctx, "group", "dedup", "body")

	if err == nil {
		t.Fatal("expected error when SendMessage fails")
	}
}

func TestReceive_Success(t *testing.T) {
	var messageReturned atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())

	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		receiveMessageFunc: func(recvCtx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			// Return a message only once, then wait for context cancellation
			if messageReturned.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{
					Messages: []sqstypes.Message{
						{
							MessageId:     aws.String("msg-123"),
							ReceiptHandle: aws.String("receipt-123"),
							Body:          aws.String(`{"test":"data"}`),
							Attributes: map[string]string{
								string(sqstypes.MessageSystemAttributeNameMessageGroupId): "channel-1",
							},
						},
					},
				}, nil
			}
			// Block until context is cancelled
			<-recvCtx.Done()
			return nil, recvCtx.Err()
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	initCtx := t.Context()
	_, _ = client.Init(initCtx)

	sinkCh := make(chan *types.FifoQueueItem, 10)
	receiveDone := make(chan struct{})

	go func() {
		_ = client.Receive(ctx, sinkCh)
		close(receiveDone)
	}()

	select {
	case item := <-sinkCh:
		if item.MessageID != "msg-123" {
			t.Errorf("expected message ID 'msg-123', got %q", item.MessageID)
		}
		if item.SlackChannelID != "channel-1" {
			t.Errorf("expected channel ID 'channel-1', got %q", item.SlackChannelID)
		}
		if item.Body != `{"test":"data"}` {
			t.Errorf("expected body, got %q", item.Body)
		}
		if item.Ack == nil {
			t.Error("expected Ack function to be set")
		}
		if item.Nack == nil {
			t.Error("expected Nack function to be set")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	cancel()
	<-receiveDone
}

func TestReceive_NotInitialized(t *testing.T) {
	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger())

	sinkCh := make(chan *types.FifoQueueItem)

	err := client.Receive(context.Background(), sinkCh)

	if err == nil {
		t.Fatal("expected error when not initialized")
	}
}

func TestReceive_ContextCancellation(t *testing.T) {
	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx, cancel := context.WithCancel(context.Background())
	_, _ = client.Init(ctx)

	sinkCh := make(chan *types.FifoQueueItem)
	done := make(chan error)

	go func() {
		done <- client.Receive(ctx, sinkCh)
	}()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Receive to exit")
	}
}

func TestReceive_ErrorWithRetry(t *testing.T) {
	var receiveCount atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		receiveMessageFunc: func(recvCtx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			count := receiveCount.Add(1)
			if count == 1 {
				// First call returns error - schedule context cancellation to interrupt the retry delay
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				return nil, errors.New("temporary error")
			}
			// Second call won't happen if cancel() works, but handle it just in case
			<-recvCtx.Done()
			return nil, recvCtx.Err()
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	initCtx := t.Context()
	_, _ = client.Init(initCtx)

	sinkCh := make(chan *types.FifoQueueItem)

	// Receive will retry on errors until context is cancelled.
	// The first error triggers a retry delay, during which we cancel the context.
	_ = client.Receive(ctx, sinkCh)

	// Verify at least one receive call was made
	if receiveCount.Load() < 1 {
		t.Errorf("expected at least 1 receive call, got %d", receiveCount.Load())
	}
}

func TestDeleteMessage(t *testing.T) {
	var capturedInput *sqs.DeleteMessageInput

	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		deleteMessageFunc: func(_ context.Context, input *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			capturedInput = input
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	client.deleteMessage("msg-123", "receipt-handle-123")

	if capturedInput == nil {
		t.Fatal("expected DeleteMessage to be called")
	}

	if *capturedInput.ReceiptHandle != "receipt-handle-123" {
		t.Errorf("expected receipt handle 'receipt-handle-123', got %q", *capturedInput.ReceiptHandle)
	}
}

func TestChangeMessageVisibility(t *testing.T) {
	var capturedInput *sqs.ChangeMessageVisibilityInput

	mockClient := &mockSQSClient{
		getQueueUrlFunc: func(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.example.com/queue.fifo")}, nil
		},
		changeMessageVisibilityFunc: func(_ context.Context, input *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			capturedInput = input
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	client := New(&aws.Config{}, "test-queue.fifo", newMockLogger(), WithSQSClient(mockClient))

	ctx := t.Context()

	_, _ = client.Init(ctx)

	err := client.changeMessageVisibility(ctx, "msg-123", "receipt-handle-123")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if capturedInput == nil {
		t.Fatal("expected ChangeMessageVisibility to be called")
	}

	if *capturedInput.ReceiptHandle != "receipt-handle-123" {
		t.Errorf("expected receipt handle 'receipt-handle-123', got %q", *capturedInput.ReceiptHandle)
	}

	if capturedInput.VisibilityTimeout != client.opts.sqsVisibilityTimeoutSeconds {
		t.Errorf("expected visibility timeout %d, got %d", client.opts.sqsVisibilityTimeoutSeconds, capturedInput.VisibilityTimeout)
	}
}

func TestTrySend(t *testing.T) {
	ch := make(chan string, 1)

	err := trySend(context.Background(), "test", ch)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	select {
	case msg := <-ch:
		if msg != "test" {
			t.Errorf("expected 'test', got %q", msg)
		}
	default:
		t.Error("expected message in channel")
	}
}

func TestTrySend_ContextCancelled(t *testing.T) {
	ch := make(chan string) // Unbuffered, will block

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := trySend(ctx, "test", ch)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
