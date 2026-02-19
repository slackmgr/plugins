//nolint:paralleltest,testpackage // Tests use shared resources and need access to unexported functions
package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/slackmgr/types"
)

func TestNewWebhookHandler(t *testing.T) {
	awsCfg := &aws.Config{}
	handler := NewWebhookHandler(awsCfg)

	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	if handler.awsCfg != awsCfg {
		t.Error("expected awsCfg to be set")
	}

	if handler.initialized {
		t.Error("expected initialized to be false before Init()")
	}
}

func TestNewWebhookHandlerWithClient(t *testing.T) {
	mockClient := &mockSQSClient{}
	handler := newWebhookHandlerWithClient(mockClient)

	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	if handler.client != mockClient {
		t.Error("expected client to be set")
	}

	if !handler.initialized {
		t.Error("expected initialized to be true")
	}
}

func TestWebhookHandler_Init(t *testing.T) {
	awsCfg := &aws.Config{}
	handler := NewWebhookHandler(awsCfg)

	result, err := handler.Init(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != handler {
		t.Error("expected Init to return the same handler")
	}

	if !handler.initialized {
		t.Error("expected initialized to be true after Init()")
	}

	if handler.client == nil {
		t.Error("expected client to be created")
	}
}

func TestWebhookHandler_Init_AlreadyInitialized(t *testing.T) {
	mockClient := &mockSQSClient{}
	handler := newWebhookHandlerWithClient(mockClient)

	result, err := handler.Init(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result != handler {
		t.Error("expected Init to return the same handler")
	}

	// Client should remain the mock, not be replaced
	if handler.client != mockClient {
		t.Error("expected client to remain unchanged")
	}
}

func TestShouldHandleWebhook_SQSUrl(t *testing.T) {
	handler := NewWebhookHandler(&aws.Config{})

	tests := []struct {
		url      string
		expected bool
	}{
		{"https://sqs.us-east-1.amazonaws.com/123456789/queue", true},
		{"https://sqs.eu-west-1.amazonaws.com/123456789/queue.fifo", true},
		{"https://sqs.ap-southeast-1.amazonaws.com/queue", true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := handler.ShouldHandleWebhook(context.Background(), tt.url)
			if result != tt.expected {
				t.Errorf("expected %v for URL %q, got %v", tt.expected, tt.url, result)
			}
		})
	}
}

func TestShouldHandleWebhook_NonSQSUrl(t *testing.T) {
	handler := NewWebhookHandler(&aws.Config{})

	tests := []struct {
		url      string
		expected bool
	}{
		{"https://example.com/webhook", false},
		{"https://api.slack.com/callback", false},
		{"https://sns.us-east-1.amazonaws.com/topic", false},
		{"http://sqs.example.com/queue", false}, // http, not https
		{"https://not-sqs.amazonaws.com/queue", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := handler.ShouldHandleWebhook(context.Background(), tt.url)
			if result != tt.expected {
				t.Errorf("expected %v for URL %q, got %v", tt.expected, tt.url, result)
			}
		})
	}
}

func TestHandleWebhook_FifoQueue(t *testing.T) {
	var capturedInput *sqs.SendMessageInput

	mockClient := &mockSQSClient{
		sendMessageFunc: func(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedInput = input
			return &sqs.SendMessageOutput{MessageId: aws.String("msg-123")}, nil
		},
	}

	handler := newWebhookHandlerWithClient(mockClient)

	data := &types.WebhookCallback{
		ID:        "callback-123",
		ChannelID: "C12345",
		MessageID: "msg-456",
		Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	err := handler.HandleWebhook(context.Background(), "https://sqs.example.com/queue.fifo", data, newMockLogger())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if capturedInput == nil {
		t.Fatal("expected SendMessage to be called")
	}

	if *capturedInput.QueueUrl != "https://sqs.example.com/queue.fifo" {
		t.Errorf("expected queue URL 'https://sqs.example.com/queue.fifo', got %q", *capturedInput.QueueUrl)
	}

	if *capturedInput.MessageGroupId != "C12345" {
		t.Errorf("expected group ID 'C12345', got %q", *capturedInput.MessageGroupId)
	}

	if capturedInput.MessageDeduplicationId == nil || *capturedInput.MessageDeduplicationId == "" {
		t.Error("expected deduplication ID to be set")
	}

	if capturedInput.MessageBody == nil || *capturedInput.MessageBody == "" {
		t.Error("expected body to be set")
	}
}

func TestHandleWebhook_StandardQueue(t *testing.T) {
	var capturedInput *sqs.SendMessageInput

	mockClient := &mockSQSClient{
		sendMessageFunc: func(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedInput = input
			return &sqs.SendMessageOutput{MessageId: aws.String("msg-123")}, nil
		},
	}

	handler := newWebhookHandlerWithClient(mockClient)

	data := &types.WebhookCallback{
		ID:        "callback-123",
		ChannelID: "C12345",
		MessageID: "msg-456",
		Timestamp: time.Now(),
	}

	err := handler.HandleWebhook(context.Background(), "https://sqs.example.com/queue", data, newMockLogger())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if capturedInput == nil {
		t.Fatal("expected SendMessage to be called")
	}

	// Standard queue should not have group ID or dedup ID
	if capturedInput.MessageGroupId != nil {
		t.Errorf("expected no group ID for standard queue, got %q", *capturedInput.MessageGroupId)
	}

	if capturedInput.MessageDeduplicationId != nil {
		t.Errorf("expected no dedup ID for standard queue, got %q", *capturedInput.MessageDeduplicationId)
	}
}

func TestHandleWebhook_NotInitialized(t *testing.T) {
	handler := NewWebhookHandler(&aws.Config{})

	data := &types.WebhookCallback{
		ID:        "callback-123",
		ChannelID: "C12345",
	}

	err := handler.HandleWebhook(context.Background(), "https://sqs.example.com/queue.fifo", data, newMockLogger())

	if err == nil {
		t.Fatal("expected error when not initialized")
	}
}

func TestHandleWebhook_SendError(t *testing.T) {
	mockClient := &mockSQSClient{
		sendMessageFunc: func(_ context.Context, _ *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			return nil, errors.New("SQS send failed")
		},
	}

	handler := newWebhookHandlerWithClient(mockClient)

	data := &types.WebhookCallback{
		ID:        "callback-123",
		ChannelID: "C12345",
	}

	err := handler.HandleWebhook(context.Background(), "https://sqs.example.com/queue.fifo", data, newMockLogger())

	if err == nil {
		t.Fatal("expected error when SendMessage fails")
	}
}

func TestHash_Deterministic(t *testing.T) {
	hash1 := hash("a", "b", "c")
	hash2 := hash("a", "b", "c")

	if hash1 != hash2 {
		t.Errorf("expected same input to produce same hash, got %q and %q", hash1, hash2)
	}
}

func TestHash_DifferentInputs(t *testing.T) {
	hash1 := hash("a", "b", "c")
	hash2 := hash("a", "b", "d")

	if hash1 == hash2 {
		t.Error("expected different inputs to produce different hashes")
	}
}

func TestHash_NullByteDelimiter(t *testing.T) {
	// Without null byte delimiter, "ab" + "" + "c" would equal "a" + "bc"
	// The null byte delimiter prevents this collision
	hash1 := hash("ab", "", "c")
	hash2 := hash("a", "bc")

	if hash1 == hash2 {
		t.Error("expected hash to use null byte delimiter to prevent collisions")
	}
}

func TestHash_EmptyStrings(t *testing.T) {
	// Should not panic with empty strings
	hash1 := hash("", "", "")
	hash2 := hash("")

	if hash1 == "" {
		t.Error("expected non-empty hash for empty inputs")
	}

	if hash1 == hash2 {
		t.Error("expected different number of empty strings to produce different hashes")
	}
}

func TestHash_SingleInput(t *testing.T) {
	result := hash("single")

	if result == "" {
		t.Error("expected non-empty hash")
	}

	// Verify it's a valid base64 string (length should be appropriate for SHA256)
	// SHA256 is 32 bytes, base64 encoded = 44 characters (with padding)
	if len(result) != 44 {
		t.Errorf("expected 44 character base64 hash, got %d characters", len(result))
	}
}

func TestSendToFifoQueue(t *testing.T) {
	var capturedInput *sqs.SendMessageInput

	mockClient := &mockSQSClient{
		sendMessageFunc: func(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedInput = input
			return &sqs.SendMessageOutput{}, nil
		},
	}

	handler := newWebhookHandlerWithClient(mockClient)

	err := handler.sendToFifoQueue(context.Background(), "https://queue.fifo", "group-1", "dedup-1", `{"data":"test"}`, newMockLogger())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if *capturedInput.QueueUrl != "https://queue.fifo" {
		t.Errorf("expected queue URL, got %q", *capturedInput.QueueUrl)
	}

	if *capturedInput.MessageGroupId != "group-1" {
		t.Errorf("expected group ID 'group-1', got %q", *capturedInput.MessageGroupId)
	}

	if *capturedInput.MessageDeduplicationId != "dedup-1" {
		t.Errorf("expected dedup ID 'dedup-1', got %q", *capturedInput.MessageDeduplicationId)
	}

	if *capturedInput.MessageBody != `{"data":"test"}` {
		t.Errorf("expected body, got %q", *capturedInput.MessageBody)
	}
}

func TestSendToStdQueue(t *testing.T) {
	var capturedInput *sqs.SendMessageInput

	mockClient := &mockSQSClient{
		sendMessageFunc: func(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
			capturedInput = input
			return &sqs.SendMessageOutput{}, nil
		},
	}

	handler := newWebhookHandlerWithClient(mockClient)

	err := handler.sendToStdQueue(context.Background(), "https://queue", `{"data":"test"}`, newMockLogger())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if *capturedInput.QueueUrl != "https://queue" {
		t.Errorf("expected queue URL, got %q", *capturedInput.QueueUrl)
	}

	if *capturedInput.MessageBody != `{"data":"test"}` {
		t.Errorf("expected body, got %q", *capturedInput.MessageBody)
	}

	if capturedInput.MessageGroupId != nil {
		t.Error("expected no group ID for standard queue")
	}

	if capturedInput.MessageDeduplicationId != nil {
		t.Error("expected no dedup ID for standard queue")
	}
}
