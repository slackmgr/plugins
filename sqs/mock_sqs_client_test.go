//nolint:testpackage // Mock must be in sqs package to access unexported types
package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/slackmgr/types"
)

// mockSQSClient is a mock implementation of the sqsClient interface for testing.
type mockSQSClient struct {
	getQueueUrlFunc             func(ctx context.Context, input *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	sendMessageFunc             func(ctx context.Context, input *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	receiveMessageFunc          func(ctx context.Context, input *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteMessageFunc           func(ctx context.Context, input *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	changeMessageVisibilityFunc func(ctx context.Context, input *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockSQSClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	if m.getQueueUrlFunc != nil {
		return m.getQueueUrlFunc(ctx, params, optFns...)
	}
	return &sqs.GetQueueUrlOutput{}, nil
}

func (m *mockSQSClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.sendMessageFunc != nil {
		return m.sendMessageFunc(ctx, params, optFns...)
	}
	return &sqs.SendMessageOutput{}, nil
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if m.receiveMessageFunc != nil {
		return m.receiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.deleteMessageFunc != nil {
		return m.deleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.changeMessageVisibilityFunc != nil {
		return m.changeMessageVisibilityFunc(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// mockLogger is a no-op logger for testing.
type mockLogger struct{}

//nolint:ireturn // Must return interface to implement types.Logger
func (m *mockLogger) WithField(_ string, _ any) types.Logger { return m }

//nolint:ireturn // Must return interface to implement types.Logger
func (m *mockLogger) WithFields(_ map[string]any) types.Logger { return m }
func (m *mockLogger) Debug(_ string)                           {}
func (m *mockLogger) Debugf(_ string, _ ...any)                {}
func (m *mockLogger) Info(_ string)                            {}
func (m *mockLogger) Infof(_ string, _ ...any)                 {}
func (m *mockLogger) Warn(_ string)                            {}
func (m *mockLogger) Warnf(_ string, _ ...any)                 {}
func (m *mockLogger) Error(_ string)                           {}
func (m *mockLogger) Errorf(_ string, _ ...any)                {}
func (m *mockLogger) Fatal(_ string)                           {}
func (m *mockLogger) Fatalf(_ string, _ ...any)                {}

//nolint:ireturn // Returns interface for convenience in tests
func newMockLogger() types.Logger {
	return &mockLogger{}
}
