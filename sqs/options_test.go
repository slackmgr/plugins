//nolint:paralleltest,testpackage // Tests use shared resources and need access to unexported functions
package sqs

import (
	"testing"
	"time"
)

func TestNewOptions_Defaults(t *testing.T) {
	opts := newOptions()

	tests := []struct {
		name     string
		got      any
		expected any
	}{
		{"sqsVisibilityTimeoutSeconds", opts.sqsVisibilityTimeoutSeconds, int32(30)},
		{"sqsReceiveMaxNumberOfMessages", opts.sqsReceiveMaxNumberOfMessages, int32(1)},
		{"sqsReceiveWaitTimeSeconds", opts.sqsReceiveWaitTimeSeconds, int32(20)},
		{"sqsAPIMaxRetryAttempts", opts.sqsAPIMaxRetryAttempts, 5},
		{"sqsAPIMaxRetryBackoffDelay", opts.sqsAPIMaxRetryBackoffDelay, 10 * time.Second},
		{"maxMessageExtension", opts.maxMessageExtension, 10 * time.Minute},
		{"maxOutstandingMessages", opts.maxOutstandingMessages, 100},
		{"maxOutstandingBytes", opts.maxOutstandingBytes, int(1e6)},
		{"sqsClient", opts.sqsClient, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, tt.got)
			}
		})
	}
}

func TestValidate_ValidOptions(t *testing.T) {
	opts := newOptions()
	if err := opts.validate(); err != nil {
		t.Errorf("expected no error for valid options, got %v", err)
	}
}

func TestValidate_InvalidVisibilityTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout int32
		wantErr bool
	}{
		{"too low", 9, true},
		{"minimum valid", 10, false},
		{"maximum valid", 3600, false},
		{"too high", 3601, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.sqsVisibilityTimeoutSeconds = tt.timeout
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidMaxMessages(t *testing.T) {
	tests := []struct {
		name    string
		max     int32
		wantErr bool
	}{
		{"too low", 0, true},
		{"minimum valid", 1, false},
		{"maximum valid", 10, false},
		{"too high", 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.sqsReceiveMaxNumberOfMessages = tt.max
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidWaitTime(t *testing.T) {
	tests := []struct {
		name    string
		wait    int32
		wantErr bool
	}{
		{"too low", 2, true},
		{"minimum valid", 3, false},
		{"maximum valid", 20, false},
		{"too high", 21, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.sqsReceiveWaitTimeSeconds = tt.wait
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidRetryAttempts(t *testing.T) {
	tests := []struct {
		name    string
		retries int
		wantErr bool
	}{
		{"negative", -1, true},
		{"minimum valid", 0, false},
		{"maximum valid", 10, false},
		{"too high", 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.sqsAPIMaxRetryAttempts = tt.retries
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidBackoffDelay(t *testing.T) {
	tests := []struct {
		name    string
		delay   time.Duration
		wantErr bool
	}{
		{"too low", 500 * time.Millisecond, true},
		{"minimum valid", 1 * time.Second, false},
		{"maximum valid", 30 * time.Second, false},
		{"too high", 31 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.sqsAPIMaxRetryBackoffDelay = tt.delay
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidMaxExtension(t *testing.T) {
	tests := []struct {
		name    string
		ext     time.Duration
		wantErr bool
	}{
		{"too low", 30 * time.Second, true},
		{"minimum valid", 1 * time.Minute, false},
		{"maximum valid", time.Hour, false},
		{"too high", time.Hour + time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.maxMessageExtension = tt.ext
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidMaxOutstandingMessages(t *testing.T) {
	tests := []struct {
		name    string
		max     int
		wantErr bool
	}{
		{"zero", 0, true},
		{"minimum valid", 1, false},
		{"large value", 10000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.maxOutstandingMessages = tt.max
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestValidate_InvalidMaxOutstandingBytes(t *testing.T) {
	tests := []struct {
		name    string
		max     int
		wantErr bool
	}{
		{"too low", 9999, true},
		{"minimum valid", 10000, false},
		{"large value", 10000000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := newOptions()
			opts.maxOutstandingBytes = tt.max
			err := opts.validate()

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestWithSqsVisibilityTimeout(t *testing.T) {
	opts := newOptions()
	WithSqsVisibilityTimeout(60)(opts)

	if opts.sqsVisibilityTimeoutSeconds != 60 {
		t.Errorf("expected 60, got %d", opts.sqsVisibilityTimeoutSeconds)
	}
}

func TestWithSqsReceiveMaxNumberOfMessages(t *testing.T) {
	opts := newOptions()
	WithSqsReceiveMaxNumberOfMessages(5)(opts)

	if opts.sqsReceiveMaxNumberOfMessages != 5 {
		t.Errorf("expected 5, got %d", opts.sqsReceiveMaxNumberOfMessages)
	}
}

func TestWithSqsReceiveWaitTimeSeconds(t *testing.T) {
	opts := newOptions()
	WithSqsReceiveWaitTimeSeconds(10)(opts)

	if opts.sqsReceiveWaitTimeSeconds != 10 {
		t.Errorf("expected 10, got %d", opts.sqsReceiveWaitTimeSeconds)
	}
}

func TestWithSqsAPIMaxRetryAttempts(t *testing.T) {
	opts := newOptions()
	WithSqsAPIMaxRetryAttempts(3)(opts)

	if opts.sqsAPIMaxRetryAttempts != 3 {
		t.Errorf("expected 3, got %d", opts.sqsAPIMaxRetryAttempts)
	}
}

func TestWithSqsAPIMaxRetryBackoffDelay(t *testing.T) {
	opts := newOptions()
	WithSqsAPIMaxRetryBackoffDelay(5 * time.Second)(opts)

	if opts.sqsAPIMaxRetryBackoffDelay != 5*time.Second {
		t.Errorf("expected 5s, got %v", opts.sqsAPIMaxRetryBackoffDelay)
	}
}

func TestWithMaxMessageExtension(t *testing.T) {
	opts := newOptions()
	WithMaxMessageExtension(30 * time.Minute)(opts)

	if opts.maxMessageExtension != 30*time.Minute {
		t.Errorf("expected 30m, got %v", opts.maxMessageExtension)
	}
}

func TestWithMaxOutstandingMessages(t *testing.T) {
	opts := newOptions()
	WithMaxOutstandingMessages(500)(opts)

	if opts.maxOutstandingMessages != 500 {
		t.Errorf("expected 500, got %d", opts.maxOutstandingMessages)
	}
}

func TestWithMaxOutstandingBytes(t *testing.T) {
	opts := newOptions()
	WithMaxOutstandingBytes(5000000)(opts)

	if opts.maxOutstandingBytes != 5000000 {
		t.Errorf("expected 5000000, got %d", opts.maxOutstandingBytes)
	}
}

func TestWithSQSClient(t *testing.T) {
	opts := newOptions()
	mockClient := &mockSQSClient{}
	WithSQSClient(mockClient)(opts)

	if opts.sqsClient != mockClient {
		t.Error("expected sqsClient to be set to mock client")
	}
}

func TestOptionsChaining(t *testing.T) {
	opts := newOptions()

	options := []Option{
		WithSqsVisibilityTimeout(60),
		WithSqsReceiveMaxNumberOfMessages(5),
		WithSqsReceiveWaitTimeSeconds(15),
		WithSqsAPIMaxRetryAttempts(3),
	}

	for _, o := range options {
		o(opts)
	}

	if opts.sqsVisibilityTimeoutSeconds != 60 {
		t.Errorf("expected visibility timeout 60, got %d", opts.sqsVisibilityTimeoutSeconds)
	}
	if opts.sqsReceiveMaxNumberOfMessages != 5 {
		t.Errorf("expected max messages 5, got %d", opts.sqsReceiveMaxNumberOfMessages)
	}
	if opts.sqsReceiveWaitTimeSeconds != 15 {
		t.Errorf("expected wait time 15, got %d", opts.sqsReceiveWaitTimeSeconds)
	}
	if opts.sqsAPIMaxRetryAttempts != 3 {
		t.Errorf("expected retry attempts 3, got %d", opts.sqsAPIMaxRetryAttempts)
	}
}
