package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/slackmgr/types"
)

// mockAPI is a mock implementation of API for testing.
type mockAPI struct {
	putItemFunc            func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	queryFunc              func(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	getItemFunc            func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	scanFunc               func(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	batchWriteItemFunc     func(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	transactWriteItemFunc  func(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	describeTableFunc      func(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	describeTimeToLiveFunc func(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	deleteItemFunc         func(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

func (m *mockAPI) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItemFunc != nil {
		return m.putItemFunc(ctx, params, optFns...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockAPI) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, params, optFns...)
	}
	return &dynamodb.QueryOutput{}, nil
}

func (m *mockAPI) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if m.getItemFunc != nil {
		return m.getItemFunc(ctx, params, optFns...)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockAPI) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if m.scanFunc != nil {
		return m.scanFunc(ctx, params, optFns...)
	}
	return &dynamodb.ScanOutput{}, nil
}

func (m *mockAPI) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if m.batchWriteItemFunc != nil {
		return m.batchWriteItemFunc(ctx, params, optFns...)
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (m *mockAPI) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItemFunc != nil {
		return m.transactWriteItemFunc(ctx, params, optFns...)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (m *mockAPI) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if m.describeTableFunc != nil {
		return m.describeTableFunc(ctx, params, optFns...)
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockAPI) DescribeTimeToLive(ctx context.Context, params *dynamodb.DescribeTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	if m.describeTimeToLiveFunc != nil {
		return m.describeTimeToLiveFunc(ctx, params, optFns...)
	}
	return &dynamodb.DescribeTimeToLiveOutput{}, nil
}

func (m *mockAPI) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if m.deleteItemFunc != nil {
		return m.deleteItemFunc(ctx, params, optFns...)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

// mockIssue is a mock implementation of types.Issue for testing.
type mockIssue struct {
	channelID     string
	uniqueID      string
	correlationID string
	isOpen        bool
	currentPostID string
	body          []byte
}

func (m *mockIssue) ChannelID() string        { return m.channelID }
func (m *mockIssue) UniqueID() string         { return m.uniqueID }
func (m *mockIssue) GetCorrelationID() string { return m.correlationID }
func (m *mockIssue) IsOpen() bool             { return m.isOpen }
func (m *mockIssue) CurrentPostID() string    { return m.currentPostID }

func (m *mockIssue) MarshalJSON() ([]byte, error) {
	if m.body != nil {
		return m.body, nil
	}
	return json.Marshal(map[string]any{
		"channelId":     m.channelID,
		"uniqueId":      m.uniqueID,
		"correlationId": m.correlationID,
		"isOpen":        m.isOpen,
		"currentPostId": m.currentPostID,
	})
}

// mockMoveMapping is a mock implementation of types.MoveMapping for testing.
type mockMoveMapping struct {
	channelID     string
	uniqueID      string
	correlationID string
	body          []byte
}

func (m *mockMoveMapping) ChannelID() string        { return m.channelID }
func (m *mockMoveMapping) UniqueID() string         { return m.uniqueID }
func (m *mockMoveMapping) GetCorrelationID() string { return m.correlationID }

func (m *mockMoveMapping) MarshalJSON() ([]byte, error) {
	if m.body != nil {
		return m.body, nil
	}
	return json.Marshal(map[string]any{
		"channelId":     m.channelID,
		"uniqueId":      m.uniqueID,
		"correlationId": m.correlationID,
	})
}

func newTestClient(mock *mockAPI) *Client {
	fixedTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	cfg := aws.Config{}
	client := New(&cfg, "test-table",
		WithAPI(mock),
		WithClock(func() time.Time { return fixedTime }),
	)
	_ = client.Connect()
	return client
}

// ==================== Connect Tests ====================

func TestConnect_Success(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	cfg := aws.Config{}
	client := New(&cfg, "test-table", WithAPI(mock))

	err := client.Connect()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestConnect_InvalidOptions(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	cfg := aws.Config{}
	client := New(&cfg, "test-table",
		WithAPI(mock),
		WithAlertsTimeToLive(0),
	)

	err := client.Connect()

	if err == nil {
		t.Error("expected error for invalid options, got nil")
	}
}

// ==================== SaveAlert Tests ====================

func TestSaveAlert_Success(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	alert := &types.Alert{
		SlackChannelID: "C123456",
		Header:         "Test Alert",
		Timestamp:      time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
	}

	err := client.SaveAlert(context.Background(), alert)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if capturedInput == nil {
		t.Fatal("expected PutItem to be called")
	}
	if *capturedInput.TableName != "test-table" {
		t.Errorf("expected table name 'test-table', got %s", *capturedInput.TableName)
	}
	pkAttr, ok := capturedInput.Item[PartitionKey].(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("expected partition key to be a string")
	}
	if pkAttr.Value != "C123456" {
		t.Errorf("expected partition key 'C123456', got %s", pkAttr.Value)
	}
}

func TestSaveAlert_NilAlert(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.SaveAlert(context.Background(), nil)

	if err == nil {
		t.Error("expected error for nil alert, got nil")
	}
	if err.Error() != "alert cannot be nil" {
		t.Errorf("expected 'alert cannot be nil', got %s", err.Error())
	}
}

func TestSaveAlert_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	alert := &types.Alert{
		SlackChannelID: "",
		Header:         "Test Alert",
	}

	err := client.SaveAlert(context.Background(), alert)

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestSaveAlert_PutItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}
	client := newTestClient(mock)

	alert := &types.Alert{
		SlackChannelID: "C123456",
		Header:         "Test Alert",
		Timestamp:      time.Now(),
	}

	err := client.SaveAlert(context.Background(), alert)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestSaveAlert_TTLCalculation(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	fixedTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	cfg := aws.Config{}
	client := New(&cfg, "test-table",
		WithAPI(mock),
		WithClock(func() time.Time { return fixedTime }),
		WithAlertsTimeToLive(24*time.Hour),
	)
	_ = client.Connect()

	alert := &types.Alert{
		SlackChannelID: "C123456",
		Header:         "Test Alert",
		Timestamp:      time.Now(),
	}

	err := client.SaveAlert(context.Background(), alert)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	ttlAttr, ok := capturedInput.Item[TTLAttr].(*dynamodbtypes.AttributeValueMemberN)
	if !ok {
		t.Fatal("expected TTL to be a number")
	}
	expectedTTL := strconv.FormatInt(fixedTime.Add(24*time.Hour).Unix(), 10)
	if ttlAttr.Value != expectedTTL {
		t.Errorf("expected TTL %s, got %s", expectedTTL, ttlAttr.Value)
	}
}

// ==================== SaveIssue Tests ====================

func TestSaveIssue_Success(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.SaveIssue(context.Background(), issue)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if capturedInput == nil {
		t.Fatal("expected PutItem to be called")
	}
}

func TestSaveIssue_NilIssue(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.SaveIssue(context.Background(), nil)

	if err == nil {
		t.Error("expected error for nil issue, got nil")
	}
}

func TestSaveIssue_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID: "",
		uniqueID:  "unique-123",
	}

	err := client.SaveIssue(context.Background(), issue)

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestSaveIssue_OpenIssue(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.SaveIssue(context.Background(), issue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	isOpenAttr, ok := capturedInput.Item[IsOpenAttr]
	if !ok {
		t.Fatal("expected is_open attribute to be set")
	}
	isOpenStr, ok := isOpenAttr.(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("expected is_open to be a string")
	}
	if isOpenStr.Value != "true" {
		t.Error("expected is_open to be 'true'")
	}
	if _, hasTTL := capturedInput.Item[TTLAttr]; hasTTL {
		t.Error("expected no TTL for open issue")
	}
}

func TestSaveIssue_ClosedIssue(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        false,
	}

	err := client.SaveIssue(context.Background(), issue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if _, hasIsOpen := capturedInput.Item[IsOpenAttr]; hasIsOpen {
		t.Error("expected no is_open attribute for closed issue")
	}
	if _, hasTTL := capturedInput.Item[TTLAttr]; !hasTTL {
		t.Error("expected TTL for closed issue")
	}
}

func TestSaveIssue_WithPostID(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
		currentPostID: "post-123",
	}

	err := client.SaveIssue(context.Background(), issue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	postIDAttr, ok := capturedInput.Item[PostIDAttr]
	if !ok {
		t.Fatal("expected post_id attribute to be set")
	}
	postIDStr, ok := postIDAttr.(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatal("expected post_id to be a string")
	}
	if postIDStr.Value != "post-123" {
		t.Error("expected post_id to be 'post-123'")
	}
}

func TestSaveIssue_PutItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.SaveIssue(context.Background(), issue)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== SaveIssues Tests ====================

func TestSaveIssues_Empty(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.SaveIssues(context.Background())
	if err != nil {
		t.Errorf("expected no error for empty issues, got %v", err)
	}
}

func TestSaveIssues_SingleIssue(t *testing.T) {
	t.Parallel()
	putItemCalled := false
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			putItemCalled = true
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.SaveIssues(context.Background(), issue)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !putItemCalled {
		t.Error("expected PutItem to be called for single issue")
	}
}

func TestSaveIssues_MultipleIssues(t *testing.T) {
	t.Parallel()
	batchWriteCalled := false
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchWriteCalled = true
			if len(params.RequestItems["test-table"]) != 3 {
				t.Errorf("expected 3 items in batch, got %d", len(params.RequestItems["test-table"]))
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2", isOpen: true},
		&mockIssue{channelID: "C3", uniqueID: "u3", correlationID: "c3", isOpen: true},
	}

	err := client.SaveIssues(context.Background(), issues...)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !batchWriteCalled {
		t.Error("expected BatchWriteItem to be called")
	}
}

func TestSaveIssues_NilIssue(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		nil,
	}

	err := client.SaveIssues(context.Background(), issues...)

	if err == nil {
		t.Error("expected error for nil issue, got nil")
	}
}

func TestSaveIssues_BatchLimit(t *testing.T) {
	t.Parallel()
	batchCount := 0
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchCount++
			itemCount := len(params.RequestItems["test-table"])
			if itemCount > 25 {
				t.Errorf("batch exceeded limit: got %d items", itemCount)
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issues := make([]types.Issue, 30)
	for i := range 30 {
		issues[i] = &mockIssue{
			channelID:     "C123456",
			uniqueID:      "u" + strconv.Itoa(i),
			correlationID: "c" + strconv.Itoa(i),
			isOpen:        true,
		}
	}

	err := client.SaveIssues(context.Background(), issues...)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if batchCount != 2 {
		t.Errorf("expected 2 batches, got %d", batchCount)
	}
}

func TestSaveIssues_RetryUnprocessedItems(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			callCount++
			if callCount == 1 {
				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
						"test-table": params.RequestItems["test-table"][:1],
					},
				}, nil
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2", isOpen: true},
	}

	err := client.SaveIssues(context.Background(), issues...)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls (1 initial + 1 retry), got %d", callCount)
	}
}

func TestSaveIssues_BatchWriteError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2", isOpen: true},
	}

	err := client.SaveIssues(context.Background(), issues...)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== MoveIssue Tests ====================

func TestMoveIssue_Success(t *testing.T) {
	t.Parallel()
	transactCalled := false
	mock := &mockAPI{
		transactWriteItemFunc: func(_ context.Context, params *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			transactCalled = true
			if len(params.TransactItems) != 2 {
				t.Errorf("expected 2 transact items, got %d", len(params.TransactItems))
			}
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C222222",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "C222222")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !transactCalled {
		t.Error("expected TransactWriteItems to be called")
	}
}

func TestMoveIssue_NilIssue(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.MoveIssue(context.Background(), nil, "C111111", "C222222")

	if err == nil {
		t.Error("expected error for nil issue, got nil")
	}
}

func TestMoveIssue_EmptySourceChannel(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID: "C222222",
		uniqueID:  "unique-123",
	}

	err := client.MoveIssue(context.Background(), issue, "", "C222222")

	if err == nil {
		t.Error("expected error for empty source channel, got nil")
	}
}

func TestMoveIssue_EmptyTargetChannel(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID: "C222222",
		uniqueID:  "unique-123",
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "")

	if err == nil {
		t.Error("expected error for empty target channel, got nil")
	}
}

func TestMoveIssue_SameSourceAndTarget(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID: "C111111",
		uniqueID:  "unique-123",
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "C111111")

	if err == nil {
		t.Error("expected error for same source and target, got nil")
	}
}

func TestMoveIssue_ChannelMismatch(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID: "C333333",
		uniqueID:  "unique-123",
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "C222222")

	if err == nil {
		t.Error("expected error for channel mismatch, got nil")
	}
}

func TestMoveIssue_TransactionError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		transactWriteItemFunc: func(_ context.Context, _ *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return nil, errors.New("transaction failed")
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C222222",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "C222222")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== FindOpenIssueByCorrelationID Tests ====================

func TestFindOpenIssueByCorrelationID_Found(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-123"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"test":"data"}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", correlationID)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "unique-123" {
		t.Errorf("expected id 'unique-123', got %s", id)
	}
	if string(body) != `{"test":"data"}` {
		t.Errorf("expected body '{\"test\":\"data\"}', got %s", string(body))
	}
}

func TestFindOpenIssueByCorrelationID_NotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", "test-correlation")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "" {
		t.Errorf("expected empty id, got %s", id)
	}
	if body != nil {
		t.Errorf("expected nil body, got %s", string(body))
	}
}

func TestFindOpenIssueByCorrelationID_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "", "test-correlation")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestFindOpenIssueByCorrelationID_EmptyCorrelationID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", "")

	if err == nil {
		t.Error("expected error for empty correlation ID, got nil")
	}
}

func TestFindOpenIssueByCorrelationID_MultipleResults(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-1"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{}`},
					},
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-2"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", correlationID)

	if err == nil {
		t.Error("expected error for multiple results, got nil")
	}
}

func TestFindOpenIssueByCorrelationID_QueryError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("query failed")
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", "test-correlation")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== FindIssueBySlackPostID Tests ====================

func TestFindIssueBySlackPostID_Found(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	issueSK := "ISSUE#C123456#" + encodedCorrelation + "#unique-123"
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey: &dynamodbtypes.AttributeValueMemberS{Value: issueSK},
					},
				},
			}, nil
		},
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: issueSK},
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"test":"data"}`},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "unique-123" {
		t.Errorf("expected id 'unique-123', got %s", id)
	}
	if string(body) != `{"test":"data"}` {
		t.Errorf("expected body '{\"test\":\"data\"}', got %s", string(body))
	}
}

func TestFindIssueBySlackPostID_NotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "" {
		t.Errorf("expected empty id, got %s", id)
	}
	if body != nil {
		t.Errorf("expected nil body, got %s", string(body))
	}
}

func TestFindIssueBySlackPostID_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "", "post-123")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestFindIssueBySlackPostID_EmptyPostID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "")

	if err == nil {
		t.Error("expected error for empty post ID, got nil")
	}
}

func TestFindIssueBySlackPostID_MultipleResults(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{SortKey: &dynamodbtypes.AttributeValueMemberS{Value: "sk1"}},
					{SortKey: &dynamodbtypes.AttributeValueMemberS{Value: "sk2"}},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")

	if err == nil {
		t.Error("expected error for multiple results, got nil")
	}
}

// ==================== FindActiveChannels Tests ====================

func TestFindActiveChannels_Success(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C111111"}},
					{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C222222"}},
					{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C111111"}},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	channels, err := client.FindActiveChannels(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(channels) != 2 {
		t.Errorf("expected 2 unique channels, got %d", len(channels))
	}
}

func TestFindActiveChannels_Empty(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}
	client := newTestClient(mock)

	channels, err := client.FindActiveChannels(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(channels) != 0 {
		t.Errorf("expected 0 channels, got %d", len(channels))
	}
}

func TestFindActiveChannels_Pagination(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			callCount++
			if callCount == 1 {
				return &dynamodb.QueryOutput{
					Items: []map[string]dynamodbtypes.AttributeValue{
						{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C111111"}},
					},
					LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
						"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
					},
				}, nil
			}
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C222222"}},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	channels, err := client.FindActiveChannels(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 query calls for pagination, got %d", callCount)
	}
	if len(channels) != 2 {
		t.Errorf("expected 2 channels, got %d", len(channels))
	}
}

func TestFindActiveChannels_QueryError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("query failed")
		},
	}
	client := newTestClient(mock)

	_, err := client.FindActiveChannels(context.Background())

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== LoadOpenIssuesInChannel Tests ====================

func TestLoadOpenIssuesInChannel_Success(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-1"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"id":"1"}`},
					},
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-2"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"id":"2"}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(issues) != 2 {
		t.Errorf("expected 2 issues, got %d", len(issues))
	}
}

func TestLoadOpenIssuesInChannel_Empty(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{},
			}, nil
		},
	}
	client := newTestClient(mock)

	issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(issues) != 0 {
		t.Errorf("expected 0 issues, got %d", len(issues))
	}
}

func TestLoadOpenIssuesInChannel_QueryError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("query failed")
		},
	}
	client := newTestClient(mock)

	_, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== SaveMoveMapping Tests ====================

func TestSaveMoveMapping_Success(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	mapping := &mockMoveMapping{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
	}

	err := client.SaveMoveMapping(context.Background(), mapping)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if capturedInput == nil {
		t.Fatal("expected PutItem to be called")
	}
}

func TestSaveMoveMapping_NilMapping(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.SaveMoveMapping(context.Background(), nil)

	if err == nil {
		t.Error("expected error for nil mapping, got nil")
	}
}

func TestSaveMoveMapping_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	mapping := &mockMoveMapping{
		channelID: "",
		uniqueID:  "unique-123",
	}

	err := client.SaveMoveMapping(context.Background(), mapping)

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestSaveMoveMapping_PutItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}
	client := newTestClient(mock)

	mapping := &mockMoveMapping{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
	}

	err := client.SaveMoveMapping(context.Background(), mapping)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== FindMoveMapping Tests ====================

func TestFindMoveMapping_Found(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"test":"mapping"}`},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	body, err := client.FindMoveMapping(context.Background(), "C123456", "corr-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if string(body) != `{"test":"mapping"}` {
		t.Errorf("expected body '{\"test\":\"mapping\"}', got %s", string(body))
	}
}

func TestFindMoveMapping_NotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: nil,
			}, nil
		},
	}
	client := newTestClient(mock)

	body, err := client.FindMoveMapping(context.Background(), "C123456", "corr-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if body != nil {
		t.Errorf("expected nil body, got %s", string(body))
	}
}

func TestFindMoveMapping_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.FindMoveMapping(context.Background(), "", "corr-123")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestFindMoveMapping_EmptyCorrelationID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.FindMoveMapping(context.Background(), "C123456", "")

	if err == nil {
		t.Error("expected error for empty correlation ID, got nil")
	}
}

func TestFindMoveMapping_GetItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("get item failed")
		},
	}
	client := newTestClient(mock)

	_, err := client.FindMoveMapping(context.Background(), "C123456", "corr-123")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== DeleteMoveMapping Tests ====================

func TestDeleteMoveMapping_Success(t *testing.T) {
	t.Parallel()
	deleteCalled := false
	mock := &mockAPI{
		deleteItemFunc: func(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			deleteCalled = true
			return &dynamodb.DeleteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DeleteMoveMapping(context.Background(), "C123456", "corr-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !deleteCalled {
		t.Error("expected DeleteItem to be called")
	}
}

func TestDeleteMoveMapping_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.DeleteMoveMapping(context.Background(), "", "corr-123")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestDeleteMoveMapping_EmptyCorrelationID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.DeleteMoveMapping(context.Background(), "C123456", "")

	if err == nil {
		t.Error("expected error for empty correlation ID, got nil")
	}
}

func TestDeleteMoveMapping_DeleteItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		deleteItemFunc: func(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
			return nil, errors.New("delete failed")
		},
	}
	client := newTestClient(mock)

	err := client.DeleteMoveMapping(context.Background(), "C123456", "corr-123")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== SaveChannelProcessingState Tests ====================

func TestSaveChannelProcessingState_Success(t *testing.T) {
	t.Parallel()
	var capturedInput *dynamodb.PutItemInput
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			capturedInput = params
			return &dynamodb.PutItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	state := &types.ChannelProcessingState{
		ChannelID: "C123456",
	}

	err := client.SaveChannelProcessingState(context.Background(), state)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if capturedInput == nil {
		t.Fatal("expected PutItem to be called")
	}
}

func TestSaveChannelProcessingState_NilState(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.SaveChannelProcessingState(context.Background(), nil)

	if err == nil {
		t.Error("expected error for nil state, got nil")
	}
}

func TestSaveChannelProcessingState_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	state := &types.ChannelProcessingState{
		ChannelID: "",
	}

	err := client.SaveChannelProcessingState(context.Background(), state)

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestSaveChannelProcessingState_PutItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		putItemFunc: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb error")
		},
	}
	client := newTestClient(mock)

	state := &types.ChannelProcessingState{
		ChannelID: "C123456",
	}

	err := client.SaveChannelProcessingState(context.Background(), state)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== FindChannelProcessingState Tests ====================

func TestFindChannelProcessingState_Found(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"channelId":"C123456"}`},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	state, err := client.FindChannelProcessingState(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if state == nil {
		t.Fatal("expected state, got nil")
	}
	if state.ChannelID != "C123456" {
		t.Errorf("expected channel ID 'C123456', got %s", state.ChannelID)
	}
}

func TestFindChannelProcessingState_NotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: nil,
			}, nil
		},
	}
	client := newTestClient(mock)

	state, err := client.FindChannelProcessingState(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if state != nil {
		t.Errorf("expected nil state, got %+v", state)
	}
}

func TestFindChannelProcessingState_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.FindChannelProcessingState(context.Background(), "")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestFindChannelProcessingState_GetItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("get item failed")
		},
	}
	client := newTestClient(mock)

	_, err := client.FindChannelProcessingState(context.Background(), "C123456")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== Init Tests ====================

func TestInit_SkipSchemaValidation(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.Init(context.Background(), true)
	if err != nil {
		t.Errorf("expected no error when skipping validation, got %v", err)
	}
}

func TestInit_TableNotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return nil, &dynamodbtypes.ResourceNotFoundException{Message: aws.String("table not found")}
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for missing table, got nil")
	}
}

func TestInit_DescribeTableError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return nil, errors.New("describe table failed")
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestInit_ValidSchema(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{
				Table: &dynamodbtypes.TableDescription{
					TableStatus: dynamodbtypes.TableStatusActive,
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String(PartitionKey), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String(SortKey), KeyType: dynamodbtypes.KeyTypeRange},
					},
					GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndexDescription{
						{
							IndexName:   aws.String(GSIPostID),
							IndexStatus: dynamodbtypes.IndexStatusActive,
							KeySchema: []dynamodbtypes.KeySchemaElement{
								{AttributeName: aws.String(PartitionKey)},
								{AttributeName: aws.String(PostIDAttr)},
							},
							Projection: &dynamodbtypes.Projection{
								ProjectionType:   dynamodbtypes.ProjectionTypeInclude,
								NonKeyAttributes: []string{SortKey},
							},
						},
						{
							IndexName:   aws.String(GSIIsOpen),
							IndexStatus: dynamodbtypes.IndexStatusActive,
							KeySchema: []dynamodbtypes.KeySchemaElement{
								{AttributeName: aws.String(IsOpenAttr)},
								{AttributeName: aws.String(SortKey)},
							},
							Projection: &dynamodbtypes.Projection{
								ProjectionType:   dynamodbtypes.ProjectionTypeInclude,
								NonKeyAttributes: []string{BodyAttr},
							},
						},
					},
				},
			}, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &dynamodbtypes.TimeToLiveDescription{
					TimeToLiveStatus: dynamodbtypes.TimeToLiveStatusEnabled,
					AttributeName:    aws.String(TTLAttr),
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)
	if err != nil {
		t.Errorf("expected no error for valid schema, got %v", err)
	}
}

func TestInit_InvalidPartitionKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{
				Table: &dynamodbtypes.TableDescription{
					TableStatus: dynamodbtypes.TableStatusActive,
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("wrong_pk"), KeyType: dynamodbtypes.KeyTypeHash},
						{AttributeName: aws.String(SortKey), KeyType: dynamodbtypes.KeyTypeRange},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for invalid partition key, got nil")
	}
}

// ==================== DropAllData Tests ====================

func TestDropAllData_Success(t *testing.T) {
	t.Parallel()
	scanCount := 0
	batchDeleteCount := 0
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			scanCount++
			if scanCount == 1 {
				return &dynamodb.ScanOutput{
					Items: []map[string]dynamodbtypes.AttributeValue{
						{
							PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
							SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
						},
					},
				}, nil
			}
			return &dynamodb.ScanOutput{Items: []map[string]dynamodbtypes.AttributeValue{}}, nil
		},
		batchWriteItemFunc: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchDeleteCount++
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if batchDeleteCount != 1 {
		t.Errorf("expected 1 batch delete call, got %d", batchDeleteCount)
	}
}

func TestDropAllData_EmptyTable(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{Items: []map[string]dynamodbtypes.AttributeValue{}}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())
	if err != nil {
		t.Errorf("expected no error for empty table, got %v", err)
	}
}

func TestDropAllData_ScanError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return nil, errors.New("scan failed")
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())

	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestDropAllData_BatchDeleteError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
						SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
					},
				},
			}, nil
		},
		batchWriteItemFunc: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			return nil, errors.New("batch delete failed")
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())

	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ==================== Helper Function Tests ====================

func TestBuildIssueSortKey_Valid(t *testing.T) {
	t.Parallel()
	sk, err := buildIssueSortKey("C123456", "corr-id", "unique-id")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	encodedCorr := base64.URLEncoding.EncodeToString([]byte("corr-id"))
	expected := "ISSUE#C123456#" + encodedCorr + "#unique-id"
	if sk != expected {
		t.Errorf("expected %s, got %s", expected, sk)
	}
}

func TestBuildIssueSortKey_EmptyChannel(t *testing.T) {
	t.Parallel()
	_, err := buildIssueSortKey("", "corr-id", "unique-id")

	if err == nil {
		t.Error("expected error for empty channel, got nil")
	}
}

func TestBuildIssueSortKey_ChannelWithHash(t *testing.T) {
	t.Parallel()
	_, err := buildIssueSortKey("C#123456", "corr-id", "unique-id")

	if err == nil {
		t.Error("expected error for channel with #, got nil")
	}
}

func TestBuildIssueSortKey_NoCorrelationID(t *testing.T) {
	t.Parallel()
	sk, err := buildIssueSortKey("C123456", "", "unique-id")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if sk != "ISSUE#C123456" {
		t.Errorf("expected 'ISSUE#C123456', got %s", sk)
	}
}

func TestBuildIssueSortKey_NoUniqueID(t *testing.T) {
	t.Parallel()
	sk, err := buildIssueSortKey("C123456", "corr-id", "")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	encodedCorr := base64.URLEncoding.EncodeToString([]byte("corr-id"))
	expected := "ISSUE#C123456#" + encodedCorr
	if sk != expected {
		t.Errorf("expected %s, got %s", expected, sk)
	}
}

func TestGetIssueUniqueIDFromSortKey_Valid(t *testing.T) {
	t.Parallel()
	encodedCorr := base64.URLEncoding.EncodeToString([]byte("corr-id"))
	sk := "ISSUE#C123456#" + encodedCorr + "#unique-123"

	id, err := getIssueUniqueIDFromSortKey(sk)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "unique-123" {
		t.Errorf("expected 'unique-123', got %s", id)
	}
}

func TestGetIssueUniqueIDFromSortKey_InvalidFormat(t *testing.T) {
	t.Parallel()
	_, err := getIssueUniqueIDFromSortKey("ISSUE#C123456")

	if err == nil {
		t.Error("expected error for invalid format, got nil")
	}
}

func TestGetIssueUniqueIDFromSortKey_NotIssueSortKey(t *testing.T) {
	t.Parallel()
	_, err := getIssueUniqueIDFromSortKey("ALERT#C123456#corr#unique")

	if err == nil {
		t.Error("expected error for non-issue sort key, got nil")
	}
}

func TestBuildMoveMappingSortKey_Valid(t *testing.T) {
	t.Parallel()
	sk, err := buildMoveMappingSortKey("C123456", "corr-id")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	encodedCorr := base64.URLEncoding.EncodeToString([]byte("corr-id"))
	expected := "MOVEMAPPING#C123456#" + encodedCorr
	if sk != expected {
		t.Errorf("expected %s, got %s", expected, sk)
	}
}

func TestBuildMoveMappingSortKey_EmptyChannel(t *testing.T) {
	t.Parallel()
	_, err := buildMoveMappingSortKey("", "corr-id")

	if err == nil {
		t.Error("expected error for empty channel, got nil")
	}
}

func TestBuildMoveMappingSortKey_ChannelWithHash(t *testing.T) {
	t.Parallel()
	_, err := buildMoveMappingSortKey("C#123456", "corr-id")

	if err == nil {
		t.Error("expected error for channel with #, got nil")
	}
}

func TestBuildProcessingStateSortKey_Valid(t *testing.T) {
	t.Parallel()
	sk, err := buildProcessingStateSortKey("C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if sk != "PROCESSINGSTATE#C123456" {
		t.Errorf("expected 'PROCESSINGSTATE#C123456', got %s", sk)
	}
}

func TestBuildProcessingStateSortKey_EmptyChannel(t *testing.T) {
	t.Parallel()
	_, err := buildProcessingStateSortKey("")

	if err == nil {
		t.Error("expected error for empty channel, got nil")
	}
}

func TestBuildProcessingStateSortKey_ChannelWithHash(t *testing.T) {
	t.Parallel()
	_, err := buildProcessingStateSortKey("C#123456")

	if err == nil {
		t.Error("expected error for channel with #, got nil")
	}
}

func TestGetStringValue_Valid(t *testing.T) {
	t.Parallel()
	attr := &dynamodbtypes.AttributeValueMemberS{Value: "test-value"}

	result := getStringValue(attr)

	if result != "test-value" {
		t.Errorf("expected 'test-value', got %s", result)
	}
}

func TestGetStringValue_WrongType(t *testing.T) {
	t.Parallel()
	attr := &dynamodbtypes.AttributeValueMemberN{Value: "123"}

	result := getStringValue(attr)

	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestGetStringValue_Nil(t *testing.T) {
	t.Parallel()
	result := getStringValue(nil)

	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

// ==================== Options Tests ====================

func TestWithAPI(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	cfg := aws.Config{}
	client := New(&cfg, "test-table", WithAPI(mock))

	if client.opts.dynamoDBAPI != mock {
		t.Error("expected dynamoDBAPI option to be set")
	}
}

func TestWithClock(t *testing.T) {
	t.Parallel()
	fixedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return fixedTime }
	cfg := aws.Config{}
	client := New(&cfg, "test-table", WithClock(clock))

	if client.opts.clock() != fixedTime {
		t.Error("expected clock option to be set")
	}
}

// ==================== Additional Edge Case Tests ====================

func TestSaveIssues_ExactlyBatchLimit(t *testing.T) {
	t.Parallel()
	batchCount := 0
	var lastBatchSize int
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchCount++
			lastBatchSize = len(params.RequestItems["test-table"])
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	// Create exactly 25 issues (the batch limit)
	issues := make([]types.Issue, 25)
	for i := range 25 {
		issues[i] = &mockIssue{
			channelID:     "C123456",
			uniqueID:      "u" + strconv.Itoa(i),
			correlationID: "c" + strconv.Itoa(i),
			isOpen:        true,
		}
	}

	err := client.SaveIssues(context.Background(), issues...)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if batchCount != 1 {
		t.Errorf("expected 1 batch, got %d", batchCount)
	}
	if lastBatchSize != 25 {
		t.Errorf("expected batch size 25, got %d", lastBatchSize)
	}
}

func TestSaveIssues_ContextCancellation(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			callCount++
			// Always return unprocessed items to trigger retry
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
					"test-table": params.RequestItems["test-table"][:1],
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately to test context cancellation during retry wait
	cancel()

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2", isOpen: true},
	}

	err := client.SaveIssues(ctx, issues...)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestDropAllData_Pagination(t *testing.T) {
	t.Parallel()
	scanCount := 0
	batchDeleteCount := 0
	mock := &mockAPI{
		scanFunc: func(_ context.Context, params *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			scanCount++
			if scanCount == 1 {
				return &dynamodb.ScanOutput{
					Items: []map[string]dynamodbtypes.AttributeValue{
						{
							PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
							SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
						},
					},
					LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
						"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
					},
				}, nil
			}
			if scanCount == 2 {
				return &dynamodb.ScanOutput{
					Items: []map[string]dynamodbtypes.AttributeValue{
						{
							PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C2"},
							SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk2"},
						},
					},
				}, nil
			}
			return &dynamodb.ScanOutput{Items: []map[string]dynamodbtypes.AttributeValue{}}, nil
		},
		batchWriteItemFunc: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchDeleteCount++
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if scanCount != 2 {
		t.Errorf("expected 2 scan calls for pagination, got %d", scanCount)
	}
	if batchDeleteCount != 2 {
		t.Errorf("expected 2 batch delete calls, got %d", batchDeleteCount)
	}
}

func TestDropAllData_UnprocessedItems(t *testing.T) {
	t.Parallel()
	batchCallCount := 0
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
						SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
					},
				},
			}, nil
		},
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			batchCallCount++
			if batchCallCount == 1 {
				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
						"test-table": params.RequestItems["test-table"],
					},
				}, nil
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if batchCallCount != 2 {
		t.Errorf("expected 2 batch calls (1 initial + 1 retry), got %d", batchCallCount)
	}
}

func TestDropAllData_UnprocessedItemsExhausted(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
						SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
					},
				},
			}, nil
		},
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			// Always return unprocessed items to exhaust retries
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
					"test-table": params.RequestItems["test-table"],
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(context.Background())
	if err == nil {
		t.Error("expected error for exhausted retries, got nil")
	}
	if !strings.Contains(err.Error(), "unprocessed items") {
		t.Errorf("expected error about unprocessed items, got %v", err)
	}
}

func TestDropAllData_ContextCancellation(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
						SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
					},
				},
				LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
				},
			}, nil
		},
		batchWriteItemFunc: func(_ context.Context, _ *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	client := newTestClient(mock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.DropAllData(ctx)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestLoadOpenIssuesInChannel_EmptyChannelID(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.LoadOpenIssuesInChannel(context.Background(), "")

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
	if err.Error() != "channel ID cannot be empty" {
		t.Errorf("expected 'channel ID cannot be empty', got %s", err.Error())
	}
}

func TestSaveMoveMapping_MarshalError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	mapping := &mockMoveMapping{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		body:          []byte{0xff, 0xfe}, // Invalid JSON to trigger marshal error
	}

	// The mockMoveMapping returns body directly if set, so this should succeed
	// but the body is invalid JSON. We need to make MarshalJSON return an error.
	// For a proper test, we need a mock that returns an error from MarshalJSON.
	err := client.SaveMoveMapping(context.Background(), mapping)
	// Since mockMoveMapping doesn't return an error for this case, this test
	// verifies the path works with valid input
	if err != nil {
		t.Errorf("expected no error with mock mapping, got %v", err)
	}
}

func TestMoveIssue_MarshalError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		transactWriteItemFunc: func(_ context.Context, _ *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C222222",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
		body:          []byte{0xff, 0xfe}, // Invalid but MarshalJSON returns it directly
	}

	// Similar to above, the mock returns body directly
	err := client.MoveIssue(context.Background(), issue, "C111111", "C222222")
	if err != nil {
		t.Errorf("expected no error with mock issue, got %v", err)
	}
}

func TestFindActiveChannels_ContextCancellation(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C111111"}},
				},
				LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.FindActiveChannels(ctx)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestLoadOpenIssuesInChannel_ContextCancellation(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-1"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"id":"1"}`},
					},
				},
				LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
					"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.LoadOpenIssuesInChannel(ctx, "C123456")
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestLoadOpenIssuesInChannel_EmptyBody(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-1"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: ""},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(issues) != 1 {
		t.Errorf("expected 1 issue, got %d", len(issues))
	}
	if issues["unique-1"] != nil {
		t.Errorf("expected nil body for empty body, got %s", string(issues["unique-1"]))
	}
}

func TestFindOpenIssueByCorrelationID_EmptyBody(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-123"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: ""},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", correlationID)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "unique-123" {
		t.Errorf("expected id 'unique-123', got %s", id)
	}
	if body != nil {
		t.Errorf("expected nil body for empty body, got %s", string(body))
	}
}

func TestFindIssueBySlackPostID_EmptyBody(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	issueSK := "ISSUE#C123456#" + encodedCorrelation + "#unique-123"
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey: &dynamodbtypes.AttributeValueMemberS{Value: issueSK},
					},
				},
			}, nil
		},
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: issueSK},
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: ""},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "unique-123" {
		t.Errorf("expected id 'unique-123', got %s", id)
	}
	if body != nil {
		t.Errorf("expected nil body for empty body, got %s", string(body))
	}
}

func TestFindMoveMapping_EmptyBody(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: ""},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	body, err := client.FindMoveMapping(context.Background(), "C123456", "corr-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if body != nil {
		t.Errorf("expected nil body for empty body, got %s", string(body))
	}
}

// marshalErrIssue is an Issue implementation whose MarshalJSON always returns an error.
type marshalErrIssue struct {
	channelID     string
	uniqueID      string
	correlationID string
	isOpen        bool
}

func (m *marshalErrIssue) ChannelID() string        { return m.channelID }
func (m *marshalErrIssue) UniqueID() string         { return m.uniqueID }
func (m *marshalErrIssue) GetCorrelationID() string { return m.correlationID }
func (m *marshalErrIssue) IsOpen() bool             { return m.isOpen }
func (m *marshalErrIssue) CurrentPostID() string    { return "" }
func (m *marshalErrIssue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshal error")
}

// marshalErrMoveMapping is a MoveMapping implementation whose MarshalJSON always returns an error.
type marshalErrMoveMapping struct {
	channelID     string
	correlationID string
}

func (m *marshalErrMoveMapping) ChannelID() string        { return m.channelID }
func (m *marshalErrMoveMapping) UniqueID() string         { return "" }
func (m *marshalErrMoveMapping) GetCorrelationID() string { return m.correlationID }
func (m *marshalErrMoveMapping) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshal error")
}

// validTableOutput returns a DescribeTableOutput with a fully valid schema for Init tests.
func validTableOutput() *dynamodb.DescribeTableOutput {
	return &dynamodb.DescribeTableOutput{
		Table: &dynamodbtypes.TableDescription{
			TableStatus: dynamodbtypes.TableStatusActive,
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String(PartitionKey), KeyType: dynamodbtypes.KeyTypeHash},
				{AttributeName: aws.String(SortKey), KeyType: dynamodbtypes.KeyTypeRange},
			},
			GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndexDescription{
				{
					IndexName:   aws.String(GSIPostID),
					IndexStatus: dynamodbtypes.IndexStatusActive,
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String(PartitionKey)},
						{AttributeName: aws.String(PostIDAttr)},
					},
					Projection: &dynamodbtypes.Projection{
						ProjectionType:   dynamodbtypes.ProjectionTypeInclude,
						NonKeyAttributes: []string{SortKey},
					},
				},
				{
					IndexName:   aws.String(GSIIsOpen),
					IndexStatus: dynamodbtypes.IndexStatusActive,
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String(IsOpenAttr)},
						{AttributeName: aws.String(SortKey)},
					},
					Projection: &dynamodbtypes.Projection{
						ProjectionType:   dynamodbtypes.ProjectionTypeInclude,
						NonKeyAttributes: []string{BodyAttr},
					},
				},
			},
		},
	}
}

// validTTLOutput returns a DescribeTimeToLiveOutput with TTL enabled on the correct attribute.
func validTTLOutput() *dynamodb.DescribeTimeToLiveOutput {
	return &dynamodb.DescribeTimeToLiveOutput{
		TimeToLiveDescription: &dynamodbtypes.TimeToLiveDescription{
			TimeToLiveStatus: dynamodbtypes.TimeToLiveStatusEnabled,
			AttributeName:    aws.String(TTLAttr),
		},
	}
}

// ==================== Additional Options Tests ====================

func TestWithIssuesTimeToLive(t *testing.T) {
	t.Parallel()
	cfg := aws.Config{}
	d := 7 * 24 * time.Hour
	client := New(&cfg, "test-table", WithIssuesTimeToLive(d))

	if client.opts.issuesTimeToLive != d {
		t.Errorf("expected issues TTL %v, got %v", d, client.opts.issuesTimeToLive)
	}
}

func TestValidate_InvalidIssuesTTL(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	cfg := aws.Config{}
	client := New(&cfg, "test-table",
		WithAPI(mock),
		WithIssuesTimeToLive(-1),
	)

	err := client.Connect()

	if err == nil {
		t.Error("expected error for invalid issues TTL, got nil")
	}
}

// ==================== Connect Tests (additional) ====================

func TestConnect_WithoutInjectedAPI(t *testing.T) {
	t.Parallel()
	cfg := aws.Config{Region: "us-east-1"}
	client := New(&cfg, "test-table") // no WithAPI — uses real AWS SDK client

	err := client.Connect()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if client.client == nil {
		t.Error("expected client to be initialized")
	}
}

// ==================== Init Tests (additional) ====================

func TestInit_EmptyKeySchema(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.KeySchema = nil
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for empty key schema, got nil")
	}
}

func TestInit_SimpleKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.KeySchema = []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String(PartitionKey), KeyType: dynamodbtypes.KeyTypeHash},
			}
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for simple primary key, got nil")
	}
}

func TestInit_InvalidSortKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.KeySchema[1].AttributeName = aws.String("wrong_sk")
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for invalid sort key, got nil")
	}
}

func TestInit_TableNotActive(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.TableStatus = dynamodbtypes.TableStatusCreating
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for inactive table, got nil")
	}
}

func TestInit_DescribeTimeToLiveError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return validTableOutput(), nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return nil, errors.New("ttl describe failed")
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error from DescribeTimeToLive, got nil")
	}
}

func TestInit_TTLDescriptionNil(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return validTableOutput(), nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{TimeToLiveDescription: nil}, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for nil TTL description, got nil")
	}
}

func TestInit_TTLNotEnabled(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return validTableOutput(), nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			out := validTTLOutput()
			out.TimeToLiveDescription.TimeToLiveStatus = dynamodbtypes.TimeToLiveStatusDisabled
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for disabled TTL, got nil")
	}
}

func TestInit_WrongTTLAttribute(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return validTableOutput(), nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			out := validTTLOutput()
			out.TimeToLiveDescription.AttributeName = aws.String("wrong_ttl")
			return out, nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for wrong TTL attribute, got nil")
	}
}

func TestInit_GSINotFound(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes = nil
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for missing GSI, got nil")
	}
}

func TestInit_GSIWrongPartitionKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].KeySchema[0].AttributeName = aws.String("wrong_pk")
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for GSI with wrong partition key, got nil")
	}
}

func TestInit_GSISimpleKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].KeySchema = []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String(PartitionKey)},
			}
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for GSI with simple key when composite expected, got nil")
	}
}

func TestInit_GSIWrongSortKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].KeySchema[1].AttributeName = aws.String("wrong_sk")
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for GSI with wrong sort key, got nil")
	}
}

func TestInit_GSINotActive(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].IndexStatus = dynamodbtypes.IndexStatusCreating
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for inactive GSI, got nil")
	}
}

func TestInit_GSIWrongProjectionType(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].Projection.ProjectionType = dynamodbtypes.ProjectionTypeAll
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for GSI with wrong projection type, got nil")
	}
}

func TestInit_GSIMissingNonKeyAttribute(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			out.Table.GlobalSecondaryIndexes[0].Projection.NonKeyAttributes = nil
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error for GSI missing non-key attribute, got nil")
	}
}

func TestInit_GSIIsOpenFails(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		describeTableFunc: func(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			out := validTableOutput()
			// Remove only GSIIsOpen; GSIPostID passes, GSIIsOpen fails.
			out.Table.GlobalSecondaryIndexes = out.Table.GlobalSecondaryIndexes[:1]
			return out, nil
		},
		describeTimeToLiveFunc: func(_ context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return validTTLOutput(), nil
		},
	}
	client := newTestClient(mock)

	err := client.Init(context.Background(), false)

	if err == nil {
		t.Error("expected error when GSIIsOpen is missing, got nil")
	}
}

// ==================== DropAllData Tests (additional) ====================

func TestDropAllData_ContextCancellationDuringRetry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())

	mock := &mockAPI{
		scanFunc: func(_ context.Context, _ *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
			return &dynamodb.ScanOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: "C1"},
						SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: "sk1"},
					},
				},
			}, nil
		},
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			cancel() // cancel after first batch call so the retry select hits ctx.Done()
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
					"test-table": params.RequestItems["test-table"],
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	err := client.DropAllData(ctx)

	if err == nil {
		t.Error("expected error for cancelled context during retry, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// ==================== SaveIssue Tests (additional) ====================

func TestSaveIssue_MarshalError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &marshalErrIssue{
		channelID:     "C123456",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
	}

	err := client.SaveIssue(context.Background(), issue)

	if err == nil {
		t.Error("expected error for marshal failure, got nil")
	}
}

func TestSaveIssue_InvalidChannelInSortKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C#123456", // '#' causes buildIssueSortKey to fail
		uniqueID:      "unique-123",
		correlationID: "corr-123",
	}

	err := client.SaveIssue(context.Background(), issue)

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

// ==================== SaveIssues Tests (additional) ====================

func TestSaveIssues_EmptyChannelInMultiple(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "", uniqueID: "u2", correlationID: "c2", isOpen: true},
	}

	err := client.SaveIssues(context.Background(), issues...)

	if err == nil {
		t.Error("expected error for empty channel ID, got nil")
	}
}

func TestSaveIssues_CreateIssueItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&marshalErrIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2"},
	}

	err := client.SaveIssues(context.Background(), issues...)

	if err == nil {
		t.Error("expected error from createIssueItem, got nil")
	}
}

func TestSaveIssues_MaxRetriesExhausted(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		batchWriteItemFunc: func(_ context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]dynamodbtypes.WriteRequest{
					"test-table": params.RequestItems["test-table"],
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	issues := []types.Issue{
		&mockIssue{channelID: "C1", uniqueID: "u1", correlationID: "c1", isOpen: true},
		&mockIssue{channelID: "C2", uniqueID: "u2", correlationID: "c2", isOpen: true},
	}

	err := client.SaveIssues(context.Background(), issues...)

	if err == nil {
		t.Error("expected error after max retries exhausted, got nil")
	}
	if !strings.Contains(err.Error(), "unprocessed items") {
		t.Errorf("expected error about unprocessed items, got %v", err)
	}
}

// ==================== MoveIssue Tests (additional) ====================

func TestMoveIssue_SourceChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &mockIssue{
		channelID:     "C222222",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
		isOpen:        true,
	}

	err := client.MoveIssue(context.Background(), issue, "C#111111", "C222222")

	if err == nil {
		t.Error("expected error for source channel ID containing '#', got nil")
	}
}

func TestMoveIssue_CreateIssueItemError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	issue := &marshalErrIssue{
		channelID:     "C222222",
		uniqueID:      "unique-123",
		correlationID: "corr-123",
	}

	err := client.MoveIssue(context.Background(), issue, "C111111", "C222222")

	if err == nil {
		t.Error("expected error from createIssueItem, got nil")
	}
}

// ==================== FindOpenIssueByCorrelationID Tests (additional) ====================

func TestFindOpenIssueByCorrelationID_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C#123456", "corr-123")

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

func TestFindOpenIssueByCorrelationID_InvalidSortKeyFromQuery(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "INVALID_SORT_KEY"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123456", "corr-123")

	if err == nil {
		t.Error("expected error for invalid sort key format, got nil")
	}
}

// ==================== FindIssueBySlackPostID Tests (additional) ====================

func TestFindIssueBySlackPostID_QueryError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, errors.New("query failed")
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")

	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestFindIssueBySlackPostID_GetItemError(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	issueSK := "ISSUE#C123456#" + encodedCorrelation + "#unique-123"
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{SortKey: &dynamodbtypes.AttributeValueMemberS{Value: issueSK}},
				},
			}, nil
		},
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return nil, errors.New("get item failed")
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")

	if err == nil {
		t.Error("expected error from GetItem, got nil")
	}
}

func TestFindIssueBySlackPostID_GetItemNotFound(t *testing.T) {
	t.Parallel()
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	issueSK := "ISSUE#C123456#" + encodedCorrelation + "#unique-123"
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{SortKey: &dynamodbtypes.AttributeValueMemberS{Value: issueSK}},
				},
			}, nil
		},
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: nil}, nil
		},
	}
	client := newTestClient(mock)

	id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if id != "" || body != nil {
		t.Error("expected empty result for missing item")
	}
}

func TestFindIssueBySlackPostID_InvalidSortKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{SortKey: &dynamodbtypes.AttributeValueMemberS{Value: "INVALID_SORT_KEY"}},
				},
			}, nil
		},
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "INVALID_SORT_KEY"},
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{}`},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123456", "post-123")

	if err == nil {
		t.Error("expected error for invalid sort key, got nil")
	}
}

// ==================== LoadOpenIssuesInChannel Tests (additional) ====================

func TestLoadOpenIssuesInChannel_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.LoadOpenIssuesInChannel(context.Background(), "C#123456")

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

func TestLoadOpenIssuesInChannel_Pagination(t *testing.T) {
	t.Parallel()
	callCount := 0
	correlationID := "test-correlation"
	encodedCorrelation := base64.URLEncoding.EncodeToString([]byte(correlationID))
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			callCount++
			if callCount == 1 {
				return &dynamodb.QueryOutput{
					Items: []map[string]dynamodbtypes.AttributeValue{
						{
							SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-1"},
							BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"id":"1"}`},
						},
					},
					LastEvaluatedKey: map[string]dynamodbtypes.AttributeValue{
						"pk": &dynamodbtypes.AttributeValueMemberS{Value: "continue"},
					},
				}, nil
			}
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "ISSUE#C123456#" + encodedCorrelation + "#unique-2"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{"id":"2"}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 query calls for pagination, got %d", callCount)
	}
	if len(issues) != 2 {
		t.Errorf("expected 2 issues, got %d", len(issues))
	}
}

func TestLoadOpenIssuesInChannel_InvalidSortKey(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		queryFunc: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{
				Items: []map[string]dynamodbtypes.AttributeValue{
					{
						SortKey:  &dynamodbtypes.AttributeValueMemberS{Value: "INVALID_SORT_KEY"},
						BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: `{}`},
					},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, err := client.LoadOpenIssuesInChannel(context.Background(), "C123456")

	if err == nil {
		t.Error("expected error for invalid sort key, got nil")
	}
}

// ==================== SaveMoveMapping Tests (additional) ====================

func TestSaveMoveMapping_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	mapping := &mockMoveMapping{
		channelID:     "C#123456",
		correlationID: "corr-123",
	}

	err := client.SaveMoveMapping(context.Background(), mapping)

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

func TestSaveMoveMapping_MarshalJSONError(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	mapping := &marshalErrMoveMapping{
		channelID:     "C123456",
		correlationID: "corr-123",
	}

	err := client.SaveMoveMapping(context.Background(), mapping)

	if err == nil {
		t.Error("expected error from MarshalJSON, got nil")
	}
}

// ==================== FindMoveMapping Tests (additional) ====================

func TestFindMoveMapping_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.FindMoveMapping(context.Background(), "C#123456", "corr-123")

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

// ==================== DeleteMoveMapping Tests (additional) ====================

func TestDeleteMoveMapping_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	err := client.DeleteMoveMapping(context.Background(), "C#123456", "corr-123")

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

// ==================== SaveChannelProcessingState Tests (additional) ====================

func TestSaveChannelProcessingState_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	state := &types.ChannelProcessingState{ChannelID: "C#123456"}

	err := client.SaveChannelProcessingState(context.Background(), state)

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

// ==================== FindChannelProcessingState Tests (additional) ====================

func TestFindChannelProcessingState_ChannelWithHash(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{}
	client := newTestClient(mock)

	_, err := client.FindChannelProcessingState(context.Background(), "C#123456")

	if err == nil {
		t.Error("expected error for channel ID containing '#', got nil")
	}
}

func TestFindChannelProcessingState_InvalidJSON(t *testing.T) {
	t.Parallel()
	mock := &mockAPI{
		getItemFunc: func(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{
				Item: map[string]dynamodbtypes.AttributeValue{
					BodyAttr: &dynamodbtypes.AttributeValueMemberS{Value: "not valid json {{{"},
				},
			}, nil
		},
	}
	client := newTestClient(mock)

	_, err := client.FindChannelProcessingState(context.Background(), "C123456")

	if err == nil {
		t.Error("expected error for invalid JSON body, got nil")
	}
}
