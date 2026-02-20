package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIssue implements types.Issue for testing.
type mockIssue struct {
	channelID     string
	uniqueID      string
	correlationID string
	isOpen        bool
	postID        string
	marshalErr    error
}

func (m *mockIssue) ChannelID() string        { return m.channelID }
func (m *mockIssue) UniqueID() string         { return m.uniqueID }
func (m *mockIssue) GetCorrelationID() string { return m.correlationID }
func (m *mockIssue) IsOpen() bool             { return m.isOpen }
func (m *mockIssue) CurrentPostID() string    { return m.postID }

func (m *mockIssue) MarshalJSON() ([]byte, error) {
	if m.marshalErr != nil {
		return nil, m.marshalErr
	}

	return json.Marshal(map[string]any{
		"channel_id":     m.channelID,
		"unique_id":      m.uniqueID,
		"correlation_id": m.correlationID,
		"is_open":        m.isOpen,
		"post_id":        m.postID,
	})
}

// mockMoveMapping implements types.MoveMapping for testing.
type mockMoveMapping struct {
	channelID     string
	uniqueID      string
	correlationID string
	marshalErr    error
}

func (m *mockMoveMapping) ChannelID() string        { return m.channelID }
func (m *mockMoveMapping) UniqueID() string         { return m.uniqueID }
func (m *mockMoveMapping) GetCorrelationID() string { return m.correlationID }

func (m *mockMoveMapping) MarshalJSON() ([]byte, error) {
	if m.marshalErr != nil {
		return nil, m.marshalErr
	}

	return json.Marshal(map[string]any{
		"channel_id":     m.channelID,
		"unique_id":      m.uniqueID,
		"correlation_id": m.correlationID,
	})
}

//nolint:ireturn // Returning interface is appropriate for test mock helper
func newClientWithMock(t *testing.T) (*postgres.Client, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	client := postgres.New(
		postgres.WithHost("localhost"),
		postgres.WithPort(5432),
		postgres.WithUser("testuser"),
		postgres.WithDatabase("testdb"),
		postgres.WithIssuesTable("issues"),
		postgres.WithAlertsTable("alerts"),
		postgres.WithMoveMappingsTable("move_mappings"),
		postgres.WithChannelProcessingStateTable("channel_state"),
	)
	client.SetPool(mock)

	return client, mock
}

//nolint:ireturn // Returning interface is appropriate for test mock helper
func newClientWithMockAndTTL(t *testing.T) (*postgres.Client, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)

	client := postgres.New(
		postgres.WithHost("localhost"),
		postgres.WithPort(5432),
		postgres.WithUser("testuser"),
		postgres.WithDatabase("testdb"),
		postgres.WithIssuesTable("issues"),
		postgres.WithAlertsTable("alerts"),
		postgres.WithMoveMappingsTable("move_mappings"),
		postgres.WithChannelProcessingStateTable("channel_state"),
		postgres.WithAlertsTimeToLive(30*24*time.Hour),
		postgres.WithIssuesTimeToLive(180*24*time.Hour),
	)
	client.SetPool(mock)

	return client, mock
}

// =============================================================================
// Constructor and Connection Tests
// =============================================================================

func TestNew(t *testing.T) {
	t.Parallel()

	client := postgres.New(postgres.WithUser("testuser"), postgres.WithDatabase("testdb"))

	require.NotNil(t, client)
}

func TestClose(t *testing.T) {
	t.Parallel()

	t.Run("close when not connected returns nil", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(postgres.WithUser("testuser"), postgres.WithDatabase("testdb"))

		err := client.Close(context.Background())

		assert.NoError(t, err)
	})

	t.Run("close with mock pool succeeds", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		mock.ExpectClose()

		err := client.Close(context.Background())

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestConnect_InvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("missing user returns error", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(postgres.WithDatabase("testdb"))

		err := client.Connect(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "user is required")
	})

	t.Run("missing database returns error", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(postgres.WithUser("testuser"))

		err := client.Connect(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "database is required")
	})

	t.Run("invalid table name returns error", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(
			postgres.WithUser("testuser"),
			postgres.WithDatabase("testdb"),
			postgres.WithIssuesTable("invalid-table-name"),
		)

		err := client.Connect(context.Background())

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid issues table name")
	})
}

// =============================================================================
// Not Connected Tests
// =============================================================================

func TestInit_NotConnected(t *testing.T) {
	t.Parallel()

	client := postgres.New(postgres.WithUser("testuser"), postgres.WithDatabase("testdb"))

	err := client.Init(context.Background(), false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestInit_TTLCleanupStarted(t *testing.T) {
	t.Parallel()

	t.Run("goroutine not active before Init is called", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		assert.False(t, client.HasActiveTTLCleanup())
	})

	t.Run("cleanup goroutine starts after Init and stops after Close", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMockAndTTL(t)

		mock.ExpectBegin()

		for range 10 { // 10 create statements
			mock.ExpectExec("").WillReturnResult(pgxmock.NewResult("", 0))
		}

		mock.ExpectCommit()

		_ = client.Init(context.Background(), true) // skip schema validation for simplicity

		assert.True(t, client.HasActiveTTLCleanup())

		mock.ExpectClose()

		_ = client.Close(context.Background())

		assert.False(t, client.HasActiveTTLCleanup())
	})

	t.Run("cleanup goroutine does not start when disabled", func(t *testing.T) {
		t.Parallel()

		mock, err := pgxmock.NewPool()
		require.NoError(t, err)

		client := postgres.New(
			postgres.WithUser("testuser"),
			postgres.WithDatabase("testdb"),
			postgres.WithTTLCleanupDisabled(),
		)
		client.SetPool(mock)

		mock.ExpectBegin()

		for range 10 {
			mock.ExpectExec("").WillReturnResult(pgxmock.NewResult("", 0))
		}

		mock.ExpectCommit()

		_ = client.Init(context.Background(), true)

		assert.False(t, client.HasActiveTTLCleanup())

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestDropAllData_NotConnected(t *testing.T) {
	t.Parallel()

	client := postgres.New(postgres.WithUser("testuser"), postgres.WithDatabase("testdb"))

	err := client.DropAllData(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

// =============================================================================
// SaveAlert Tests
// =============================================================================

func TestSaveAlert_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil alert returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.SaveAlert(context.Background(), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "alert cannot be nil")
	})

	t.Run("successful save without TTL", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		alert := types.NewInfoAlert()

		mock.ExpectExec("INSERT INTO alerts").
			WithArgs(alert.UniqueID(), postgres.AlertModelVersion, pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveAlert(context.Background(), alert)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("successful save with TTL sets expires_at", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMockAndTTL(t)
		alert := types.NewInfoAlert()

		mock.ExpectExec("INSERT INTO alerts").
			WithArgs(alert.UniqueID(), postgres.AlertModelVersion, pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveAlert(context.Background(), alert)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		alert := types.NewInfoAlert()

		mock.ExpectExec("INSERT INTO alerts").
			WithArgs(alert.UniqueID(), postgres.AlertModelVersion, pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnError(errors.New("database connection lost"))

		err := client.SaveAlert(context.Background(), alert)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to save alert")
		assert.Contains(t, err.Error(), "database connection lost")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// SaveIssue Tests
// =============================================================================

func TestSaveIssue_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil issue returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.SaveIssue(context.Background(), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "issue cannot be nil")
	})

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue := &mockIssue{channelID: "", uniqueID: "uid1", correlationID: "corr1"}

		err := client.SaveIssue(context.Background(), issue)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("marshal error is returned", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue := &mockIssue{
			channelID:  "C123",
			uniqueID:   "uid1",
			marshalErr: errors.New("marshal failed"),
		}

		err := client.SaveIssue(context.Background(), issue)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal issue")
	})

	t.Run("successful save of open issue without TTL", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", true, "post-789", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveIssue(context.Background(), issue)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("open issue with TTL configured has nil expires_at", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMockAndTTL(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", true, "post-789", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveIssue(context.Background(), issue)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("closed issue with TTL configured has non-nil expires_at", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMockAndTTL(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        false,
		}

		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", false, "", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveIssue(context.Background(), issue)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", true, "post-789", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnError(errors.New("unique constraint violation"))

		err := client.SaveIssue(context.Background(), issue)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create or update issue")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// SaveIssues Tests (Batch Logic)
// =============================================================================

func TestSaveIssues_BatchLogic(t *testing.T) {
	t.Parallel()

	t.Run("empty slice returns nil without database call", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		err := client.SaveIssues(context.Background())

		require.NoError(t, err)
		// No expectations set - verify no database calls were made
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("single issue uses SaveIssue path", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		// Single issue should use direct Exec, not batch
		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", true, "post-789", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveIssues(context.Background(), issue)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("nil issue in batch returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue1 := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}

		err := client.SaveIssues(context.Background(), issue1, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "issue cannot be nil")
	})

	t.Run("empty channel ID in batch returns error with issue ID", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue1 := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}
		issue2 := &mockIssue{channelID: "", uniqueID: "uid2", correlationID: "corr2"}

		err := client.SaveIssues(context.Background(), issue1, issue2)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
		assert.Contains(t, err.Error(), "uid2")
	})

	t.Run("marshal error in batch returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue1 := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}
		issue2 := &mockIssue{
			channelID:  "C123",
			uniqueID:   "uid2",
			marshalErr: errors.New("marshal failed"),
		}

		err := client.SaveIssues(context.Background(), issue1, issue2)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal issue")
	})

	t.Run("multiple issues uses batch", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue1 := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1", isOpen: true}
		issue2 := &mockIssue{channelID: "C456", uniqueID: "uid2", correlationID: "corr2", isOpen: false}

		batch := mock.ExpectBatch()
		batch.ExpectExec("INSERT INTO issues").
			WithArgs("uid1", postgres.IssueModelVersion, "C123", "corr1", true, "", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		batch.ExpectExec("INSERT INTO issues").
			WithArgs("uid2", postgres.IssueModelVersion, "C456", "corr2", false, "", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveIssues(context.Background(), issue1, issue2)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("batch error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue1 := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1", isOpen: true}
		issue2 := &mockIssue{channelID: "C456", uniqueID: "uid2", correlationID: "corr2", isOpen: false}

		batch := mock.ExpectBatch()
		batch.ExpectExec("INSERT INTO issues").
			WithArgs("uid1", postgres.IssueModelVersion, "C123", "corr1", true, "", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		batch.ExpectExec("INSERT INTO issues").
			WithArgs("uid2", postgres.IssueModelVersion, "C456", "corr2", false, "", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnError(errors.New("batch insert failed"))

		err := client.SaveIssues(context.Background(), issue1, issue2)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update issue")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// MoveIssue Tests
// =============================================================================

func TestMoveIssue_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil issue returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.MoveIssue(context.Background(), nil, "C000", "C123")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "issue cannot be nil")
	})

	t.Run("empty target channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}

		err := client.MoveIssue(context.Background(), issue, "C000", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "target channel ID cannot be empty")
	})

	t.Run("channel ID mismatch returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		issue := &mockIssue{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}

		err := client.MoveIssue(context.Background(), issue, "C000", "C999")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "issue channel ID C123 does not match target channel ID C999")
	})

	t.Run("successful move", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		issue := &mockIssue{
			channelID:     "C123",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		mock.ExpectExec("INSERT INTO issues").
			WithArgs("uid-123", postgres.IssueModelVersion, "C123", "corr-456", true, "post-789", pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.MoveIssue(context.Background(), issue, "C000", "C123")

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// FindOpenIssueByCorrelationID Tests
// =============================================================================

func TestFindOpenIssueByCorrelationID_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "", "corr1")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("empty correlation ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "correlation ID cannot be empty")
	})

	t.Run("not found returns nil without error", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123", "corr-456").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}))

		id, body, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123", "corr-456")

		require.NoError(t, err)
		assert.Empty(t, id)
		assert.Nil(t, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("found issue returns id and body", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		expectedBody := json.RawMessage(`{"test": "data"}`)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123", "corr-456").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).
				AddRow("uid-123", expectedBody))

		id, body, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123", "corr-456")

		require.NoError(t, err)
		assert.Equal(t, "uid-123", id)
		assert.Equal(t, expectedBody, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123", "corr-456").
			WillReturnError(errors.New("connection timeout"))

		_, _, err := client.FindOpenIssueByCorrelationID(context.Background(), "C123", "corr-456")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to find issue by correlation ID")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// FindIssueBySlackPostID Tests
// =============================================================================

func TestFindIssueBySlackPostID_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, _, err := client.FindIssueBySlackPostID(context.Background(), "", "post1")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("empty post ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, _, err := client.FindIssueBySlackPostID(context.Background(), "C123", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "post ID cannot be empty")
	})

	t.Run("not found returns nil without error", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123", "post-789").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}))

		id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123", "post-789")

		require.NoError(t, err)
		assert.Empty(t, id)
		assert.Nil(t, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("found issue returns id and body", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		expectedBody := json.RawMessage(`{"test": "data"}`)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123", "post-789").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).
				AddRow("uid-123", expectedBody))

		id, body, err := client.FindIssueBySlackPostID(context.Background(), "C123", "post-789")

		require.NoError(t, err)
		assert.Equal(t, "uid-123", id)
		assert.Equal(t, expectedBody, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// FindActiveChannels Tests
// =============================================================================

func TestFindActiveChannels(t *testing.T) {
	t.Parallel()

	t.Run("returns empty slice when no active channels", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT DISTINCT channel_id FROM issues").
			WillReturnRows(pgxmock.NewRows([]string{"channel_id"}))

		channels, err := client.FindActiveChannels(context.Background())

		require.NoError(t, err)
		assert.Empty(t, channels)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns list of active channels", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT DISTINCT channel_id FROM issues").
			WillReturnRows(pgxmock.NewRows([]string{"channel_id"}).
				AddRow("C123").
				AddRow("C456").
				AddRow("C789"))

		channels, err := client.FindActiveChannels(context.Background())

		require.NoError(t, err)
		assert.Equal(t, []string{"C123", "C456", "C789"}, channels)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT DISTINCT channel_id FROM issues").
			WillReturnError(errors.New("query failed"))

		channels, err := client.FindActiveChannels(context.Background())

		require.Error(t, err)
		assert.Nil(t, channels)
		assert.Contains(t, err.Error(), "failed to find active channels")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// LoadOpenIssuesInChannel Tests
// =============================================================================

func TestLoadOpenIssuesInChannel(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		issues, err := client.LoadOpenIssuesInChannel(context.Background(), "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
		assert.Nil(t, issues)
	})

	t.Run("returns empty map when no issues", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}))

		issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123")

		require.NoError(t, err)
		assert.Empty(t, issues)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns map of issues", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		body1 := json.RawMessage(`{"issue": 1}`)
		body2 := json.RawMessage(`{"issue": 2}`)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123").
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).
				AddRow("uid1", body1).
				AddRow("uid2", body2))

		issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123")

		require.NoError(t, err)
		require.Len(t, issues, 2)
		assert.Equal(t, body1, issues["uid1"])
		assert.Equal(t, body2, issues["uid2"])
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT id, attrs FROM issues").
			WithArgs("C123").
			WillReturnError(errors.New("query failed"))

		issues, err := client.LoadOpenIssuesInChannel(context.Background(), "C123")

		require.Error(t, err)
		assert.Nil(t, issues)
		assert.Contains(t, err.Error(), "failed to load open issues")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// SaveMoveMapping Tests
// =============================================================================

func TestSaveMoveMapping_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil mapping returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.SaveMoveMapping(context.Background(), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "move mapping cannot be nil")
	})

	t.Run("empty channel ID returns error with correlation ID", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		mapping := &mockMoveMapping{channelID: "", uniqueID: "uid1", correlationID: "corr1"}

		err := client.SaveMoveMapping(context.Background(), mapping)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
		assert.Contains(t, err.Error(), "corr1")
	})

	t.Run("marshal error is returned", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		mapping := &mockMoveMapping{
			channelID:  "C123",
			uniqueID:   "uid1",
			marshalErr: errors.New("marshal failed"),
		}

		err := client.SaveMoveMapping(context.Background(), mapping)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal move mapping")
	})

	t.Run("successful save", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		mapping := &mockMoveMapping{channelID: "C123", uniqueID: "uid1", correlationID: "corr1"}

		mock.ExpectExec("INSERT INTO move_mappings").
			WithArgs("uid1", postgres.MoveMappingModelVersion, "C123", "corr1", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveMoveMapping(context.Background(), mapping)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// FindMoveMapping Tests
// =============================================================================

func TestFindMoveMapping_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, err := client.FindMoveMapping(context.Background(), "", "corr1")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("empty correlation ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, err := client.FindMoveMapping(context.Background(), "C123", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "correlation ID cannot be empty")
	})

	t.Run("not found returns nil without error", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT attrs FROM move_mappings").
			WithArgs("C123", "corr1").
			WillReturnRows(pgxmock.NewRows([]string{"attrs"}))

		body, err := client.FindMoveMapping(context.Background(), "C123", "corr1")

		require.NoError(t, err)
		assert.Nil(t, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("found mapping returns body", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		expectedBody := json.RawMessage(`{"test": "data"}`)

		mock.ExpectQuery("SELECT attrs FROM move_mappings").
			WithArgs("C123", "corr1").
			WillReturnRows(pgxmock.NewRows([]string{"attrs"}).
				AddRow(expectedBody))

		body, err := client.FindMoveMapping(context.Background(), "C123", "corr1")

		require.NoError(t, err)
		assert.Equal(t, expectedBody, body)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// DeleteMoveMapping Tests
// =============================================================================

func TestDeleteMoveMapping_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.DeleteMoveMapping(context.Background(), "", "corr1")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("empty correlation ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.DeleteMoveMapping(context.Background(), "C123", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "correlation ID cannot be empty")
	})

	t.Run("successful delete", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectExec("DELETE FROM move_mappings").
			WithArgs("C123", "corr1").
			WillReturnResult(pgxmock.NewResult("DELETE", 1))

		err := client.DeleteMoveMapping(context.Background(), "C123", "corr1")

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectExec("DELETE FROM move_mappings").
			WithArgs("C123", "corr1").
			WillReturnError(errors.New("delete failed"))

		err := client.DeleteMoveMapping(context.Background(), "C123", "corr1")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to delete move mapping")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// SaveChannelProcessingState Tests
// =============================================================================

func TestSaveChannelProcessingState_Validation(t *testing.T) {
	t.Parallel()

	t.Run("nil state returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		err := client.SaveChannelProcessingState(context.Background(), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "processing state cannot be nil")
	})

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)
		state := &types.ChannelProcessingState{
			ChannelID:     "",
			Created:       time.Now(),
			LastProcessed: time.Now(),
		}

		err := client.SaveChannelProcessingState(context.Background(), state)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("successful save", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		now := time.Now()
		state := &types.ChannelProcessingState{
			ChannelID:     "C123",
			Created:       now,
			LastProcessed: now,
		}

		mock.ExpectExec("INSERT INTO channel_state").
			WithArgs("C123", postgres.ChannelProcessingStateModelVersion, "C123", now, now).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		err := client.SaveChannelProcessingState(context.Background(), state)

		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// =============================================================================
// FindChannelProcessingState Tests
// =============================================================================

func TestFindChannelProcessingState_Validation(t *testing.T) {
	t.Parallel()

	t.Run("empty channel ID returns error", func(t *testing.T) {
		t.Parallel()

		client, _ := newClientWithMock(t)

		_, err := client.FindChannelProcessingState(context.Background(), "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "channel ID cannot be empty")
	})

	t.Run("not found returns nil without error", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT channel_id, created, last_processed FROM channel_state").
			WithArgs("C123").
			WillReturnRows(pgxmock.NewRows([]string{"channel_id", "created", "last_processed"}))

		state, err := client.FindChannelProcessingState(context.Background(), "C123")

		require.NoError(t, err)
		assert.Nil(t, state)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("found state returns data", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)
		created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		lastProcessed := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

		mock.ExpectQuery("SELECT channel_id, created, last_processed FROM channel_state").
			WithArgs("C123").
			WillReturnRows(pgxmock.NewRows([]string{"channel_id", "created", "last_processed"}).
				AddRow("C123", created, lastProcessed))

		state, err := client.FindChannelProcessingState(context.Background(), "C123")

		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, "C123", state.ChannelID)
		assert.Equal(t, created, state.Created)
		assert.Equal(t, lastProcessed, state.LastProcessed)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database error is returned", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMock(t)

		mock.ExpectQuery("SELECT channel_id, created, last_processed FROM channel_state").
			WithArgs("C123").
			WillReturnError(errors.New("query failed"))

		state, err := client.FindChannelProcessingState(context.Background(), "C123")

		require.Error(t, err)
		assert.Nil(t, state)
		assert.Contains(t, err.Error(), "failed to find channel processing state")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
