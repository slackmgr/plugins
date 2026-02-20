package postgres

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIssueInternal implements types.Issue for internal testing.
type mockIssueInternal struct {
	channelID     string
	uniqueID      string
	correlationID string
	isOpen        bool
	postID        string
	marshalErr    error
}

func (m *mockIssueInternal) ChannelID() string        { return m.channelID }
func (m *mockIssueInternal) UniqueID() string         { return m.uniqueID }
func (m *mockIssueInternal) GetCorrelationID() string { return m.correlationID }
func (m *mockIssueInternal) IsOpen() bool             { return m.isOpen }
func (m *mockIssueInternal) CurrentPostID() string    { return m.postID }

func (m *mockIssueInternal) MarshalJSON() ([]byte, error) {
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

func TestGetIssueInsertSQL(t *testing.T) {
	t.Parallel()

	client := New(WithIssuesTable("test_issues"))

	t.Run("generates correct SQL and args for open issue", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:     "C12345",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		sql, args, err := client.getIssueInsertSQL(issue)

		require.NoError(t, err)
		assert.Contains(t, sql, "INSERT INTO test_issues")
		assert.Contains(t, sql, "ON CONFLICT (id) DO UPDATE")
		require.Len(t, args, 8)
		assert.Equal(t, "uid-123", args[0])
		assert.Equal(t, IssueModelVersion, args[1])
		assert.Equal(t, "C12345", args[2])
		assert.Equal(t, "corr-456", args[3])
		assert.Equal(t, true, args[4])
		assert.Equal(t, "post-789", args[5])
		// args[6] is the JSON body
		assert.NotEmpty(t, args[6])
		// args[7] is expires_at — nil for open issues regardless of TTL config
		assert.Nil(t, args[7])
	})

	t.Run("generates correct SQL and args for closed issue with default TTL", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:     "C99999",
			uniqueID:      "uid-999",
			correlationID: "corr-999",
			isOpen:        false,
			postID:        "",
		}

		ttl := 180 * 24 * time.Hour
		before := time.Now()
		sql, args, err := client.getIssueInsertSQL(issue)
		after := time.Now()

		require.NoError(t, err)
		assert.Contains(t, sql, "INSERT INTO test_issues")
		require.Len(t, args, 8)
		assert.Equal(t, "uid-999", args[0])
		assert.Equal(t, "C99999", args[2])
		assert.Equal(t, false, args[4])
		assert.Empty(t, args[5])

		expiresAt, ok := args[7].(*time.Time)
		require.True(t, ok, "args[7] should be *time.Time")
		require.NotNil(t, expiresAt, "closed issues must have an expires_at")
		assert.True(t, expiresAt.After(before.Add(ttl-time.Second)), "expires_at should be roughly now+TTL")
		assert.True(t, expiresAt.Before(after.Add(ttl+time.Second)), "expires_at should be roughly now+TTL")
	})

	t.Run("open issue always has nil expires_at", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:     "C12345",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
		}

		_, args, err := client.getIssueInsertSQL(issue)

		require.NoError(t, err)
		require.Len(t, args, 8)
		assert.Nil(t, args[7], "open issues must never have an expires_at")
	})

	t.Run("custom TTL override is applied to closed issue", func(t *testing.T) {
		t.Parallel()

		ttl := 7 * 24 * time.Hour // override default 180 days with 7 days
		clientWithCustomTTL := New(WithIssuesTable("test_issues"), WithIssuesTimeToLive(ttl))

		issue := &mockIssueInternal{
			channelID:     "C99999",
			uniqueID:      "uid-999",
			correlationID: "corr-999",
			isOpen:        false,
		}

		before := time.Now()
		_, args, err := clientWithCustomTTL.getIssueInsertSQL(issue)
		after := time.Now()

		require.NoError(t, err)
		require.Len(t, args, 8)

		expiresAt, ok := args[7].(*time.Time)
		require.True(t, ok, "args[7] should be *time.Time")
		require.NotNil(t, expiresAt)
		assert.True(t, expiresAt.After(before.Add(ttl-time.Second)), "expires_at should be roughly now+TTL")
		assert.True(t, expiresAt.Before(after.Add(ttl+time.Second)), "expires_at should be roughly now+TTL")
	})

	t.Run("returns error when marshal fails", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:  "C12345",
			uniqueID:   "uid-123",
			marshalErr: errors.New("marshal failed"),
		}

		sql, args, err := client.getIssueInsertSQL(issue)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal issue")
		assert.Empty(t, sql)
		assert.Nil(t, args)
	})

	t.Run("SQL contains all required columns", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:     "C12345",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		sql, _, err := client.getIssueInsertSQL(issue)

		require.NoError(t, err)
		assert.Contains(t, sql, "id")
		assert.Contains(t, sql, "version")
		assert.Contains(t, sql, "channel_id")
		assert.Contains(t, sql, "correlation_id")
		assert.Contains(t, sql, "is_open")
		assert.Contains(t, sql, "slack_post_id")
		assert.Contains(t, sql, "attrs")
		assert.Contains(t, sql, "expires_at")
	})

	t.Run("SQL updates correct columns on conflict", func(t *testing.T) {
		t.Parallel()

		issue := &mockIssueInternal{
			channelID:     "C12345",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		sql, _, err := client.getIssueInsertSQL(issue)

		require.NoError(t, err)
		assert.Contains(t, sql, "channel_id = EXCLUDED.channel_id")
		assert.Contains(t, sql, "is_open = EXCLUDED.is_open")
		assert.Contains(t, sql, "slack_post_id = EXCLUDED.slack_post_id")
		assert.Contains(t, sql, "attrs = EXCLUDED.attrs")
		assert.Contains(t, sql, "expires_at = EXCLUDED.expires_at")
	})

	t.Run("uses configured table name", func(t *testing.T) {
		t.Parallel()

		customClient := New(WithIssuesTable("custom_issues_table"))
		issue := &mockIssueInternal{
			channelID:     "C12345",
			uniqueID:      "uid-123",
			correlationID: "corr-456",
			isOpen:        true,
			postID:        "post-789",
		}

		sql, _, err := customClient.getIssueInsertSQL(issue)

		require.NoError(t, err)
		assert.Contains(t, sql, "INSERT INTO custom_issues_table")
	})
}

func TestErrNotConnected(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "client is not connected", errNotConnected.Error())
}
