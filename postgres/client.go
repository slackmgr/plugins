//nolint:nilnil
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/slackmgr/types"
)

var errNotConnected = errors.New("client is not connected")

// pool defines the interface for database operations.
// This interface is satisfied by *pgxpool.Pool and can be mocked for testing.
type pool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Close()
	Ping(ctx context.Context) error
}

type Client struct {
	conn      pool
	opts      *options
	cancelTTL context.CancelFunc
}

func New(opts ...Option) *Client {
	o := newOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &Client{opts: o}
}

func (c *Client) Connect(ctx context.Context) error {
	// Close existing connection if any to prevent leaks
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	if err := c.opts.validate(); err != nil {
		return fmt.Errorf("invalid Postgres db configuration: %w", err)
	}

	config, err := pgxpool.ParseConfig(c.opts.connectionString())
	if err != nil {
		return fmt.Errorf("failed to parse Postgres db connection string: %w", err)
	}

	if c.opts.poolMaxConnections != nil {
		config.MaxConns = *c.opts.poolMaxConnections
	}

	if c.opts.poolMinConnections != nil {
		config.MinConns = *c.opts.poolMinConnections
	}

	if c.opts.poolMinIdleConnections != nil {
		config.MinIdleConns = *c.opts.poolMinIdleConnections
	}

	if c.opts.poolMaxConnectionLifetime != nil {
		config.MaxConnLifetime = *c.opts.poolMaxConnectionLifetime
	}

	if c.opts.poolMaxConnectionIdleTime != nil {
		config.MaxConnIdleTime = *c.opts.poolMaxConnectionIdleTime
	}

	if c.opts.poolHealthCheckPeriod != nil {
		config.HealthCheckPeriod = *c.opts.poolHealthCheckPeriod
	}

	if c.opts.poolMaxConnectionLifetimeJitter != nil {
		config.MaxConnLifetimeJitter = *c.opts.poolMaxConnectionLifetimeJitter
	}

	conn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create new Postgres connection pool: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping Postgres db: %w", err)
	}

	c.conn = conn

	return nil
}

func (c *Client) Close(_ context.Context) error {
	if c.cancelTTL != nil {
		c.cancelTTL()
		c.cancelTTL = nil
	}

	if c.conn == nil {
		return nil
	}

	c.conn.Close()

	c.conn = nil

	return nil
}

func (c *Client) Init(ctx context.Context, skipSchemaValidation bool) error {
	if c.conn == nil {
		return errNotConnected
	}

	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin init transaction: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }() // No-op if committed

	for _, sql := range c.opts.createStatements() {
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute create statement: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit init transaction: %w", err)
	}

	if !skipSchemaValidation {
		query := "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public' ORDER BY ordinal_position"

		rows, err := c.conn.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query information schema: %w", err)
		}

		defer rows.Close()

		infoRows := map[string]*dbRow{}

		for rows.Next() {
			var table, column string
			infoRow := &dbRow{}

			if err := rows.Scan(&table, &column, &infoRow.DataType, &infoRow.IsNullable); err != nil {
				return fmt.Errorf("failed to scan row from information schema: %w", err)
			}

			infoRows[table+"."+column] = infoRow
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating over rows from information schema: %w", err)
		}

		if err := c.opts.verifyCurrentDatabaseVersion(infoRows); err != nil {
			return fmt.Errorf("failed to verify current database version: %w", err)
		}
	}

	if c.cancelTTL == nil && c.opts.ttlCleanupInterval != nil {
		ttlCtx, cancel := context.WithCancel(context.Background())
		c.cancelTTL = cancel

		//nolint:contextcheck // Intentionally using a new context: the TTL goroutine must outlive the Init call.
		go c.runTTLCleanup(ttlCtx)
	}

	return nil
}

func (c *Client) DropAllData(ctx context.Context) error {
	if c.conn == nil {
		return errNotConnected
	}

	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin drop tables transaction: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }() // No-op if committed

	for _, sql := range c.opts.dropStatements() {
		if _, err := tx.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute drop statement: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit drop tables transaction: %w", err)
	}

	return nil
}

func (c *Client) SaveAlert(ctx context.Context, alert *types.Alert) error {
	if c.conn == nil {
		return errNotConnected
	}

	if alert == nil {
		return errors.New("alert cannot be nil")
	}

	body, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert: %w", err)
	}

	t := time.Now().Add(c.opts.alertsTimeToLive)
	expiresAt := &t

	param1 := alert.UniqueID()
	param2 := AlertModelVersion
	param3 := string(body)
	param4 := expiresAt

	sql := fmt.Sprintf("INSERT INTO %s (id, version, attrs, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs, expires_at = EXCLUDED.expires_at", c.opts.alertsTable)

	if _, err := c.conn.Exec(ctx, sql, param1, param2, param3, param4); err != nil {
		return fmt.Errorf("failed to save alert to Postgres db: %w", err)
	}

	return nil
}

func (c *Client) SaveIssue(ctx context.Context, issue types.Issue) error {
	if c.conn == nil {
		return errNotConnected
	}

	if issue == nil {
		return errors.New("issue cannot be nil")
	}

	if issue.ChannelID() == "" {
		return errors.New("channel ID cannot be empty")
	}

	sql, args, err := c.getIssueInsertSQL(issue)
	if err != nil {
		return err
	}

	if _, err := c.conn.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to create or update issue in Postgres db: %w", err)
	}

	return nil
}

func (c *Client) SaveIssues(ctx context.Context, issues ...types.Issue) error {
	if c.conn == nil {
		return errNotConnected
	}

	if len(issues) == 0 {
		return nil
	}

	// Use the simplified SaveIssue method if there's only one issue.
	if len(issues) == 1 {
		return c.SaveIssue(ctx, issues[0])
	}

	for _, issue := range issues {
		if issue == nil {
			return errors.New("issue cannot be nil")
		}

		if issue.ChannelID() == "" {
			return fmt.Errorf("channel ID cannot be empty for issue with ID %s", issue.UniqueID())
		}
	}

	// Use batch for better performance with multiple issues
	batch := &pgx.Batch{}

	for _, issue := range issues {
		sql, args, err := c.getIssueInsertSQL(issue)
		if err != nil {
			return err
		}

		batch.Queue(sql, args...)
	}

	results := c.conn.SendBatch(ctx, batch)

	defer results.Close()

	for range issues {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("failed to update issue in Postgres db: %w", err)
		}
	}

	return nil
}

func (c *Client) MoveIssue(ctx context.Context, issue types.Issue, _, targetChannelID string) error {
	if c.conn == nil {
		return errNotConnected
	}

	if issue == nil {
		return errors.New("issue cannot be nil")
	}

	if targetChannelID == "" {
		return errors.New("target channel ID cannot be empty")
	}

	if issue.ChannelID() != targetChannelID {
		return fmt.Errorf("issue channel ID %s does not match target channel ID %s", issue.ChannelID(), targetChannelID)
	}

	// We use the standard SaveIssue method to update the issue.
	// This will update the channel ID and any other attributes of the issue.
	if err := c.SaveIssue(ctx, issue); err != nil {
		return fmt.Errorf("failed to move issue in Postgres db: %w", err)
	}

	return nil
}

func (c *Client) FindOpenIssueByCorrelationID(ctx context.Context, channelID, correlationID string) (string, json.RawMessage, error) {
	if c.conn == nil {
		return "", nil, errNotConnected
	}

	if channelID == "" {
		return "", nil, errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return "", nil, errors.New("correlation ID cannot be empty")
	}

	param1 := channelID
	param2 := correlationID

	query := fmt.Sprintf("SELECT id, attrs FROM %s WHERE channel_id = $1 AND correlation_id = $2 AND is_open = true AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1", c.opts.issuesTable)

	row := c.conn.QueryRow(ctx, query, param1, param2)

	var id string
	var body json.RawMessage

	if err := row.Scan(&id, &body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, nil
		}

		return "", nil, fmt.Errorf("failed to find issue by correlation ID in Postgres db: %w", err)
	}

	return id, body, nil
}

func (c *Client) FindIssueBySlackPostID(ctx context.Context, channelID, postID string) (string, json.RawMessage, error) {
	if c.conn == nil {
		return "", nil, errNotConnected
	}

	if channelID == "" {
		return "", nil, errors.New("channel ID cannot be empty")
	}

	if postID == "" {
		return "", nil, errors.New("post ID cannot be empty")
	}

	param1 := channelID
	param2 := postID

	query := fmt.Sprintf("SELECT id, attrs FROM %s WHERE channel_id = $1 AND slack_post_id = $2 AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1", c.opts.issuesTable)

	row := c.conn.QueryRow(ctx, query, param1, param2)

	var id string
	var body json.RawMessage

	if err := row.Scan(&id, &body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil, nil
		}

		return "", nil, fmt.Errorf("failed to find issue by Slack post ID in Postgres db: %w", err)
	}

	return id, body, nil
}

func (c *Client) FindActiveChannels(ctx context.Context) ([]string, error) {
	if c.conn == nil {
		return nil, errNotConnected
	}

	query := fmt.Sprintf("SELECT DISTINCT channel_id FROM %s WHERE is_open = true AND (expires_at IS NULL OR expires_at > NOW())", c.opts.issuesTable)

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to find active channels in Postgres db: %w", err)
	}

	defer rows.Close()

	var channels []string

	for rows.Next() {
		var channelID string

		if err := rows.Scan(&channelID); err != nil {
			return nil, fmt.Errorf("failed to scan row for active channel: %w", err)
		}

		channels = append(channels, channelID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows for active channels: %w", err)
	}

	return channels, nil
}

func (c *Client) LoadOpenIssuesInChannel(ctx context.Context, channelID string) (map[string]json.RawMessage, error) {
	if c.conn == nil {
		return nil, errNotConnected
	}

	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	query := fmt.Sprintf("SELECT id, attrs FROM %s WHERE is_open = true AND channel_id = $1 AND (expires_at IS NULL OR expires_at > NOW())", c.opts.issuesTable)

	rows, err := c.conn.Query(ctx, query, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to load open issues from Postgres db: %w", err)
	}

	defer rows.Close()

	issues := make(map[string]json.RawMessage)

	for rows.Next() {
		var id string
		var body json.RawMessage

		if err := rows.Scan(&id, &body); err != nil {
			return nil, fmt.Errorf("failed to load open issues from Postgres db: %w", err)
		}

		issues[id] = body
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows in Postgres db: %w", err)
	}

	return issues, nil
}

func (c *Client) SaveMoveMapping(ctx context.Context, moveMapping types.MoveMapping) error {
	if c.conn == nil {
		return errNotConnected
	}

	if moveMapping == nil {
		return errors.New("move mapping cannot be nil")
	}

	if moveMapping.ChannelID() == "" {
		return fmt.Errorf("channel ID cannot be empty for move mapping with correlation ID %s", moveMapping.GetCorrelationID())
	}

	body, err := moveMapping.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal move mapping: %w", err)
	}

	param1 := moveMapping.UniqueID()
	param2 := MoveMappingModelVersion
	param3 := moveMapping.ChannelID()
	param4 := moveMapping.GetCorrelationID()
	param5 := string(body)

	sql := fmt.Sprintf("INSERT INTO %s (id, version, channel_id, correlation_id, attrs) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET attrs = EXCLUDED.attrs", c.opts.moveMappingsTable)

	if _, err := c.conn.Exec(ctx, sql, param1, param2, param3, param4, param5); err != nil {
		return fmt.Errorf("failed to save move mapping to Postgres db: %w", err)
	}

	return nil
}

func (c *Client) FindMoveMapping(ctx context.Context, channelID, correlationID string) (json.RawMessage, error) {
	if c.conn == nil {
		return nil, errNotConnected
	}

	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return nil, errors.New("correlation ID cannot be empty")
	}

	param1 := channelID
	param2 := correlationID

	query := fmt.Sprintf("SELECT attrs FROM %s WHERE channel_id = $1 AND correlation_id = $2 LIMIT 1", c.opts.moveMappingsTable)

	row := c.conn.QueryRow(ctx, query, param1, param2)

	var body json.RawMessage

	if err := row.Scan(&body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to find move mapping in Postgres db: %w", err)
	}

	return body, nil
}

func (c *Client) DeleteMoveMapping(ctx context.Context, channelID, correlationID string) error {
	if c.conn == nil {
		return errNotConnected
	}

	if channelID == "" {
		return errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return errors.New("correlation ID cannot be empty")
	}

	param1 := channelID
	param2 := correlationID

	query := fmt.Sprintf("DELETE FROM %s WHERE channel_id = $1 AND correlation_id = $2", c.opts.moveMappingsTable)

	if _, err := c.conn.Exec(ctx, query, param1, param2); err != nil {
		return fmt.Errorf("failed to delete move mapping from Postgres db: %w", err)
	}

	return nil
}

func (c *Client) SaveChannelProcessingState(ctx context.Context, state *types.ChannelProcessingState) error {
	if c.conn == nil {
		return errNotConnected
	}

	if state == nil {
		return errors.New("processing state cannot be nil")
	}

	if state.ChannelID == "" {
		return errors.New("channel ID cannot be empty")
	}

	param1 := state.ChannelID
	param2 := ChannelProcessingStateModelVersion
	param3 := state.ChannelID
	param4 := state.Created
	param5 := state.LastProcessed

	sql := fmt.Sprintf("INSERT INTO %s (id, version, channel_id, created, last_processed) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET last_processed = EXCLUDED.last_processed", c.opts.channelProcessingStateTable)

	if _, err := c.conn.Exec(ctx, sql, param1, param2, param3, param4, param5); err != nil {
		return fmt.Errorf("failed to save channel processing state to Postgres db: %w", err)
	}

	return nil
}

func (c *Client) FindChannelProcessingState(ctx context.Context, channelID string) (*types.ChannelProcessingState, error) {
	if c.conn == nil {
		return nil, errNotConnected
	}

	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	param1 := channelID

	query := fmt.Sprintf("SELECT channel_id, created, last_processed FROM %s WHERE id = $1", c.opts.channelProcessingStateTable)

	row := c.conn.QueryRow(ctx, query, param1)

	var state types.ChannelProcessingState

	if err := row.Scan(&state.ChannelID, &state.Created, &state.LastProcessed); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No processing state found for the channel
		}

		return nil, fmt.Errorf("failed to find channel processing state in Postgres db: %w", err)
	}

	return &state, nil
}

func (c *Client) getIssueInsertSQL(issue types.Issue) (string, []any, error) {
	body, err := issue.MarshalJSON()
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal issue: %w", err)
	}

	var expiresAt *time.Time

	if !issue.IsOpen() {
		t := time.Now().Add(c.opts.issuesTimeToLive)
		expiresAt = &t
	}

	param1 := issue.UniqueID()
	param2 := IssueModelVersion
	param3 := issue.ChannelID()
	param4 := issue.GetCorrelationID()
	param5 := issue.IsOpen()
	param6 := issue.CurrentPostID()
	param7 := string(body)
	param8 := expiresAt

	statement := fmt.Sprintf("INSERT INTO %s (id, version, channel_id, correlation_id, is_open, slack_post_id, attrs, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET channel_id = EXCLUDED.channel_id, is_open = EXCLUDED.is_open, slack_post_id = EXCLUDED.slack_post_id, attrs = EXCLUDED.attrs, expires_at = EXCLUDED.expires_at", c.opts.issuesTable)
	args := []any{param1, param2, param3, param4, param5, param6, param7, param8}

	return statement, args, nil
}

func (c *Client) runTTLCleanup(ctx context.Context) {
	ticker := time.NewTicker(*c.opts.ttlCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.deleteExpiredRows(ctx)
		}
	}
}

func (c *Client) deleteExpiredRows(ctx context.Context) {
	_, _ = c.conn.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < NOW()", c.opts.alertsTable))

	_, _ = c.conn.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < NOW()", c.opts.issuesTable))
}
