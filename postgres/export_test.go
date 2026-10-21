package postgres

import (
	"context"
	"time"
)

// Export internal symbols for testing.
// This file is only compiled during testing.

var (
	ExportValidateTableName = validateTableName
	ExportGetIssueInsertSQL = (*Client).getIssueInsertSQL

	ExportValidate = func(opts ...Option) error {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.validate()
	}

	ExportConnectionString = func(opts ...Option) string {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.connectionString()
	}

	ExportDropStatements = func(opts ...Option) []string {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.dropStatements()
	}

	ExportMigrations = func(opts ...Option) []migration {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.migrations()
	}
)

// Pool exports the internal pool interface for testing.
type Pool = pool

// SetPool sets the connection pool for testing purposes.
func (c *Client) SetPool(p Pool) {
	c.conn = p
}

// HasActiveTTLCleanup returns true if the background TTL cleanup goroutine is running.
func (c *Client) HasActiveTTLCleanup() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cancelTTL != nil
}

// DeleteExpiredRows runs a single TTL cleanup pass.
func (c *Client) DeleteExpiredRows(ctx context.Context) {
	c.deleteExpiredRows(ctx)
}

// SetTTLCleanupBatchPause overrides the pause between delete batches.
func (c *Client) SetTTLCleanupBatchPause(d time.Duration) {
	c.opts.ttlCleanupBatchPause = d
}

// SetInsightsPool sets the insights connection pool for testing purposes.
func (c *Client) SetInsightsPool(p Pool) {
	c.insightsConn = p
}

// SetNow overrides the clock used by insights partition maintenance.
func SetNow(now func() time.Time) Option {
	return func(o *options) { o.nowFn = now }
}

// HasActiveInsightsMaintenance returns true if the background insights maintenance goroutine is running.
func (c *Client) HasActiveInsightsMaintenance() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cancelInsight != nil
}

// MaintainInsightsPartitions runs a single insights partition maintenance pass.
func (c *Client) MaintainInsightsPartitions(ctx context.Context) error {
	return c.maintainInsightsPartitions(ctx)
}

// SetPool sets the connection pool of the reader for testing purposes.
func (r *InsightsReader) SetPool(p Pool) {
	r.conn = p
}

// ExecOnReaderPool executes a statement on the reader pool, for verifying that it is read-only.
func (r *InsightsReader) ExecOnReaderPool(ctx context.Context, sql string) error {
	_, err := r.conn.Exec(ctx, sql)

	return err
}

var (
	ExportEncodeInsightsCursor = encodeInsightsCursor
	ExportDecodeInsightsCursor = decodeInsightsCursor
)
