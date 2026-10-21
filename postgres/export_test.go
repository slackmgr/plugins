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
