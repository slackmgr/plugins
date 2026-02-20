package postgres

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

	ExportCreateStatements = func(opts ...Option) []string {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.createStatements()
	}

	ExportDropStatements = func(opts ...Option) []string {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.dropStatements()
	}

	ExportVerifyDatabaseSchema = func(opts ...Option) func(map[string]*dbRow) error {
		o := newOptions()
		for _, opt := range opts {
			opt(o)
		}

		return o.verifyCurrentDatabaseVersion
	}
)

// DBRow exports the internal dbRow type for testing.
type DBRow = dbRow

// Pool exports the internal pool interface for testing.
type Pool = pool

// SetPool sets the connection pool for testing purposes.
func (c *Client) SetPool(p Pool) {
	c.conn = p
}

// HasActiveTTLCleanup returns true if the background TTL cleanup goroutine is running.
func (c *Client) HasActiveTTLCleanup() bool {
	return c.cancelTTL != nil
}
