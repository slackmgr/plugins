package postgres

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// validIdentifier matches valid PostgreSQL unquoted identifiers.
// Must start with letter or underscore, followed by letters, digits, or underscores.
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

const (
	IssueModelVersion                  = 2
	AlertModelVersion                  = 2
	MoveMappingModelVersion            = 2
	ChannelProcessingStateModelVersion = 2

	// InsightsEventModelVersion is the value stored in the version column of every insights event row.
	InsightsEventModelVersion = 1
)

const (
	// maxInsightsTableNameLen is the maximum length of the insights events table name. Partition names
	// (<table>_pYYYYMMDD) and index names must fit in PostgreSQL's 63 character identifier limit.
	maxInsightsTableNameLen = 40

	// minInsightsRetention is the minimum insights retention. Retention has day granularity.
	minInsightsRetention = 24 * time.Hour
)

// SSLMode represents PostgreSQL SSL connection modes.
type SSLMode string

const (
	SSLModeDisable    SSLMode = "disable"     // No SSL
	SSLModeAllow      SSLMode = "allow"       // Try non-SSL first, then SSL
	SSLModePrefer     SSLMode = "prefer"      // Try SSL first, then non-SSL (default)
	SSLModeRequire    SSLMode = "require"     // Only SSL (no certificate verification)
	SSLModeVerifyCA   SSLMode = "verify-ca"   // SSL with CA verification
	SSLModeVerifyFull SSLMode = "verify-full" // SSL with CA and hostname verification
)

// Option is a functional option for configuring a Client.
type Option func(*options)

type options struct {
	host                            string
	port                            int
	user                            string
	password                        string
	database                        string
	sslMode                         SSLMode
	sslRootCert                     string
	sslCert                         string
	sslKey                          string
	poolMaxConnections              *int32
	poolMinConnections              *int32
	poolMinIdleConnections          *int32
	poolMaxConnectionLifetime       *time.Duration
	poolMaxConnectionIdleTime       *time.Duration
	poolHealthCheckPeriod           *time.Duration
	poolMaxConnectionLifetimeJitter *time.Duration
	issuesTable                     string
	alertsTable                     string
	moveMappingsTable               string
	channelProcessingStateTable     string
	schemaMigrationsTable           string
	alertsTimeToLive                time.Duration
	issuesTimeToLive                time.Duration
	ttlCleanupInterval              *time.Duration
	ttlCleanupBatchSize             int
	ttlCleanupBatchPause            time.Duration
	insightsEnabled                 bool
	insightsEventsTable             string
	insightsRetention               time.Duration
	insightsPoolMaxConnections      int32
	insightsStatementTimeout        time.Duration
	insightsReadStatementTimeout    time.Duration
	nowFn                           func() time.Time // test hook, defaults to time.Now
}

// now returns the current time, using the injectable clock if set.
func (o *options) now() time.Time {
	if o.nowFn != nil {
		return o.nowFn()
	}

	return time.Now()
}

func newOptions() *options {
	defaultCleanupInterval := time.Hour

	return &options{
		host:                        "localhost",
		port:                        5432,
		sslMode:                     SSLModePrefer,
		issuesTable:                 "issues",
		alertsTable:                 "alerts",
		moveMappingsTable:           "move_mappings",
		channelProcessingStateTable: "channel_processing_state",
		schemaMigrationsTable:       "schema_migrations",
		alertsTimeToLive:            30 * 24 * time.Hour,
		issuesTimeToLive:            180 * 24 * time.Hour,
		ttlCleanupInterval:          &defaultCleanupInterval,
		ttlCleanupBatchSize:         5000,
		ttlCleanupBatchPause:        defaultTTLCleanupBatchPause,

		insightsEventsTable:          "insights_events",
		insightsRetention:            30 * 24 * time.Hour,
		insightsPoolMaxConnections:   2,
		insightsStatementTimeout:     5 * time.Second,
		insightsReadStatementTimeout: 15 * time.Second,
	}
}

func WithHost(host string) Option {
	return func(o *options) { o.host = host }
}

func WithPort(port int) Option {
	return func(o *options) { o.port = port }
}

func WithUser(user string) Option {
	return func(o *options) { o.user = user }
}

func WithPassword(password string) Option {
	return func(o *options) { o.password = password }
}

func WithDatabase(database string) Option {
	return func(o *options) { o.database = database }
}

func WithSSLMode(mode SSLMode) Option {
	return func(o *options) { o.sslMode = mode }
}

// WithSSLRootCert sets the path to the CA certificate file used to verify the
// server's certificate. Useful when using SSLModeVerifyCA or SSLModeVerifyFull
// against a private CA that is not in the system trust store.
func WithSSLRootCert(path string) Option {
	return func(o *options) { o.sslRootCert = path }
}

// WithSSLCert sets the path to the client certificate file for mutual TLS
// authentication. Must be used together with WithSSLKey.
func WithSSLCert(path string) Option {
	return func(o *options) { o.sslCert = path }
}

// WithSSLKey sets the path to the client private key file for mutual TLS
// authentication. Must be used together with WithSSLCert.
func WithSSLKey(path string) Option {
	return func(o *options) { o.sslKey = path }
}

func WithPoolMaxConnections(n int32) Option {
	return func(o *options) { o.poolMaxConnections = &n }
}

func WithPoolMinConnections(n int32) Option {
	return func(o *options) { o.poolMinConnections = &n }
}

func WithPoolMinIdleConnections(n int32) Option {
	return func(o *options) { o.poolMinIdleConnections = &n }
}

func WithPoolMaxConnectionLifetime(d time.Duration) Option {
	return func(o *options) { o.poolMaxConnectionLifetime = &d }
}

func WithPoolMaxConnectionIdleTime(d time.Duration) Option {
	return func(o *options) { o.poolMaxConnectionIdleTime = &d }
}

func WithPoolHealthCheckPeriod(d time.Duration) Option {
	return func(o *options) { o.poolHealthCheckPeriod = &d }
}

func WithPoolMaxConnectionLifetimeJitter(d time.Duration) Option {
	return func(o *options) { o.poolMaxConnectionLifetimeJitter = &d }
}

func WithIssuesTable(name string) Option {
	return func(o *options) { o.issuesTable = name }
}

func WithAlertsTable(name string) Option {
	return func(o *options) { o.alertsTable = name }
}

func WithMoveMappingsTable(name string) Option {
	return func(o *options) { o.moveMappingsTable = name }
}

func WithChannelProcessingStateTable(name string) Option {
	return func(o *options) { o.channelProcessingStateTable = name }
}

// WithSchemaMigrationsTable sets the name of the table used to track applied
// schema migrations. Defaults to "schema_migrations".
func WithSchemaMigrationsTable(name string) Option {
	return func(o *options) { o.schemaMigrationsTable = name }
}

// WithAlertsTimeToLive sets the TTL applied to alert records. The default is
// 30 days. The duration must be greater than zero.
func WithAlertsTimeToLive(d time.Duration) Option {
	return func(o *options) { o.alertsTimeToLive = d }
}

// WithIssuesTimeToLive sets the TTL applied to closed issue records. The
// default is 180 days. The duration must be greater than zero.
func WithIssuesTimeToLive(d time.Duration) Option {
	return func(o *options) { o.issuesTimeToLive = d }
}

// WithTTLCleanupInterval sets how often the background goroutine runs to
// physically delete expired rows. Defaults to 1 hour. The duration must be
// greater than zero.
func WithTTLCleanupInterval(d time.Duration) Option {
	return func(o *options) { o.ttlCleanupInterval = &d }
}

// WithTTLCleanupDisabled disables the background TTL cleanup goroutine.
// When disabled, expired rows are excluded from reads but never physically
// deleted. Useful in tests or environments that handle cleanup externally.
func WithTTLCleanupDisabled() Option {
	return func(o *options) { o.ttlCleanupInterval = nil }
}

// WithTTLCleanupBatchSize sets the maximum number of expired rows deleted per
// statement by the background TTL cleanup. Expired rows are deleted in batches of
// this size, each in its own short transaction, until none remain. Defaults to
// 5000. The value must be greater than zero.
func WithTTLCleanupBatchSize(n int) Option {
	return func(o *options) { o.ttlCleanupBatchSize = n }
}

// WithInsightsEnabled enables recording of insights events. It is opt-in: when set, [Client.Connect] opens a
// second, small connection pool dedicated to insights, [Client.InsightsSink] and [Client.InsightsReader] return
// non-nil values, and [Client.Init] creates and drops daily partitions of the insights events table (once on
// Init, then hourly until [Client.Close]). The insights table itself is always created by the schema migrations.
func WithInsightsEnabled() Option {
	return func(o *options) { o.insightsEnabled = true }
}

// WithInsightsEventsTable sets the name of the partitioned insights events table. Defaults to "insights_events".
// The name must be a valid identifier of at most 40 characters, so that partition and index names fit in
// PostgreSQL's 63 character limit.
func WithInsightsEventsTable(name string) Option {
	return func(o *options) { o.insightsEventsTable = name }
}

// WithInsightsRetention sets how long insights events are kept. Retention is enforced by dropping whole daily
// partitions, so it has day granularity: a partition is dropped when its whole day is older than the retention.
// Defaults to 30 days. The duration must be at least 24 hours.
func WithInsightsRetention(d time.Duration) Option {
	return func(o *options) { o.insightsRetention = d }
}

// WithInsightsPoolMaxConnections sets the maximum number of connections in the insights pool (the writer pool of
// [Client], or the pool of [InsightsReader]). Defaults to 2. The value must be at least 1.
func WithInsightsPoolMaxConnections(n int32) Option {
	return func(o *options) { o.insightsPoolMaxConnections = n }
}

// WithInsightsStatementTimeout sets the server-side statement_timeout of the insights writer pool of [Client].
// Defaults to 5 seconds. The duration must be greater than zero.
func WithInsightsStatementTimeout(d time.Duration) Option {
	return func(o *options) { o.insightsStatementTimeout = d }
}

// WithInsightsReadStatementTimeout sets the server-side statement_timeout of the pool used by [InsightsReader].
// Defaults to 15 seconds. The duration must be greater than zero.
func WithInsightsReadStatementTimeout(d time.Duration) Option {
	return func(o *options) { o.insightsReadStatementTimeout = d }
}

// migration holds a versioned set of SQL statements that are applied once and
// recorded in the schema_migrations table.
type migration struct {
	version int
	stmts   []string
}

func (o *options) validate() error {
	if o.port < 1 || o.port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", o.port)
	}

	if o.user == "" {
		return errors.New("user is required")
	}

	if o.database == "" {
		return errors.New("database is required")
	}

	if !o.sslMode.isValid() {
		return fmt.Errorf("invalid SSL mode: %s", o.sslMode)
	}

	if (o.sslCert == "") != (o.sslKey == "") {
		return errors.New("sslcert and sslkey must both be set or both be empty")
	}

	if err := validateTableName(o.issuesTable); err != nil {
		return fmt.Errorf("invalid issues table name: %w", err)
	}

	if err := validateTableName(o.alertsTable); err != nil {
		return fmt.Errorf("invalid alerts table name: %w", err)
	}

	if err := validateTableName(o.moveMappingsTable); err != nil {
		return fmt.Errorf("invalid move mappings table name: %w", err)
	}

	if err := validateTableName(o.channelProcessingStateTable); err != nil {
		return fmt.Errorf("invalid channel processing state table name: %w", err)
	}

	if err := validateTableName(o.schemaMigrationsTable); err != nil {
		return fmt.Errorf("invalid schema migrations table name: %w", err)
	}

	if err := validateTableName(o.insightsEventsTable); err != nil {
		return fmt.Errorf("invalid insights events table name: %w", err)
	}

	if len(o.insightsEventsTable) > maxInsightsTableNameLen {
		return fmt.Errorf("insights events table name %q is too long: %d characters (max %d)", o.insightsEventsTable, len(o.insightsEventsTable), maxInsightsTableNameLen)
	}

	if o.insightsRetention < minInsightsRetention {
		return fmt.Errorf("insights retention must be at least %s", minInsightsRetention)
	}

	if o.insightsPoolMaxConnections < 1 {
		return errors.New("insights pool max connections must be at least 1")
	}

	if o.insightsStatementTimeout <= 0 {
		return errors.New("insights statement timeout must be greater than zero")
	}

	if o.insightsReadStatementTimeout <= 0 {
		return errors.New("insights read statement timeout must be greater than zero")
	}

	if o.alertsTimeToLive <= 0 {
		return errors.New("alerts time to live must be greater than zero")
	}

	if o.issuesTimeToLive <= 0 {
		return errors.New("issues time to live must be greater than zero")
	}

	if o.ttlCleanupInterval != nil && *o.ttlCleanupInterval <= 0 {
		return errors.New("TTL cleanup interval must be positive")
	}

	if o.ttlCleanupBatchSize <= 0 {
		return errors.New("TTL cleanup batch size must be greater than zero")
	}

	return nil
}

func validateTableName(name string) error {
	if !validIdentifier.MatchString(name) {
		return fmt.Errorf("table name %q contains invalid characters", name)
	}

	return nil
}

// isValid returns true if the SSL mode is a valid PostgreSQL SSL mode.
func (s SSLMode) isValid() bool {
	switch s {
	case SSLModeDisable, SSLModeAllow, SSLModePrefer, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull:
		return true
	default:
		return false
	}
}

func (o *options) connectionString() string {
	host := net.JoinHostPort(o.host, strconv.Itoa(o.port))

	user := url.QueryEscape(o.user)

	if o.password != "" {
		user += ":" + url.QueryEscape(o.password)
	}

	dsn := fmt.Sprintf("postgres://%s@%s/%s?sslmode=%s", user, host, url.PathEscape(o.database), o.sslMode)

	if o.sslRootCert != "" {
		dsn += "&sslrootcert=" + url.QueryEscape(o.sslRootCert)
	}

	if o.sslCert != "" {
		dsn += "&sslcert=" + url.QueryEscape(o.sslCert)
	}

	if o.sslKey != "" {
		dsn += "&sslkey=" + url.QueryEscape(o.sslKey)
	}

	return dsn
}

// migrations returns the ordered list of schema migrations. Each migration is
// applied exactly once and recorded in the schema_migrations table. To add a
// new migration, append an entry with the next version number.
func (o *options) migrations() []migration {
	return []migration{
		{
			version: 1,
			stmts: []string{
				// Issues table and indexes
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, version SMALLINT NOT NULL, channel_id text NOT NULL, correlation_id text NOT NULL, is_open boolean NOT NULL, slack_post_id text NULL, attrs JSONB NOT NULL, expires_at TIMESTAMP WITH TIME ZONE NULL);`, o.issuesTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_channel_correlation_idx ON %s (channel_id, correlation_id);`, o.issuesTable, o.issuesTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_channel_slack_post_idx ON %s (channel_id, slack_post_id);`, o.issuesTable, o.issuesTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_is_open_idx ON %s (is_open) WHERE is_open = true;`, o.issuesTable, o.issuesTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at) WHERE expires_at IS NOT NULL;`, o.issuesTable, o.issuesTable),
				// Alerts table and index
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, version SMALLINT NOT NULL, attrs JSONB NOT NULL, expires_at TIMESTAMP WITH TIME ZONE NULL);`, o.alertsTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at) WHERE expires_at IS NOT NULL;`, o.alertsTable, o.alertsTable),
				// Move mappings table and indexes
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, version SMALLINT NOT NULL, channel_id text NOT NULL, correlation_id text NOT NULL, attrs JSONB NOT NULL);`, o.moveMappingsTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_channel_correlation_idx ON %s (channel_id, correlation_id);`, o.moveMappingsTable, o.moveMappingsTable),
				// Channel processing state table
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, version SMALLINT NOT NULL, channel_id text NOT NULL, created TIMESTAMP WITH TIME ZONE NOT NULL, last_processed TIMESTAMP WITH TIME ZONE NOT NULL);`, o.channelProcessingStateTable),
			},
		},
		{
			version: 2,
			stmts: []string{
				// Enforce at most one open issue per (channel_id, correlation_id)
				fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s_open_channel_correlation_uniq ON %s (channel_id, correlation_id) WHERE is_open = true;`, o.issuesTable, o.issuesTable),
				// Enforce at most one issue per (channel_id, slack_post_id), NULLs excluded
				fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s_channel_slack_post_uniq ON %s (channel_id, slack_post_id) WHERE slack_post_id IS NOT NULL;`, o.issuesTable, o.issuesTable),
				// Enforce at most one move mapping per (channel_id, correlation_id)
				fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s_channel_correlation_uniq ON %s (channel_id, correlation_id);`, o.moveMappingsTable, o.moveMappingsTable),
			},
		},
		{
			version: 3,
			stmts: []string{
				// Persist the full channel processing state (previously only created and last_processed were stored)
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_channel_activity TIMESTAMP WITH TIME ZONE NULL;`, o.channelProcessingStateTable),
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS open_issues INTEGER NOT NULL DEFAULT 0;`, o.channelProcessingStateTable),
			},
		},
		{
			version: 4,
			stmts: []string{
				// Insights events table, partitioned by day on occurred_at. Always created (even when insights is
				// disabled) so that the migration sequence stays linear. Partitions are managed by the writer client.
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id BIGINT GENERATED ALWAYS AS IDENTITY, version SMALLINT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL, type TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL DEFAULT '', related_channel_id TEXT NOT NULL DEFAULT '', issue_id TEXT NOT NULL DEFAULT '', correlation_id TEXT NOT NULL DEFAULT '', severity TEXT NOT NULL DEFAULT '', route_key TEXT NOT NULL DEFAULT '', actor_id TEXT NOT NULL DEFAULT '', attrs JSONB NULL) PARTITION BY RANGE (occurred_at);`, o.insightsEventsTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_time_brin ON %s USING BRIN (occurred_at);`, o.insightsEventsTable, o.insightsEventsTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_chan_type_time_idx ON %s (channel_id, type, occurred_at);`, o.insightsEventsTable, o.insightsEventsTable),
				fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_issue_time_idx ON %s (issue_id, occurred_at) WHERE issue_id <> '';`, o.insightsEventsTable, o.insightsEventsTable),
			},
		},
	}
}

func (o *options) dropStatements() []string {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.issuesTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.alertsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.moveMappingsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.channelProcessingStateTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.insightsEventsTable), // also drops all partitions
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.schemaMigrationsTable),
	}
}
