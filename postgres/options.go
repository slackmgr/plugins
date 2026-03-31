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

	if o.alertsTimeToLive <= 0 {
		return errors.New("alerts time to live must be greater than zero")
	}

	if o.issuesTimeToLive <= 0 {
		return errors.New("issues time to live must be greater than zero")
	}

	if o.ttlCleanupInterval != nil && *o.ttlCleanupInterval <= 0 {
		return errors.New("TTL cleanup interval must be positive")
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

	dsn := fmt.Sprintf("postgres://%s@%s/%s?sslmode=%s", user, host, o.database, o.sslMode)

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
	}
}

func (o *options) dropStatements() []string {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.issuesTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.alertsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.moveMappingsTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.channelProcessingStateTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", o.schemaMigrationsTable),
	}
}
