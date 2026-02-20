package postgres_test

import (
	"testing"
	"time"

	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []postgres.Option
		wantErr string
	}{
		{
			name: "valid with required fields",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
			},
		},
		{
			name: "valid with all custom values",
			opts: []postgres.Option{
				postgres.WithHost("customhost"),
				postgres.WithPort(5433),
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithIssuesTable("custom_issues"),
				postgres.WithAlertsTable("custom_alerts"),
				postgres.WithMoveMappingsTable("custom_move"),
				postgres.WithChannelProcessingStateTable("custom_channel"),
			},
		},
		{
			name: "valid with TTL options",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithAlertsTimeToLive(30 * 24 * time.Hour),
				postgres.WithIssuesTimeToLive(180 * 24 * time.Hour),
				postgres.WithTTLCleanupInterval(time.Hour),
			},
		},
		{
			name: "returns error when user is empty",
			opts: []postgres.Option{
				postgres.WithDatabase("testdb"),
			},
			wantErr: "user is required",
		},
		{
			name: "returns error when database is empty",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
			},
			wantErr: "database is required",
		},
		{
			name: "returns error for invalid issues table name",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithIssuesTable("invalid-table"),
			},
			wantErr: "invalid issues table name",
		},
		{
			name: "returns error for invalid alerts table name",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithAlertsTable("table with spaces"),
			},
			wantErr: "invalid alerts table name",
		},
		{
			name: "returns error for invalid move mappings table name",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithMoveMappingsTable("123startswithnumber"),
			},
			wantErr: "invalid move mappings table name",
		},
		{
			name: "returns error for invalid channel processing state table name",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithChannelProcessingStateTable("table;drop"),
			},
			wantErr: "invalid channel processing state table name",
		},
		{
			name: "accepts table name starting with underscore",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithIssuesTable("_private_issues"),
			},
		},
		{
			name: "accepts table name with numbers",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithIssuesTable("issues_v2"),
			},
		},
		{
			name: "returns error for port above 65535",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithPort(65536),
			},
			wantErr: "port must be between 1 and 65535",
		},
		{
			name: "returns error for negative port",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithPort(-1),
			},
			wantErr: "port must be between 1 and 65535",
		},
		{
			name: "returns error for invalid SSL mode",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode("invalid"),
			},
			wantErr: "invalid SSL mode",
		},
		{
			name: "accepts valid SSL mode verify-full",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyFull),
			},
		},
		{
			name: "returns error for non-positive alerts TTL",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithAlertsTimeToLive(0),
			},
			wantErr: "alerts time to live must be greater than zero",
		},
		{
			name: "returns error for negative alerts TTL",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithAlertsTimeToLive(-1 * time.Hour),
			},
			wantErr: "alerts time to live must be greater than zero",
		},
		{
			name: "returns error for non-positive issues TTL",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithIssuesTimeToLive(0),
			},
			wantErr: "issues time to live must be greater than zero",
		},
		{
			name: "returns error for zero TTL cleanup interval",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithTTLCleanupInterval(0),
			},
			wantErr: "TTL cleanup interval must be positive",
		},
		{
			name: "returns error for negative TTL cleanup interval",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithTTLCleanupInterval(-time.Hour),
			},
			wantErr: "TTL cleanup interval must be positive",
		},
		{
			name: "disabled TTL cleanup is valid",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithTTLCleanupDisabled(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := postgres.ExportValidate(tt.opts...)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()

	connStr := postgres.ExportConnectionString(
		postgres.WithUser("testuser"),
		postgres.WithDatabase("testdb"),
	)

	assert.Contains(t, connStr, "localhost")
	assert.Contains(t, connStr, "5432")
	assert.Contains(t, connStr, "prefer")
}

func TestValidateTableName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "valid simple name", input: "users", wantErr: false},
		{name: "valid with underscore", input: "user_accounts", wantErr: false},
		{name: "valid starting with underscore", input: "_private", wantErr: false},
		{name: "valid with numbers", input: "table123", wantErr: false},
		{name: "valid mixed", input: "_my_table_v2", wantErr: false},
		{name: "valid uppercase", input: "MyTable", wantErr: false},
		{name: "invalid with hyphen", input: "my-table", wantErr: true},
		{name: "invalid with space", input: "my table", wantErr: true},
		{name: "invalid starting with number", input: "123table", wantErr: true},
		{name: "invalid with semicolon", input: "table;drop", wantErr: true},
		{name: "invalid with quotes", input: "table\"name", wantErr: true},
		{name: "invalid with dot", input: "schema.table", wantErr: true},
		{name: "invalid empty", input: "", wantErr: true},
		{name: "invalid SQL injection attempt", input: "table; DROP TABLE users;--", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := postgres.ExportValidateTableName(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "contains invalid characters")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConnectionString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []postgres.Option
		want string
	}{
		{
			name: "basic connection without password",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeDisable),
			},
			want: "postgres://testuser@localhost:5432/testdb?sslmode=disable",
		},
		{
			name: "connection with password",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithPassword("secret"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeDisable),
			},
			want: "postgres://testuser:secret@localhost:5432/testdb?sslmode=disable",
		},
		{
			name: "connection with SSL require",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeRequire),
			},
			want: "postgres://testuser@localhost:5432/testdb?sslmode=require",
		},
		{
			name: "connection with custom host and port",
			opts: []postgres.Option{
				postgres.WithHost("db.example.com"),
				postgres.WithPort(5433),
				postgres.WithUser("admin"),
				postgres.WithPassword("adminpass"),
				postgres.WithDatabase("production"),
				postgres.WithSSLMode(postgres.SSLModeDisable),
			},
			want: "postgres://admin:adminpass@db.example.com:5433/production?sslmode=disable",
		},
		{
			name: "connection with SSL verify-full",
			opts: []postgres.Option{
				postgres.WithHost("secure.db.com"),
				postgres.WithPort(5432),
				postgres.WithUser("secureuser"),
				postgres.WithPassword("securepass"),
				postgres.WithDatabase("securedb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyFull),
			},
			want: "postgres://secureuser:securepass@secure.db.com:5432/securedb?sslmode=verify-full",
		},
		{
			name: "connection with special characters in password",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithPassword("p@ss:word/with&special=chars"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeDisable),
			},
			want: "postgres://testuser:p%40ss%3Aword%2Fwith%26special%3Dchars@localhost:5432/testdb?sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := postgres.ExportConnectionString(tt.opts...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateStatements(t *testing.T) {
	t.Parallel()

	statements := postgres.ExportCreateStatements(
		postgres.WithIssuesTable("test_issues"),
		postgres.WithAlertsTable("test_alerts"),
		postgres.WithMoveMappingsTable("test_move_mappings"),
		postgres.WithChannelProcessingStateTable("test_channel_state"),
	)

	require.Len(t, statements, 10)

	t.Run("issues table creation", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, statements[0], "CREATE TABLE IF NOT EXISTS test_issues")
		assert.Contains(t, statements[0], "id text PRIMARY KEY")
		assert.Contains(t, statements[0], "channel_id text NOT NULL")
		assert.Contains(t, statements[0], "correlation_id text NOT NULL")
		assert.Contains(t, statements[0], "is_open boolean NOT NULL")
		assert.Contains(t, statements[0], "slack_post_id text NULL")
		assert.Contains(t, statements[0], "attrs JSONB NOT NULL")
		assert.Contains(t, statements[0], "expires_at TIMESTAMP WITH TIME ZONE NULL")
	})

	t.Run("issues indexes", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, statements[1], "CREATE INDEX IF NOT EXISTS test_issues_channel_correlation_idx")
		assert.Contains(t, statements[1], "ON test_issues (channel_id, correlation_id)")

		assert.Contains(t, statements[2], "CREATE INDEX IF NOT EXISTS test_issues_channel_slack_post_idx")
		assert.Contains(t, statements[2], "ON test_issues (channel_id, slack_post_id)")

		assert.Contains(t, statements[3], "CREATE INDEX IF NOT EXISTS test_issues_is_open_idx")
		assert.Contains(t, statements[3], "WHERE is_open = true")

		assert.Contains(t, statements[4], "CREATE INDEX IF NOT EXISTS test_issues_expires_at_idx")
		assert.Contains(t, statements[4], "ON test_issues (expires_at)")
		assert.Contains(t, statements[4], "WHERE expires_at IS NOT NULL")
	})

	t.Run("alerts table creation", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, statements[5], "CREATE TABLE IF NOT EXISTS test_alerts")
		assert.Contains(t, statements[5], "id text PRIMARY KEY")
		assert.Contains(t, statements[5], "attrs JSONB NOT NULL")
		assert.Contains(t, statements[5], "expires_at TIMESTAMP WITH TIME ZONE NULL")

		assert.Contains(t, statements[6], "CREATE INDEX IF NOT EXISTS test_alerts_expires_at_idx")
		assert.Contains(t, statements[6], "ON test_alerts (expires_at)")
		assert.Contains(t, statements[6], "WHERE expires_at IS NOT NULL")
	})

	t.Run("move mappings table and index", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, statements[7], "CREATE TABLE IF NOT EXISTS test_move_mappings")
		assert.Contains(t, statements[7], "channel_id text NOT NULL")
		assert.Contains(t, statements[7], "correlation_id text NOT NULL")

		assert.Contains(t, statements[8], "CREATE INDEX IF NOT EXISTS test_move_mappings_channel_correlation_idx")
	})

	t.Run("channel processing state table", func(t *testing.T) {
		t.Parallel()

		assert.Contains(t, statements[9], "CREATE TABLE IF NOT EXISTS test_channel_state")
		assert.Contains(t, statements[9], "created TIMESTAMP WITH TIME ZONE NOT NULL")
		assert.Contains(t, statements[9], "last_processed TIMESTAMP WITH TIME ZONE NOT NULL")
	})
}

func TestDropStatements(t *testing.T) {
	t.Parallel()

	statements := postgres.ExportDropStatements(
		postgres.WithIssuesTable("test_issues"),
		postgres.WithAlertsTable("test_alerts"),
		postgres.WithMoveMappingsTable("test_move_mappings"),
		postgres.WithChannelProcessingStateTable("test_channel_state"),
	)

	require.Len(t, statements, 4)
	assert.Equal(t, "DROP TABLE IF EXISTS test_issues CASCADE;", statements[0])
	assert.Equal(t, "DROP TABLE IF EXISTS test_alerts CASCADE;", statements[1])
	assert.Equal(t, "DROP TABLE IF EXISTS test_move_mappings CASCADE;", statements[2])
	assert.Equal(t, "DROP TABLE IF EXISTS test_channel_state CASCADE;", statements[3])
}

func TestVerifyCurrentDatabaseVersion(t *testing.T) {
	t.Parallel()

	verifyFn := postgres.ExportVerifyDatabaseSchema(
		postgres.WithIssuesTable("issues"),
		postgres.WithAlertsTable("alerts"),
		postgres.WithMoveMappingsTable("move_mappings"),
		postgres.WithChannelProcessingStateTable("channel_processing_state"),
	)

	validSchema := map[string]*postgres.DBRow{
		"issues.id":                               {DataType: "text", IsNullable: "NO"},
		"issues.version":                          {DataType: "smallint", IsNullable: "NO"},
		"issues.channel_id":                       {DataType: "text", IsNullable: "NO"},
		"issues.correlation_id":                   {DataType: "text", IsNullable: "NO"},
		"issues.is_open":                          {DataType: "boolean", IsNullable: "NO"},
		"issues.slack_post_id":                    {DataType: "text", IsNullable: "YES"},
		"issues.attrs":                            {DataType: "jsonb", IsNullable: "NO"},
		"issues.expires_at":                       {DataType: "timestamp with time zone", IsNullable: "YES"},
		"alerts.id":                               {DataType: "text", IsNullable: "NO"},
		"alerts.version":                          {DataType: "smallint", IsNullable: "NO"},
		"alerts.attrs":                            {DataType: "jsonb", IsNullable: "NO"},
		"alerts.expires_at":                       {DataType: "timestamp with time zone", IsNullable: "YES"},
		"move_mappings.id":                        {DataType: "text", IsNullable: "NO"},
		"move_mappings.version":                   {DataType: "smallint", IsNullable: "NO"},
		"move_mappings.channel_id":                {DataType: "text", IsNullable: "NO"},
		"move_mappings.correlation_id":            {DataType: "text", IsNullable: "NO"},
		"move_mappings.attrs":                     {DataType: "jsonb", IsNullable: "NO"},
		"channel_processing_state.id":             {DataType: "text", IsNullable: "NO"},
		"channel_processing_state.version":        {DataType: "smallint", IsNullable: "NO"},
		"channel_processing_state.channel_id":     {DataType: "text", IsNullable: "NO"},
		"channel_processing_state.created":        {DataType: "timestamp with time zone", IsNullable: "NO"},
		"channel_processing_state.last_processed": {DataType: "timestamp with time zone", IsNullable: "NO"},
	}

	t.Run("valid schema passes verification", func(t *testing.T) {
		t.Parallel()

		err := verifyFn(validSchema)
		assert.NoError(t, err)
	})

	t.Run("case insensitive data type comparison", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.id"].DataType = "TEXT"
		schema["issues.attrs"].DataType = "JSONB"

		err := verifyFn(schema)
		assert.NoError(t, err)
	})

	t.Run("missing column returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		delete(schema, "issues.channel_id")

		err := verifyFn(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
		assert.Contains(t, err.Error(), "issues.channel_id")
	})

	t.Run("missing expires_at column returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		delete(schema, "alerts.expires_at")

		err := verifyFn(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
		assert.Contains(t, err.Error(), "alerts.expires_at")
	})

	t.Run("wrong data type returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.is_open"].DataType = "text"

		err := verifyFn(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "data type mismatch")
		assert.Contains(t, err.Error(), "issues.is_open")
	})

	t.Run("wrong nullability returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.channel_id"].IsNullable = "YES"

		err := verifyFn(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "nullability mismatch")
		assert.Contains(t, err.Error(), "issues.channel_id")
	})

	t.Run("empty schema returns error", func(t *testing.T) {
		t.Parallel()

		err := verifyFn(map[string]*postgres.DBRow{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
	})
}

func TestVerifyCurrentDatabaseVersionWithCustomTables(t *testing.T) {
	t.Parallel()

	verifyFn := postgres.ExportVerifyDatabaseSchema(
		postgres.WithIssuesTable("custom_issues"),
		postgres.WithAlertsTable("custom_alerts"),
		postgres.WithMoveMappingsTable("custom_move"),
		postgres.WithChannelProcessingStateTable("custom_state"),
	)

	schema := map[string]*postgres.DBRow{
		"custom_issues.id":             {DataType: "text", IsNullable: "NO"},
		"custom_issues.version":        {DataType: "smallint", IsNullable: "NO"},
		"custom_issues.channel_id":     {DataType: "text", IsNullable: "NO"},
		"custom_issues.correlation_id": {DataType: "text", IsNullable: "NO"},
		"custom_issues.is_open":        {DataType: "boolean", IsNullable: "NO"},
		"custom_issues.slack_post_id":  {DataType: "text", IsNullable: "YES"},
		"custom_issues.attrs":          {DataType: "jsonb", IsNullable: "NO"},
		"custom_issues.expires_at":     {DataType: "timestamp with time zone", IsNullable: "YES"},
		"custom_alerts.id":             {DataType: "text", IsNullable: "NO"},
		"custom_alerts.version":        {DataType: "smallint", IsNullable: "NO"},
		"custom_alerts.attrs":          {DataType: "jsonb", IsNullable: "NO"},
		"custom_alerts.expires_at":     {DataType: "timestamp with time zone", IsNullable: "YES"},
		"custom_move.id":               {DataType: "text", IsNullable: "NO"},
		"custom_move.version":          {DataType: "smallint", IsNullable: "NO"},
		"custom_move.channel_id":       {DataType: "text", IsNullable: "NO"},
		"custom_move.correlation_id":   {DataType: "text", IsNullable: "NO"},
		"custom_move.attrs":            {DataType: "jsonb", IsNullable: "NO"},
		"custom_state.id":              {DataType: "text", IsNullable: "NO"},
		"custom_state.version":         {DataType: "smallint", IsNullable: "NO"},
		"custom_state.channel_id":      {DataType: "text", IsNullable: "NO"},
		"custom_state.created":         {DataType: "timestamp with time zone", IsNullable: "NO"},
		"custom_state.last_processed":  {DataType: "timestamp with time zone", IsNullable: "NO"},
	}

	err := verifyFn(schema)
	assert.NoError(t, err)
}

func TestModelVersionConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 2, postgres.IssueModelVersion)
	assert.Equal(t, 2, postgres.AlertModelVersion)
	assert.Equal(t, 2, postgres.MoveMappingModelVersion)
	assert.Equal(t, 2, postgres.ChannelProcessingStateModelVersion)
}

func copySchema(src map[string]*postgres.DBRow) map[string]*postgres.DBRow {
	dst := make(map[string]*postgres.DBRow, len(src))

	for k, v := range src {
		dst[k] = &postgres.DBRow{
			DataType:   v.DataType,
			IsNullable: v.IsNullable,
		}
	}

	return dst
}
