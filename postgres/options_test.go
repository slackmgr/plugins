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
		{
			name: "valid with custom migrations table",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSchemaMigrationsTable("custom_migrations"),
			},
		},
		{
			name: "returns error for invalid migrations table name",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSchemaMigrationsTable("bad-name"),
			},
			wantErr: "invalid schema migrations table name",
		},
		{
			name: "valid with CA cert",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyCA),
				postgres.WithSSLRootCert("/etc/ssl/certs/ca.crt"),
			},
		},
		{
			name: "valid with client cert and key",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeRequire),
				postgres.WithSSLCert("/etc/ssl/certs/client.crt"),
				postgres.WithSSLKey("/etc/ssl/private/client.key"),
			},
		},
		{
			name: "valid with all three cert options",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyFull),
				postgres.WithSSLRootCert("/etc/ssl/certs/ca.crt"),
				postgres.WithSSLCert("/etc/ssl/certs/client.crt"),
				postgres.WithSSLKey("/etc/ssl/private/client.key"),
			},
		},
		{
			name: "returns error when only sslcert is set",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLCert("/etc/ssl/certs/client.crt"),
			},
			wantErr: "sslcert and sslkey must both be set or both be empty",
		},
		{
			name: "returns error when only sslkey is set",
			opts: []postgres.Option{
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLKey("/etc/ssl/private/client.key"),
			},
			wantErr: "sslcert and sslkey must both be set or both be empty",
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
		{
			name: "connection with CA cert",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyCA),
				postgres.WithSSLRootCert("/etc/ssl/certs/ca.crt"),
			},
			want: "postgres://testuser@localhost:5432/testdb?sslmode=verify-ca&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca.crt",
		},
		{
			name: "connection with all TLS options",
			opts: []postgres.Option{
				postgres.WithHost("secure.db.com"),
				postgres.WithPort(5432),
				postgres.WithUser("appuser"),
				postgres.WithDatabase("appdb"),
				postgres.WithSSLMode(postgres.SSLModeVerifyFull),
				postgres.WithSSLRootCert("/etc/ssl/certs/ca.crt"),
				postgres.WithSSLCert("/etc/ssl/certs/client.crt"),
				postgres.WithSSLKey("/etc/ssl/private/client.key"),
			},
			want: "postgres://appuser@secure.db.com:5432/appdb?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca.crt&sslcert=%2Fetc%2Fssl%2Fcerts%2Fclient.crt&sslkey=%2Fetc%2Fssl%2Fprivate%2Fclient.key",
		},
		{
			name: "connection string does not include empty cert params",
			opts: []postgres.Option{
				postgres.WithHost("localhost"),
				postgres.WithPort(5432),
				postgres.WithUser("testuser"),
				postgres.WithDatabase("testdb"),
				postgres.WithSSLMode(postgres.SSLModeRequire),
			},
			want: "postgres://testuser@localhost:5432/testdb?sslmode=require",
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

func TestDropStatements(t *testing.T) {
	t.Parallel()

	statements := postgres.ExportDropStatements(
		postgres.WithIssuesTable("test_issues"),
		postgres.WithAlertsTable("test_alerts"),
		postgres.WithMoveMappingsTable("test_move_mappings"),
		postgres.WithChannelProcessingStateTable("test_channel_state"),
		postgres.WithSchemaMigrationsTable("test_schema_migrations"),
	)

	require.Len(t, statements, 5)
	assert.Equal(t, "DROP TABLE IF EXISTS test_issues CASCADE;", statements[0])
	assert.Equal(t, "DROP TABLE IF EXISTS test_alerts CASCADE;", statements[1])
	assert.Equal(t, "DROP TABLE IF EXISTS test_move_mappings CASCADE;", statements[2])
	assert.Equal(t, "DROP TABLE IF EXISTS test_channel_state CASCADE;", statements[3])
	assert.Equal(t, "DROP TABLE IF EXISTS test_schema_migrations CASCADE;", statements[4])
}

func TestModelVersionConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 2, postgres.IssueModelVersion)
	assert.Equal(t, 2, postgres.AlertModelVersion)
	assert.Equal(t, 2, postgres.MoveMappingModelVersion)
	assert.Equal(t, 2, postgres.ChannelProcessingStateModelVersion)
}
