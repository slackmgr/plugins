package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

			err := validateTableName(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "contains invalid characters")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCreateStatements(t *testing.T) {
	t.Parallel()

	o := &options{
		issuesTable:                 "test_issues",
		alertsTable:                 "test_alerts",
		moveMappingsTable:           "test_move_mappings",
		channelProcessingStateTable: "test_channel_state",
	}

	statements := o.createStatements()

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

	o := &options{
		issuesTable:                 "test_issues",
		alertsTable:                 "test_alerts",
		moveMappingsTable:           "test_move_mappings",
		channelProcessingStateTable: "test_channel_state",
	}

	statements := o.dropStatements()

	require.Len(t, statements, 4)
	assert.Equal(t, "DROP TABLE IF EXISTS test_issues CASCADE;", statements[0])
	assert.Equal(t, "DROP TABLE IF EXISTS test_alerts CASCADE;", statements[1])
	assert.Equal(t, "DROP TABLE IF EXISTS test_move_mappings CASCADE;", statements[2])
	assert.Equal(t, "DROP TABLE IF EXISTS test_channel_state CASCADE;", statements[3])
}

func TestVerifyCurrentDatabaseVersion(t *testing.T) {
	t.Parallel()

	o := &options{
		issuesTable:                 "issues",
		alertsTable:                 "alerts",
		moveMappingsTable:           "move_mappings",
		channelProcessingStateTable: "channel_processing_state",
	}

	validSchema := map[string]*dbRow{
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

		err := o.verifyCurrentDatabaseVersion(validSchema)
		assert.NoError(t, err)
	})

	t.Run("case insensitive data type comparison", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.id"].DataType = "TEXT"
		schema["issues.attrs"].DataType = "JSONB"

		err := o.verifyCurrentDatabaseVersion(schema)
		assert.NoError(t, err)
	})

	t.Run("missing column returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		delete(schema, "issues.channel_id")

		err := o.verifyCurrentDatabaseVersion(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
		assert.Contains(t, err.Error(), "issues.channel_id")
	})

	t.Run("missing expires_at column returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		delete(schema, "issues.expires_at")

		err := o.verifyCurrentDatabaseVersion(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
		assert.Contains(t, err.Error(), "issues.expires_at")
	})

	t.Run("wrong data type returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.is_open"].DataType = "text"

		err := o.verifyCurrentDatabaseVersion(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "data type mismatch")
		assert.Contains(t, err.Error(), "issues.is_open")
	})

	t.Run("wrong nullability returns error", func(t *testing.T) {
		t.Parallel()

		schema := copySchema(validSchema)
		schema["issues.channel_id"].IsNullable = "YES"

		err := o.verifyCurrentDatabaseVersion(schema)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "nullability mismatch")
		assert.Contains(t, err.Error(), "issues.channel_id")
	})

	t.Run("empty schema returns error", func(t *testing.T) {
		t.Parallel()

		err := o.verifyCurrentDatabaseVersion(map[string]*dbRow{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in current database schema")
	})
}

func TestVerifyCurrentDatabaseVersionWithCustomTables(t *testing.T) {
	t.Parallel()

	o := &options{
		issuesTable:                 "custom_issues",
		alertsTable:                 "custom_alerts",
		moveMappingsTable:           "custom_move",
		channelProcessingStateTable: "custom_state",
	}

	schema := map[string]*dbRow{
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

	err := o.verifyCurrentDatabaseVersion(schema)
	assert.NoError(t, err)
}

func copySchema(src map[string]*dbRow) map[string]*dbRow {
	dst := make(map[string]*dbRow, len(src))

	for k, v := range src {
		dst[k] = &dbRow{
			DataType:   v.DataType,
			IsNullable: v.IsNullable,
		}
	}

	return dst
}
