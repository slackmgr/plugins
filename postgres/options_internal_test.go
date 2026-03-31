package postgres

import (
	"strings"
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

func TestMigrations(t *testing.T) {
	t.Parallel()

	t.Run("versions are strictly ascending", func(t *testing.T) {
		t.Parallel()

		o := newOptions()
		migrations := o.migrations()

		require.NotEmpty(t, migrations)

		for i := 1; i < len(migrations); i++ {
			assert.Greater(t, migrations[i].version, migrations[i-1].version,
				"migration at index %d has version %d which is not greater than previous version %d",
				i, migrations[i].version, migrations[i-1].version)
		}
	})

	t.Run("each migration has at least one statement", func(t *testing.T) {
		t.Parallel()

		o := newOptions()

		for _, m := range o.migrations() {
			assert.NotEmpty(t, m.stmts, "migration version %d has no statements", m.version)
		}
	})

	t.Run("all versions are positive", func(t *testing.T) {
		t.Parallel()

		o := newOptions()

		for _, m := range o.migrations() {
			assert.Positive(t, m.version, "migration version must be positive, got %d", m.version)
		}
	})

	t.Run("migration 1 contains all four table names", func(t *testing.T) {
		t.Parallel()

		o := newOptions()
		migrations := o.migrations()

		require.NotEmpty(t, migrations)
		require.Equal(t, 1, migrations[0].version)

		var sb strings.Builder
		for _, stmt := range migrations[0].stmts {
			sb.WriteString(stmt)
		}
		combined := sb.String()

		assert.Contains(t, combined, o.issuesTable)
		assert.Contains(t, combined, o.alertsTable)
		assert.Contains(t, combined, o.moveMappingsTable)
		assert.Contains(t, combined, o.channelProcessingStateTable)
	})

	t.Run("custom table names are reflected in migration SQL", func(t *testing.T) {
		t.Parallel()

		o := &options{
			issuesTable:                 "custom_issues",
			alertsTable:                 "custom_alerts",
			moveMappingsTable:           "custom_move",
			channelProcessingStateTable: "custom_state",
		}

		migrations := o.migrations()
		require.NotEmpty(t, migrations)

		var sb strings.Builder
		for _, stmt := range migrations[0].stmts {
			sb.WriteString(stmt)
		}
		combined := sb.String()

		assert.Contains(t, combined, "custom_issues")
		assert.Contains(t, combined, "custom_alerts")
		assert.Contains(t, combined, "custom_move")
		assert.Contains(t, combined, "custom_state")
	})

	t.Run("migration 1 has 10 statements", func(t *testing.T) {
		t.Parallel()

		o := newOptions()
		migrations := o.migrations()

		require.NotEmpty(t, migrations)
		assert.Len(t, migrations[0].stmts, 10)
	})
}
