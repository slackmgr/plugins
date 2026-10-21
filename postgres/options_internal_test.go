package postgres

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/slackmgr/types"
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

	t.Run("migration 3 adds full channel processing state columns", func(t *testing.T) {
		t.Parallel()

		o := newOptions()
		migrations := o.migrations()

		require.GreaterOrEqual(t, len(migrations), 3)
		assert.Equal(t, 3, migrations[2].version)

		combined := strings.Join(migrations[2].stmts, "\n")
		assert.Contains(t, combined, "channel_processing_state ADD COLUMN IF NOT EXISTS last_channel_activity")
		assert.Contains(t, combined, "channel_processing_state ADD COLUMN IF NOT EXISTS open_issues")
	})
}

func TestMigration4_InsightsEvents(t *testing.T) {
	t.Parallel()

	o := newOptions()
	o.insightsEventsTable = "my_events"

	migrations := o.migrations()
	require.Len(t, migrations, 4)
	require.Equal(t, 4, migrations[3].version)
	require.Len(t, migrations[3].stmts, 4)

	create := migrations[3].stmts[0]
	assert.Contains(t, create, "CREATE TABLE IF NOT EXISTS my_events (id BIGINT GENERATED ALWAYS AS IDENTITY, version SMALLINT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL")
	assert.Contains(t, create, "attrs JSONB NULL) PARTITION BY RANGE (occurred_at);")

	assert.Equal(t, "CREATE INDEX IF NOT EXISTS my_events_time_brin ON my_events USING BRIN (occurred_at);", migrations[3].stmts[1])
	assert.Equal(t, "CREATE INDEX IF NOT EXISTS my_events_chan_type_time_idx ON my_events (channel_id, type, occurred_at);", migrations[3].stmts[2])
	assert.Equal(t, "CREATE INDEX IF NOT EXISTS my_events_issue_time_idx ON my_events (issue_id, occurred_at) WHERE issue_id <> '';", migrations[3].stmts[3])
}

func TestInsightsDefaults(t *testing.T) {
	t.Parallel()

	o := newOptions()

	assert.False(t, o.insightsEnabled)
	assert.Equal(t, "insights_events", o.insightsEventsTable)
	assert.Equal(t, 30*24*time.Hour, o.insightsRetention)
	assert.Equal(t, int32(2), o.insightsPoolMaxConnections)
	assert.Equal(t, 5*time.Second, o.insightsStatementTimeout)
	assert.Equal(t, 15*time.Second, o.insightsReadStatementTimeout)
	assert.WithinDuration(t, time.Now(), o.now(), time.Minute)
}

func TestMarshalInsightsAttrs(t *testing.T) {
	t.Parallel()

	t.Run("empty attributes are NULL", func(t *testing.T) {
		t.Parallel()

		attrs, err := marshalInsightsAttrs(&types.InsightsEvent{Type: types.InsightsEventAlertQueued, ChannelID: "C1", Reason: "r", Severity: types.AlertError, ActorID: "U"})
		require.NoError(t, err)
		assert.Nil(t, attrs)
	})

	for name, e := range map[string]types.InsightsEvent{
		"prevSeverity": {PrevSeverity: types.AlertInfo},
		"alertType":    {AlertType: "x"},
		"host":         {Host: "x"},
		"author":       {Author: "x"},
		"actorName":    {ActorName: "x"},
		"count":        {Count: 1},
		"detail":       {Detail: "x"},
		"values":       {Values: map[string]float64{"a": 1}},
	} {
		t.Run("only "+name, func(t *testing.T) {
			t.Parallel()

			attrs, err := marshalInsightsAttrs(&e)
			require.NoError(t, err)

			var m map[string]any
			require.NoError(t, json.Unmarshal(attrs, &m))
			assert.Len(t, m, 1)
			assert.Contains(t, m, name)
		})
	}

	t.Run("empty values map is NULL", func(t *testing.T) {
		t.Parallel()

		attrs, err := marshalInsightsAttrs(&types.InsightsEvent{Values: map[string]float64{}})
		require.NoError(t, err)
		assert.Nil(t, attrs)
	})
}

func TestInsightsPartitionHelpers(t *testing.T) {
	t.Parallel()

	o := newOptions()
	o.insightsEventsTable = "Ev"

	assert.Equal(t, "Ev_p20260102", o.insightsPartitionName(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), utcDay(time.Date(2026, 1, 2, 23, 59, 0, 0, time.UTC)))
	assert.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), utcDay(time.Date(2026, 1, 3, 1, 0, 0, 0, time.FixedZone("x", 7200))))

	re := insightsPartitionRegex("Ev")
	assert.NotNil(t, re.FindStringSubmatch("ev_p20260102"))
	assert.Nil(t, re.FindStringSubmatch("evxp20260102"))
	assert.Nil(t, re.FindStringSubmatch("ev_p20260102_x"))
}

func TestInsightsSQLWhitelists(t *testing.T) {
	t.Parallel()

	assert.Empty(t, dimensionExpr("bogus", ""))
	assert.Empty(t, aggregateExpr("bogus", "v"))

	var args sqlArgs

	_, err := groupBy([]types.InsightsDimension{"bogus"}, 0, &args)
	require.ErrorContains(t, err, "invalid dimension")
}
