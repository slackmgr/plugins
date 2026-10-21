package postgres_test

import (
	"strings"
	"testing"
	"time"

	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate_InsightsOptions(t *testing.T) {
	t.Parallel()

	base := []postgres.Option{postgres.WithUser("u"), postgres.WithDatabase("d")}

	tests := []struct {
		name    string
		opt     postgres.Option
		wantErr string
	}{
		{name: "defaults are valid", opt: postgres.WithInsightsEnabled()},
		{name: "table name 40 chars", opt: postgres.WithInsightsEventsTable(strings.Repeat("a", 40))},
		{name: "table name 41 chars", opt: postgres.WithInsightsEventsTable(strings.Repeat("a", 41)), wantErr: "too long"},
		{name: "table name invalid characters", opt: postgres.WithInsightsEventsTable("a-b"), wantErr: "invalid insights events table name"},
		{name: "table name empty", opt: postgres.WithInsightsEventsTable(""), wantErr: "invalid insights events table name"},
		{name: "retention 24h", opt: postgres.WithInsightsRetention(24 * time.Hour)},
		{name: "retention below 24h", opt: postgres.WithInsightsRetention(23 * time.Hour), wantErr: "retention must be at least"},
		{name: "retention zero", opt: postgres.WithInsightsRetention(0), wantErr: "retention must be at least"},
		{name: "pool max connections 1", opt: postgres.WithInsightsPoolMaxConnections(1)},
		{name: "pool max connections 0", opt: postgres.WithInsightsPoolMaxConnections(0), wantErr: "pool max connections"},
		{name: "statement timeout", opt: postgres.WithInsightsStatementTimeout(time.Second)},
		{name: "statement timeout zero", opt: postgres.WithInsightsStatementTimeout(0), wantErr: "statement timeout"},
		{name: "read statement timeout", opt: postgres.WithInsightsReadStatementTimeout(time.Second)},
		{name: "read statement timeout negative", opt: postgres.WithInsightsReadStatementTimeout(-1), wantErr: "read statement timeout"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := postgres.ExportValidate(append([]postgres.Option{tt.opt}, base...)...)

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestInsightsEventModelVersion(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1, postgres.InsightsEventModelVersion)
}
