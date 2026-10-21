package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// copyCapturePool wraps a mock pool and records the rows passed to CopyFrom.
type copyCapturePool struct {
	pgxmock.PgxPoolIface

	table   pgx.Identifier
	columns []string
	rows    [][]any
	err     error
}

func (p *copyCapturePool) CopyFrom(_ context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	p.table, p.columns = tableName, columnNames

	for rowSrc.Next() {
		values, err := rowSrc.Values()
		if err != nil {
			return 0, err
		}

		p.rows = append(p.rows, values)
	}

	return int64(len(p.rows)), p.err
}

var insightsTestNow = time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC)

//nolint:ireturn // test helper
func newInsightsClient(t *testing.T, opts ...postgres.Option) (*postgres.Client, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	all := append([]postgres.Option{
		postgres.WithUser("testuser"),
		postgres.WithDatabase("testdb"),
		postgres.WithInsightsEnabled(),
		postgres.WithInsightsEventsTable("ins"),
		postgres.WithInsightsRetention(7 * 24 * time.Hour),
		postgres.SetNow(func() time.Time { return insightsTestNow }),
	}, opts...)

	client := postgres.New(&types.NoopLogger{}, all...)
	client.SetInsightsPool(mock)

	return client, mock
}

func TestInsightsProviders(t *testing.T) {
	t.Parallel()

	t.Run("nil when insights is disabled", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(&types.NoopLogger{}, postgres.WithUser("u"), postgres.WithDatabase("d"))

		assert.Nil(t, client.InsightsSink())
		assert.Nil(t, client.InsightsReader())
	})

	t.Run("non-nil when insights is enabled", func(t *testing.T) {
		t.Parallel()

		client, _ := newInsightsClient(t)

		assert.NotNil(t, client.InsightsSink())
		assert.NotNil(t, client.InsightsReader())
	})

	t.Run("client implements the provider interfaces", func(t *testing.T) {
		t.Parallel()

		var c any = postgres.New(&types.NoopLogger{})

		assert.Implements(t, (*types.InsightsSinkProvider)(nil), c)
		assert.Implements(t, (*types.InsightsReaderProvider)(nil), c)
	})
}

func TestWriteInsightsEvents(t *testing.T) {
	t.Parallel()

	t.Run("empty slice is a no-op even when not connected", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(&types.NoopLogger{}, postgres.WithInsightsEnabled())

		require.NoError(t, client.WriteInsightsEvents(context.Background(), nil))
	})

	t.Run("not connected", func(t *testing.T) {
		t.Parallel()

		client := postgres.New(&types.NoopLogger{}, postgres.WithInsightsEnabled())

		err := client.WriteInsightsEvents(context.Background(), []types.InsightsEvent{{Type: types.InsightsEventAlertQueued}})
		require.ErrorContains(t, err, "not connected")
	})

	t.Run("does not use the main pool", func(t *testing.T) {
		t.Parallel()

		client, _ := newInsightsClient(t)
		main, err := pgxmock.NewPool()
		require.NoError(t, err)
		client.SetPool(main)

		capture := &copyCapturePool{PgxPoolIface: main}
		client.SetInsightsPool(capture)

		require.NoError(t, client.WriteInsightsEvents(context.Background(), []types.InsightsEvent{{Type: types.InsightsEventAlertQueued}}))
		require.NoError(t, main.ExpectationsWereMet(), "main pool must not be called")
		assert.Len(t, capture.rows, 1)
	})

	t.Run("writes rows with NULL or JSON attrs", func(t *testing.T) {
		t.Parallel()

		client, mock := newInsightsClient(t)
		capture := &copyCapturePool{PgxPoolIface: mock}
		client.SetInsightsPool(capture)

		eventTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.FixedZone("x", 3600))

		err := client.WriteInsightsEvents(context.Background(), []types.InsightsEvent{
			{Time: eventTime, Type: types.InsightsEventAlertNewIssue, ChannelID: "C1"},
			{
				Time: eventTime, Type: types.InsightsEventIssueArchived, Reason: "r", ChannelID: "C1", RelatedChannelID: "C2",
				IssueID: "i", CorrelationID: "cor", Severity: types.AlertError, PrevSeverity: types.AlertWarning, RouteKey: "rk",
				AlertType: "at", Host: "h", Author: "a", ActorID: "U1", ActorName: "Name", Count: 3, Detail: "d",
				Values: map[string]float64{"open_s": 1.5},
			},
		})
		require.NoError(t, err)

		assert.Equal(t, pgx.Identifier{"ins"}, capture.table)
		assert.Equal(t, []string{
			"version", "occurred_at", "type", "reason", "channel_id", "related_channel_id", "issue_id",
			"correlation_id", "severity", "route_key", "actor_id", "attrs",
		}, capture.columns)
		require.Len(t, capture.rows, 2)

		minimal := capture.rows[0]
		assert.Equal(t, int16(postgres.InsightsEventModelVersion), minimal[0])
		minimalTime, ok := minimal[1].(time.Time)
		require.True(t, ok)
		assert.True(t, eventTime.Equal(minimalTime))
		assert.Equal(t, time.UTC, minimalTime.Location())
		assert.Equal(t, "alert.new_issue", minimal[2])
		assert.Equal(t, "C1", minimal[4])
		assert.Nil(t, minimal[11], "attrs must be SQL NULL when empty")

		full := capture.rows[1]
		assert.Equal(t, []any{"r", "C1", "C2", "i", "cor", "error", "rk", "U1"}, []any{full[3], full[4], full[5], full[6], full[7], full[8], full[9], full[10]})
		fullAttrs, ok := full[11].([]byte)
		require.True(t, ok)
		assert.JSONEq(t,
			`{"prevSeverity":"warning","alertType":"at","host":"h","author":"a","actorName":"Name","count":3,"detail":"d","values":{"open_s":1.5}}`,
			string(fullAttrs))
	})

	t.Run("wraps CopyFrom errors", func(t *testing.T) {
		t.Parallel()

		client, mock := newInsightsClient(t)
		client.SetInsightsPool(&copyCapturePool{PgxPoolIface: mock, err: errors.New("boom")})

		err := client.WriteInsightsEvents(context.Background(), []types.InsightsEvent{{Type: types.InsightsEventAlertQueued}})
		require.ErrorContains(t, err, "failed to write insights events")
		require.ErrorContains(t, err, "boom")
	})

	t.Run("returns an error for values that cannot be marshaled", func(t *testing.T) {
		t.Parallel()

		client, mock := newInsightsClient(t)
		client.SetInsightsPool(&copyCapturePool{PgxPoolIface: mock})

		err := client.WriteInsightsEvents(context.Background(), []types.InsightsEvent{
			{Type: types.InsightsEventChannelStats, Values: map[string]float64{"x": nan()}},
		})
		require.ErrorContains(t, err, "failed to marshal insights event attributes")
	})
}

func nan() float64 {
	zero := 0.0

	return zero / zero
}

// =============================================================================
// Partition maintenance
// =============================================================================

const (
	lockSQL     = "SELECT pg_try_advisory_xact_lock($1)"
	setLockSQL  = "SET LOCAL lock_timeout = '2s'"
	listPartSQL = "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass"
)

func expectLock(mock pgxmock.PgxPoolIface, acquired bool) {
	mock.ExpectBegin()
	mock.ExpectQuery(lockSQL).WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"locked"}).AddRow(acquired))
}

func createSQL(table string, day time.Time) string {
	return "CREATE TABLE IF NOT EXISTS " + table + "_p" + day.Format("20060102") + " PARTITION OF " + table +
		" FOR VALUES FROM ('" + day.Format("2006-01-02") + " 00:00:00+00') TO ('" + day.AddDate(0, 0, 1).Format("2006-01-02") + " 00:00:00+00')"
}

// expectCreatePartitionsAt expects the create step for the five days from yesterday to today plus three, relative to now.
func expectCreatePartitionsAt(mock pgxmock.PgxPoolIface, table string, now time.Time) {
	expectLock(mock, true)
	mock.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))

	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	for i := -1; i <= 3; i++ {
		mock.ExpectExec(createSQL(table, today.AddDate(0, 0, i))).WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
	}
}

func expectCreatePartitions(mock pgxmock.PgxPoolIface) {
	expectCreatePartitionsAt(mock, "ins", insightsTestNow)
}

func expectListPartitions(mock pgxmock.PgxPoolIface, names ...string) {
	expectLock(mock, true)
	mock.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))

	rows := pgxmock.NewRows([]string{"relname"})
	for _, n := range names {
		rows.AddRow(n)
	}

	mock.ExpectQuery(listPartSQL).WithArgs("ins").WillReturnRows(rows)
}

func TestMaintainInsightsPartitions_NotConnected(t *testing.T) {
	t.Parallel()

	client := postgres.New(&types.NoopLogger{}, postgres.WithInsightsEnabled())

	require.ErrorContains(t, client.MaintainInsightsPartitions(context.Background()), "not connected")
}

func TestMaintainInsightsPartitions_Create(t *testing.T) {
	t.Parallel()

	t.Run("creates partitions from yesterday to today plus three", func(t *testing.T) {
		t.Parallel()

		client, mock := newInsightsClient(t)

		expectCreatePartitions(mock)
		mock.ExpectCommit()
		expectListPartitions(mock)
		mock.ExpectCommit()

		require.NoError(t, client.MaintainInsightsPartitions(context.Background()))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does nothing when the advisory lock is not acquired", func(t *testing.T) {
		t.Parallel()

		client, mock := newInsightsClient(t)

		for range 2 { // create step and drop step
			expectLock(mock, false)
			mock.ExpectCommit()
		}

		require.NoError(t, client.MaintainInsightsPartitions(context.Background()))
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMaintainInsightsPartitions_Drop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		now       time.Time
		retention time.Duration
		existing  []string
		dropped   []string
	}{
		{
			name:      "drops partitions whose whole day is older than the retention",
			now:       insightsTestNow, // cutoff 2026-03-03 15:00
			retention: 7 * 24 * time.Hour,
			existing:  []string{"ins_p20260301", "ins_p20260302", "ins_p20260303", "ins_p20260310"},
			dropped:   []string{"ins_p20260301", "ins_p20260302"},
		},
		{
			name:      "upper bound equal to the cutoff is dropped",
			now:       time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC), // cutoff 2026-03-03 00:00
			retention: 7 * 24 * time.Hour,
			existing:  []string{"ins_p20260302", "ins_p20260303"},
			dropped:   []string{"ins_p20260302"},
		},
		{
			name:      "ignores names that are not managed partitions",
			now:       insightsTestNow,
			retention: 24 * time.Hour,
			existing:  []string{"ins_pfoo", "other_p20200101", "ins_p2026030", "ins_p20261399", "ins_p20200101_x", "INS_P20200101"},
			dropped:   nil,
		},
		{
			name:      "nothing to drop",
			now:       insightsTestNow,
			retention: 30 * 24 * time.Hour,
			existing:  []string{"ins_p20260309"},
			dropped:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := tt.now
			client, mock := newInsightsClient(t, postgres.WithInsightsRetention(tt.retention), postgres.SetNow(func() time.Time { return now }))

			expectCreatePartitionsAt(mock, "ins", now)
			mock.ExpectCommit()

			expectListPartitions(mock, tt.existing...)

			for _, name := range tt.dropped {
				mock.ExpectExec("DROP TABLE IF EXISTS " + name).WillReturnResult(pgxmock.NewResult("DROP TABLE", 0))
			}

			mock.ExpectCommit()

			require.NoError(t, client.MaintainInsightsPartitions(context.Background()))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMaintainInsightsPartitions_UppercaseTableName(t *testing.T) {
	t.Parallel()

	client, mock := newInsightsClient(t, postgres.WithInsightsEventsTable("MyIns"))

	expectCreatePartitionsAt(mock, "MyIns", insightsTestNow)
	mock.ExpectCommit()

	// PostgreSQL folds the unquoted name to lower case.
	expectLock(mock, true)
	mock.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
	mock.ExpectQuery(listPartSQL).WithArgs("MyIns").WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow("myins_p20200101"))
	mock.ExpectExec("DROP TABLE IF EXISTS myins_p20200101").WillReturnResult(pgxmock.NewResult("", 0))
	mock.ExpectCommit()

	require.NoError(t, client.MaintainInsightsPartitions(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMaintainInsightsPartitions_Errors(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")

	tests := []struct {
		name    string
		setup   func(mock pgxmock.PgxPoolIface) // expectations for the failing step; the other step is expected to succeed or lock-skip
		wantErr string
		step    string // "create" or "drop"
	}{
		{
			name:    "create: begin fails",
			step:    "create",
			setup:   func(m pgxmock.PgxPoolIface) { m.ExpectBegin().WillReturnError(boom) },
			wantErr: "failed to begin insights maintenance transaction",
		},
		{
			name: "create: lock query fails",
			step: "create",
			setup: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectQuery(lockSQL).WithArgs(pgxmock.AnyArg()).WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to acquire insights partition lock",
		},
		{
			name: "create: commit after lock not acquired fails",
			step: "create",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, false)
				m.ExpectCommit().WillReturnError(boom)
			},
			wantErr: "failed to commit insights maintenance transaction",
		},
		{
			name: "create: set lock timeout fails",
			step: "create",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, true)
				m.ExpectExec(setLockSQL).WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to set insights lock timeout",
		},
		{
			name: "create: create partition fails",
			step: "create",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, true)
				m.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
				m.ExpectExec(createSQL("ins", insightsTestNow.AddDate(0, 0, -1).Truncate(24*time.Hour))).WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to create insights partition for 2026-03-09",
		},
		{
			name: "create: commit fails",
			step: "create",
			setup: func(m pgxmock.PgxPoolIface) {
				expectCreatePartitions(m)
				m.ExpectCommit().WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to commit insights partition creation",
		},
		{
			name: "drop: list fails",
			step: "drop",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, true)
				m.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
				m.ExpectQuery(listPartSQL).WithArgs("ins").WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to list insights partitions",
		},
		{
			name: "drop: scan fails",
			step: "drop",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, true)
				m.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
				m.ExpectQuery(listPartSQL).WithArgs("ins").WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow("x").RowError(0, boom))
				m.ExpectRollback()
			},
			wantErr: "failed to scan insights partition name",
		},
		{
			name: "drop: rows error",
			step: "drop",
			setup: func(m pgxmock.PgxPoolIface) {
				expectLock(m, true)
				m.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
				m.ExpectQuery(listPartSQL).WithArgs("ins").WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow("ins_p20200101").CloseError(boom))
				m.ExpectRollback()
			},
			wantErr: "failed to list insights partitions",
		},
		{
			name: "drop: drop fails",
			step: "drop",
			setup: func(m pgxmock.PgxPoolIface) {
				expectListPartitions(m, "ins_p20200101")
				m.ExpectExec("DROP TABLE IF EXISTS ins_p20200101").WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to drop insights partition ins_p20200101",
		},
		{
			name: "drop: commit fails",
			step: "drop",
			setup: func(m pgxmock.PgxPoolIface) {
				expectListPartitions(m)
				m.ExpectCommit().WillReturnError(boom)
				m.ExpectRollback()
			},
			wantErr: "failed to commit insights partition drop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, mock := newInsightsClient(t)

			if tt.step == "create" {
				tt.setup(mock)
				// The drop step still runs after a failed create step.
				expectLock(mock, false)
				mock.ExpectCommit()
			} else {
				expectCreatePartitions(mock)
				mock.ExpectCommit()
				tt.setup(mock)
			}

			err := client.MaintainInsightsPartitions(context.Background())
			require.ErrorContains(t, err, tt.wantErr)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// =============================================================================
// Init and Close
// =============================================================================

func TestInit_InsightsMaintenance(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) (*postgres.Client, pgxmock.PgxPoolIface, pgxmock.PgxPoolIface) {
		t.Helper()

		main, err := pgxmock.NewPool()
		require.NoError(t, err)

		insights, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
		require.NoError(t, err)

		client := postgres.New(&types.NoopLogger{},
			postgres.WithUser("testuser"), postgres.WithDatabase("testdb"),
			postgres.WithTTLCleanupDisabled(), postgres.WithInsightsEnabled(),
			postgres.SetNow(func() time.Time { return insightsTestNow }),
		)
		client.SetPool(main)
		client.SetInsightsPool(insights)

		return client, main, insights
	}

	t.Run("runs maintenance after migrations, starts goroutine and Close stops it and closes both pools", func(t *testing.T) {
		t.Parallel()

		client, main, insights := newClient(t)

		expectMigrations(main)

		expectCreatePartitionsAt(insights, "insights_events", insightsTestNow)
		insights.ExpectCommit()
		expectLock(insights, true)
		insights.ExpectExec(setLockSQL).WillReturnResult(pgxmock.NewResult("SET", 0))
		insights.ExpectQuery(listPartSQL).WithArgs("insights_events").WillReturnRows(pgxmock.NewRows([]string{"relname"}))
		insights.ExpectCommit()

		require.NoError(t, client.Init(context.Background(), false))
		assert.True(t, client.HasActiveInsightsMaintenance())

		// A second Init does not start another goroutine (and runs maintenance again).
		expectMigrationsApplied(main)
		expectLock(insights, false)
		insights.ExpectCommit()
		expectLock(insights, false)
		insights.ExpectCommit()
		require.NoError(t, client.Init(context.Background(), false))

		main.ExpectClose()
		insights.ExpectClose()

		require.NoError(t, client.Close(context.Background()))
		assert.False(t, client.HasActiveInsightsMaintenance())
		require.NoError(t, main.ExpectationsWereMet())
		require.NoError(t, insights.ExpectationsWereMet())
	})

	t.Run("maintenance errors do not fail Init", func(t *testing.T) {
		t.Parallel()

		client, main, insights := newClient(t)

		expectMigrations(main)
		insights.ExpectBegin().WillReturnError(errors.New("boom"))
		insights.ExpectBegin().WillReturnError(errors.New("boom"))

		require.NoError(t, client.Init(context.Background(), false))
		assert.True(t, client.HasActiveInsightsMaintenance())

		main.ExpectClose()
		insights.ExpectClose()
		require.NoError(t, client.Close(context.Background()))
	})

	t.Run("missing insights pool does not fail Init", func(t *testing.T) {
		t.Parallel()

		client, main, _ := newClient(t)
		client.SetInsightsPool(nil)

		expectMigrations(main)

		require.NoError(t, client.Init(context.Background(), false))

		main.ExpectClose()
		require.NoError(t, client.Close(context.Background()))
	})

	t.Run("no maintenance when insights is disabled", func(t *testing.T) {
		t.Parallel()

		client, mock := newClientWithMockAndTTL(t)
		expectMigrations(mock)

		require.NoError(t, client.Init(context.Background(), false))
		assert.False(t, client.HasActiveInsightsMaintenance())

		mock.ExpectClose()
		require.NoError(t, client.Close(context.Background()))
	})
}

// expectMigrationsApplied sets up expectations for an Init() where all migrations are already applied.
func expectMigrationsApplied(mock pgxmock.PgxPoolIface) {
	mock.ExpectExec("").WillReturnResult(pgxmock.NewResult("", 0))

	for range 4 {
		mock.ExpectBegin()
		mock.ExpectExec("pg_advisory_xact_lock").WithArgs(pgxmock.AnyArg()).WillReturnResult(pgxmock.NewResult("", 0))
		mock.ExpectQuery("SELECT EXISTS").WithArgs(pgxmock.AnyArg()).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectCommit()
	}
}
