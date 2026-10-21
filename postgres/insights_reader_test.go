package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	qFrom = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	qTo   = time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
)

const (
	baseWhere  = " FROM ins WHERE occurred_at >= $1 AND occurred_at < $2"
	listSelect = "SELECT id, occurred_at, type, reason, channel_id, related_channel_id, issue_id, correlation_id, severity, route_key, actor_id, attrs FROM ins WHERE "
)

//nolint:ireturn // test helper
func newReader(t *testing.T) (types.InsightsReader, pgxmock.PgxPoolIface) {
	t.Helper()

	client, mock := newInsightsClient(t)

	return client.InsightsReader(), mock
}

func filter() types.InsightsFilter { return types.InsightsFilter{From: qFrom, To: qTo} }

func TestInsightsReader_NotConnected(t *testing.T) {
	t.Parallel()

	client := postgres.New(&types.NoopLogger{}, postgres.WithInsightsEnabled())
	standalone := postgres.NewInsightsReader(&types.NoopLogger{})
	ctx := context.Background()

	for name, reader := range map[string]types.InsightsReader{"client": client.InsightsReader(), "standalone": standalone} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := reader.CountInsightsEvents(ctx, &types.InsightsCountQuery{InsightsFilter: filter()})
			require.ErrorContains(t, err, "not connected")

			_, err = reader.InsightsValueStats(ctx, &types.InsightsStatsQuery{InsightsFilter: filter(), ValueKey: "k", Aggregates: []types.InsightsAggregate{types.InsightsAggregateSum}})
			require.ErrorContains(t, err, "not connected")

			_, err = reader.ListInsightsEvents(ctx, &types.InsightsListQuery{InsightsFilter: filter()})
			require.ErrorContains(t, err, "not connected")

			_, err = reader.LoadOpenIssues(ctx, nil)
			require.ErrorContains(t, err, "not connected")
		})
	}

	require.ErrorContains(t, standalone.CheckSchema(ctx), "not connected")
}

func TestInsightsReader_NilAndInvalidQueries(t *testing.T) {
	t.Parallel()

	reader, mock := newReader(t)
	ctx := context.Background()

	_, err := reader.CountInsightsEvents(ctx, nil)
	require.Error(t, err)

	_, err = reader.InsightsValueStats(ctx, nil)
	require.Error(t, err)

	_, err = reader.ListInsightsEvents(ctx, nil)
	require.Error(t, err)

	_, err = reader.CountInsightsEvents(ctx, &types.InsightsCountQuery{})
	require.ErrorContains(t, err, "from and to are required")

	_, err = reader.InsightsValueStats(ctx, &types.InsightsStatsQuery{InsightsFilter: filter()})
	require.ErrorContains(t, err, "invalid value key")

	_, err = reader.ListInsightsEvents(ctx, &types.InsightsListQuery{InsightsFilter: filter(), Limit: -1})
	require.ErrorContains(t, err, "limit")

	require.NoError(t, mock.ExpectationsWereMet(), "no query must reach the database")
}

func TestCountInsightsEvents(t *testing.T) {
	t.Parallel()

	t.Run("total", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery("SELECT COUNT(*)"+baseWhere).WithArgs(qFrom, qTo).WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(5)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{InsightsFilter: filter()})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(5), rows[0].Count)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("all filters", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		f := filter()
		f.ChannelIDs = []string{"C1"}
		f.RelatedChannelIDs = []string{"C2"}
		f.Types = []types.InsightsEventType{types.InsightsEventAlertQueued}
		f.Reasons = []string{"r"}
		f.Severities = []types.AlertSeverity{types.AlertError}
		f.RouteKeys = []string{"rk"}
		f.IssueID, f.CorrelationID, f.ActorID = "i", "c", "a"

		mock.ExpectQuery("SELECT COUNT(*)"+baseWhere+
			" AND channel_id = ANY($3) AND related_channel_id = ANY($4) AND type = ANY($5) AND reason = ANY($6)"+
			" AND severity = ANY($7) AND route_key = ANY($8) AND issue_id = $9 AND correlation_id = $10 AND actor_id = $11").
			WithArgs(qFrom, qTo, []string{"C1"}, []string{"C2"}, []string{"alert.queued"}, []string{"r"}, []string{"error"}, []string{"rk"}, "i", "c", "a").
			WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(0)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{InsightsFilter: f})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Zero(t, rows[0].Count)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("bucket and channel", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		bucket := time.Date(2026, 3, 1, 0, 15, 0, 0, time.FixedZone("x", 3600))

		mock.ExpectQuery("SELECT to_timestamp(floor(extract(epoch FROM occurred_at)::float8 / $1::float8) * $1::float8), channel_id, COUNT(*)"+
			" FROM ins WHERE occurred_at >= $2 AND occurred_at < $3 GROUP BY 1, 2 ORDER BY 1, 2 LIMIT $4").
			WithArgs(900.0, qFrom, qTo, 10001).
			WillReturnRows(pgxmock.NewRows([]string{"b", "c", "n"}).AddRow(bucket, "C1", int64(3)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter: filter(),
			InsightsGrouping: types.InsightsGrouping{
				GroupBy: []types.InsightsDimension{types.InsightsDimensionBucket, types.InsightsDimensionChannel},
				Bucket:  15 * time.Minute,
			},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.True(t, bucket.Equal(rows[0].Bucket))
		assert.Equal(t, time.UTC, rows[0].Bucket.Location())
		assert.Equal(t, "C1", rows[0].ChannelID)
		assert.Equal(t, int64(3), rows[0].Count)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("distinct, ordered by count with limit", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		mock.ExpectQuery("SELECT issue_id, COUNT(DISTINCT issue_id) FILTER (WHERE issue_id <> '')"+baseWhere+" GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT $3").
			WithArgs(qFrom, qTo, 6).
			WillReturnRows(pgxmock.NewRows([]string{"i", "n"}).AddRow("i1", int64(4)).AddRow("i2", int64(2)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter:   filter(),
			InsightsGrouping: types.InsightsGrouping{GroupBy: []types.InsightsDimension{types.InsightsDimensionIssue}, Limit: 5},
			Distinct:         types.InsightsDimensionIssue,
			OrderByCountDesc: true,
		})
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, "i1", rows[0].IssueID)
		assert.Equal(t, int64(2), rows[1].Count)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("every dimension is mapped to the right column and key field", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		mock.ExpectQuery("SELECT to_timestamp(floor(extract(epoch FROM occurred_at)::float8 / $1::float8) * $1::float8), channel_id, related_channel_id, type, reason, severity, route_key, "+
			"COALESCE(attrs->>'alertType', ''), COALESCE(attrs->>'host', ''), COALESCE(attrs->>'author', ''), actor_id, correlation_id, issue_id, COUNT(*)"+
			" FROM ins WHERE occurred_at >= $2 AND occurred_at < $3 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 LIMIT $4").
			WithArgs(60.0, qFrom, qTo, 10001).
			WillReturnRows(pgxmock.NewRows(make([]string, 14)).AddRow(qFrom, "channel", "related", "type", "reason", "warning", "route", "alertType", "host", "author", "actor", "corr", "issue", int64(1)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter: filter(),
			InsightsGrouping: types.InsightsGrouping{
				Bucket: time.Minute,
				GroupBy: []types.InsightsDimension{
					types.InsightsDimensionBucket, types.InsightsDimensionChannel, types.InsightsDimensionRelatedChannel, types.InsightsDimensionType,
					types.InsightsDimensionReason, types.InsightsDimensionSeverity, types.InsightsDimensionRouteKey, types.InsightsDimensionAlertType,
					types.InsightsDimensionHost, types.InsightsDimensionAuthor, types.InsightsDimensionActor, types.InsightsDimensionCorrelationID,
					types.InsightsDimensionIssue,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, types.InsightsCountRow{
			InsightsGroupKey: types.InsightsGroupKey{
				Bucket: qFrom, ChannelID: "channel", RelatedChannelID: "related", Type: "type", Reason: "reason", Severity: types.AlertWarning,
				RouteKey: "route", AlertType: "alertType", Host: "host", Author: "author", ActorID: "actor", CorrelationID: "corr", IssueID: "issue",
			},
			Count: 1,
		}, *rows[0])
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("boom")
		query := &types.InsightsCountQuery{InsightsFilter: filter()}

		tests := map[string]struct {
			expect  func(e *pgxmock.ExpectedQuery)
			wantErr string
		}{
			"query": {func(e *pgxmock.ExpectedQuery) { e.WillReturnError(boom) }, "failed to query insights counts"},
			"scan": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow("not a number"))
			}, "failed to scan insights count row"},
			"rows": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(int64(1)).CloseError(boom))
			}, "failed to query insights counts"},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				reader, mock := newReader(t)
				tt.expect(mock.ExpectQuery("SELECT COUNT(*)"+baseWhere).WithArgs(qFrom, qTo))

				_, err := reader.CountInsightsEvents(context.Background(), query)
				require.ErrorContains(t, err, tt.wantErr)
			})
		}
	})
}

func TestInsightsValueStats(t *testing.T) {
	t.Parallel()

	value := "(attrs->'values'->>$1::text)::float8"
	where := " FROM ins WHERE occurred_at >= $2 AND occurred_at < $3 AND attrs->'values' ? $1::text"
	allAggs := []types.InsightsAggregate{
		types.InsightsAggregateSum, types.InsightsAggregateAvg, types.InsightsAggregateMin, types.InsightsAggregateMax,
		types.InsightsAggregateP50, types.InsightsAggregateP90, types.InsightsAggregateP99,
	}

	t.Run("all aggregates, ungrouped, duplicates removed", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		mock.ExpectQuery("SELECT COUNT(*), SUM("+value+"), AVG("+value+"), MIN("+value+"), MAX("+value+"), "+
			"percentile_cont(0.5) WITHIN GROUP (ORDER BY "+value+"), percentile_cont(0.9) WITHIN GROUP (ORDER BY "+value+"), "+
			"percentile_cont(0.99) WITHIN GROUP (ORDER BY "+value+")"+where).
			WithArgs("open_s", qFrom, qTo).
			WillReturnRows(pgxmock.NewRows(make([]string, 8)).AddRow(int64(10), 550.0, 55.0, 10.0, 100.0, 55.0, 91.0, 99.1))

		rows, err := reader.InsightsValueStats(context.Background(), &types.InsightsStatsQuery{
			InsightsFilter: filter(), ValueKey: "open_s", Aggregates: append(allAggs, types.InsightsAggregateSum),
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, int64(10), rows[0].Count)
		assert.Equal(t, map[types.InsightsAggregate]float64{
			types.InsightsAggregateSum: 550, types.InsightsAggregateAvg: 55, types.InsightsAggregateMin: 10, types.InsightsAggregateMax: 100,
			types.InsightsAggregateP50: 55, types.InsightsAggregateP90: 91, types.InsightsAggregateP99: 99.1,
		}, rows[0].Values)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("grouped by bucket", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		bv := "(attrs->'values'->>$2::text)::float8"
		mock.ExpectQuery("SELECT to_timestamp(floor(extract(epoch FROM occurred_at)::float8 / $1::float8) * $1::float8), reason, COUNT(*), MAX("+bv+")"+
			" FROM ins WHERE occurred_at >= $3 AND occurred_at < $4 AND attrs->'values' ? $2::text GROUP BY 1, 2 ORDER BY 1, 2 LIMIT $5").
			WithArgs(3600.0, "open_s", qFrom, qTo, 10001).
			WillReturnRows(pgxmock.NewRows(make([]string, 4)).AddRow(qFrom, "auto_resolved", int64(2), 7.0))

		rows, err := reader.InsightsValueStats(context.Background(), &types.InsightsStatsQuery{
			InsightsFilter: filter(), ValueKey: "open_s", Aggregates: []types.InsightsAggregate{types.InsightsAggregateMax},
			InsightsGrouping: types.InsightsGrouping{GroupBy: []types.InsightsDimension{types.InsightsDimensionBucket, types.InsightsDimensionReason}, Bucket: time.Hour},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "auto_resolved", rows[0].Reason)
		assert.True(t, qFrom.Equal(rows[0].Bucket))
		assert.InDelta(t, 7.0, rows[0].Values[types.InsightsAggregateMax], 0.0001)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ungrouped with no matching events returns no rows", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery("SELECT COUNT(*), SUM("+value+")"+where).WithArgs("k", qFrom, qTo).
			WillReturnRows(pgxmock.NewRows([]string{"n", "s"}).AddRow(int64(0), nil))

		rows, err := reader.InsightsValueStats(context.Background(), &types.InsightsStatsQuery{
			InsightsFilter: filter(), ValueKey: "k", Aggregates: []types.InsightsAggregate{types.InsightsAggregateSum},
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("NULL aggregates are omitted", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery("SELECT COUNT(*), SUM("+value+")"+where).WithArgs("k", qFrom, qTo).
			WillReturnRows(pgxmock.NewRows([]string{"n", "s"}).AddRow(int64(1), nil))

		rows, err := reader.InsightsValueStats(context.Background(), &types.InsightsStatsQuery{
			InsightsFilter: filter(), ValueKey: "k", Aggregates: []types.InsightsAggregate{types.InsightsAggregateSum},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Empty(t, rows[0].Values)
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("boom")
		query := &types.InsightsStatsQuery{InsightsFilter: filter(), ValueKey: "k", Aggregates: []types.InsightsAggregate{types.InsightsAggregateSum}}
		sql := "SELECT COUNT(*), SUM(" + value + ")" + where

		tests := map[string]struct {
			expect  func(e *pgxmock.ExpectedQuery)
			wantErr string
		}{
			"query": {func(e *pgxmock.ExpectedQuery) { e.WillReturnError(boom) }, "failed to query insights stats"},
			"scan": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"n", "s"}).AddRow("x", 1.0))
			}, "failed to scan insights stats row"},
			"rows": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"n", "s"}).AddRow(int64(1), 1.0).CloseError(boom))
			}, "failed to query insights stats"},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				reader, mock := newReader(t)
				tt.expect(mock.ExpectQuery(sql).WithArgs("k", qFrom, qTo))

				_, err := reader.InsightsValueStats(context.Background(), query)
				require.ErrorContains(t, err, tt.wantErr)
			})
		}
	})
}

func TestInsightsReader_RowLimit(t *testing.T) {
	t.Parallel()

	issueRows := func(n int) *pgxmock.Rows {
		rows := pgxmock.NewRows([]string{"i", "n"})
		for i := range n {
			rows.AddRow(fmt.Sprintf("i%d", i), int64(1))
		}

		return rows
	}

	countSQL := "SELECT issue_id, COUNT(*)" + baseWhere + " GROUP BY 1 ORDER BY 1 LIMIT $3"
	byIssue := []types.InsightsDimension{types.InsightsDimensionIssue}

	t.Run("explicit limit drops the extra row", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery(countSQL).WithArgs(qFrom, qTo, 3).WillReturnRows(issueRows(3))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter: filter(), InsightsGrouping: types.InsightsGrouping{GroupBy: byIssue, Limit: 2},
		})
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, "i1", rows[1].IssueID)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("count without limit exactly at the maximum", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery(countSQL).WithArgs(qFrom, qTo, types.MaxInsightsRows+1).WillReturnRows(issueRows(types.MaxInsightsRows))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter: filter(), InsightsGrouping: types.InsightsGrouping{GroupBy: byIssue},
		})
		require.NoError(t, err)
		assert.Len(t, rows, types.MaxInsightsRows)
	})

	t.Run("count without limit over the maximum", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery(countSQL).WithArgs(qFrom, qTo, types.MaxInsightsRows+1).WillReturnRows(issueRows(types.MaxInsightsRows + 1))

		_, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{
			InsightsFilter: filter(), InsightsGrouping: types.InsightsGrouping{GroupBy: byIssue},
		})
		require.ErrorIs(t, err, types.ErrInsightsTooManyRows)
	})

	t.Run("stats without limit over the maximum", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		rows := pgxmock.NewRows([]string{"i", "n", "s"})
		for i := range types.MaxInsightsRows + 1 {
			rows.AddRow(fmt.Sprintf("i%d", i), int64(1), 1.0)
		}

		mock.ExpectQuery("SELECT issue_id, COUNT(*), SUM((attrs->'values'->>$1::text)::float8)"+
			" FROM ins WHERE occurred_at >= $2 AND occurred_at < $3 AND attrs->'values' ? $1::text GROUP BY 1 ORDER BY 1 LIMIT $4").
			WithArgs("k", qFrom, qTo, types.MaxInsightsRows+1).WillReturnRows(rows)

		_, err := reader.InsightsValueStats(context.Background(), &types.InsightsStatsQuery{
			InsightsFilter: filter(), InsightsGrouping: types.InsightsGrouping{GroupBy: byIssue},
			ValueKey: "k", Aggregates: []types.InsightsAggregate{types.InsightsAggregateSum},
		})
		require.ErrorIs(t, err, types.ErrInsightsTooManyRows)
	})
}

func TestListInsightsEvents(t *testing.T) {
	t.Parallel()

	cols := []string{"id", "occurred_at", "type", "reason", "channel_id", "related_channel_id", "issue_id", "correlation_id", "severity", "route_key", "actor_id", "attrs"}
	t1 := time.Date(2026, 3, 1, 10, 0, 0, 123456000, time.FixedZone("x", 7200))
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)

	attrs, err := json.Marshal(map[string]any{
		"prevSeverity": "warning", "alertType": "at", "host": "h", "author": "a", "actorName": "n", "count": 4, "detail": "d",
		"values": map[string]float64{"open_s": 2},
	})
	require.NoError(t, err)

	t.Run("ascending without cursor, single page", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)

		mock.ExpectQuery(listSelect+"occurred_at >= $1 AND occurred_at < $2 ORDER BY occurred_at ASC, id ASC LIMIT $3").
			WithArgs(qFrom, qTo, 101).
			WillReturnRows(pgxmock.NewRows(cols).
				AddRow(int64(1), t1, "alert.queued", "", "C1", "", "", "", "", "", "", []byte(nil)).
				AddRow(int64(2), t2, "issue.archived", "r", "C1", "C2", "i", "c", "error", "rk", "U1", attrs))

		page, err := reader.ListInsightsEvents(context.Background(), &types.InsightsListQuery{InsightsFilter: filter()})
		require.NoError(t, err)
		require.Len(t, page.Events, 2)
		assert.Empty(t, page.NextCursor)

		first := page.Events[0]
		assert.True(t, t1.Equal(first.Time))
		assert.Equal(t, time.UTC, first.Time.Location())
		assert.Equal(t, types.InsightsEvent{Time: first.Time, Type: types.InsightsEventAlertQueued, ChannelID: "C1"}, *first)
		assert.Nil(t, first.Values)

		assert.Equal(t, types.InsightsEvent{
			Time: page.Events[1].Time, Type: types.InsightsEventIssueArchived, Reason: "r", ChannelID: "C1", RelatedChannelID: "C2", IssueID: "i",
			CorrelationID: "c", Severity: types.AlertError, PrevSeverity: types.AlertWarning, RouteKey: "rk", AlertType: "at", Host: "h",
			Author: "a", ActorID: "U1", ActorName: "n", Count: 4, Detail: "d", Values: map[string]float64{"open_s": 2},
		}, *page.Events[1])
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("descending with cursor returns next cursor when there are more rows", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		cursor := postgres.ExportEncodeInsightsCursor(t3, 99)

		mock.ExpectQuery(listSelect+"occurred_at >= $1 AND occurred_at < $2 AND (occurred_at, id) < ($3, $4) ORDER BY occurred_at DESC, id DESC LIMIT $5").
			WithArgs(qFrom, qTo, t3.UTC(), int64(99), 3).
			WillReturnRows(pgxmock.NewRows(cols).
				AddRow(int64(8), t2, "alert.queued", "", "C1", "", "", "", "", "", "", []byte(nil)).
				AddRow(int64(7), t1, "alert.queued", "", "C1", "", "", "", "", "", "", []byte(nil)).
				AddRow(int64(6), t1, "alert.queued", "", "C1", "", "", "", "", "", "", []byte(nil)))

		page, err := reader.ListInsightsEvents(context.Background(), &types.InsightsListQuery{InsightsFilter: filter(), Cursor: cursor, Limit: 2, Descending: true})
		require.NoError(t, err)
		require.Len(t, page.Events, 2)
		assert.Equal(t, postgres.ExportEncodeInsightsCursor(t1, 7), page.NextCursor)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("invalid cursor", func(t *testing.T) {
		t.Parallel()

		reader, _ := newReader(t)

		_, err := reader.ListInsightsEvents(context.Background(), &types.InsightsListQuery{InsightsFilter: filter(), Cursor: "!!!"})
		require.ErrorContains(t, err, "invalid cursor")
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("boom")
		sql := listSelect + "occurred_at >= $1 AND occurred_at < $2 ORDER BY occurred_at ASC, id ASC LIMIT $3"

		tests := map[string]struct {
			expect  func(e *pgxmock.ExpectedQuery)
			wantErr string
		}{
			"query": {func(e *pgxmock.ExpectedQuery) { e.WillReturnError(boom) }, "failed to query insights events"},
			"scan": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows(cols).AddRow("x", t1, "t", "", "", "", "", "", "", "", "", []byte(nil)))
			}, "failed to scan insights event"},
			"bad attrs": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows(cols).AddRow(int64(1), t1, "t", "", "", "", "", "", "", "", "", []byte("{bad")))
			}, "failed to unmarshal insights event attributes"},
			"rows": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows(cols).AddRow(int64(1), t1, "t", "", "", "", "", "", "", "", "", []byte(nil)).CloseError(boom))
			}, "failed to query insights events"},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				reader, mock := newReader(t)
				tt.expect(mock.ExpectQuery(sql).WithArgs(qFrom, qTo, 101))

				_, err := reader.ListInsightsEvents(context.Background(), &types.InsightsListQuery{InsightsFilter: filter()})
				require.ErrorContains(t, err, tt.wantErr)
			})
		}
	})
}

func TestInsightsCursor(t *testing.T) {
	t.Parallel()

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()

		ts := time.Date(2026, 3, 1, 10, 0, 0, 123456000, time.UTC)

		gotTime, gotID, err := postgres.ExportDecodeInsightsCursor(postgres.ExportEncodeInsightsCursor(ts, 42))
		require.NoError(t, err)
		assert.True(t, ts.Equal(gotTime))
		assert.Equal(t, int64(42), gotID)
	})

	for name, cursor := range map[string]string{
		"not base64":     "!!!",
		"no separator":   "MTIz", // "123"
		"bad nanos":      "eDoxMg",
		"bad id":         "MTIzOng",
		"empty":          "",
		"negative parse": "MTpmb28", // "1:foo"
	} {
		t.Run("invalid: "+name, func(t *testing.T) {
			t.Parallel()

			_, _, err := postgres.ExportDecodeInsightsCursor(cursor)
			require.ErrorContains(t, err, "invalid cursor")
		})
	}
}

func TestLoadOpenIssues(t *testing.T) {
	t.Parallel()

	const base = "SELECT id, attrs FROM issues WHERE is_open = true AND (expires_at IS NULL OR expires_at > NOW())"

	t.Run("all channels", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery(base).WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).AddRow("i1", []byte(`{"a":1}`)).AddRow("i2", []byte(`{"b":2}`)))

		issues, err := reader.LoadOpenIssues(context.Background(), nil)
		require.NoError(t, err)
		assert.Equal(t, map[string]json.RawMessage{"i1": json.RawMessage(`{"a":1}`), "i2": json.RawMessage(`{"b":2}`)}, issues)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("filtered by channels", func(t *testing.T) {
		t.Parallel()

		reader, mock := newReader(t)
		mock.ExpectQuery(base + " AND channel_id = ANY($1)").WithArgs([]string{"C1", "C2"}).
			WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).AddRow("i1", []byte(`{}`)))

		issues, err := reader.LoadOpenIssues(context.Background(), []string{"C1", "C2"})
		require.NoError(t, err)
		assert.Len(t, issues, 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("boom")

		tests := map[string]struct {
			expect  func(e *pgxmock.ExpectedQuery)
			wantErr string
		}{
			"query": {func(e *pgxmock.ExpectedQuery) { e.WillReturnError(boom) }, "failed to query open issues"},
			"scan": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).AddRow("i", []byte(`{}`)).RowError(0, boom))
			}, "failed to scan open issue"},
			"rows": {func(e *pgxmock.ExpectedQuery) {
				e.WillReturnRows(pgxmock.NewRows([]string{"id", "attrs"}).AddRow("i", []byte(`{}`)).CloseError(boom))
			}, "failed to query open issues"},
		}

		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				reader, mock := newReader(t)
				tt.expect(mock.ExpectQuery(base))

				_, err := reader.LoadOpenIssues(context.Background(), nil)
				require.ErrorContains(t, err, tt.wantErr)
			})
		}
	})
}

// =============================================================================
// Standalone reader
// =============================================================================

func TestInsightsReader_CheckSchema(t *testing.T) {
	t.Parallel()

	const sql = "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"

	newStandalone := func(t *testing.T) (*postgres.InsightsReader, pgxmock.PgxPoolIface) {
		t.Helper()

		mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
		require.NoError(t, err)

		reader := postgres.NewInsightsReader(&types.NoopLogger{}, postgres.WithUser("u"), postgres.WithDatabase("d"))
		reader.SetPool(mock)

		return reader, mock
	}

	t.Run("current schema", func(t *testing.T) {
		t.Parallel()

		reader, mock := newStandalone(t)
		mock.ExpectQuery(sql).WillReturnRows(pgxmock.NewRows([]string{"v"}).AddRow(4))

		require.NoError(t, reader.CheckSchema(context.Background()))
	})

	t.Run("schema too old", func(t *testing.T) {
		t.Parallel()

		reader, mock := newStandalone(t)
		mock.ExpectQuery(sql).WillReturnRows(pgxmock.NewRows([]string{"v"}).AddRow(3))

		err := reader.CheckSchema(context.Background())
		require.ErrorContains(t, err, "insights schema not found: schema version 3 < 4; run Init with a privileged user first")
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		reader, mock := newStandalone(t)
		mock.ExpectQuery(sql).WillReturnError(errors.New("permission denied"))

		err := reader.CheckSchema(context.Background())
		require.ErrorContains(t, err, "failed to read schema version")
		require.ErrorContains(t, err, "permission denied")
	})

	t.Run("queries work through the standalone reader", func(t *testing.T) {
		t.Parallel()

		reader, mock := newStandalone(t)
		mock.ExpectQuery("SELECT COUNT(*) FROM insights_events WHERE occurred_at >= $1 AND occurred_at < $2").WithArgs(qFrom, qTo).
			WillReturnRows(pgxmock.NewRows([]string{"n"}).AddRow(int64(2)))

		rows, err := reader.CountInsightsEvents(context.Background(), &types.InsightsCountQuery{InsightsFilter: filter()})
		require.NoError(t, err)
		assert.Equal(t, int64(2), rows[0].Count)
	})
}

func TestInsightsReader_ConnectAndClose(t *testing.T) {
	t.Parallel()

	t.Run("invalid configuration closes the existing pool and returns an error", func(t *testing.T) {
		t.Parallel()

		mock, err := pgxmock.NewPool()
		require.NoError(t, err)
		mock.ExpectClose()

		reader := postgres.NewInsightsReader(&types.NoopLogger{}) // no user and database
		reader.SetPool(mock)

		require.ErrorContains(t, reader.Connect(context.Background()), "invalid Postgres db configuration")
		require.NoError(t, mock.ExpectationsWereMet())
		require.ErrorContains(t, reader.CheckSchema(context.Background()), "not connected")
	})

	t.Run("unreachable database", func(t *testing.T) {
		t.Parallel()

		reader := postgres.NewInsightsReader(&types.NoopLogger{},
			postgres.WithUser("u"), postgres.WithDatabase("d"), postgres.WithHost("127.0.0.1"), postgres.WithPort(1), postgres.WithSSLMode(postgres.SSLModeDisable))

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		require.ErrorContains(t, reader.Connect(ctx), "failed to connect insights reader")
	})

	t.Run("close is idempotent", func(t *testing.T) {
		t.Parallel()

		mock, err := pgxmock.NewPool()
		require.NoError(t, err)
		mock.ExpectClose()

		reader := postgres.NewInsightsReader(&types.NoopLogger{}, postgres.WithUser("u"), postgres.WithDatabase("d"))
		reader.SetPool(mock)

		reader.Close()
		reader.Close()
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
