package postgres

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/slackmgr/types"
)

// insightsMinSchemaVersion is the schema version (migration) that introduced the insights events table.
const insightsMinSchemaVersion = 4

var errInvalidInsightsCursor = errors.New("invalid cursor")

var (
	_ types.InsightsReader = (*InsightsReader)(nil)
	_ types.InsightsReader = (*insightsQueries)(nil)
)

// InsightsReader returns a [types.InsightsReader] backed by the
// insights pool, or nil if insights is not enabled (see [WithInsightsEnabled]).
//
// It implements [types.InsightsReaderProvider].
func (c *Client) InsightsReader() types.InsightsReader { //nolint:ireturn // interface mandated by types.InsightsReaderProvider
	if !c.opts.insightsEnabled {
		return nil
	}

	return &insightsQueries{conn: func() pool { return c.insightsConn }, opts: c.opts}
}

// InsightsReader is a standalone, read-only [types.InsightsReader] for consumers (e.g. dashboards) that only
// have SELECT privileges. It never runs migrations or DDL and never starts background goroutines.
//
// Do not call [Client.Init] from a read-only consumer, because it runs DDL. Use [NewInsightsReader] instead.
// See the README for the required grants.
type InsightsReader struct {
	*insightsQueries

	logger types.Logger
	opts   *options
	conn   pool
}

// NewInsightsReader creates a read-only insights reader. It accepts the same options as [New]; the connection
// options, the table names, [WithInsightsPoolMaxConnections] and [WithInsightsReadStatementTimeout] are used.
// Call [InsightsReader.Connect] before use.
func NewInsightsReader(logger types.Logger, opts ...Option) *InsightsReader {
	o := newOptions()
	for _, opt := range opts {
		opt(o)
	}

	r := &InsightsReader{logger: logger.WithField("plugin", "postgres"), opts: o}
	r.insightsQueries = &insightsQueries{conn: func() pool { return r.conn }, opts: o}

	return r
}

// Connect validates the options and opens a single connection pool with default_transaction_read_only=on and
// the read statement timeout. It does not touch the schema; use [InsightsReader.CheckSchema] for that.
func (r *InsightsReader) Connect(ctx context.Context) error {
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}

	if err := r.opts.validate(); err != nil {
		return fmt.Errorf("invalid Postgres db configuration: %w", err)
	}

	conn, err := openInsightsPool(ctx, r.opts, map[string]string{
		"default_transaction_read_only": "on",
		"statement_timeout":             strconv.FormatInt(r.opts.insightsReadStatementTimeout.Milliseconds(), 10),
		"application_name":              "slackmgr-insights-reader",
	})
	if err != nil {
		return fmt.Errorf("failed to connect insights reader: %w", err)
	}

	r.conn = conn
	r.logger.Info("insights reader connected")

	return nil
}

// Close closes the connection pool. It is safe to call more than once.
func (r *InsightsReader) Close() {
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

// CheckSchema verifies that the database schema includes the insights events table, by reading the maximum
// applied migration version. It returns a descriptive error if the schema is too old.
func (r *InsightsReader) CheckSchema(ctx context.Context) error {
	if r.conn == nil {
		return errNotConnected
	}

	var version int

	query := "SELECT COALESCE(MAX(version), 0) FROM " + r.opts.schemaMigrationsTable
	if err := r.conn.QueryRow(ctx, query).Scan(&version); err != nil {
		return fmt.Errorf("failed to read schema version: %w", err)
	}

	if version < insightsMinSchemaVersion {
		return fmt.Errorf("insights schema not found: schema version %d < %d; run Init with a privileged user first", version, insightsMinSchemaVersion)
	}

	return nil
}

// insightsQueries implements the insights queries over a connection pool. It is shared by [Client] and [InsightsReader].
type insightsQueries struct {
	conn func() pool
	opts *options
}

// sqlArgs collects bind parameters and returns their placeholders.
type sqlArgs []any

func (a *sqlArgs) add(v any) string {
	*a = append(*a, v)

	return "$" + strconv.Itoa(len(*a))
}

// dimensionExpr returns the whitelisted SQL expression for a dimension. Nothing from the query is interpolated.
func dimensionExpr(d types.InsightsDimension, bucketParam string) string {
	switch d {
	case types.InsightsDimensionBucket:
		return fmt.Sprintf("to_timestamp(floor(extract(epoch FROM occurred_at)::float8 / %[1]s::float8) * %[1]s::float8)", bucketParam)
	case types.InsightsDimensionChannel:
		return "channel_id"
	case types.InsightsDimensionRelatedChannel:
		return "related_channel_id"
	case types.InsightsDimensionType:
		return "type"
	case types.InsightsDimensionReason:
		return "reason"
	case types.InsightsDimensionSeverity:
		return "severity"
	case types.InsightsDimensionRouteKey:
		return "route_key"
	case types.InsightsDimensionAlertType:
		return "COALESCE(attrs->>'alertType', '')"
	case types.InsightsDimensionHost:
		return "COALESCE(attrs->>'host', '')"
	case types.InsightsDimensionAuthor:
		return "COALESCE(attrs->>'author', '')"
	case types.InsightsDimensionActor:
		return "actor_id"
	case types.InsightsDimensionCorrelationID:
		return "correlation_id"
	case types.InsightsDimensionIssue:
		return "issue_id"
	default:
		return ""
	}
}

// groupBy builds the group expressions (and the bucket bind parameter, if needed) for the dimensions.
func groupBy(dims []types.InsightsDimension, bucket time.Duration, args *sqlArgs) ([]string, error) {
	exprs := make([]string, 0, len(dims))
	bucketParam := ""

	for _, d := range dims {
		if d == types.InsightsDimensionBucket && bucketParam == "" {
			bucketParam = args.add(bucket.Seconds())
		}

		expr := dimensionExpr(d, bucketParam)
		if expr == "" {
			return nil, fmt.Errorf("invalid dimension %q", d)
		}

		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// scanTargets returns the scan destinations for the group columns, and a function that stores the scanned
// values in a group key.
func groupScanTargets(dims []types.InsightsDimension) ([]any, func(*types.InsightsGroupKey)) {
	strs := make([]string, len(dims))
	var bucket time.Time

	dests := make([]any, len(dims))

	for i, d := range dims {
		if d == types.InsightsDimensionBucket {
			dests[i] = &bucket
		} else {
			dests[i] = &strs[i]
		}
	}

	apply := func(k *types.InsightsGroupKey) {
		for i, d := range dims {
			switch d {
			case types.InsightsDimensionBucket:
				k.Bucket = bucket.UTC()
			case types.InsightsDimensionChannel:
				k.ChannelID = strs[i]
			case types.InsightsDimensionRelatedChannel:
				k.RelatedChannelID = strs[i]
			case types.InsightsDimensionType:
				k.Type = types.InsightsEventType(strs[i])
			case types.InsightsDimensionReason:
				k.Reason = strs[i]
			case types.InsightsDimensionSeverity:
				k.Severity = types.AlertSeverity(strs[i])
			case types.InsightsDimensionRouteKey:
				k.RouteKey = strs[i]
			case types.InsightsDimensionAlertType:
				k.AlertType = strs[i]
			case types.InsightsDimensionHost:
				k.Host = strs[i]
			case types.InsightsDimensionAuthor:
				k.Author = strs[i]
			case types.InsightsDimensionActor:
				k.ActorID = strs[i]
			case types.InsightsDimensionCorrelationID:
				k.CorrelationID = strs[i]
			case types.InsightsDimensionIssue:
				k.IssueID = strs[i]
			}
		}
	}

	return dests, apply
}

func toStrings[T ~string](in []T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}

	return out
}

// whereClause builds the WHERE conditions (without the WHERE keyword) for the filter.
func whereClause(f *types.InsightsFilter, args *sqlArgs) []string {
	conds := []string{
		"occurred_at >= " + args.add(f.From),
		"occurred_at < " + args.add(f.To),
	}

	lists := []struct {
		column string
		values []string
	}{
		{"channel_id", f.ChannelIDs},
		{"related_channel_id", f.RelatedChannelIDs},
		{"type", toStrings(f.Types)},
		{"reason", f.Reasons},
		{"severity", toStrings(f.Severities)},
		{"route_key", f.RouteKeys},
	}

	for _, l := range lists {
		if len(l.values) > 0 {
			conds = append(conds, l.column+" = ANY("+args.add(l.values)+")")
		}
	}

	if f.IssueID != "" {
		conds = append(conds, "issue_id = "+args.add(f.IssueID))
	}

	if f.CorrelationID != "" {
		conds = append(conds, "correlation_id = "+args.add(f.CorrelationID))
	}

	if f.ActorID != "" {
		conds = append(conds, "actor_id = "+args.add(f.ActorID))
	}

	return conds
}

// groupTail builds the GROUP BY / ORDER BY / LIMIT tail. Group columns are referenced by position. The group
// columns are the first n select columns; the count (if ordering by count) is column n+1.
func groupTail(n int, orderByCountDesc bool, limit int, args *sqlArgs) string {
	if n == 0 {
		return ""
	}

	positions := make([]string, n)
	for i := range positions {
		positions[i] = strconv.Itoa(i + 1)
	}

	tail := " GROUP BY " + strings.Join(positions, ", ")

	if orderByCountDesc {
		tail += " ORDER BY " + strconv.Itoa(n+1) + " DESC, " + strings.Join(positions, ", ")
	} else {
		tail += " ORDER BY " + strings.Join(positions, ", ")
	}

	return tail + " LIMIT " + args.add(limit)
}

// CountInsightsEvents counts events matching the query, grouped by the query dimensions.
func (q *insightsQueries) CountInsightsEvents(ctx context.Context, query *types.InsightsCountQuery) ([]*types.InsightsCountRow, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("invalid insights count query: %w", err)
	}

	conn, err := q.pool()
	if err != nil {
		return nil, err
	}

	var args sqlArgs

	exprs, err := groupBy(query.GroupBy, query.Bucket, &args)
	if err != nil {
		return nil, err
	}

	countExpr := "COUNT(*)"

	if query.Distinct != "" {
		expr := dimensionExpr(query.Distinct, "")
		if expr == "" {
			return nil, fmt.Errorf("invalid distinct dimension %q", query.Distinct)
		}

		countExpr = fmt.Sprintf("COUNT(DISTINCT %s) FILTER (WHERE %s <> '')", expr, expr)
	}

	conds := whereClause(&query.InsightsFilter, &args)

	stmt := "SELECT " + strings.Join(append(exprs, countExpr), ", ") + " FROM " + q.opts.insightsEventsTable +
		" WHERE " + strings.Join(conds, " AND ") +
		groupTail(len(exprs), query.OrderByCountDesc, query.EffectiveLimit(), &args)

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query insights counts: %w", err)
	}

	defer rows.Close()

	result := make([]*types.InsightsCountRow, 0)

	for rows.Next() {
		dests, apply := groupScanTargets(query.GroupBy)
		row := &types.InsightsCountRow{}

		if err := rows.Scan(append(dests, &row.Count)...); err != nil {
			return nil, fmt.Errorf("failed to scan insights count row: %w", err)
		}

		apply(&row.InsightsGroupKey)
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to query insights counts: %w", err)
	}

	return result, nil
}

// aggregateExpr returns the whitelisted SQL expression for an aggregate of the value expression v.
func aggregateExpr(a types.InsightsAggregate, v string) string {
	switch a {
	case types.InsightsAggregateSum:
		return "SUM(" + v + ")"
	case types.InsightsAggregateAvg:
		return "AVG(" + v + ")"
	case types.InsightsAggregateMin:
		return "MIN(" + v + ")"
	case types.InsightsAggregateMax:
		return "MAX(" + v + ")"
	case types.InsightsAggregateP50:
		return "percentile_cont(0.5) WITHIN GROUP (ORDER BY " + v + ")"
	case types.InsightsAggregateP90:
		return "percentile_cont(0.9) WITHIN GROUP (ORDER BY " + v + ")"
	case types.InsightsAggregateP99:
		return "percentile_cont(0.99) WITHIN GROUP (ORDER BY " + v + ")"
	default:
		return ""
	}
}

// InsightsValueStats computes aggregates of one numeric event value, grouped by the query dimensions.
func (q *insightsQueries) InsightsValueStats(ctx context.Context, query *types.InsightsStatsQuery) ([]*types.InsightsStatsRow, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("invalid insights stats query: %w", err)
	}

	conn, err := q.pool()
	if err != nil {
		return nil, err
	}

	var args sqlArgs

	exprs, err := groupBy(query.GroupBy, query.Bucket, &args)
	if err != nil {
		return nil, err
	}

	keyParam := args.add(query.ValueKey)
	value := "(attrs->'values'->>" + keyParam + "::text)::float8"

	// Distinct aggregates, in request order.
	var aggregates []types.InsightsAggregate

	selects := append([]string{}, exprs...)
	selects = append(selects, "COUNT(*)")

	for _, a := range query.Aggregates {
		expr := aggregateExpr(a, value)
		if expr == "" {
			return nil, fmt.Errorf("invalid aggregate %q", a)
		}

		if !slices.Contains(aggregates, a) {
			aggregates = append(aggregates, a)
			selects = append(selects, expr)
		}
	}

	conds := whereClause(&query.InsightsFilter, &args)
	conds = append(conds, "attrs->'values' ? "+keyParam+"::text")

	stmt := "SELECT " + strings.Join(selects, ", ") + " FROM " + q.opts.insightsEventsTable +
		" WHERE " + strings.Join(conds, " AND ") +
		groupTail(len(exprs), false, query.EffectiveLimit(), &args)

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query insights stats: %w", err)
	}

	defer rows.Close()

	result := make([]*types.InsightsStatsRow, 0)

	for rows.Next() {
		dests, apply := groupScanTargets(query.GroupBy)
		row := &types.InsightsStatsRow{Values: make(map[types.InsightsAggregate]float64, len(aggregates))}
		aggValues := make([]sql.NullFloat64, len(aggregates))

		dests = append(dests, &row.Count)
		for i := range aggValues {
			dests = append(dests, &aggValues[i])
		}

		if err := rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("failed to scan insights stats row: %w", err)
		}

		// An ungrouped aggregate over zero rows still produces one row. Return no rows instead.
		if len(query.GroupBy) == 0 && row.Count == 0 {
			continue
		}

		apply(&row.InsightsGroupKey)

		for i, a := range aggregates {
			if aggValues[i].Valid {
				row.Values[a] = aggValues[i].Float64
			}
		}

		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to query insights stats: %w", err)
	}

	return result, nil
}

// encodeInsightsCursor encodes the keyset position of an event as an opaque cursor.
func encodeInsightsCursor(t time.Time, id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(t.UnixNano(), 10) + ":" + strconv.FormatInt(id, 10)))
}

// decodeInsightsCursor decodes a cursor created by encodeInsightsCursor.
func decodeInsightsCursor(cursor string) (time.Time, int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, 0, errInvalidInsightsCursor
	}

	nanosStr, idStr, ok := strings.Cut(string(raw), ":")
	if !ok {
		return time.Time{}, 0, errInvalidInsightsCursor
	}

	nanos, err := strconv.ParseInt(nanosStr, 10, 64)
	if err != nil {
		return time.Time{}, 0, errInvalidInsightsCursor
	}

	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return time.Time{}, 0, errInvalidInsightsCursor
	}

	return time.Unix(0, nanos).UTC(), id, nil
}

// ListInsightsEvents returns a page of events ordered by time, using keyset pagination on (occurred_at, id).
func (q *insightsQueries) ListInsightsEvents(ctx context.Context, query *types.InsightsListQuery) (*types.InsightsEventPage, error) {
	if query == nil {
		return nil, errors.New("query cannot be nil")
	}

	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("invalid insights list query: %w", err)
	}

	conn, err := q.pool()
	if err != nil {
		return nil, err
	}

	var args sqlArgs

	conds := whereClause(&query.InsightsFilter, &args)

	op, order := ">", "ASC"
	if query.Descending {
		op, order = "<", "DESC"
	}

	if query.Cursor != "" {
		t, id, err := decodeInsightsCursor(query.Cursor)
		if err != nil {
			return nil, err
		}

		conds = append(conds, fmt.Sprintf("(occurred_at, id) %s (%s, %s)", op, args.add(t), args.add(id)))
	}

	limit := query.EffectiveLimit()

	stmt := "SELECT id, occurred_at, type, reason, channel_id, related_channel_id, issue_id, correlation_id, severity, route_key, actor_id, attrs FROM " +
		q.opts.insightsEventsTable + " WHERE " + strings.Join(conds, " AND ") +
		" ORDER BY occurred_at " + order + ", id " + order + " LIMIT " + args.add(limit+1)

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query insights events: %w", err)
	}

	defer rows.Close()

	page := &types.InsightsEventPage{Events: make([]*types.InsightsEvent, 0, limit)}

	var lastID int64

	for rows.Next() {
		var (
			id                                                                   int64
			occurredAt                                                           time.Time
			typ, reason, channel, related, issue, cor, severity, routeKey, actor string
			attrs                                                                []byte
		)

		if err := rows.Scan(&id, &occurredAt, &typ, &reason, &channel, &related, &issue, &cor, &severity, &routeKey, &actor, &attrs); err != nil {
			return nil, fmt.Errorf("failed to scan insights event: %w", err)
		}

		if len(page.Events) == limit {
			// The extra row only tells us that there is another page.
			page.NextCursor = encodeInsightsCursor(page.Events[limit-1].Time, lastID)

			break
		}

		e := types.InsightsEvent{
			Time: occurredAt.UTC(), Type: types.InsightsEventType(typ), Reason: reason, ChannelID: channel,
			RelatedChannelID: related, IssueID: issue, CorrelationID: cor, Severity: types.AlertSeverity(severity),
			RouteKey: routeKey, ActorID: actor,
		}

		if len(attrs) > 0 {
			var a insightsAttrs
			if err := json.Unmarshal(attrs, &a); err != nil {
				return nil, fmt.Errorf("failed to unmarshal insights event attributes: %w", err)
			}

			e.PrevSeverity = types.AlertSeverity(a.PrevSeverity)
			e.AlertType, e.Host, e.Author, e.ActorName = a.AlertType, a.Host, a.Author, a.ActorName
			e.Count, e.Detail, e.Values = a.Count, a.Detail, a.Values
		}

		page.Events = append(page.Events, &e)
		lastID = id
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to query insights events: %w", err)
	}

	return page, nil
}

// LoadOpenIssues loads all open, non-expired issues in the given channels (all channels if empty), keyed by issue ID.
func (q *insightsQueries) LoadOpenIssues(ctx context.Context, channelIDs []string) (map[string]json.RawMessage, error) {
	conn, err := q.pool()
	if err != nil {
		return nil, err
	}

	var args sqlArgs

	stmt := "SELECT id, attrs FROM " + q.opts.issuesTable + " WHERE is_open = true AND (expires_at IS NULL OR expires_at > NOW())"

	if len(channelIDs) > 0 {
		stmt += " AND channel_id = ANY(" + args.add(channelIDs) + ")"
	}

	rows, err := conn.Query(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query open issues: %w", err)
	}

	defer rows.Close()

	issues := make(map[string]json.RawMessage)

	for rows.Next() {
		var (
			id   string
			body []byte
		)

		if err := rows.Scan(&id, &body); err != nil {
			return nil, fmt.Errorf("failed to scan open issue: %w", err)
		}

		issues[id] = json.RawMessage(body)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to query open issues: %w", err)
	}

	return issues, nil
}

func (q *insightsQueries) pool() (pool, error) { //nolint:ireturn // internal interface
	if q.conn == nil {
		return nil, errNotConnected
	}

	p := q.conn()
	if p == nil {
		return nil, errNotConnected
	}

	return p, nil
}
