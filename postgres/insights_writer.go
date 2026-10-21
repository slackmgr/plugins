package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/slackmgr/types"
)

const (
	// insightsPartitionLockKey is a stable advisory lock ID used to ensure that only one instance at a time
	// creates or drops insights partitions. It must differ from [migrationLockKey].
	insightsPartitionLockKey = int64(5843299)

	// insightsMaintenanceInterval is how often the background goroutine maintains insights partitions.
	insightsMaintenanceInterval = time.Hour

	// insightsMaintenanceTimeout bounds a single maintenance run.
	insightsMaintenanceTimeout = time.Minute

	// insightsPartitionDaysBefore and insightsPartitionDaysAfter define the range of daily partitions that are
	// pre-created, relative to today (UTC).
	insightsPartitionDaysBefore = 1
	insightsPartitionDaysAfter  = 3

	// insightsLockTimeout is the lock_timeout used for partition DDL, so that DDL never waits long for readers.
	insightsLockTimeout = "2s"
)

// insightsColumns returns the explicit column list used when writing events (the id column is generated).
func insightsColumns() []string {
	return []string{
		"version", "occurred_at", "type", "reason", "channel_id", "related_channel_id", "issue_id",
		"correlation_id", "severity", "route_key", "actor_id", "attrs",
	}
}

// insightsAttrs is the plugin-owned JSON layout of the attrs column. It holds the event fields that are
// rarely used for filtering. It is derived exclusively from [types.InsightsEvent].
type insightsAttrs struct {
	PrevSeverity string             `json:"prevSeverity,omitempty"`
	AlertType    string             `json:"alertType,omitempty"`
	Host         string             `json:"host,omitempty"`
	Author       string             `json:"author,omitempty"`
	ActorName    string             `json:"actorName,omitempty"`
	Count        int                `json:"count,omitempty"`
	Detail       string             `json:"detail,omitempty"`
	Values       map[string]float64 `json:"values,omitempty"`
}

// isEmpty returns true if no attribute is set.
func (a *insightsAttrs) isEmpty() bool {
	return a.PrevSeverity == "" && a.AlertType == "" && a.Host == "" && a.Author == "" && a.ActorName == "" &&
		a.Count == 0 && a.Detail == "" && len(a.Values) == 0
}

// marshalInsightsAttrs returns the attrs column value for the event: nil (SQL NULL) when there are no attributes.
func marshalInsightsAttrs(e *types.InsightsEvent) ([]byte, error) {
	a := insightsAttrs{
		PrevSeverity: string(e.PrevSeverity),
		AlertType:    e.AlertType,
		Host:         e.Host,
		Author:       e.Author,
		ActorName:    e.ActorName,
		Count:        e.Count,
		Detail:       e.Detail,
		Values:       e.Values,
	}

	if a.isEmpty() {
		return nil, nil
	}

	return json.Marshal(a)
}

// openInsightsPool creates and pings a pgx pool with the given runtime parameters and the insights max connections.
func openInsightsPool(ctx context.Context, o *options, runtimeParams map[string]string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(o.connectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse Postgres db connection string: %w", err)
	}

	config.MaxConns = o.insightsPoolMaxConnections

	maps.Copy(config.ConnConfig.RuntimeParams, runtimeParams)

	conn, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create new Postgres connection pool: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping Postgres db: %w", err)
	}

	return conn, nil
}

// InsightsSink returns the insights sink, or nil if insights is not enabled (see [WithInsightsEnabled]).
// It implements [types.InsightsSinkProvider]. The returned sink uses a dedicated connection pool and never
// touches the main pool.
func (c *Client) InsightsSink() types.InsightsSink { //nolint:ireturn // interface mandated by types.InsightsSinkProvider
	if !c.opts.insightsEnabled {
		return nil
	}

	return c
}

// WriteInsightsEvents writes the events to the insights table with a single COPY. It never uses the main pool.
// It is a no-op for an empty slice, and returns an error if insights is not enabled or the client is not connected.
func (c *Client) WriteInsightsEvents(ctx context.Context, events []types.InsightsEvent) error {
	if len(events) == 0 {
		return nil
	}

	if c.insightsConn == nil {
		return errNotConnected
	}

	rows := make([][]any, 0, len(events))

	for i := range events {
		e := &events[i]

		attrs, err := marshalInsightsAttrs(e)
		if err != nil {
			return fmt.Errorf("failed to marshal insights event attributes: %w", err)
		}

		var attrsValue any // untyped nil is encoded as SQL NULL
		if attrs != nil {
			attrsValue = attrs
		}

		rows = append(rows, []any{
			int16(InsightsEventModelVersion), e.Time.UTC(), string(e.Type), e.Reason, e.ChannelID, e.RelatedChannelID,
			e.IssueID, e.CorrelationID, string(e.Severity), e.RouteKey, e.ActorID, attrsValue,
		})
	}

	if _, err := c.insightsConn.CopyFrom(ctx, pgx.Identifier{c.opts.insightsEventsTable}, insightsColumns(), pgx.CopyFromRows(rows)); err != nil {
		return fmt.Errorf("failed to write insights events to Postgres db: %w", err)
	}

	return nil
}

// connectInsights opens the dedicated insights writer pool.
func (c *Client) connectInsights(ctx context.Context) (*pgxpool.Pool, error) {
	conn, err := openInsightsPool(ctx, c.opts, map[string]string{
		"statement_timeout":  strconv.FormatInt(c.opts.insightsStatementTimeout.Milliseconds(), 10),
		"lock_timeout":       "1000",
		"synchronous_commit": "off",
		"application_name":   "slackmgr-insights",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect insights pool: %w", err)
	}

	return conn, nil
}

// startInsightsMaintenance runs partition maintenance once and starts the hourly background goroutine
// (unless already started). Errors are logged and never returned: insights must not make Init fail.
func (c *Client) startInsightsMaintenance(ctx context.Context) {
	if err := c.maintainInsightsPartitions(ctx); err != nil {
		c.logger.Errorf("failed to maintain insights partitions: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancelInsight != nil {
		return
	}

	maintCtx, cancel := context.WithCancel(context.Background())
	c.cancelInsight = cancel
	c.insightsWg.Add(1)

	//nolint:contextcheck // Intentionally using a new context: the goroutine must outlive the Init call.
	go c.runInsightsMaintenance(maintCtx)
}

func (c *Client) runInsightsMaintenance(ctx context.Context) {
	defer c.insightsWg.Done()

	ticker := time.NewTicker(insightsMaintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCtx, cancel := context.WithTimeout(ctx, insightsMaintenanceTimeout)
			err := c.maintainInsightsPartitions(runCtx)
			cancel()

			if err != nil && ctx.Err() == nil {
				c.logger.Errorf("failed to maintain insights partitions: %v", err)
			}
		}
	}
}

// maintainInsightsPartitions creates the upcoming daily partitions and drops partitions older than the retention.
// Both steps always run; their errors are joined.
func (c *Client) maintainInsightsPartitions(ctx context.Context) error {
	if c.insightsConn == nil {
		return errNotConnected
	}

	createErr := c.createInsightsPartitions(ctx)
	dropErr := c.dropOldInsightsPartitions(ctx)

	return errors.Join(createErr, dropErr)
}

// insightsPartitionName returns the partition name for the UTC day starting at day.
func (o *options) insightsPartitionName(day time.Time) string {
	return o.insightsEventsTable + "_p" + day.Format("20060102")
}

// utcDay returns midnight UTC of the day containing t.
func utcDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()

	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// beginInsightsMaintenanceTx begins a transaction on the insights pool and takes the transaction-scoped partition
// advisory lock. It returns acquired=false (with the transaction already committed) if another instance holds the lock.
func (c *Client) beginInsightsMaintenanceTx(ctx context.Context) (pgx.Tx, bool, error) { //nolint:ireturn // pgx.Tx is an interface
	tx, err := c.insightsConn.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to begin insights maintenance transaction: %w", err)
	}

	var locked bool
	if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", insightsPartitionLockKey).Scan(&locked); err != nil {
		_ = tx.Rollback(ctx)
		return nil, false, fmt.Errorf("failed to acquire insights partition lock: %w", err)
	}

	if !locked {
		// Another instance is doing the maintenance. Commit to end the transaction.
		if err := tx.Commit(ctx); err != nil {
			return nil, false, fmt.Errorf("failed to commit insights maintenance transaction: %w", err)
		}

		return nil, false, nil
	}

	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+insightsLockTimeout+"'"); err != nil {
		_ = tx.Rollback(ctx)
		return nil, false, fmt.Errorf("failed to set insights lock timeout: %w", err)
	}

	return tx, true, nil
}

// createInsightsPartitions creates the daily partitions from today-1 to today+3 (UTC) if they do not exist.
func (c *Client) createInsightsPartitions(ctx context.Context) error {
	tx, acquired, err := c.beginInsightsMaintenanceTx(ctx)
	if err != nil || !acquired {
		return err
	}

	defer func() { _ = tx.Rollback(ctx) }() // No-op if committed

	today := utcDay(c.opts.now())

	for i := -insightsPartitionDaysBefore; i <= insightsPartitionDaysAfter; i++ {
		day := today.AddDate(0, 0, i)
		next := day.AddDate(0, 0, 1)

		stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
			c.opts.insightsPartitionName(day), c.opts.insightsEventsTable, day.Format("2006-01-02 00:00:00+00"), next.Format("2006-01-02 00:00:00+00"))

		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create insights partition for %s: %w", day.Format("2006-01-02"), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit insights partition creation: %w", err)
	}

	return nil
}

// dropOldInsightsPartitions drops the daily partitions whose whole day is older than the retention.
func (c *Client) dropOldInsightsPartitions(ctx context.Context) error {
	tx, acquired, err := c.beginInsightsMaintenanceTx(ctx)
	if err != nil || !acquired {
		return err
	}

	defer func() { _ = tx.Rollback(ctx) }() // No-op if committed

	names, err := c.listInsightsPartitions(ctx, tx)
	if err != nil {
		return err
	}

	cutoff := c.opts.now().UTC().Add(-c.opts.insightsRetention)
	partitionRegex := insightsPartitionRegex(c.opts.insightsEventsTable)

	for _, name := range names {
		match := partitionRegex.FindStringSubmatch(name)
		if match == nil {
			continue // Not a partition managed by us
		}

		day, err := time.Parse("20060102", match[1])
		if err != nil {
			continue
		}

		// The partition covers [day, day+1). Drop it when the whole range is older than the cutoff.
		if day.AddDate(0, 0, 1).After(cutoff) {
			continue
		}

		// The name matched the strict regex above, so it is safe to interpolate.
		if _, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
			return fmt.Errorf("failed to drop insights partition %s: %w", name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit insights partition drop: %w", err)
	}

	return nil
}

// insightsPartitionRegex matches the names of partitions created by this plugin. PostgreSQL folds unquoted
// identifiers to lower case, so the catalog name is lower case.
func insightsPartitionRegex(table string) *regexp.Regexp {
	return regexp.MustCompile("^" + regexp.QuoteMeta(strings.ToLower(table)) + `_p(\d{8})$`)
}

// listInsightsPartitions returns the names of all child partitions of the insights table.
func (c *Client) listInsightsPartitions(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx,
		"SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass",
		c.opts.insightsEventsTable)
	if err != nil {
		return nil, fmt.Errorf("failed to list insights partitions: %w", err)
	}

	defer rows.Close()

	var names []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan insights partition name: %w", err)
		}

		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list insights partitions: %w", err)
	}

	return names, nil
}
