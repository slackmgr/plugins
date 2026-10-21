//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
	"github.com/slackmgr/types/dbtests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationOptions() []postgres.Option {
	return []postgres.Option{
		postgres.WithHost("localhost"),
		postgres.WithPort(5432),
		postgres.WithUser("postgres"),
		postgres.WithPassword("qwerty"),
		postgres.WithDatabase("slack_manager"),
		postgres.WithSSLMode(postgres.SSLModeDisable),
		postgres.WithIssuesTable("__issues_integration_test"),
		postgres.WithSchemaMigrationsTable("__schema_migrations_integration_test"),
		postgres.WithInsightsEventsTable(integrationInsightsTable),
	}
}

func newIntegrationInsightsReader(t *testing.T) *postgres.InsightsReader {
	t.Helper()

	reader := postgres.NewInsightsReader(&types.NoopLogger{}, integrationOptions()...)
	require.NoError(t, reader.Connect(context.Background()))
	t.Cleanup(reader.Close)

	return reader
}

func TestInsightsIntegration(t *testing.T) {
	require.NotNil(t, client.InsightsSink())
	require.NotNil(t, client.InsightsReader())

	dbtests.RunAllInsightsTests(t, client, client.InsightsSink(), client.InsightsReader())
}

func TestInsightsStandaloneReaderIntegration(t *testing.T) {
	reader := newIntegrationInsightsReader(t)

	require.NoError(t, reader.CheckSchema(context.Background()))

	dbtests.RunAllInsightsTests(t, client, client.InsightsSink(), reader)
}

func TestInsightsReaderIsReadOnlyIntegration(t *testing.T) {
	reader := newIntegrationInsightsReader(t)

	err := reader.ExecOnReaderPool(context.Background(), "DELETE FROM "+integrationInsightsTable)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only transaction")
}

func TestInsightsCheckSchemaTooOldIntegration(t *testing.T) {
	opts := append(integrationOptions(), postgres.WithSchemaMigrationsTable("__no_such_migrations_table"))
	reader := postgres.NewInsightsReader(&types.NoopLogger{}, opts...)
	require.NoError(t, reader.Connect(context.Background()))
	t.Cleanup(reader.Close)

	require.Error(t, reader.CheckSchema(context.Background()))
}

func partitionNames(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		"SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1::regclass ORDER BY 1", integrationInsightsTable)
	require.NoError(t, err)
	defer rows.Close()

	var names []string

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}

	require.NoError(t, rows.Err())

	return names
}

func TestInsightsPartitionMaintenanceIntegration(t *testing.T) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://postgres:qwerty@localhost:5432/slack_manager?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	oldPartition := integrationInsightsTable + "_p20200101"
	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')", oldPartition, integrationInsightsTable))
	require.NoError(t, err)

	// Unrelated child tables must be left alone.
	foreign := integrationInsightsTable + "_pfoo"
	_, err = pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('2020-02-01 00:00:00+00') TO ('2020-02-02 00:00:00+00')", foreign, integrationInsightsTable))
	require.NoError(t, err)

	require.NoError(t, client.MaintainInsightsPartitions(ctx))
	// Idempotent
	require.NoError(t, client.MaintainInsightsPartitions(ctx))

	names := partitionNames(t, pool)

	today := time.Now().UTC()
	for i := -1; i <= 3; i++ {
		assert.Contains(t, names, integrationInsightsTable+"_p"+today.AddDate(0, 0, i).Format("20060102"))
	}

	assert.NotContains(t, names, oldPartition)
	assert.Contains(t, names, foreign)
	assert.Len(t, names, 6)

	_, err = pool.Exec(ctx, "DROP TABLE "+foreign)
	require.NoError(t, err)
}

// TestInsightsParentOnlyGrantIntegration verifies that a role with SELECT on the parent table only can read
// events stored in partitions created after the grant.
func TestInsightsParentOnlyGrantIntegration(t *testing.T) {
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, "postgres://postgres:qwerty@localhost:5432/slack_manager?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	role := "__insights_reader_integration_test"

	for _, stmt := range []string{
		"DROP OWNED BY " + role,
		"DROP ROLE IF EXISTS " + role,
	} {
		_, _ = admin.Exec(ctx, stmt) // The first statement fails if the role does not exist.
	}

	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'readerpw'", role))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	})

	for _, stmt := range []string{
		fmt.Sprintf("GRANT SELECT ON %s, %s, %s TO %s", "__issues_integration_test", integrationInsightsTable, "__schema_migrations_integration_test", role),
	} {
		_, err = admin.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	// A partition created after the grant: drop and recreate today's partition, then write an event.
	sink := client.InsightsSink()
	channel := "C_GRANT_TEST"
	now := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, sink.WriteInsightsEvents(ctx, []types.InsightsEvent{{Time: now, Type: types.InsightsEventAlertQueued, ChannelID: channel}}))

	opts := append(integrationOptions(), postgres.WithUser(role), postgres.WithPassword("readerpw"))
	reader := postgres.NewInsightsReader(&types.NoopLogger{}, opts...)
	require.NoError(t, reader.Connect(ctx))
	t.Cleanup(reader.Close)

	require.NoError(t, reader.CheckSchema(ctx))

	page, err := reader.ListInsightsEvents(ctx, &types.InsightsListQuery{InsightsFilter: types.InsightsFilter{From: now.Add(-time.Hour), To: now.Add(time.Hour), ChannelIDs: []string{channel}}})
	require.NoError(t, err)
	assert.Len(t, page.Events, 1)

	// Creating a new partition after the grant needs no new grant.
	future := time.Now().UTC().AddDate(0, 0, 10)
	partition := integrationInsightsTable + "_p" + future.Format("20060102")
	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s 00:00:00+00') TO ('%s 00:00:00+00')",
		partition, integrationInsightsTable, future.Format("2006-01-02"), future.AddDate(0, 0, 1).Format("2006-01-02")))
	require.NoError(t, err)

	_, err = admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s (version, occurred_at, type, channel_id) VALUES (1, '%s 12:00:00+00', 'alert.queued', '%s')", integrationInsightsTable, future.Format("2006-01-02"), channel))
	require.NoError(t, err)

	page, err = reader.ListInsightsEvents(ctx, &types.InsightsListQuery{InsightsFilter: types.InsightsFilter{From: future.Add(-24 * time.Hour), To: future.AddDate(0, 0, 2), ChannelIDs: []string{channel}}})
	require.NoError(t, err)
	assert.Len(t, page.Events, 1, "role with SELECT on the parent only can read a partition created after the grant")

	// The role cannot write.
	require.Error(t, reader.ExecOnReaderPool(ctx, "DELETE FROM "+integrationInsightsTable))

	_, err = admin.Exec(ctx, "DROP TABLE "+partition)
	require.NoError(t, err)
}
