//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	postgres "github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
	"github.com/slackmgr/types/dbtests"
)

var client *postgres.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	c := postgres.New(
		&types.NoopLogger{},
		postgres.WithHost("localhost"),
		postgres.WithPort(5432),
		postgres.WithUser("postgres"),
		postgres.WithPassword("qwerty"),
		postgres.WithDatabase("slack_manager"),
		postgres.WithSSLMode(postgres.SSLModeDisable),
		postgres.WithIssuesTable("__issues_integration_test"),
		postgres.WithAlertsTable("__alerts_integration_test"),
		postgres.WithMoveMappingsTable("__move_mappings_integration_test"),
		postgres.WithChannelProcessingStateTable("__channel_processing_state_integration_test"),
		postgres.WithSchemaMigrationsTable("__schema_migrations_integration_test"),
		postgres.WithTTLCleanupDisabled(),
	)

	// Verify that the client implements the types.DB interface
	var _ types.DB = c

	err := c.Connect(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Ensure the database is clean before running tests
	err = c.DropAllData(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("failed to drop integration test tables: %w", err))
		os.Exit(1)
	}

	err = c.Init(ctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	client = c

	code := m.Run()

	err = client.DropAllData(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("failed to drop integration test tables: %w", err))
		os.Exit(1)
	}

	err = client.Close(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("failed to close client: %w", err))
		os.Exit(1)
	}

	os.Exit(code)
}

func TestSaveAlertIntegration(t *testing.T) {
	dbtests.TestSaveAlert(t, client)
}

func TestSaveIssueIntegration(t *testing.T) {
	dbtests.TestSaveIssue(t, client)
}

func TestFindOpenIssueByCorrelationIDIntegration(t *testing.T) {
	dbtests.TestFindOpenIssueByCorrelationID(t, client)
}

func TestFindIssueBySlackPostIDIntegration(t *testing.T) {
	dbtests.TestFindIssueBySlackPostID(t, client)
}

func TestSaveIssuesIntegration(t *testing.T) {
	dbtests.TestSaveIssues(t, client)
}

func TestFindActiveChannelsIntegration(t *testing.T) {
	dbtests.TestFindActiveChannels(t, client)
}

func TestLoadOpenIssuesInChannelIntegration(t *testing.T) {
	dbtests.TestLoadOpenIssuesInChannel(t, client)
}

func TestCreatingAndFindingMoveMappingsIntegration(t *testing.T) {
	dbtests.TestCreatingAndFindingMoveMappings(t, client)
}

func TestDeletingMoveMappingsIntegration(t *testing.T) {
	dbtests.TestDeletingMoveMappings(t, client)
}

func TestCreatingAndFindingChannelProcessingStateIntegration(t *testing.T) {
	dbtests.TestCreatingAndFindingChannelProcessingState(t, client)
}

func TestChannelProcessingState_EdgeCasesIntegration(t *testing.T) {
	dbtests.TestChannelProcessingState_EdgeCases(t, client)
}

func TestConcurrentMoveMappingIntegration(t *testing.T) {
	dbtests.TestConcurrentMoveMapping(t, client)
}

func TestConcurrentSaveIssueIntegration(t *testing.T) {
	dbtests.TestConcurrentSaveIssue(t, client)
}

func TestContextCancellationIntegration(t *testing.T) {
	dbtests.TestContextCancellation(t, client)
}

func TestFindActiveChannels_ManyChannelsIntegration(t *testing.T) {
	dbtests.TestFindActiveChannels_ManyChannels(t, client)
}

func TestInitIntegration(t *testing.T) {
	dbtests.TestInit(t, client)
}

func TestInit_WithSchemaValidationIntegration(t *testing.T) {
	dbtests.TestInit_WithSchemaValidation(t, client)
}

func TestLoadOpenIssuesInChannel_LargeDatasetIntegration(t *testing.T) {
	dbtests.TestLoadOpenIssuesInChannel_LargeDataset(t, client)
}

func TestMoveIssueIntegration(t *testing.T) {
	dbtests.TestMoveIssue(t, client)
}

func TestMoveIssue_EdgeCasesIntegration(t *testing.T) {
	dbtests.TestMoveIssue_EdgeCases(t, client)
}

func TestSaveIssue_ComplexAlertIntegration(t *testing.T) {
	dbtests.TestSaveIssue_ComplexAlert(t, client)
}

func TestSpecialCharactersInCorrelationIDIntegration(t *testing.T) {
	dbtests.TestSpecialCharactersInCorrelationID(t, client)
}
