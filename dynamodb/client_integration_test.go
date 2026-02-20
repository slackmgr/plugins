//go:build integration

package dynamodb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	dynamodb "github.com/slackmgr/plugins/dynamodb"
	"github.com/slackmgr/types"
	"github.com/slackmgr/types/dbtests"
)

var client *dynamodb.Client

func TestMain(m *testing.M) {
	ctx := context.Background()

	region := os.Getenv("AWS_REGION")
	tableName := os.Getenv("DYNAMODB_TABLE_NAME")

	if region == "" || tableName == "" {
		fmt.Fprintln(os.Stderr, "AWS_REGION and DYNAMODB_TABLE_NAME environment variables must be set for integration tests")
		os.Exit(1)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	c := dynamodb.New(&awsCfg, tableName, dynamodb.WithAlertsTimeToLive(time.Minute), dynamodb.WithIssuesTimeToLive(time.Minute))

	// Verify that the client implements the types.DB interface
	var _ types.DB = c

	err = c.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Ensure the database is clean before running tests
	err = c.DropAllData(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("failed to delete all items: %w", err))
		os.Exit(1)
	}

	err = c.Init(ctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	client = c

	code := m.Run()

	os.Exit(code)
}

func TestSaveAlert(t *testing.T) {
	dbtests.TestSaveAlert(t, client)
}

func TestSaveIssue(t *testing.T) {
	dbtests.TestSaveIssue(t, client)
}

func TestFindOpenIssueByCorrelationID(t *testing.T) {
	dbtests.TestFindOpenIssueByCorrelationID(t, client)
}

func TestFindIssueBySlackPostID(t *testing.T) {
	dbtests.TestFindIssueBySlackPostID(t, client)
}

func TestSaveIssues(t *testing.T) {
	dbtests.TestSaveIssues(t, client)
}

func TestFindActiveChannels(t *testing.T) {
	dbtests.TestFindActiveChannels(t, client)
}

func TestLoadOpenIssuesInChannel(t *testing.T) {
	dbtests.TestLoadOpenIssuesInChannel(t, client)
}

func TestCreatingAndFindingMoveMappings(t *testing.T) {
	dbtests.TestCreatingAndFindingMoveMappings(t, client)
}

func TestDeletingMoveMappings(t *testing.T) {
	dbtests.TestDeletingMoveMappings(t, client)
}

func TestCreatingAndFindingChannelProcessingState(t *testing.T) {
	dbtests.TestCreatingAndFindingChannelProcessingState(t, client)
}
