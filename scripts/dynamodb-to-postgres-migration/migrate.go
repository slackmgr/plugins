package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/slackmgr/plugins/postgres"
	"github.com/slackmgr/types"
)

type stats struct {
	scanned      int
	issues       int
	moveMappings int
	skipped      int
}

func (s stats) print(dryRun bool) {
	prefix := ""
	if dryRun {
		prefix = "[DRY RUN] "
	}
	log.Printf("%sscanned=%d  issues=%d  move_mappings=%d  skipped=%d",
		prefix, s.scanned, s.issues, s.moveMappings, s.skipped)
}

func run(ctx context.Context, cfg *appConfig) error {
	// 1. Load AWS config via the standard chain (env vars, ~/.aws/credentials,
	//    instance/task roles, SSO, etc.). AWS_PROFILE is honoured automatically.
	log.Println("Loading AWS configuration...")
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.awsRegion))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}
	dynamo := dynamodb.NewFromConfig(awsCfg)

	// 2. Connect the postgres plugin, run schema migrations, then verify the
	//    data tables are empty before touching anything.
	log.Printf("Connecting to Postgres at %s:%d/%s...", cfg.pgHost, cfg.pgPort, cfg.pgDatabase)

	pgOpts := []postgres.Option{
		postgres.WithHost(cfg.pgHost),
		postgres.WithPort(cfg.pgPort),
		postgres.WithUser(cfg.pgUser),
		postgres.WithDatabase(cfg.pgDatabase),
		postgres.WithSSLMode(cfg.pgSSLMode),
		postgres.WithIssuesTable(cfg.pgIssuesTable),
		postgres.WithMoveMappingsTable(cfg.pgMoveMappingsTable),
		postgres.WithTTLCleanupDisabled(), // not needed during migration
	}
	if cfg.pgPassword != "" {
		pgOpts = append(pgOpts, postgres.WithPassword(cfg.pgPassword))
	}
	if cfg.pgSSLRootCert != "" {
		pgOpts = append(pgOpts, postgres.WithSSLRootCert(cfg.pgSSLRootCert))
	}
	if cfg.pgSSLCert != "" {
		pgOpts = append(pgOpts, postgres.WithSSLCert(cfg.pgSSLCert))
	}
	if cfg.pgSSLKey != "" {
		pgOpts = append(pgOpts, postgres.WithSSLKey(cfg.pgSSLKey))
	}

	pgClient := postgres.New(&types.NoopLogger{}, pgOpts...)
	if err := pgClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to Postgres: %w", err)
	}
	defer func() {
		if err := pgClient.Close(ctx); err != nil {
			log.Printf("Warning: error closing Postgres connection: %v", err)
		}
	}()

	log.Println("Running Postgres schema migrations...")
	if err := pgClient.Init(ctx, false); err != nil {
		return fmt.Errorf("failed to initialise Postgres schema: %w", err)
	}
	log.Println("Schema ready.")

	log.Println("Checking that issues and move_mappings tables are empty...")
	if err := checkDataTablesEmpty(ctx, cfg); err != nil {
		return err
	}
	log.Println("Pre-check passed: data tables are empty.")

	// 3. Full-table scan of DynamoDB, paginating through all items.
	log.Printf("Scanning DynamoDB table %q (region: %s)...", cfg.dynamoTable, cfg.awsRegion)

	var s stats
	lastReport := time.Now()
	const reportInterval = 5 * time.Second

	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(cfg.dynamoTable),
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		output, err := dynamo.Scan(ctx, scanInput)
		if err != nil {
			return fmt.Errorf("DynamoDB scan failed: %w", err)
		}

		for _, item := range output.Items {
			s.scanned++

			sk := stringAttr(item["sk"])
			switch {
			case strings.HasPrefix(sk, "ISSUE#"):
				if !cfg.dryRun {
					if err := migrateIssue(ctx, pgClient, item); err != nil {
						return fmt.Errorf("issue migration failed (sk=%s): %w", sk, err)
					}
				}
				s.issues++

			case strings.HasPrefix(sk, "MOVEMAPPING#"):
				if !cfg.dryRun {
					if err := migrateMoveMapping(ctx, pgClient, item); err != nil {
						return fmt.Errorf("move mapping migration failed (sk=%s): %w", sk, err)
					}
				}
				s.moveMappings++

			default:
				// ALERT# and PROCESSINGSTATE# items are intentionally skipped.
				s.skipped++
			}

			if time.Since(lastReport) >= reportInterval {
				s.print(cfg.dryRun)
				lastReport = time.Now()
			}
		}

		if output.LastEvaluatedKey == nil {
			break
		}
		scanInput.ExclusiveStartKey = output.LastEvaluatedKey
	}

	if cfg.dryRun {
		log.Println("[DRY RUN] Scan complete. No data was written to Postgres.")
	} else {
		log.Println("Migration complete.")
	}
	s.print(cfg.dryRun)
	return nil
}

// migrateIssue parses a DynamoDB issue item and writes it to Postgres.
//
// DynamoDB sort key format: ISSUE#<channelID>#<base64URLCorrID>#<uniqueID>
func migrateIssue(ctx context.Context, pg *postgres.Client, item map[string]dynamodbtypes.AttributeValue) error {
	sk := stringAttr(item["sk"])
	pk := stringAttr(item["pk"]) // partition key == channelID
	body := stringAttr(item["body"])

	if body == "" {
		return fmt.Errorf("missing body attribute (sk=%s)", sk)
	}

	// Split into exactly 4 segments: ["ISSUE", channelID, base64CorrID, uniqueID].
	// channelID cannot contain '#' (enforced by the DynamoDB plugin).
	parts := strings.SplitN(sk, "#", 4)
	if len(parts) != 4 {
		return fmt.Errorf("unexpected issue sort key format: %q", sk)
	}

	corrIDBytes, err := base64.URLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("failed to base64-decode correlation ID in sort key %q: %w", sk, err)
	}

	issue := &rawIssue{
		channelID:     pk,
		uniqueID:      parts[3],
		correlationID: string(corrIDBytes),
		isOpen:        stringAttr(item["is_open"]) == "true",
		postID:        stringAttr(item["post_id"]),
		body:          json.RawMessage(body),
	}
	return pg.SaveIssue(ctx, issue)
}

// migrateMoveMapping parses a DynamoDB move-mapping item and writes it to Postgres.
//
// DynamoDB sort key format: MOVEMAPPING#<channelID>#<base64URLCorrID>
func migrateMoveMapping(ctx context.Context, pg *postgres.Client, item map[string]dynamodbtypes.AttributeValue) error {
	sk := stringAttr(item["sk"])
	pk := stringAttr(item["pk"]) // partition key == channelID
	body := stringAttr(item["body"])

	if body == "" {
		return fmt.Errorf("missing body attribute (sk=%s)", sk)
	}

	// Split into exactly 3 segments: ["MOVEMAPPING", channelID, base64CorrID].
	parts := strings.SplitN(sk, "#", 3)
	if len(parts) != 3 {
		return fmt.Errorf("unexpected move mapping sort key format: %q", sk)
	}

	corrIDBytes, err := base64.URLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("failed to base64-decode correlation ID in sort key %q: %w", sk, err)
	}

	mm := &rawMoveMapping{
		channelID:     pk,
		correlationID: string(corrIDBytes),
		body:          json.RawMessage(body),
	}
	return pg.SaveMoveMapping(ctx, mm)
}

// stringAttr extracts the string value from a DynamoDB String attribute.
// Returns "" if the attribute is absent or not of type AttributeValueMemberS.
func stringAttr(attr dynamodbtypes.AttributeValue) string {
	if v, ok := attr.(*dynamodbtypes.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}
