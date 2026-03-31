// Package main is a one-shot migration tool that reads issues and move
// mappings from a DynamoDB single-table and writes them into a fresh
// PostgreSQL database using the slackmgr postgres plugin.
//
// Set DRYRUN=true to scan DynamoDB and validate the data without writing
// anything to Postgres. In dry-run mode the schema migrations still run so
// the tables exist and are confirmed to be empty before the scan begins.
package main

import (
	"context"
	"log"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	log.Printf("Source: DynamoDB table=%q region=%s", cfg.dynamoTable, cfg.awsRegion)
	log.Printf("Target: Postgres %s@%s:%d/%s (ssl=%s)", cfg.pgUser, cfg.pgHost, cfg.pgPort, cfg.pgDatabase, cfg.pgSSLMode)

	if err := run(context.Background(), cfg); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}
