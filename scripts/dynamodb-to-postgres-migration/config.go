package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/slackmgr/plugins/postgres"
)

type appConfig struct {
	awsRegion           string
	dynamoTable         string
	pgHost              string
	pgPort              int
	pgUser              string
	pgPassword          string
	pgDatabase          string
	pgSSLMode           postgres.SSLMode
	pgSSLRootCert       string
	pgSSLCert           string
	pgSSLKey            string
	pgIssuesTable       string
	pgMoveMappingsTable string
	dryRun              bool
}

func loadConfig() (*appConfig, error) {
	cfg := &appConfig{
		pgHost:              envOrDefault("PG_HOST", "localhost"),
		pgSSLMode:           postgres.SSLMode(envOrDefault("PG_SSL_MODE", string(postgres.SSLModePrefer))),
		pgIssuesTable:       envOrDefault("PG_ISSUES_TABLE", "issues"),
		pgMoveMappingsTable: envOrDefault("PG_MOVE_MAPPINGS_TABLE", "move_mappings"),
	}

	var missing []string

	cfg.awsRegion = os.Getenv("AWS_REGION")
	if cfg.awsRegion == "" {
		missing = append(missing, "AWS_REGION")
	}

	cfg.dynamoTable = os.Getenv("DYNAMODB_TABLE")
	if cfg.dynamoTable == "" {
		missing = append(missing, "DYNAMODB_TABLE")
	}

	cfg.pgUser = os.Getenv("PG_USER")
	if cfg.pgUser == "" {
		missing = append(missing, "PG_USER")
	}

	cfg.pgDatabase = os.Getenv("PG_DATABASE")
	if cfg.pgDatabase == "" {
		missing = append(missing, "PG_DATABASE")
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	portStr := envOrDefault("PG_PORT", "5432")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("PG_PORT must be a valid integer: %w", err)
	}
	cfg.pgPort = port

	cfg.pgPassword = os.Getenv("PG_PASSWORD")
	cfg.pgSSLRootCert = os.Getenv("PG_SSL_ROOT_CERT")
	cfg.pgSSLCert = os.Getenv("PG_SSL_CERT")
	cfg.pgSSLKey = os.Getenv("PG_SSL_KEY")
	cfg.dryRun = os.Getenv("DRYRUN") == "true"

	return cfg, nil
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
