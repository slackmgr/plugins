package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pgConnectionString builds a libpq-style connection URL from appConfig.
func pgConnectionString(cfg *appConfig) string {
	host := net.JoinHostPort(cfg.pgHost, strconv.Itoa(cfg.pgPort))
	user := url.QueryEscape(cfg.pgUser)
	if cfg.pgPassword != "" {
		user += ":" + url.QueryEscape(cfg.pgPassword)
	}
	dsn := fmt.Sprintf("postgres://%s@%s/%s?sslmode=%s", user, host, cfg.pgDatabase, cfg.pgSSLMode)
	if cfg.pgSSLRootCert != "" {
		dsn += "&sslrootcert=" + url.QueryEscape(cfg.pgSSLRootCert)
	}
	if cfg.pgSSLCert != "" {
		dsn += "&sslcert=" + url.QueryEscape(cfg.pgSSLCert)
	}
	if cfg.pgSSLKey != "" {
		dsn += "&sslkey=" + url.QueryEscape(cfg.pgSSLKey)
	}
	return dsn
}

// checkDataTablesEmpty aborts if the issues or move_mappings tables contain
// any rows. The tables may already exist (e.g. after a prior schema migration
// run); only the row counts matter.
func checkDataTablesEmpty(ctx context.Context, cfg *appConfig) error {
	pool, err := pgxpool.New(ctx, pgConnectionString(cfg))
	if err != nil {
		return fmt.Errorf("failed to connect to Postgres for pre-check: %w", err)
	}
	defer pool.Close()

	tables := []string{cfg.pgIssuesTable, cfg.pgMoveMappingsTable}
	for _, table := range tables {
		var count int
		if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
			return fmt.Errorf("failed to count rows in %s: %w", table, err)
		}
		if count > 0 {
			return fmt.Errorf("table %q already contains %d row(s) — migration requires empty data tables", table, count)
		}
	}
	return nil
}
