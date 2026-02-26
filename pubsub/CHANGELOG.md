# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `pubsub/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

## [Unreleased]

## [0.1.2] - 2026-02-26

### Changed

- CI: added govulncheck to per-plugin CI and code scanning workflow
- Updated Go module dependencies (google/pubsub, slackmgr/types v0.4.0)

## [0.1.1] - 2026-02-22

### Changed

- Updated Go module dependencies
- CI: refactored Security job to install gosec via `go install` using the host Go toolchain, run from the module directory, use stable Go, and enforce failure on findings; gosec removed from golangci-lint
- CI: fixed `setup-go` `cache-dependency-path` for subdirectory modules

## [0.1.0] - 2026-02-20

_Initial version in the slackmgr/plugins monorepo. The plugin was previously developed privately; this is its first public release._

### Added

- Google Cloud Pub/Sub queue consumer (`Client`) with ordered message delivery and deduplication via message attributes
- Pub/Sub publisher/subscriber with configurable thresholds (delay, count, byte) and extension settings
- `WebhookHandler` for dynamic topic publishing with GCP topic name validation
- Functional options pattern for all configuration (`WithPublisher*`, `WithSubscriber*`)
- Dependency injection interfaces for testing (`pubsubClient`, `pubsubPublisher`, `pubsubSubscriber`)

[Unreleased]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.2...HEAD
[0.1.2]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.1...pubsub/v0.1.2
[0.1.1]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.0...pubsub/v0.1.1
[0.1.0]: https://github.com/slackmgr/plugins/releases/tag/pubsub/v0.1.0
