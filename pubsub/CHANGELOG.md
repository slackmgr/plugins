# Changelog

All notable changes to this module will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions are tagged using the `pubsub/vX.Y.Z` convention per the
[Go multi-module tag convention](https://go.dev/doc/modules/managing-source#multiple-module-source).

## [Unreleased]

## [0.2.0] - 2026-10-21

### Changed

- Minimum Go version is now 1.26
- Bump `github.com/slackmgr/types` dependency to v0.7.1
- Update all direct dependencies (`cloud.google.com/go/pubsub/v2` v2.7.0, `cloud.google.com/go/iam` v1.11.0, `cloud.google.com/go/auth` v0.20.0)
- Bump `golang.org/x/net` to v0.61.0 and `google.golang.org/grpc` to v1.84.0 (fixes GO-2026-6617, GO-2026-6612, GO-2026-6611, GO-2026-6603 and GO-2026-6348)
- CI and code scanning workflows read the Go version (major.minor) from `go.mod`/`go.work` and install the latest patch release (`check-latest: true`), so all jobs, including gosec and both govulncheck runs, use the same Go version. Workflows also run when `go.work` changes
- Update GitHub Actions to latest versions (`actions/checkout` v7.0.1, `actions/setup-go` v7.0.0, `github/codeql-action` v4.39.0, `golangci/golangci-lint-action` v9.3.0)
- Disable the `exhaustruct_v5` linter (renamed in golangci-lint 2.15), matching the existing `exhaustruct` exclusion
- README: Go requirement updated to 1.26+

## [0.1.6] - 2026-05-11

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.6.1

## [0.1.5] - 2026-05-08

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.2

## [0.1.4] - 2026-05-07

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.5.1

## [0.1.3] - 2026-04-14

### Changed

- Bump `github.com/slackmgr/types` dependency to v0.4.1

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

[Unreleased]: https://github.com/slackmgr/plugins/compare/pubsub/v0.2.0...HEAD
[0.2.0]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.6...pubsub/v0.2.0
[0.1.6]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.5...pubsub/v0.1.6
[0.1.5]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.4...pubsub/v0.1.5
[0.1.4]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.3...pubsub/v0.1.4
[0.1.3]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.2...pubsub/v0.1.3
[0.1.2]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.1...pubsub/v0.1.2
[0.1.1]: https://github.com/slackmgr/plugins/compare/pubsub/v0.1.0...pubsub/v0.1.1
[0.1.0]: https://github.com/slackmgr/plugins/releases/tag/pubsub/v0.1.0
